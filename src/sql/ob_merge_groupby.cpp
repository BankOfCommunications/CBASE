/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_groupby.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_merge_groupby.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObMergeGroupBy::ObMergeGroupBy()
  :last_input_row_(NULL),
    is_analytic_func_(false)//add liumz, [ROW_NUMBER]20150824
{
}

ObMergeGroupBy::~ObMergeGroupBy()
{
  last_input_row_ = NULL;
}

void ObMergeGroupBy::reset()
{
  last_input_row_ = NULL;
  aggr_func_.reset();
  ObGroupBy::reset();
}

void ObMergeGroupBy::reuse()
{
  last_input_row_ = NULL;
  aggr_func_.reuse();
  ObGroupBy::reuse();
}


int ObMergeGroupBy::open()
{
  int ret = OB_SUCCESS;
  last_input_row_ = NULL;
  const ObRowDesc *child_row_desc = NULL;
  if (OB_SUCCESS != (ret = ObGroupBy::open()))
  {
    TBSYS_LOG(WARN, "failed to open child op, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = child_op_->get_row_desc(child_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  //mod liumz, [ROW_NUMBER]20150824
  /*else if (OB_SUCCESS != (ret = aggr_func_.init(*child_row_desc, aggr_columns_)))
  {
    TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
  }*/
  else
  {
    if (!is_analytic_func())
    {
      if (OB_SUCCESS != (ret = aggr_func_.init(*child_row_desc, aggr_columns_)))
      {
        TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = aggr_func_.init(*child_row_desc, anal_columns_, true)))
      {
        TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
      }
    }    
  }
  //mod:e
  return ret;
}

int ObMergeGroupBy::close()
{
  int ret = OB_SUCCESS;
  last_input_row_ = NULL;
  aggr_func_.destroy();
  ret = ObGroupBy::close();
  return ret;
}

int ObMergeGroupBy::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  const ObRowDesc &r = aggr_func_.get_row_desc();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= r.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &r;
  }
  return ret;
}

// if there is no group columns, is_same_group returns true
int ObMergeGroupBy::is_same_group(const ObRow &row1, const ObRow &row2, bool &result)
{
  int ret = OB_SUCCESS;
  result = true;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  //add liumz, [ROW_NUMBER]20150826:b
  ObArray<ObGroupColumn> &ref_columns = group_columns_;
  if (is_analytic_func())
  {
    ref_columns = partition_columns_;
  }
  //add:e
  //mod liumz, [ROW_NUMBER]20150826:b
  //for (int64_t i = 0; i < group_columns_.count(); ++i)
  for (int64_t i = 0; i < ref_columns.count(); ++i)
  {
    //const ObGroupColumn &group_col = group_columns_.at(static_cast<int32_t>(i));
    const ObGroupColumn &group_col = ref_columns.at(static_cast<int32_t>(i));
    //mod:e
    if (OB_SUCCESS != (ret = row1.get_cell(group_col.table_id_, group_col.column_id_, cell1)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d tid=%lu cid=%lu",
                ret, group_col.table_id_, group_col.column_id_);
      break;
    }
    else if (OB_SUCCESS != (ret = row2.get_cell(group_col.table_id_, group_col.column_id_, cell2)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d tid=%lu cid=%lu",
                ret, group_col.table_id_, group_col.column_id_);
      break;
    }
    else if (*cell1 != *cell2)
    {
      result = false;
      break;
    }
  } // end for
  return ret;
}

int ObMergeGroupBy::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  //add liumz, [ROW_NUMBER]20150824:b
  if (OB_SUCCESS == ret && !is_analytic_func())
  {//add:e
    if (NULL == last_input_row_)
    {
      // get the first input row of one group
      if (OB_SUCCESS != (ret = child_op_->get_next_row(last_input_row_)))
      {
        if (OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
        }
        else
        {
          /* add liumz, bugfix[select count(*) from (select c1 from t1 group by c1)]20170718
           * when ret==OB_ITER_END, set last_input_row_ to NULL,
           * otherwise, when child_op_ is ObMergeGroupBy, endless loop occurs!
           */
          last_input_row_ = NULL;
        }
      }
    }

    if (OB_SUCCESS == ret && NULL != last_input_row_)
    {
      if (OB_SUCCESS != (ret = aggr_func_.prepare(*last_input_row_))) // the first row of this group
      {
        TBSYS_LOG(WARN, "failed to init aggr cells, err=%d", ret);
      }
      else
      {
        last_input_row_ = NULL;
      }
    }

    if (OB_SUCCESS == ret)
    {
      bool same_group = false;
      const ObRow *input_row = NULL;
      while (OB_SUCCESS == (ret = child_op_->get_next_row(input_row)))
      {
        if (OB_SUCCESS != (ret = is_same_group(aggr_func_.get_curr_row(), *input_row, same_group)))
        {
          TBSYS_LOG(WARN, "failed to check group, err=%d", ret);
          break;
        }
        else if (same_group)
        {
          if (OB_SUCCESS != (ret = aggr_func_.process(*input_row)))
          {
            TBSYS_LOG(WARN, "failed to calc aggr, err=%d", ret);
            break;
          }
          else if (0 < mem_size_limit_ && mem_size_limit_ < aggr_func_.get_used_mem_size())
          {
            TBSYS_LOG(WARN, "merge group by has exceeded the mem limit, limit=%ld used=%ld",
                      mem_size_limit_, aggr_func_.get_used_mem_size());
            ret = OB_EXCEED_MEM_LIMIT;
            break;
          }
        }
        else if (OB_SUCCESS != (ret = aggr_func_.get_result(row)))
        {
          TBSYS_LOG(WARN, "failed to calculate avg, err=%d", ret);
        }
        else
        {
          last_input_row_ = input_row;
          break;
        }
      } // end while
      if (OB_ITER_END == ret)
      {
        // the last group
        if (OB_SUCCESS != (ret = aggr_func_.get_result(row)))
        {
          TBSYS_LOG(WARN, "failed to calculate avg, err=%d", ret);
        }
      }
    }
  //add liumz, [ROW_NUMBER]20150824:b
  }
  else if (OB_SUCCESS == ret && is_analytic_func())
  {
    if (NULL == last_input_row_)
    {
      if (OB_SUCCESS != (ret = child_op_->get_next_row(last_input_row_)))
      {
        if (OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
        }
      }
      if (OB_SUCCESS == ret && NULL != last_input_row_)
      {
        if (OB_SUCCESS != (ret = aggr_func_.prepare(*last_input_row_))) // the first row of this group
        {
          TBSYS_LOG(WARN, "failed to init analytic cells, err=%d", ret);
        }
      }
    }
    else
    {
      bool same_group = false;
      const ObRow *input_row = NULL;
      if (OB_SUCCESS == (ret = child_op_->get_next_row(input_row)))
      {
        if (OB_SUCCESS != (ret = is_same_group(aggr_func_.get_curr_row(), *input_row, same_group)))
        {
          TBSYS_LOG(WARN, "failed to check group, err=%d", ret);
        }
        else if (same_group)
        {
          if (OB_SUCCESS != (ret = aggr_func_.process(*input_row)))
          {
            TBSYS_LOG(WARN, "failed to calc analytic, err=%d", ret);
          }
          else if (0 < mem_size_limit_ && mem_size_limit_ < aggr_func_.get_used_mem_size())
          {
            TBSYS_LOG(WARN, "merge group by has exceeded the mem limit, limit=%ld used=%ld",
                      mem_size_limit_, aggr_func_.get_used_mem_size());
            ret = OB_EXCEED_MEM_LIMIT;
          }
          else
          {
            last_input_row_ = input_row;
          }
        }
        else if (!same_group)
        {
          if (OB_SUCCESS != (ret = aggr_func_.prepare(*input_row))) // the first row of this group
          {
            TBSYS_LOG(WARN, "failed to init analytic cells, err=%d", ret);
          }
          else
          {
            last_input_row_ = input_row;
          }
        }
      }
      else if (OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = aggr_func_.get_result(row)))
      {
        TBSYS_LOG(WARN, "failed to get result row, err=%d", ret);
      }
    }
  }
  //add:e
  return ret;
}

PHY_OPERATOR_ASSIGN(ObMergeGroupBy)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObMergeGroupBy);
  reset();
  if ((ret == ObGroupBy::assign(other)) == OB_SUCCESS)
  {
    set_int_div_as_double(o_ptr->get_int_div_as_double());
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMergeGroupBy, PHY_MERGE_GROUP_BY);
  }
}

DEFINE_SERIALIZE(ObMergeGroupBy)
{
  int ret = OB_SUCCESS;
  if ((ret = ObGroupBy::serialize(buf, buf_len, pos)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to serialize ObGroupBy, ret= %d", ret);
  }
  else if ((ret = encode_bool(buf, buf_len, pos, get_int_div_as_double())))
  {
    TBSYS_LOG(WARN, "serialize get_int_div_as_double fail. ret=%d", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObMergeGroupBy)
{
  int ret = OB_SUCCESS;
  bool did_int_div_as_double = false;
  if ((ret = ObGroupBy::deserialize(buf, data_len, pos)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to deserialize ObGroupBy. ret=%d", ret);
  }
  else if ((ret = decode_bool(buf, data_len, pos, &did_int_div_as_double)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to deserialize get_int_div_as_double. ret=%d", ret);
  }
  else
  {
    set_int_div_as_double(did_int_div_as_double);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObMergeGroupBy)
{
  int64_t size = 0;
  size += ObGroupBy::get_serialize_size();
  size += encoded_length_vi64(get_int_div_as_double());
  return size;
}
