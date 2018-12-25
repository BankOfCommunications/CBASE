/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_merge_distinct.cpp,  06/05/2012 05:36:55 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   ObMergeDistinct implementation
 *
 */
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "ob_merge_distinct.h"
#include "common/ob_row.h"
#include "common/ob_row_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;


ObMergeDistinct::ObMergeDistinct()
  : got_first_row_(false), last_row_buf_(NULL)
{
}


ObMergeDistinct::~ObMergeDistinct()
{
}

void ObMergeDistinct::reset()
{
  distinct_columns_.clear();
  got_first_row_ = false;
  //last_row_.reset(false, ObRow::DEFAULT_NULL);
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  last_row_buf_ = NULL;
  ObSingleChildPhyOperator::reset();
}

void ObMergeDistinct::reuse()
{
  distinct_columns_.clear();
  got_first_row_ = false;
  last_row_buf_ = NULL;
  ObSingleChildPhyOperator::reuse();
}

int ObMergeDistinct::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child_op, ret=%d", ret);
  }
  else if (NULL == (last_row_buf_ = (char *)ob_malloc(OB_ROW_BUF_SIZE, 0)))
  {
    TBSYS_LOG(WARN, "fail to alloc %lu bytes memory", OB_ROW_BUF_SIZE);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    got_first_row_ = false;
  }
  return ret;
}


int ObMergeDistinct::close()
{
  int ret = OB_SUCCESS;
  got_first_row_ = false;
  if (NULL != last_row_buf_)
  {
    ob_free(last_row_buf_);
    last_row_buf_ = NULL;
  }
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::close()))
  {
    TBSYS_LOG(WARN, "failed to close child_op, ret=%d", ret);
  }
  return ret;
}

int ObMergeDistinct::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObMergeDistinct::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *this_row = NULL;
  bool got_distinct_row = false;
  bool equal = false;
  ObString compact_row;

  compact_row.assign(last_row_buf_,OB_ROW_BUF_SIZE);

  if (NULL == child_op_)
  {
    TBSYS_LOG(WARN, "child_op_ is not set");
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret)
  {
    if (got_first_row_)
    {
      while(OB_SUCCESS == ret && !got_distinct_row)
      {
        ret = child_op_->get_next_row(this_row);
        if (OB_SUCCESS != ret)
        {
          break;
        }
        else
        {
          // compare current_row and last_row
          if (OB_SUCCESS != (ret = compare_equal(this_row, &last_row_, equal)))
          {
            TBSYS_LOG(WARN, "fail to compare this_row and last_row_. ret=%d", ret);
          }
          else if (!equal)
          {
            got_distinct_row = true;
            /* return value */
            row = this_row;
            /* save this row to local buffer. last_row_buf_ reused */
            if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*this_row, compact_row, last_row_)))
            {
              TBSYS_LOG(WARN, "fail to convert row to compact row. ret=%d" ,ret);
              break;
            }
            //TBSYS_LOG(DEBUG, "find a distinct row");
            break;
          }
          else
          {
            //TBSYS_LOG(DEBUG, "equal of this & last row. skip");
          }
        }
      }/* end while */
    }
    else /* first row, always output */
    {
      if (OB_SUCCESS != (ret = child_op_->get_next_row(row)))
      {
        TBSYS_LOG(WARN, "fail to get next row. ret=%d", ret);
      }
      else if (NULL != row)
      {
        if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*row, compact_row, last_row_)))
        {
          TBSYS_LOG(WARN, "fail to convert row to compact row. ret=%d" ,ret);
        }
        got_first_row_ = true;
      }
    }
  }

  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "fail to get next row. ret=%d, got_first_row_=%d, got_distinct_row=%d",
        ret, got_first_row_, got_distinct_row);
  }
  return ret;
}


int ObMergeDistinct::compare_equal(const common::ObRow *this_row, const common::ObRow *last_row, bool &result) const
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t column_count = 0;
  ObDistinctColumn column;
  const ObObj *this_obj = NULL;
  const ObObj *last_obj = NULL;
  bool cmp_val = false;

  result = false;

  if (NULL == this_row || NULL == last_row)
  {
    TBSYS_LOG(WARN, "compared row invalid. this_row=%p, last_row=%p", this_row, last_row);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    column_count = distinct_columns_.count();
    for (i = 0; i < column_count; i++)
    {
      if (OB_SUCCESS != (ret = distinct_columns_.at(static_cast<int32_t>(i), column)))
      {
        TBSYS_LOG(WARN, "fail to get column from distinct_column_ array. ret=%d, i=%ld, column_count=%ld",
            ret, i, column_count);
        break;
      }
      else if (OB_SUCCESS != (ret = this_row->get_cell(column.table_id_, column.column_id_, this_obj)))
      {
        TBSYS_LOG(WARN,"fail to get cell. ret=%d, table_id=%lu, column_id=%lu",
            ret, column.table_id_, column.column_id_);
        break;
      }
      else if (OB_SUCCESS != (ret = last_row->get_cell(column.table_id_, column.column_id_, last_obj)))
      {
        TBSYS_LOG(WARN,"fail to get cell. ret=%d, table_id=%lu, column_id=%lu",
            ret, column.table_id_, column.column_id_);
        break;
      }
      else if (NULL == this_obj || NULL == last_obj)
      {
        TBSYS_LOG(WARN, "cell is null. this_obj=%p, last_obj=%p", this_obj, last_obj);
        ret = OB_ERROR;
        break;
      }
      else
      {
        cmp_val = ((*this_obj) == (*last_obj));
        //this_obj->dump();
        //last_obj->dump();
        if (false == cmp_val)
        {
          break;
        }
      }
    }/* end for */
  }
  if (OB_SUCCESS == ret)
  {
    result = cmp_val;
  }
  return ret;
}


int ObMergeDistinct::add_distinct_column(const uint64_t tid, const uint64_t cid)
{
  int ret = OB_SUCCESS;
  ObDistinctColumn col;

  col.table_id_ = tid;
  col.column_id_ = cid;
  if (OB_SUCCESS != (ret = distinct_columns_.push_back(col)))
  {
    TBSYS_LOG(WARN, "fail to add new column(%lu, %lu) to distinct_columns_. ret=%d", tid, cid, ret);
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMergeDistinct, PHY_MERGE_DISTINCT);
  }
}

int64_t ObMergeDistinct::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MergeDistinct(columns=[");
  for (int64_t i = 0; i < distinct_columns_.count(); ++i)
  {
    const ObDistinctColumn &col = distinct_columns_.at(static_cast<int32_t>(i));
    if (OB_INVALID_ID != col.table_id_)
    {
      databuff_printf(buf, buf_len, pos, "<%lu, %lu>", col.table_id_, col.column_id_);
    }
    else
    {
      databuff_printf(buf, buf_len, pos, "<NULL, %lu>", col.column_id_);
    }
    if (i != distinct_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObMergeDistinct)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObMergeDistinct);
  reset();
  distinct_columns_ = o_ptr->distinct_columns_;
  return ret;
}

