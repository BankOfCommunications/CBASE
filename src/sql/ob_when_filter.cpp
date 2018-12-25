/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_when_filter.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_when_filter.h"
#include "common/serialization.h"
#include "common/ob_tsi_factory.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObWhenFilter::ObWhenFilter()
  : could_do_next_(false), child_num_(0)
{
}

ObWhenFilter::~ObWhenFilter()
{
}

void ObWhenFilter::reset()
{
  filters_.clear();
  could_do_next_ = false;
  child_num_ = 0;
  ObMultiChildrenPhyOperator::reset();
}

void ObWhenFilter::reuse()
{
  filters_.clear();
  could_do_next_ = false;
  child_num_ = 0;
  ObMultiChildrenPhyOperator::reuse();
}


int ObWhenFilter::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  if ((ret = ObMultiChildrenPhyOperator::set_child(child_idx, child_operator)) == OB_SUCCESS)
  {
    if (ObMultiChildrenPhyOperator::get_child_num() > child_num_)
    {
      child_num_++;
    }
  }
  return ret;
}

int ObWhenFilter::add_filter(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = filters_.push_back(expr)))
  {
    TBSYS_LOG(WARN, "failed to add column, err=%d", ret);
  }
  return ret;
}

int ObWhenFilter::open()
{
  int ret = OB_SUCCESS;
  ObRowDesc tmp_row_desc;
  //ObRow tmp_row;
  tmp_row.clear();
  tmp_row.set_row_desc(tmp_row_desc);
  if (OB_UNLIKELY(ObMultiChildrenPhyOperator::get_child_num() <= 0))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "ObWhenFilter has no child operator, ret=%d", ret);
  }
  else
  {
    for (int32_t i = 1; ret == OB_SUCCESS && i < ObMultiChildrenPhyOperator::get_child_num(); i++)
    {
      const ObRow *child_row = NULL;
      ObPhyOperator *op = NULL;
      if ((op = get_child(i)) == NULL)
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Can not get %dth child of ObWhenFilter ret=%d", i, ret);
        break;
      }
      else if ((ret = op->open()) != OB_SUCCESS)
      {
        if (!IS_SQL_ERR(ret))
        {
          TBSYS_LOG(WARN, "Open the %dth child of ObWhenFilter failed, ret=%d", i, ret);
        }
        break;
      }
      else if ((ret = op->get_next_row(child_row)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Get nex row of %dth child failed ret=%d", i, ret);
        break;
      }
      switch (op->get_type())
      {
        case PHY_ROW_COUNT:
        {
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          const ObObj *cell = NULL;
          if ((ret = child_row->raw_get_cell(0, cell, table_id, column_id)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Wrong index of ObRowCount, idx=%d ret=%d", 0, ret);
          }
          else if ((ret = tmp_row_desc.add_column_desc(table_id, column_id)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add column to ObRowDesc failed, ret=%d", ret);
          }
          else if ((ret = tmp_row.set_cell(table_id, column_id, *cell)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add cell to ObRow failed, ret=%d", ret);
          }
          break;
        }
        default:
        {
          ret = OB_ERR_ILLEGAL_TYPE;
          TBSYS_LOG(WARN, "Unknown child type of ObWhenFilter, index=%d, ret=%d", i, ret);
        }
      }
    }
    const ObObj *result = NULL;
    for (int32_t i = 0; ret == OB_SUCCESS && i < filters_.count(); ++i)
    {
      ObSqlExpression &expr = filters_.at(i);
      if (OB_SUCCESS != (ret = expr.calc(tmp_row, result)))
      {
        TBSYS_LOG(WARN, "Failed to calc expression, err=%d", ret);
      }
      else if (!result->is_true())
      {
        ret = OB_ERR_WHEN_UNSATISFIED;
        char buf[OB_MAX_RESULT_MESSAGE_LENGTH];
        int64_t pos = 0;
        databuff_printf(buf, OB_MAX_RESULT_MESSAGE_LENGTH, 
                        pos, "Number %ld When filter failed, ret=%d", when_number_, ret);
        for (int32_t i = 1; i < ObMultiChildrenPhyOperator::get_child_num(); i++)
        {
          const common::ObObj *cell;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          if (tmp_row.raw_get_cell(i - 1, cell, table_id, column_id) != OB_SUCCESS)
            break;
          switch (get_child(i)->get_type())
          {
            case PHY_ROW_COUNT:
            {
              int64_t row_count = -1;
              if (cell->get_int(row_count) != OB_SUCCESS)
                break;
              databuff_printf(buf, OB_MAX_RESULT_MESSAGE_LENGTH, 
                              pos, ", func %d: ROW_COUNT()=%ld", i, row_count);
              break;
            }
            default:
            {
              break;
            }
          }
        }
        buf[OB_MAX_RESULT_MESSAGE_LENGTH == pos ? pos - 1 : pos] = '\0';
        TBSYS_LOG(USER_ERROR, buf);
      }
    } // end for
    // close when functions when out of use wheather successed or failed
    for (int32_t i = 1; ret == OB_SUCCESS && i < ObMultiChildrenPhyOperator::get_child_num(); i++)
    {
      if ((ret = get_child(i)->close()) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Close the %dth child of ObWhenFilter failed, ret=%d", i, ret);
      }
    }
    if (ret == OB_SUCCESS)
    {
      if ((ret =  get_child(0)->open()) == OB_SUCCESS)
      {
        could_do_next_ = true;
      }
      else
      {
        if (!IS_SQL_ERR(ret))
        {
          TBSYS_LOG(WARN, "Open the first child of ObWhenFilter failed, ret=%d", ret);
        }
      }
    }
  }
  return ret;
}

int ObWhenFilter::close()
{
  could_do_next_ = false;
  return ObMultiChildrenPhyOperator::close();
}

int ObWhenFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_child(0) == NULL))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "children_ops_[0] is NULL");
  }
  else
  {
    ret = get_child(0)->get_row_desc(row_desc);
  }
  return ret;
}

int ObWhenFilter::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObMultiChildrenPhyOperator::get_child_num() <= 0))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObWhenFilter has no child, ret=%d", ret);
  }
  else if (!could_do_next_)
  {
    ret = OB_ITER_END;
  }
  else
  {
    ret = get_child(0)->get_next_row(row);
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObWhenFilter, PHY_WHEN_FILTER);
  }
}

int64_t ObWhenFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "WhenFilter(when number=%lud, filters=[", when_number_);
  for (int32_t i = 0; i < filters_.count(); ++i)
  {
    pos += filters_.at(i).to_string(buf + pos, buf_len - pos);
    if (i != filters_.count() - 1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  databuff_printf(buf, buf_len, pos, "])\n, (when operators=[");
  for (int32_t i = 0; i < ObMultiChildrenPhyOperator::get_child_num(); ++i)
  {
    pos += get_child(i)->to_string(buf + pos, buf_len - pos);
    if (i != ObMultiChildrenPhyOperator::get_child_num() - 1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}


DEFINE_SERIALIZE(ObWhenFilter)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, ObMultiChildrenPhyOperator::get_child_num())))
  {
    TBSYS_LOG(WARN, "fail to serialize child_num_. ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, when_number_)))
  {
    TBSYS_LOG(WARN, "fail to serialize when_number_. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, filters_.count())))
  {
    TBSYS_LOG(WARN, "fail to serialize filter expr count. ret=%d", ret);
  }
  else
  {
    for (int64_t i = 0; i < filters_.count(); ++i)
    {
      const ObSqlExpression &expr = filters_.at(i);
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))))
      {
        TBSYS_LOG(WARN, "filter expr serialize fail. ret=%d", ret);
        break;
      }
    } // end for
  }
  if (0 >= filters_.count())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  return ret;
}

DEFINE_DESERIALIZE(ObWhenFilter)
{
  int ret = OB_SUCCESS;
  int64_t expr_count = 0;
  if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &child_num_)))
  {
    TBSYS_LOG(WARN, "fail to deserialize child_num_. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &when_number_)))
  {
    TBSYS_LOG(WARN, "fail to deserialize when_number_. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &expr_count)))
  {
    TBSYS_LOG(WARN, "fail to deserialize expr count. ret=%d", ret);
  }
  else
  {
    //for (int64_t i = 0; i < expr_count; i++)
    //{
    //  ObSqlExpression expr;
    //  if (OB_SUCCESS != (ret = expr.deserialize(buf, data_len, pos)))
    //  {
    //    TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
    //    break;
    //  }
    //  else
    //  {
    //    if (OB_SUCCESS != (ret = add_filter(expr)))
    //    {
    //      TBSYS_LOG(WARN, "fail to add expression to filter.ret=%d, buf=%p, data_len=%ld, pos=%ld",
    //                      ret, buf, data_len, pos);
    //      break;
    //    }
    //  }
    //}
    ObSqlExpression expr;
    for (int64_t i = 0; i < expr_count; i++)
    {
      if (OB_SUCCESS != (ret = add_filter(expr)))
      {
        TBSYS_LOG(WARN, "fail to add expression to filter.ret=%d, buf=%p, data_len=%ld, pos=%ld",
                        ret, buf, data_len, pos);
        break;
      }
      if (OB_SUCCESS != (ret = filters_.at(filters_.count() - 1).deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
        break;
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObWhenFilter)
{
  int64_t size = 0;
  size += encoded_length_vi32(child_num_);
  size += encoded_length_vi64(filters_.count());
  for (int64_t i = 0; i < filters_.count(); ++i)
  {
    const ObSqlExpression &expr = filters_.at(i);
    size += expr.get_serialize_size();
  }
  return size;
}

PHY_OPERATOR_ASSIGN(ObWhenFilter)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObWhenFilter);
  could_do_next_ = false;
  child_num_ = o_ptr->child_num_;
  when_number_ = o_ptr->when_number_;
  for (int64_t i = 0; i < o_ptr->filters_.count(); i++)
  {
    if ((ret = filters_.push_back(o_ptr->filters_.at(i))) == OB_SUCCESS)
    {
      filters_.at(i).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  return ret;
}




