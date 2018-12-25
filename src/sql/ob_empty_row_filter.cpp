/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_empty_row_filter.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_empty_row_filter.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

void ObEmptyRowFilter::reset()
{
  cur_row_desc_.reset();
  //cur_row_.reset(false, ObRow::DEFAULT_NULL);
  ObSingleChildPhyOperator::reset();
}

void ObEmptyRowFilter::reuse()
{
  cur_row_desc_.reset();
  //cur_row_.reset(false, ObRow::DEFAULT_NULL);
  ObSingleChildPhyOperator::reuse();
}

int ObEmptyRowFilter::open()
{
  int ret = OB_SUCCESS;
  ret = ObSingleChildPhyOperator::open();
  const ObRowDesc *row_desc = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = child_op_->get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "empty row filter row desc[%s] child[%s]", to_cstring(*row_desc), to_cstring(*child_op_));
    }

    cur_row_desc_.reset();
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_desc->get_column_num(); i ++)
    {
      if (OB_SUCCESS != (ret = row_desc->get_tid_cid(i, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "fail to get tid cid:ret[%d]", ret);
      }
      if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
      {
        if (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(table_id, column_id)))
        {
          TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    cur_row_.set_row_desc(cur_row_desc_);
    TBSYS_LOG(DEBUG, "empty row filter cur row desc[%s]", to_cstring(cur_row_desc_));
  }
  return ret;
}

int ObEmptyRowFilter::get_next_row(const common::ObRow *&row)
{
  int ret = common::OB_SUCCESS;
  bool is_row_empty = false;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  do
  {
    ret = child_op_->get_next_row(row);
    if (common::OB_SUCCESS == ret)
    {
      if (OB_UNLIKELY(common::OB_SUCCESS != (ret = row->get_is_row_empty(is_row_empty))))
      {
        TBSYS_LOG(WARN, "fail to get is row empty:ret[%d]", ret);
      }
      else if (!is_row_empty)
      {
        int64_t count = 0;
        for (int64_t i = 0; OB_SUCCESS == ret && i < row->get_column_num(); i ++)
        {
          if (OB_SUCCESS != (ret = row->raw_get_cell(i, cell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
          }
          if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
          {
            if (OB_SUCCESS != (ret = cur_row_.raw_set_cell(count ++, *cell)))
            {
              TBSYS_LOG(WARN, "fail to set cell:ret[%d]", ret);
            }
          }
        }
        row = &cur_row_;
        break;
      }
    }
    else if (OB_UNLIKELY(OB_ITER_END != ret))
    {
      if (!IS_SQL_ERR(ret))
      {
        TBSYS_LOG(WARN, "fail to get next row [%d]", ret);
      }
    }
  }
  while (common::OB_SUCCESS == ret);
  return ret;
}

int ObEmptyRowFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &cur_row_desc_;
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObEmptyRowFilter, PHY_EMPTY_ROW_FILTER);
  }
}

int64_t ObEmptyRowFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObEmptyRowFilter()\n");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

enum ObPhyOperatorType ObEmptyRowFilter::get_type() const
{
  return PHY_EMPTY_ROW_FILTER;
}

PHY_OPERATOR_ASSIGN(ObEmptyRowFilter)
{
  UNUSED(other);
  return common::OB_SUCCESS;
}

DEFINE_SERIALIZE(ObEmptyRowFilter)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return OB_SUCCESS;
}

DEFINE_DESERIALIZE(ObEmptyRowFilter)
{
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  return OB_SUCCESS;
}

DEFINE_GET_SERIALIZE_SIZE(ObEmptyRowFilter)
{
  return 0;
}
