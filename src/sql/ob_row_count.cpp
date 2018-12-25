/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_count.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_row_count.h"
#include "ob_ups_modify.h"
#include "common/serialization.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObRowCount::ObRowCount()
  : table_id_(OB_INVALID_ID), column_id_(OB_INVALID_ID)
  , when_func_index_(OB_INVALID_INDEX), already_read_(false)
{
}

ObRowCount::~ObRowCount()
{
}

void ObRowCount::reset()
{
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  row_desc_.reset();
  //row_.reset(false, ObRow::DEFAULT_NOP);
  already_read_ = false;
  ObSingleChildPhyOperator::reset();
}

void ObRowCount::reuse()
{
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  row_desc_.reset();
  //row_.reset(false, ObRow::DEFAULT_NOP);
  already_read_ = false;
  ObSingleChildPhyOperator::reuse();
}

int ObRowCount::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  if (child_operator.get_type() != PHY_UPS_MODIFY
      && child_operator.get_type() != PHY_UPS_MODIFY_WITH_DML_TYPE)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "ObRowCount child must be ObUpsModify or ObUpsModifyWithDmlType type");
  }
  else
  {
    ObSingleChildPhyOperator::set_child(child_idx, child_operator);
  }
  return ret;
}

int ObRowCount::cons_row_desc()
{
  int ret = OB_SUCCESS;
  if (0 != row_desc_.get_column_num())
  {
    ret = OB_INIT_TWICE;
    TBSYS_LOG(WARN, "Row desc should be empty");
  }
  else if (table_id_ == OB_INVALID_ID && column_id_ == OB_INVALID_ID)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "Table Id/Column Id must be set");
  }
  else if ((ret = row_desc_.add_column_desc(table_id_, column_id_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to construct row desc, err=%d", ret);
  }
  return ret;
}

int ObRowCount::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    if (!IS_SQL_ERR(ret))
    {
      TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
    }
  }
  else if (OB_SUCCESS != (ret = cons_row_desc()))
  {
    TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
  }
  return ret;
}

int ObRowCount::close()
{
  reset();
  return ObSingleChildPhyOperator::close();
}

int ObRowCount::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (row_desc_.get_column_num() <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObRowCount must be opened first");
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObRowCount::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_desc_.get_column_num() <= 0))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObRowCount must be opened first");
  }
  else if (!already_read_)
  {
    int64_t row_count = 0;
    ObAffectedRows *ups_affected_rows = dynamic_cast<ObAffectedRows*>(child_op_);
    if (ups_affected_rows == NULL)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "ObRowCount child must be ObAffectedRows type");
    }
    else if ((ret = ups_affected_rows->get_affected_rows(row_count)) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "Get affected row failed, ret=%d", ret);
    }
    else
    {
      ObObj row_num;
      row_num.set_int(row_count);
      row_.set_row_desc(row_desc_);
      if ((ret = row_.set_cell(table_id_, column_id_, row_num)))
      {
        TBSYS_LOG(WARN, "Failed to set row cell ret=%d", ret);
      }
      else
      {
        row = &row_;
        already_read_ = true;
      }
    }
  }
  else
  {
    ret = common::OB_ITER_END;
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObRowCount, PHY_ROW_COUNT);
  }
}

int64_t ObRowCount::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "RowCount(table_id=%lu, column_id=%lu)\n",
                  table_id_, column_id_);
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf + pos, buf_len - pos);
    pos += pos2;
  }
  return pos;
}


DEFINE_SERIALIZE(ObRowCount)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_))))
  {
    TBSYS_LOG(WARN, "fail to serialize table_id_. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(column_id_))))
  {
    TBSYS_LOG(WARN, "fail to serialize column_id_. ret=%d", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObRowCount)
{
  int ret = OB_SUCCESS;
  int64_t val = -1;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &val)))
    {
      TBSYS_LOG(WARN, "fail to deserialize table_id. ret=%d", ret);
    }
    else
    {
      table_id_ = static_cast<uint64_t>(val);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &val)))
    {
      TBSYS_LOG(WARN, "fail to deserialize column_id. ret=%d", ret);
    }
    else
    {
      column_id_ = static_cast<uint64_t>(val);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowCount)
{
  int64_t size = 0;
  size += encoded_length_vi64(static_cast<int64_t>(table_id_));
  size += encoded_length_vi64(static_cast<int64_t>(column_id_));
  return size;
}

PHY_OPERATOR_ASSIGN(ObRowCount)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObRowCount);
  table_id_ = o_ptr->table_id_;
  column_id_ = o_ptr->column_id_;
  row_desc_.reset();
  row_.clear();
  already_read_ = o_ptr->already_read_;
  return ret;
}

