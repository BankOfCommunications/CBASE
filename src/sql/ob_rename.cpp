/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rename.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_rename.h"
#include "ob_sql_expression.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObRename::ObRename():
  table_id_(0), base_table_id_(0),
  org_row_(NULL), org_row_desc_(NULL), row_desc_()
{
}

ObRename::~ObRename()
{
}

void ObRename::reset()
{
  table_id_ = 0;
  base_table_id_ = 0;
  column_ids_.clear();
  org_row_ = NULL;
  org_row_desc_ = NULL;
  table_id_ = OB_INVALID_ID;
  base_table_id_ = OB_INVALID_ID;
  column_ids_.clear();
  org_row_ = NULL;
  org_row_desc_ = NULL;
  child_op_ = NULL;
  row_desc_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObRename::reuse()
{
  table_id_ = 0;
  base_table_id_ = 0;
  column_ids_.clear();
  org_row_ = NULL;
  org_row_desc_ = NULL;
  row_desc_.reset();
  ObSingleChildPhyOperator::reuse();
}

int ObRename::set_table(const uint64_t table_id, const uint64_t base_table_id)
{
  int ret = OB_SUCCESS;
  if (table_id <= 0 || base_table_id <= 0)
  {
    TBSYS_LOG(WARN, "invalid id: table_id=%ld, base_table_id=%ld", table_id, base_table_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    table_id_ = table_id;
    base_table_id_ = base_table_id;
  }
  return ret;
}

//add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
bool ObRename::is_base_table_id_valid()
{
  return (base_table_id_ != OB_INVALID_ID);
}
//add:e

int ObRename::add_column_id(const uint64_t& column_id)
{
  int ret = OB_SUCCESS;
  if ((ret = column_ids_.push_back(column_id)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add column id failed, column_id=%lu, ret=%d", column_id, ret);
  }
  return ret;
}

int ObRename::open()
{
  int ret = OB_SUCCESS;
  org_row_ = NULL;
  org_row_desc_ = NULL;
  if (table_id_ <= 0 || base_table_id_ <= 0)
  {
    TBSYS_LOG(WARN, "invalid id: table_id=%ld, base_table_id=%ld", table_id_, base_table_id_);
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc()))
  {
    TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
  }
  return ret;
}

int ObRename::close()
{
  org_row_ = NULL;
  org_row_desc_ = NULL;
  row_desc_.reset();
  return ObSingleChildPhyOperator::close();
}

int ObRename::cons_row_desc()
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    TBSYS_LOG(ERROR, "child op is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = child_op_->get_row_desc(org_row_desc_)))
  {
    TBSYS_LOG(WARN, "failed to get original row desc, err=%d", ret);
  }
  else if (column_ids_.count() != 0 && column_ids_.count() != org_row_desc_->get_column_num())
  {
    TBSYS_LOG(ERROR, "Column ids are not initiated well");
    ret = OB_NOT_INIT;
  }
  else
  {
    OB_ASSERT(org_row_desc_);
    OB_ASSERT(0 ==row_desc_.get_column_num());
    uint64_t column_id = OB_INVALID_ID;
    for (int64_t idx = 0; idx < org_row_desc_->get_column_num(); idx++)
    {
      column_id = (column_ids_.count() == 0)? OB_APP_MIN_COLUMN_ID + idx : column_ids_.at(idx);
      if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id_, column_id)))
      {
        TBSYS_LOG(WARN, "fail to add tid and cid to row desc. idx=%ld, tid=%lu, ret=%d",
                  idx, table_id_, ret);
        break;
      }
    }
  }
  return ret;
}

int ObRename::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init, call open() first");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObRename::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    // reset row desc for under-layer operator
    if (NULL != org_row_desc_ && NULL != org_row_)
    {
      org_row_->set_row_desc(*org_row_desc_);
    }
    if (OB_SUCCESS == (ret = child_op_->get_next_row(row)))
    {
      OB_ASSERT(row);
      org_row_ = const_cast<ObRow *>(row);
      const_cast<ObRow *>(row)->set_row_desc(row_desc_);
    }
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObRename, PHY_RENAME);
  }
}

int64_t ObRename::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Rename(out_tid=%lu, base_tid=%lu)\n",
                  table_id_, base_table_id_);
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

DEFINE_SERIALIZE(ObRename)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_))))
  {
    TBSYS_LOG(WARN, "fail to encode table_id_:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(base_table_id_))))
  {
    TBSYS_LOG(WARN, "fail to encode base_table_id_:ret[%d]", ret);
  }
  if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, column_ids_.count())))
  {
    TBSYS_LOG(WARN, "fail to encode renamed columns count:ret[%d]", ret);
  }
  else
  {
    for (int64_t i  =0; OB_SUCCESS == ret && i < column_ids_.count(); i++)
    {
      if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(column_ids_.at(i)))))
      {
        TBSYS_LOG(WARN, "fail to serialize sort column:ret[%d]", ret);
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObRename)
{
  int ret = OB_SUCCESS;
  int64_t table_id = 0;
  int64_t base_table_id = 0;
  int64_t columns_count = 0;
  int64_t column_id = 0;
  reset();
  if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &table_id)))
  {
    TBSYS_LOG(WARN, "fail to decode table_id:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &base_table_id)))
  {
    TBSYS_LOG(WARN, "fail to decode base_table_id:ret[%d]", ret);
  }
  else
  {
    table_id_ = static_cast<uint64_t>(table_id);
    base_table_id_ = static_cast<uint64_t>(base_table_id);
    if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &columns_count)))
    {
      TBSYS_LOG(WARN, "decode columns_count fail:ret[%d]", ret);
    }
    else
    {
      for (int64_t i = 0; OB_SUCCESS == ret && i < columns_count; i++)
      {
        if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &column_id)))
        {
          TBSYS_LOG(WARN, "decode column id failed : ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = column_ids_.push_back(static_cast<uint64_t>(column_id))))
        {
          TBSYS_LOG(WARN, "fail to add column id to array:ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRename)
{
  int64_t size = 0;
  size += encoded_length_vi64(table_id_);
  size += encoded_length_vi64(base_table_id_);
  size += serialization::encoded_length_vi64(column_ids_.count());
  for (int64_t i = 0; i < column_ids_.count(); i++)
  {
    size += encoded_length_vi64(column_ids_.at(i));
  }
  return size;
}

PHY_OPERATOR_ASSIGN(ObRename)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObRename);
  reset();
  table_id_ = o_ptr->table_id_;
  base_table_id_ = o_ptr->base_table_id_;
  column_ids_ = o_ptr->column_ids_;
  return ret;
}

ObPhyOperatorType ObRename::get_type() const
{
  return PHY_RENAME;
}
