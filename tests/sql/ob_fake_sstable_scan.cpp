/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_sstable_scan.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_fake_sstable_scan.h"

using namespace oceanbase;
using namespace sql;
using namespace test;

int ObFakeSSTableScan::set_scan_param(const sstable::ObSSTableScanParam &param)
{
  int ret = OB_SUCCESS;
  scan_param_ = param;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  table_id = scan_param_.get_table_id();

  if(OB_INVALID_ID == table_id)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table id is invalid");
  }

  for(int64_t i=0;OB_SUCCESS == ret && i<scan_param_.get_column_id_size();i++)
  {
    column_id = scan_param_.get_column_id()[i];
    if(OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id, column_id)))
    {
      TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    curr_row_.set_row_desc(row_desc_);
  }
  return ret;
}

ObFakeSSTableScan::ObFakeSSTableScan(const char *file_name)
  :file_table_(file_name)
{
}

int ObFakeSSTableScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int ObFakeSSTableScan::open()
{
  return file_table_.open();
}

int ObFakeSSTableScan::close()
{
  return file_table_.close();
}

int ObFakeSSTableScan::get_next_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  const ObRow *tmp_row = NULL;

  ret = file_table_.get_next_row(rowkey, tmp_row);
  if(OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret)
  {
    for(int64_t i=0;OB_SUCCESS == ret && i<row_desc_.get_column_num();i++)
    {
      if(OB_SUCCESS != (ret = row_desc_.get_tid_cid(i, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "get tid cid fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = tmp_row->get_cell(table_id, column_id, cell)))
      {
        TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = curr_row_.set_cell(table_id, column_id, *cell)))
      {
        TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(0 == row_desc_.get_column_num())
    {
      row = tmp_row;
    }
    else
    {
      row = &curr_row_;
    }
  }
  return ret;
}

int64_t ObFakeSSTableScan::to_string(char* buf, const int64_t buf_len) const
{
  return file_table_.to_string(buf, buf_len);
}

int ObFakeSSTableScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  return file_table_.get_row_desc(row_desc);
}


