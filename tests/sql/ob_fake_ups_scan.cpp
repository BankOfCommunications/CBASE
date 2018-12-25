/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_scan.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_fake_ups_scan.h"

using namespace oceanbase;
using namespace sql;
using namespace test;


ObFakeUpsScan::ObFakeUpsScan(const char *file_name)
  :file_table_(file_name)
{
}

int ObFakeUpsScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int ObFakeUpsScan::open()
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = file_table_.open()))
  {
    TBSYS_LOG(WARN, "open file table fail:ret[%d]", ret);
  }
  if(OB_SUCCESS == ret)
  {
    cur_ups_row_.set_row_desc(row_desc_);
  }
  return ret;
}

int ObFakeUpsScan::close()
{
  return file_table_.close();
}

int ObFakeUpsScan::get_next_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  const ObRow *tmp_row = NULL;

  ret = file_table_.get_next_row(rowkey, tmp_row);
  if(OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "get next row fail");
  }

  const ObUpsRow *tmp_ups_row = NULL;
  if(OB_SUCCESS == ret)
  {
    if(NULL == (tmp_ups_row = dynamic_cast<const ObUpsRow *>(tmp_row)))
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "should be ups row:ret[%d]", ret);
    }
    else
    {
      cur_ups_row_.set_is_delete_row(tmp_ups_row->get_is_delete_row());
    }
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
      else if(OB_SUCCESS != (ret = cur_ups_row_.set_cell(table_id, column_id, *cell)))
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
      row = &cur_ups_row_;
    }
  }

  return ret;
}

int64_t ObFakeUpsScan::to_string(char* buf, const int64_t buf_len) const
{
  return file_table_.to_string(buf, buf_len);
}

