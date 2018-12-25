/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_multi_get.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_fake_ups_multi_get.h"
#include "common/utility.h"

using namespace oceanbase;
using namespace sql;
using namespace test;
using namespace common;

ObFakeUpsMultiGet::ObFakeUpsMultiGet(const char *file_name)
  :curr_idx_(0),
  ups_scan_(file_name)
{
}

void ObFakeUpsMultiGet::reset()
{
  ObUpsMultiGet::reset();
  ups_scan_.reset();
  row_map_.clear();
  rowkey_arena_.reuse();
  curr_idx_ = 0;
}

int ObFakeUpsMultiGet::open()
{
  int ret = OB_SUCCESS;
  const ObRowkey *rowkey = NULL;
  const ObRow *tmp_row = NULL;
  const ObUpsRow *ups_row = NULL;
  curr_idx_ = 0;

  TBSYS_LOG(DEBUG, "open fake ups multi get operator");

  if(NULL == get_param_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param is null");
    return ret;
  }

  if(NULL == curr_row_.get_row_desc())
  {
    TBSYS_LOG(WARN, "set row desc first");
    return OB_ERROR;
  }
  
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = ups_scan_.open()))
    {
      TBSYS_LOG(WARN, "file table open fail:ret[%d]", ret);
    }
  }

  while(OB_SUCCESS == ret)
  {
    ret = ups_scan_.get_next_row(rowkey, tmp_row);
    if(OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      ups_row = dynamic_cast<const ObUpsRow *>(tmp_row);
      if(NULL == ups_row)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "should be ups row");
      }
    }

    const ObRowStore::StoredRow *stored_row = NULL;
    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = row_store_.add_ups_row(*ups_row, stored_row)))
      {
        TBSYS_LOG(WARN, "add ups row fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      ObRowkey tmp_rowkey;
      rowkey->deep_copy(tmp_rowkey, rowkey_arena_);
      row_map_[tmp_rowkey] = stored_row;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = ups_scan_.close()))
    {
      TBSYS_LOG(WARN, "file table close fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObFakeUpsMultiGet::close()
{
  return ups_scan_.close();
}


int ObFakeUpsMultiGet::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  return ret;
}

int ObFakeUpsMultiGet::get_next_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObGetParam::ObRowIndex row_index;

  if(curr_idx_ >= get_param_->get_row_size())
  {
    ret = OB_ITER_END;
  }

  if(OB_SUCCESS == ret)
  {
    row_index = get_param_->get_row_index()[curr_idx_];
    if(0 >= row_index.size_)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "row size error");
    }
  }

  if(OB_SUCCESS == ret)
  {
    rowkey = &((*get_param_)[row_index.offset_]->row_key_);
    map<ObRowkey, const ObRowStore::StoredRow *>::iterator iter = row_map_.find(*rowkey);
    const ObRowStore::StoredRow * stored_row = NULL;
    if(row_map_.end() == iter)
    {
      curr_row_.reuse();
    }
    else
    {
      stored_row = iter->second;
      ObUpsRowUtil::convert(ups_scan_.get_table_id(), stored_row->get_compact_row(), curr_row_);
    }
    row = &curr_row_;
  }

  if(OB_SUCCESS == ret)
  {
    curr_idx_ ++;
  }

  if(OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "ups multi get rowkey:[%s]", to_cstring(*rowkey));
  }

  return ret;
}

//就是做投影操作
int ObFakeUpsMultiGet::convert(const ObUpsRow &from, ObUpsRow &to)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *cell = NULL;

  to.set_is_delete_row(from.get_is_delete_row());

  for(int64_t i=0;OB_SUCCESS == ret && i<to.get_column_num();i++)
  {
    if(OB_SUCCESS != (ret = to.get_row_desc()->get_tid_cid(i, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "get_tid_cid fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = from.get_cell(table_id, column_id, cell)))
    {
      TBSYS_LOG(WARN, "get_cell fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = to.set_cell(table_id, column_id, *cell)))
    {
      TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
    }
  }
  return ret;
}



