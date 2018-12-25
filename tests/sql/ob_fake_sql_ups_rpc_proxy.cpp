/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_sql_ups_rpc_proxy.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_fake_sql_ups_rpc_proxy.h"
#include "common/utility.h"

using namespace oceanbase;
using namespace common;

ObFakeSqlUpsRpcProxy::ObFakeSqlUpsRpcProxy()
  :ups_scan_file_(NULL),
  ups_multi_get_file_(NULL)
{
}

ObFakeSqlUpsRpcProxy::~ObFakeSqlUpsRpcProxy()
{
}

int ObFakeSqlUpsRpcProxy::sql_ups_get(const ObGetParam & get_param, ObNewScanner & new_scanner, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);

  if(NULL == ups_multi_get_file_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "ups multi get file is null");
    return ret;
  }

  TBSYS_LOG(DEBUG, "fake sql ups rpc proxy:[%s]", to_cstring(get_param));

  ObFakeUpsMultiGet multi_get(ups_multi_get_file_);
  const ObRowkey *rowkey = NULL;
  const ObRow *row = NULL;
  int64_t count = 0;

  ObRowDesc row_desc;
  ObRowkey last_rowkey;
  for(int64_t i=0;i<get_param.get_cell_size();i++)
  {
    ObCellInfo *cell = get_param[i];
    bool first_line = false;
    if(last_rowkey.length() == 0)
    {
      last_rowkey = cell->row_key_;
      first_line = true;
    }
    else if(last_rowkey == cell->row_key_)
    {
      first_line = true;
    }

    if(first_line)
    {
      row_desc.add_column_desc(cell->table_id_, cell->column_id_);
    }
    else
    {
      break;
    }
  }

  if(row_desc.get_column_num() <= 0)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "row desc is invalid");
    return ret;
  }
  else
  {
    multi_get.set_row_desc(row_desc);
    multi_get.set_get_param(get_param);
  }

  if(OB_SUCCESS != (ret = multi_get.open()))
  {
    TBSYS_LOG(WARN, "open multi get fail:ret[%d]", ret);
  }

  while(OB_SUCCESS == ret)
  {
    ret = multi_get.get_next_row(rowkey, row);
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
      if(OB_SUCCESS != (ret = new_scanner.add_row(*rowkey, *row)))
      {
        TBSYS_LOG(WARN, "add row fail:ret[%d]", ret);
      }
      else
      {
        count ++;
      }
    }
  }

  multi_get.close();

  if(OB_SUCCESS == ret)
  {
    new_scanner.set_is_req_fullfilled(true, count);
  }
  return ret;
}

int ObFakeSqlUpsRpcProxy::sql_ups_scan(const ObScanParam & scan_param, ObNewScanner & new_scanner, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);

  if(NULL == ups_scan_file_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "ups scan file is null");
    return ret;
  }

  ObRowDesc row_desc;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;

  if(OB_SUCCESS == ret)
  {
    table_id = scan_param.get_range()->table_id_;
    if(OB_INVALID_ID == table_id)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "table id is invalid");
      return ret;
    }
  }
  
  if(OB_SUCCESS == ret)
  {
    for(int64_t i=0;i<scan_param.get_column_id_size();i++)
    {
      column_id = scan_param.get_column_id()[i];
      row_desc.add_column_desc(table_id, column_id);
    }
  }

  new_scanner.set_range(*(scan_param.get_range()));

  ObFakeUpsScan ups_scan(ups_scan_file_);

  if(OB_SUCCESS != (ret = ups_scan.open()))
  {
    TBSYS_LOG(WARN, "open ups scan fail:ret[%d]", ret);
  }

  const ObRowkey *rowkey = NULL;
  const ObRow *row = NULL;
  const ObUpsRow *ups_row = NULL;
  ObUpsRow tmp_row;
  const ObObj *cell = NULL;
  tmp_row.set_row_desc(row_desc);

  int64_t count = 0;
  while(OB_SUCCESS == ret)
  {
    ret = ups_scan.get_next_row(rowkey, row);
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
      ups_row = dynamic_cast<const ObUpsRow *>(row);
      if(NULL == ups_row)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "must be ObUpsRow");
      }
      else
      {
        tmp_row.set_is_delete_row(ups_row->get_is_delete_row());
      }
    }

    if(OB_SUCCESS == ret)
    {
      for(int64_t i=0;OB_SUCCESS == ret && i<row_desc.get_column_num();i++)
      {
        if(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "get tid cid fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = row->get_cell(table_id, column_id, cell)))
        {
          TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = tmp_row.set_cell(table_id, column_id, *cell)))
        {
          TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
        }
      }
    }

    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = new_scanner.add_row(*rowkey, tmp_row)))
      {
        TBSYS_LOG(WARN, "add row fail:ret[%d]", ret);
      }
      else
      {
        count ++;
      }
    }
  }

  ups_scan.close();

  if(OB_SUCCESS == ret)
  {
    new_scanner.set_is_req_fullfilled(true, count);
  }

  return ret;
}

