/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_sql_ups_rpc_proxy2.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_fake_sql_ups_rpc_proxy2.h"
#include "common/ob_new_scanner_helper.h"

using namespace oceanbase;
using namespace common;
using namespace sql::test;

ObFakeSqlUpsRpcProxy2::ObFakeSqlUpsRpcProxy2()
  :column_count_(0),
  mem_size_limit_(0)
{
}

ObFakeSqlUpsRpcProxy2::~ObFakeSqlUpsRpcProxy2()
{
}

void ObFakeSqlUpsRpcProxy2::set_mem_size_limit(const int64_t limit)
{
  mem_size_limit_ = limit;
}

int ObFakeSqlUpsRpcProxy2::check_incremental_data_range(int64_t table_id, ObVersionRange &version, ObVersionRange &new_range)
{
  UNUSED(table_id);
  UNUSED(version);
  UNUSED(new_range);
  return OB_SUCCESS;
}

int ObFakeSqlUpsRpcProxy2::sql_ups_get(const ObGetParam & get_param, ObNewScanner & new_scanner, const int64_t timeout) 
{
  UNUSED(timeout);

  int ret = OB_SUCCESS;
  ObUpsRow ups_row;
  ObRowDesc row_desc;

  new_scanner.clear();

  if( 0 == column_count_ )
  {
    ret = OB_INVALID_ARGUMENT;
  }

  for(int i=1;OB_SUCCESS == ret && i<=column_count_;i++)
  {
    if(OB_SUCCESS != (ret = row_desc.add_column_desc(TABLE_ID, i)))
    {
      TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    ups_row.set_row_desc(row_desc);
  }

  if(OB_SUCCESS == ret)
  {
    if(get_param.get_cell_size() / column_count_ > MAX_ROW_COUNT)
    {
      if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(false, MAX_ROW_COUNT)))
      {
        TBSYS_LOG(WARN, "set is req fullfilled fail:ret[%d]", ret);
      }
    }
    else
    {
      if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(true, get_param.get_row_size())))
      {
        TBSYS_LOG(WARN, "set is req fullfilled fail:ret[%d]", ret);
      }
    }
  }

  int64_t row_count = 0;
  for(int64_t i=0;OB_SUCCESS == ret && i<get_param.get_cell_size();i+=column_count_)
  {
    if( row_count >= MAX_ROW_COUNT )
    {
      break;
    }

    ObRowkey rowkey;

    for(int64_t j=0;OB_SUCCESS == ret && j<column_count_;j++)
    {
      ObCellInfo *cell = get_param[i + j];
      rowkey = cell->row_key_;
      if(OB_SUCCESS != (ret = ups_row.raw_set_cell(j, cell->value_)))
      {
        TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      row_count ++;
      if(OB_SUCCESS != (ret = new_scanner.add_row(rowkey, ups_row)))
      {
        TBSYS_LOG(WARN, "add row fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int64_t ObFakeSqlUpsRpcProxy2::get_int(ObRowkey rowkey) 
{
  int64_t ret = 0;
  int num = strlen("rowkey_");
  ObString key;
  rowkey.ptr()[0].get_varchar(key);
  int pow = 1;
  for(int i=5-1;i>=0;i--)
  {
    char c = key.ptr()[i+num];
    ret += (c - '0') * pow;
    pow *= 10;
  }
  return ret;
}

int ObFakeSqlUpsRpcProxy2::gen_new_scanner(const ObScanParam & scan_param, ObNewScanner & new_scanner) 
{
  int ret = OB_SUCCESS;
  const ObNewRange *const range = scan_param.get_range();
  int start = get_int(range->start_key_);
  int end = get_int(range->end_key_);

  new_scanner.clear();

  ObBorderFlag border_flag;
  border_flag = range->border_flag_;

  if (start+100 >= end)
  {
    if(OB_SUCCESS != (ret = gen_new_scanner(range->table_id_, start, end, border_flag, new_scanner, true)))
    {
      TBSYS_LOG(WARN, "gen new scanner fail:ret[%d]", ret);
    }
  }
  else
  {
    border_flag.set_inclusive_end();
    if(OB_SUCCESS != (ret = gen_new_scanner(range->table_id_, start, start+100, border_flag, new_scanner, false)))
    {
      TBSYS_LOG(WARN, "gen new scanner fail:ret[%d]", ret);
    }
  }
  return ret;
}


int ObFakeSqlUpsRpcProxy2::gen_new_scanner(uint64_t table_id, int64_t start_rowkey, int64_t end_rowkey, ObBorderFlag border_flag, ObNewScanner &new_scanner, bool is_fullfilled)
{
  int ret = OB_SUCCESS;
  char rowkey_buf[100];
  ObString rowkey_str;
  ObObj rowkey_obj;
  ObRowkey rowkey;

  ObScanner scanner;

  ObNewRange range;
  range.table_id_ = table_id;
  range.border_flag_ = border_flag;
  gen_new_range(start_rowkey, end_rowkey, range_buf_, range);
  
  if(OB_SUCCESS != (ret = scanner.set_range(range)))
  {
    TBSYS_LOG(WARN, "scanner set range fail:ret[%d]", ret);
  }

  ObUpsRow ups_row;
  ObRowDesc row_desc;
  ObObj value;

  if(OB_SUCCESS == ret)
  {
    for(uint64_t i = 0;OB_SUCCESS == ret && i<COLUMN_NUMS;i++)
    {
      if(OB_SUCCESS != (ret = row_desc.add_column_desc(TABLE_ID, i+OB_APP_MIN_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
      }
    }
    ups_row.set_row_desc(row_desc);
  }

  int64_t start = border_flag.inclusive_start() ? start_rowkey : start_rowkey + 1;
  int64_t end = border_flag.inclusive_end() ? end_rowkey : end_rowkey - 1;

  ObCellInfo cell_info;


  if(OB_SUCCESS == ret)
  {
    for(int64_t i=start;OB_SUCCESS == ret && i<=end;i++)
    {
      sprintf(rowkey_buf, "rowkey_%05ld", i);
      rowkey_str.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));
      rowkey_obj.set_varchar(rowkey_str);
      rowkey.assign(&rowkey_obj, 1);

      for(int64_t j=0;OB_SUCCESS == ret && j<COLUMN_NUMS;j++)
      {
        cell_info.table_id_ = TABLE_ID;
        cell_info.row_key_ = rowkey;
        cell_info.column_id_ = j+OB_APP_MIN_COLUMN_ID;
        cell_info.value_.set_int(i * 1000 + j);
        if(OB_SUCCESS != (ret = scanner.add_cell(cell_info)))
        {
          TBSYS_LOG(WARN, "scanner add cell fail:ret[%d]", ret);
        }
      }
    }
  }


  int64_t fullfilled_cell_num = (end - start + 1) * COLUMN_NUMS;

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = scanner.set_is_req_fullfilled(is_fullfilled, fullfilled_cell_num)))
    {
      TBSYS_LOG(WARN, "set fullfilled fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = ObNewScannerHelper::convert(scanner, &row_desc, new_scanner)))
    {
      TBSYS_LOG(WARN, "convert scanner to new scanner fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObFakeSqlUpsRpcProxy2::sql_ups_scan(const ObScanParam & scan_param, ObNewScanner & new_scanner, const int64_t timeout) 
{
  UNUSED(timeout);

  int ret = OB_SUCCESS;

  if(mem_size_limit_ != 0)
  {
    new_scanner.set_mem_size_limit(mem_size_limit_);
  }

  if(OB_SUCCESS != (ret = gen_new_scanner(scan_param, new_scanner)))
  {
    TBSYS_LOG(WARN, "gen new scanner fail:ret[%d]", ret);
  }
  return ret;
}

