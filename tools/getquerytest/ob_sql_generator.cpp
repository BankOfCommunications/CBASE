/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_generator.cpp,  02/04/2013 09:32:05 AM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   i
 * 
 */
#include "common/utility.h"
#include "ob_sql_generator.h"
#include <assert.h>
#include "tbsys.h"
#include <stdlib.h>
#include <stdio.h>

using namespace oceanbase::common;
const char *ObSqlGenerator::index_table_name_ = "index_table";
const char *ObSqlGenerator::get_table_name_ = "get_table";
int ObSqlGenerator::fail_counter_ = 0;

int64_t ObSqlGenerator::make_rowkey(int64_t index)
{
  return (hash(index) << 32) | index;
}

int64_t ObSqlGenerator::hash(int64_t val)
{
  return val % 0xf13f13;
}

int ObSqlGenerator::make_insert_sql(char *buf_index, int buf_index_len, char *buf_get, int buf_get_len, int64_t index)
{
  UNUSED(buf_index);
  UNUSED(buf_index_len);
  UNUSED(buf_get);
  UNUSED(buf_get_len);
  UNUSED(index);
  return -1;
}

int ObSqlGenerator::make_batch_insert_sql(char *buf_index, int buf_index_len, char *buf_get, int buf_get_len, int64_t start, int64_t count)
{
  int64_t column[3] = {0,5151,1212};
  int ret = 0;
  int64_t pos_get = 0;
  int64_t pos_index = 0;
  int64_t index = 0;
  
  databuff_printf(buf_index, buf_index_len, pos_index, "insert into %s values ", index_table_name_);
  databuff_printf(buf_get, buf_get_len, pos_get, "insert into %s values ", get_table_name_);
  for (int64_t cnt = 0; 0 == ret && cnt < count; cnt++)
  {
    index = start + cnt;
    assert(index < 0xffffffff);
    int64_t rowkey = (hash(index) << 32) | index;
    databuff_printf(buf_index, buf_index_len, pos_index, "(%ld, %ld)", index, rowkey);
    databuff_printf(buf_get, buf_get_len, pos_get, "(%ld, %ld, %ld, %ld, %ld, %ld)", rowkey, rowkey, index, column[0], column[1], column[2]);
    if (cnt != (count - 1))
    {
      databuff_printf(buf_index, buf_index_len, pos_index, ",");
      databuff_printf(buf_get, buf_get_len, pos_get, ",");
    }    
  }
  if (pos_index >= buf_index_len || pos_get >= buf_get_len)
  {
    TBSYS_LOG(WARN, "buf not enough");
    ret = -1;
    fail_counter_++;
  }
  return ret;
}


int ObSqlGenerator::make_select_index_table_sql(char *buf, int buf_len, int64_t index_first, int64_t index_last)
{
  int ret = 0;
  int64_t used_len = 0;
  used_len = snprintf(buf, buf_len, "select * from %s where index>=%ld and index<=%ld", index_table_name_, index_first, index_last);
  static int cnter = 0;
  if (cnter % 100 == 0)
  {
    TBSYS_LOG(INFO, "%s", buf);
  }
  cnter++;
  if (used_len >= buf_len)
  {
    ret = -1;
    fail_counter_++;
  }
  return ret;
}

int ObSqlGenerator::make_select_get_table_sql(char *buf, int buf_len, int64_t index_first, int64_t index_last, MYSQL &mysql, int64_t &row_cnt)
{
  int ret = 0;
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "select * from %s where (rowkey, rowkey2) in (", get_table_name_);
  
  // mysql data: [index], [rowkey]
  // iterate all rowkeys
  MYSQL_RES *res = mysql_store_result(&mysql);
  if (NULL != res)
  {
    MYSQL_ROW row;
    while(NULL != (row = mysql_fetch_row(res)))
    {
      row_cnt++;
      databuff_printf(buf, buf_len, pos, "(%s, %s),", row[1], row[1]);
    }
    if (row_cnt > 0)
    {
      databuff_printf(buf, buf_len, pos, "(-1, -1))");
      if (pos >= buf_len)
      {
        ret = -1;
      }
      assert(pos < buf_len);
      if (row_cnt > 0)
        assert(row_cnt == (index_last- index_first + 1));
    }
    else
    {
      ret = -1;
    }
    mysql_free_result(res);
  }
  else
  {
    ret = -1;
  }
  if (0 != ret)
  {
    fail_counter_++;
  }
  return ret;
}

int ObSqlGenerator::verify(int64_t index_first, int64_t index_last, MYSQL &mysql)
{
  int ret = 0;
  int64_t column[3] = {0,5151,1212};
  MYSQL_RES *res = mysql_store_result(&mysql);
  MYSQL_ROW row;
  int64_t row_cnt = 0;
  while(NULL != (row = mysql_fetch_row(res)))
  {
    if (false == (row[0] || row[1] || row[2] || row[3] || row[4] || row[5]))
    {
      TBSYS_LOG(WARN, "NULL row. check if OB internal request timeout");
      ret = -1;
      break;
    }
    row_cnt++;
    int64_t rowkey = atoll(row[0]);
    int64_t rowkey2 = atoll(row[1]);
    int64_t index = atoll(row[2]);
    int64_t c1 = atoll(row[3]);
    int64_t c2 = atoll(row[4]);
    int64_t c3 = atoll(row[5]);
    assert(rowkey == make_rowkey(index));
    assert(rowkey == rowkey2);
    assert(c1 == column[0]);
    assert(c2 == column[1]);
    assert(c3 == column[2]);
    if ((rowkey != make_rowkey(index))
        || c1 != column[0]
        || c2 != column[1]
        || c3 != column[2])
    {
      ret = -1;
    }
  }
  mysql_free_result(res);
  if (0 == ret && row_cnt > 0)
  {
    if (row_cnt != (index_last - index_first + 1))
    {
      TBSYS_LOG(WARN, "unexpected: row=%ld, expect=%ld", row_cnt, (index_last - index_first + 1));
    }
    assert(row_cnt == (index_last - index_first + 1));
  }
  if (0 != ret)
  {
    fail_counter_++;
  }
  return ret;
}

int ObSqlGenerator::fail_counter()
{
  return fail_counter_;
}

int ObSqlGenerator::drop_index_table(char *buf, int buf_len)
{
  snprintf(buf, buf_len, "drop table if exists index_table");
  return 0;
}
int ObSqlGenerator::drop_get_table(char *buf, int buf_len)
{
  snprintf(buf, buf_len, "drop table if exists get_table");
  return 0;
}
int ObSqlGenerator::create_index_table(char *buf, int buf_len)
{
  snprintf(buf, buf_len, "create table index_table (index int primary key, rowkey int)");
  return 0;
}
int ObSqlGenerator::create_get_table(char *buf, int buf_len)
{
  snprintf(buf, buf_len, "create table get_table (rowkey int, rowkey2 int, primary key(rowkey, rowkey2), index int, c1 int, c2 int, c3 int)");
  return 0;
}
