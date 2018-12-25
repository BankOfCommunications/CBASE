/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_reader.cpp,  02/01/2013 04:23:59 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   Sql Writer
 * 
 */
#include <stdint.h>
#include "tbsys.h"
#include "ob_sql_writer.h"
#include "ob_sql_generator.h"

ObSqlWriter::ObSqlWriter()
{
}

ObSqlWriter::~ObSqlWriter()
{
}

int ObSqlWriter::write(int64_t start, int64_t count)
{
  int ret = 0;
  static const int buf_index_len = 102400;
  char buf_index[buf_index_len];
  static const int buf_get_len = 102400;
  char buf_get[buf_get_len];
  TBSYS_LOG(DEBUG, "WRITE: start=%ld, count=%ld", start, count);
  // start transaction
  ret = ObSqlGenerator::make_batch_insert_sql(buf_index, buf_index_len, buf_get, buf_get_len, start, count);
  if (0 == ret)
  {
    TBSYS_LOG(DEBUG, "insert into index and get table sql: %s,%s", buf_index, buf_get);
    if (0 == ret)
    {
      ret = mysql_query(&mysql_, buf_index);
      if (0 != ret)
      {
        TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf_index, mysql_error(&mysql_));
      }
    }
    if (0 == ret)
    {
      ret = mysql_query(&mysql_, buf_get);
      if (0 != ret)
      {
        TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf_get, mysql_error(&mysql_));
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "fail to make batch sql! ret=%d", ret);
  }
  // commit or rollback
  return ret;
}

