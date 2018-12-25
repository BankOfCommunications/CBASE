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
 *   Sql Reader
 * 
 */

#include <mysql/mysql.h>
#include "ob_sql_reader.h"
#include "ob_sql_generator.h"
#include "tbsys.h"

ObSqlReader::ObSqlReader()
{
}

ObSqlReader::~ObSqlReader()
{
}

// random read row
int ObSqlReader::read(int64_t max_row_idx)
{
  // sleep(1);
  int ret = 0;
  int batch_get_count = 100;
  static const int buf_len = 102400;
  char buf[buf_len];
  // random read row here
  if (max_row_idx > 1)
  {
    int64_t first = rand() % max_row_idx;
    int64_t last = first + batch_get_count - 1;
    if (first == 0)
    {
      first = 1;
    }
    if (last >= max_row_idx)
    {
      last = max_row_idx -1;
    }

    TBSYS_LOG(DEBUG, "first=%ld, last=%ld", first, last);
    if (0 == ObSqlGenerator::make_select_index_table_sql(buf, buf_len, first, last))
    {
      TBSYS_LOG(DEBUG, "selet index table sql: %s", buf);
      ret = mysql_query(&mysql_, buf);
      if (0 != ret) 
      {
        TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf, mysql_error(&mysql_));
      }
      else
      {
        int64_t row_cnt = 0;
        if (0 == ObSqlGenerator::make_select_get_table_sql(buf, buf_len, first, last, mysql_, row_cnt))
        {
          if (row_cnt > 0)
          {
            TBSYS_LOG(DEBUG, "select get table sql: %s", buf);
            ret = mysql_query(&mysql_, buf);
            if (0 != ret) 
            {
              TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf, mysql_error(&mysql_));
            }
            assert(0 == ObSqlGenerator::verify(first, last, mysql_));
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to make_select_get_table_sql. fail count=%d", ObSqlGenerator::fail_counter());
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "fail to make_select_index_table_sql. fail count=%d", ObSqlGenerator::fail_counter());
    }
  }
  return ret;
}

