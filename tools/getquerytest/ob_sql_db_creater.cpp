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
 *   Sql DbCreater
 * 
 */
#include <stdint.h>
#include "tbsys.h"
#include "ob_sql_db_creater.h"
#include "ob_sql_generator.h"

ObSqlDbCreater::ObSqlDbCreater()
{
}

ObSqlDbCreater::~ObSqlDbCreater()
{
}

int ObSqlDbCreater::create()
{
  int ret = 0;
  static const int buf_index_len = 102400;
  char buf_index[buf_index_len];
  static const int buf_get_len = 102400;
  char buf_get[buf_get_len];
  // start transaction
  if (0 == ret)
  {
    ret = ObSqlGenerator::drop_index_table(buf_index, buf_index_len);
    if (0 == ret)
    {
      if (0 == ret)
      {
        ret = mysql_query(&mysql_, buf_index);
        if (0 != ret)
        {
          TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf_index, mysql_error(&mysql_));
        }
      }
    }
  }
  if (0 == ret)
  {
    ret = ObSqlGenerator::drop_get_table(buf_get, buf_get_len);
    if (0 == ret)
    {
      if (0 == ret)
      {
        ret = mysql_query(&mysql_, buf_get);
        if (0 != ret)
        {
          TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf_get, mysql_error(&mysql_));
        }
      }
    }
  }
  if (0 == ret)
  {
    ret = ObSqlGenerator::create_index_table(buf_index, buf_index_len);
    if (0 == ret)
    {
      if (0 == ret)
      {
        ret = mysql_query(&mysql_, buf_index);
        if (0 != ret)
        {
          TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf_index, mysql_error(&mysql_));
        }
      }
    }
  }
  if (0 == ret)
  {
    ret = ObSqlGenerator::create_get_table(buf_get, buf_get_len);
    if (0 == ret)
    {
      if (0 == ret)
      {
        ret = mysql_query(&mysql_, buf_get);
        if (0 != ret)
        {
          TBSYS_LOG(WARN, "fail to execute %s. \nERR:%s", buf_get, mysql_error(&mysql_));
        }
      }
    }
  }

  if (0 != ret)
  {
    TBSYS_LOG(WARN, "fail to init database");
  }
  else
  {
    TBSYS_LOG(INFO, "create index_table, get_table success");
  }
  return ret;
}

