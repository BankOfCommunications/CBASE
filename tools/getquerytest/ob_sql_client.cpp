/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_client.cpp,  02/01/2013 05:06:52 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   MySQL Wrapper Imp
 * 
 */

#include <stdlib.h>
#include "tbsys.h"
#include "ob_sql_client.h"

extern char *g_mysql_ip;
extern int g_mysql_port;

int ObSqlClient::open()
{
  int ret = 0;
  // don't allow open twice
  if (!opened_)
  {
    mysql_init(&mysql_);
    //if (!mysql_real_connect(&mysql_,"10.232.36.29", "admin", "admin", "testdb", 4797, NULL, 0))
    if (!mysql_real_connect(&mysql_, g_mysql_ip, "admin", "admin", "testdb", g_mysql_port, NULL, 0))
    {
      ret = -1;
      TBSYS_LOG(WARN, "fail to open db. ERR:%s",  mysql_error(&mysql_));
    }
    else
    {
      opened_ = true;
    }
  }
  return ret;
}

int ObSqlClient::close()
{
  mysql_close(&mysql_);
  return 0;
}

