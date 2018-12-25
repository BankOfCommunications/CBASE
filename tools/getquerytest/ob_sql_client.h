/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_client.h,  02/01/2013 04:59:34 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   MySQL wrapper
 * 
 */
#ifndef _OB_SQL_CLIENT_
#define _OB_SQL_CLIENT_


#include <mysql/mysql.h>

class ObSqlClient
{
  public:
    ObSqlClient() : opened_(false){}
    ~ObSqlClient() {}
    int open();
    int close();
  protected:
    MYSQL mysql_;
  private:
    bool opened_;
};

#endif
