/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_reader.h,  02/01/2013 04:25:15 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   Sql reader
 * 
 */

#include <stdint.h>
#include "ob_sql_client.h"

class ObSqlDbCreater : public ObSqlClient
{
  public:
  ObSqlDbCreater();
  ~ObSqlDbCreater();
  int create();
};

