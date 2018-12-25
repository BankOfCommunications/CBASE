/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: client.h,v 0.1 2012/02/23 19:29:31 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CLIENT_H__
#define __OCEANBASE_CLIENT_H__

#include "util.h"

class Client
{
  public:
    Client()
    {
      // empty
    }
    virtual ~Client()
    {
      // empty
    }

  public:
    virtual int exec_query(char* query_sql, Array& pos) = 0;
    virtual int exec_update(char* update_sql) = 0;
};

#endif //__CLIENT_H__

