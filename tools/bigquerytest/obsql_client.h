/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_api.h,v 0.1 2012/02/22 13:43:14 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_OB_API_H__
#define __OCEANBASE_OB_API_H__
#include "common/ob_define.h"
#include "util.h"
#include "libpq-fe.h"

// OceanBase postgre API
class ObSqlClient
{
  public:
    ObSqlClient();
    ~ObSqlClient();

    int init(char* hostaddr, int port);
    int close();

  public:
    int exec_query(char* query_sql, Array& res);
    int exec_update(char* update_sql);

  private:
    int connect();
    int translate_res(PGresult* res, Array& array);

  private:
    PGconn* get_conn();
    int create_thread_key();
    int delete_thread_key();
    static void destroy_thread_key(void* ptr);

  private:
    char host_addr_[128];
    int port_;
    //PGconn* conn_;

    pthread_key_t key_;
    bool is_init_;
};

#endif //__OB_API_H__

