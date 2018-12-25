/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mysql_api.h,v 0.1 2012/02/22 13:43:19 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_MYSQL_API_H__
#define __OCEANBASE_MYSQL_API_H__

#include "util.h"
#include "mysql.h"
#include "client.h"

class MysqlClient : public Client
{
  public:
    MysqlClient();
    ~MysqlClient();

    int init(char* host, int port, char* mysql_user, char* mysql_pass, char* mysql_db);
    int close();

  public:
    int exec_query(char* query_sql, Array& res);
    int exec_update(char* update_sql);

  private:
    int connect();
    MYSQL* get_conn();
    int create_thread_key();
    int delete_thread_key();
    static void destroy_thread_key(void* ptr);

  private:
    char host_[128];
    int port_;
    pthread_key_t key_;
    char mysql_user_[128];
    char mysql_pass_[128];
    char mysql_db_[128];
    //MYSQL* conn_;
};

#endif //__MYSQL_API_H__

