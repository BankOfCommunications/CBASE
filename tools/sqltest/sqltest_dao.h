/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sqltest_dao.h,v 0.1 2012/02/24 10:08:18 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_SQLTEST_DAO_H__
#define __OCEANBASE_SQLTEST_DAO_H__

#include "obsql_client.h"
#include "mysql_client.h"
#include "util.h"
#include "prefix_info.h"

class SqlTestDao
{
  public:
    SqlTestDao();
    ~SqlTestDao();

    int init(char* ob_host, int ob_port, char* mysql_host, int mysql_port,
        char* mysql_user, char* mysql_pass, char* mysql_db);

    int exec_query(char* obsql_query, char* mysql_query, uint64_t prefix);
    int exec_update(char* obsql_update, char* mysql_udpate, uint64_t prefix);

    int exec_query_ob(char* obsql_query, Array& ob_res);
    int exec_update_ob(char* obsql_udpate);

    ObSqlClient& get_ob_client()
    {
      assert(NULL != ob_client_);
      return *((ObSqlClient*) ob_client_);
    }

    MysqlClient& get_mysql_client()
    {
      assert(NULL != mysql_client_);
      return *((MysqlClient*) mysql_client_);
    }

  private:
    int compare_result(Array& ob_res, Array& mysql_res);
    int need_compare(uint64_t prefix, bool& compare);
    int set_flag(uint64_t prefix, int flag);

  private:
    char ob_host_[128];
    int ob_port_;
    char mysql_host_[128];
    int mysql_port_;
    Client* ob_client_;
    Client* mysql_client_;
    PrefixInfo prefix_info_;
};

#endif //__SQLTEST_DAO_H__

