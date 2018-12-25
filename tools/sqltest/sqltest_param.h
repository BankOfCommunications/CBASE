/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sqltest_param.h,v 0.1 2012/02/24 10:08:18 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_SQLTEST_PARAM_H_
#define OCEANBASE_SQLTEST_PARAM_H_

#include <stdint.h>
#include "common/ob_define.h"
#include "util.h"

class SqlTestParam
{
  public:
    static const int64_t DEFAULT_TIMEOUT              = 2000000; //2s
    static const int64_t DEFAULT_WRITE_THREAD_COUNT   = 2;
    static const int64_t DEFAULT_READ_THERAD_COUNT    = 20;

    SqlTestParam();
    ~SqlTestParam();

  public:
    int load_from_config();
    void dump_param();

    inline const int64_t get_write_thread_count() const 
    {
      return write_thread_count_; 
    }

    inline const int64_t get_read_thread_count() const 
    {
      return read_thread_count_; 
    }

    inline void set_client_id(int8_t client_id)
    {
      client_id_ = client_id;
    }

    inline int8_t get_client_id() const
    {
      return client_id_;
    }

    inline char* get_ob_ip()
    {
      return ob_ip_;
    }

    inline int get_ob_port()
    {
      return ob_port_;
    }

    inline char* get_mysql_ip()
    {
      return mysql_ip_;
    }

    inline int get_mysql_port()
    {
      return mysql_port_;
    }
    char* get_mysql_db()
    {
      return mysql_db_;
    }

    char* get_mysql_user()
    {
      return mysql_user_;
    }

    char* get_mysql_pass()
    {
      return mysql_pass_;
    }

    inline char* get_pattern_file()
    {
      return pattern_file_;
    }

    inline char* get_schema_file()
    {
      return schema_file_;
    }

  private:
    int load_string(char* dest, const int64_t size, 
        const char* section, const char* name, bool not_null);

  private:
    int8_t client_id_;
    int64_t write_thread_count_;
    int64_t read_thread_count_;
    char ob_ip_[128];
    int ob_port_;
    char mysql_ip_[128];
    int mysql_port_;
    char pattern_file_[128];
    char schema_file_[128];
    char mysql_user_[128];
    char mysql_pass_[128];
    char mysql_db_[128];
};

#endif //OCEANBASE_SQLTEST_PARAM_H_
