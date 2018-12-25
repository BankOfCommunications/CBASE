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

class BigqueryTestParam
{
  public:
    static const int64_t DEFAULT_TIMEOUT              = 2000000; //2s
    static const int64_t DEFAULT_WRITE_THREAD_COUNT   = 2;
    static const int64_t DEFAULT_READ_THERAD_COUNT    = 20;

    BigqueryTestParam();
    ~BigqueryTestParam();

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

    /*
    inline char* get_ob_ip()
    {
      return ob_ip_;
    }

    inline int get_ob_port()
    {
      return ob_port_;
    }
    */
    inline char* get_ob_addr()
    {
      return ob_addr_;
    }

    /*
    inline char* get_ob_rs_ip()
    {
      return ob_rs_ip_;
    }

    inline int get_ob_rs_port()
    {
      return ob_rs_port_;
    }
    */

  private:
    int load_string(char* dest, const int64_t size, 
        const char* section, const char* name, bool not_null);

  private:
    int8_t client_id_;
    int64_t write_thread_count_;
    int64_t read_thread_count_;
    //char ob_ip_[128];
    //int ob_port_;
    //char ob_rs_ip_[128];
    //int ob_rs_port_;
    char ob_addr_[1024];
};

#endif //OCEANBASE_SQLTEST_PARAM_H_
