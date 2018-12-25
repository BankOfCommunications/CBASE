/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: bigquery_writer.h,v 0.1 2012/03/31 16:05:08 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_BIGQUERY_WRITER_H__
#define __OCEANBASE_BIGQUERY_WRITER_H__

#include "key_generator.h"
#include "value_generator.h"
#include "prefix_info.h"

class BigqueryWriter
{
  public:
    BigqueryWriter();
    ~BigqueryWriter();

    int init(MysqlClient& ob_client, PrefixInfo& prefix_info);

  public:
    int write_data(uint64_t prefix);

  private:
    int prepare();
    int write_data(uint64_t prefix, uint64_t suffix, int64_t* col_ary, int64_t arr_len,
        MYSQL_BIND* bind_ary, int64_t& bind_ary_size, int64_t* col_arr_store);
    void translate_uint64(char* rowkey, uint64_t value);
    int commit();

  private:
    static const int64_t NUM_PER_MUTATION = 1024;

  private:
    PrefixInfo* prefix_info_;
    ValueGenerator value_gen_;
    //OB* ob_;
    //OB_SET* req_;
    MysqlClient* mysql_client_;
};

#endif //__BIGQUERY_WRITER_H__

