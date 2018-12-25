/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: bigquery_reader.h,v 0.1 2012/03/31 16:05:22 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_BIGQUERY_READER_H__
#define __OCEANBASE_BIGQUERY_READER_H__

#include "mysql_client.h"
#include "value_generator.h"
#include "bigquery.h"

typedef int (*gen_fun)(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);

class BigqueryReader
{
  public:
    BigqueryReader();
    ~BigqueryReader();

    int init(MysqlClient& ob_client, PrefixInfo& prefix_info);

  public:
    int read_data(uint64_t prefix);

  private:
    int get_bigquery(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    int get_bigquery_one_col(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    static int get_bigquery_one_equal_col1(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    static int get_bigquery_one_equal_col2(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    static int get_bigquery_two_equal_seq_col1(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    static int get_bigquery_two_equal_cycle_col1(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    static int get_bigquery_two_equal_cycle_col2(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    int get_bigquery_two_col(uint64_t prefix, ValueRule& rule, Bigquery& query, Array& result);
    int verify_result(Array& expected_result, Array& result);
  private:
    MysqlClient* ob_client_;
    PrefixInfo* prefix_info_;
};

#endif //__BIGQUERY_READER_H__

