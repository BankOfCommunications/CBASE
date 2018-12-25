/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sql_builder.h,v 0.1 2012/02/21 10:49:19 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_SQL_BUILDER_H__
#define __OCEANBASE_SQL_BUILDER_H__

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "util.h"
#include "key_generator.h"
#include "sqltest_schema.h"

// This class is not thread safe
class SqlBuilder
{
  public:
    SqlBuilder();
    ~SqlBuilder();
    int init(SqlTestSchema& schema);

  public:

    int generate_read_sql(const char* sql_pattern, char* sql_result, int buf_len, int key_len, uint64_t* keys);
    int generate_write_sql(char* obsql_result, char* mysql_result, int key_len, uint64_t* keys);

  private:
    int generate_write_sql_ob(char* obsql_result);
    int generate_write_sql_mysql(char* mysql_result);
    int64_t get_value(const char* key_type, int key_len, uint64_t* keys);

  private:
    static const char* TIMESTAMP_COL_NAME;

    ValueGenerator value_gen_;
    SqlTestSchema* schema_;
};

#endif //__SQL_BUILDER_H__

