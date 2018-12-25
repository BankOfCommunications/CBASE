/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_sqltest_schema.h,v 0.1 2012/02/21 15:33:54 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_OB_SQLTEST_SCHEMA_H__
#define __OCEANBASE_OB_SQLTEST_SCHEMA_H__

#include "common/ob_schema.h"
#include "common/ob_define.h"
#include "util.h"

class SqlTestSchema
{
  public:
    SqlTestSchema();
    ~SqlTestSchema();

    int init();

    ObSchemaManagerV2& get_schema_mgr();
    int set_keys(int table_idx, int num_keys, const char** keys);
    int init_table_schema();
    int init_column_schema(const uint64_t table_id, const int table_idx);

  public:
    const int get_table_count() const;
    const int get_key_count(int table_idx) const;
    const int get_column_count(int table_idx) const;

    const char* get_table_name(int table_idx) const;
    const char* get_column_name(int table_idx, int column_idx) const;
    const int get_column_type(int table_idx, int column_idx) const;
    const char* get_random_column_name(int table_idx) const;
    const char* get_key_name(int table_idx, int key_idx) const;

  private:
    static const int MAX_TABLE_NUM = 4; // maximum number of tables
    static const int MAX_KEY_NUM = 10; // maximum number of primary key columns in one table
    static const int MAX_COLUMN_NUM = 64; // maximum number of normal columns in one table
    static const int MAX_NAME_LENGTH = 64; // maximum length of column name

  private:
    bool is_init_;
    ObSchemaManagerV2 schema_manager_;
    int num_tables_;
    char tables_[MAX_TABLE_NUM][MAX_NAME_LENGTH];
    int num_keys_[MAX_TABLE_NUM];
    char keys_[MAX_TABLE_NUM][MAX_KEY_NUM][MAX_NAME_LENGTH];
    int num_columns_[MAX_TABLE_NUM];
    char columns_[MAX_TABLE_NUM][MAX_COLUMN_NUM][MAX_NAME_LENGTH];
    int column_types_[MAX_TABLE_NUM][MAX_COLUMN_NUM];
};

#endif //__OB_SQLTEST_SCHEMA_H__

