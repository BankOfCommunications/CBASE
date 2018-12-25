/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: bigquery.h,v 0.1 2012/04/01 11:30:56 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_BIGQUERY_H__
#define __OCEANBASE_BIGQUERY_H__
#include "util.h"
#include "common/ob_object.h"
#include "common/ob_string.h"

class Bigquery
{
  public:
    Bigquery();
    ~Bigquery();

  public:
    void set_table_name(const char* table_name);
    char* get_table_name();

    void add_groupby_column(const char* column_name);
    void add_orderby_column(const char* column_name, bool is_asc = true);
    void add_select_column(const char* column_name);
    void add_having_filter(const char* column_name, int64_t type, int64_t col_val);
    void add_filter(const char* column_name, int64_t type, int64_t col_val);
    void set_distinct(bool is_distinct = true);
    void set_limit(int64_t limit, int64_t offset);

    char* to_sql();

  private:
    char* translate_op(int64_t op_type);
    char* translate_obj(const ObObj& obj);
    char* translate_int(int64_t int_val);
    char* translate_str(const ObString& str);

  public:
    enum OpType
    {
      INVALID_TYPE = 0,
      ORDERBY_ASC,
      ORDERBY_DEC,
      FILTER_EQ,
      FILTER_LT,
      FILTER_GT,
      FILTER_LE,
      FILTER_GE
    };

  private:
    static const int64_t MAX_COLUMN_NUM = 8;
    static const int64_t MAX_SQL_LENGTH = 4096;

    struct ColumnFilter
    {
      char column_name[32];
      int64_t type;
      ObObj col_val;
    };

    struct OrderBy
    {
      char column_name[32];
      int64_t type;
    };
    char table_name_[64];
    char groupby_columns_[MAX_COLUMN_NUM][32];
    int64_t groupby_num_;
    OrderBy orderby_columns_[MAX_COLUMN_NUM];
    int64_t orderby_num_;
    char select_columns_[MAX_COLUMN_NUM][32];
    int64_t select_num_;
    bool set_limit_;
    int64_t limit_;
    int64_t offset_;
    ColumnFilter filter_columns_[MAX_COLUMN_NUM];
    int64_t filter_num_;
    ColumnFilter having_columns_[MAX_COLUMN_NUM];
    int64_t having_num_;
    bool is_distinct_;
    char sql_[MAX_SQL_LENGTH];
    char tmp_buf_[512];
};

#endif //__BIGQUERY_H__

