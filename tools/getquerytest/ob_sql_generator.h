/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_generator.h,  02/04/2013 09:29:23 AM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   sql generator
 * 
 */
#ifndef _OB_SQL_GENERATOR_
#define _OB_SQL_GENERATOR_

#include <mysql/mysql.h>
class ObSqlGenerator
{
  public:
    static int make_insert_sql(char *buf_index, int buf_index_len, char *buf_get, int buf_get_len, int64_t index);
    static int make_batch_insert_sql(char *buf_index, int buf_index_len, char *buf_get, int buf_get_len, int64_t start, int64_t count);
    static int make_select_index_table_sql(char *buf_index, int buf_index_len, int64_t index_start, int64_t index_end);
    static int make_select_get_table_sql(char *buf_index, int buf_index_len, int64_t index_start, int64_t index_end, MYSQL &mysql, int64_t &row_cnt);
    static int verify(int64_t index_start, int64_t index_end, MYSQL &mysql);
    static int fail_counter();
    static int drop_index_table(char *buf, int buf_len);
    static int drop_get_table(char *buf, int buf_len);
    static int create_index_table(char *buf, int buf_len);
    static int create_get_table(char *buf, int buf_len);
  private:
    static int64_t hash(int64_t);
    static int64_t make_rowkey(int64_t index);
  private:
    static const char *index_table_name_;
    static const char *get_table_name_;
    static int fail_counter_;
};

#endif
