/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sql_builder.cpp,v 0.1 2012/02/21 10:53:22 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "sql_builder.h"
#include <ctype.h>

const char* SqlBuilder::TIMESTAMP_COL_NAME = "timestamp";

SqlBuilder::SqlBuilder()
{
  schema_ = NULL;
}

SqlBuilder::~SqlBuilder()
{
}

int SqlBuilder::init(SqlTestSchema& schema)
{
  int err = 0;

  assert(schema_ == NULL);
  schema_ = &schema;

  return err;
}

const int NUM_KEY_WORDS = 3;
const char* KEY_WORDS[NUM_KEY_WORDS] = {"KEY0", "KEY1", "NUM"};

int64_t SqlBuilder::get_value(const char* key_type, int key_len, uint64_t* keys)
{
  int64_t value = 0;

  if (!strcmp(key_type, KEY_WORDS[0]))
  {
    value = keys[0];
  }
  else if (!strcmp(key_type, KEY_WORDS[1]))
  {
    value = keys[1];
  }
  else
  {
    value_gen_.next_int_value(value);
  }

  return value;
}

int SqlBuilder::generate_read_sql(const char* sql_pattern, char* sql_result, int buf_len, int key_len, uint64_t* keys)
{
  int err = OB_SUCCESS;

  assert(sql_pattern != NULL && sql_result != NULL && buf_len > 0);
  assert(key_len >= 2 && keys != NULL);

  char* tmp_str = new char[2048];
  char* p = NULL;
  strcpy(sql_result, sql_pattern);
  while (1)
  {
    bool flag = false;
    for (int i = 0; i < NUM_KEY_WORDS; ++i)
    {
      p = strstr(sql_result, KEY_WORDS[i]);
      if (p != NULL)
      {
        flag = true;
        *p = '\0';
        p += strlen(KEY_WORDS[i]);
        int64_t value = get_value(KEY_WORDS[i], key_len, keys);
        sprintf(tmp_str, "%s%ld%s", sql_result, value, p);
        strcpy(sql_result, tmp_str);
      }
    }

    if (!flag)
    {
      break;
    }
  }

  delete[] tmp_str;
  return err;
}

int SqlBuilder::generate_write_sql(char* obsql_result, char* mysql_result, int key_len, uint64_t* keys)
{
  int err = 0;
  assert(NULL != obsql_result && NULL != mysql_result);
  assert(NULL != schema_);
  assert(key_len >= 2 && keys != NULL);

  //// Generate OB sql statement (key part)
  int key_num = schema_ -> get_key_count(0);
  assert(key_num == 2);
  int col_num = schema_-> get_column_count(0);
  sprintf(obsql_result, "insert into %s(", schema_->get_table_name(0));

  strcat(obsql_result, "rowkey,");
  for (int i = 0; i < col_num; ++i)
  {
    strcat(obsql_result, schema_->get_column_name(0, i));
    if (i != col_num - 1)
    {
      strcat(obsql_result, ",");
    }
    else
    {
      strcat(obsql_result, ")");
    }
  }

  ///// Generate Mysql sql statement (key part)
  sprintf(mysql_result, "replace into %s(", schema_->get_table_name(0));
  for (int i = 0; i < key_num; ++i)
  {
    strcat(mysql_result, schema_->get_key_name(0, i));
    strcat(mysql_result, ",");
  }
  for (int i = 0; i < col_num; ++i)
  {
    strcat(mysql_result, schema_->get_column_name(0, i));
    if (i != col_num - 1)
    {
      strcat(mysql_result, ",");
    }
    else
    {
      strcat(mysql_result, ")");
    }
  }

  ///// Generate Ob sql statement (value part1: key value)
  strcat(obsql_result, " values(");
  sprintf(obsql_result, "%s0X%016lx%016lx,", obsql_result, keys[0], keys[1]);

  ///// Generate Mysql sql statement (value part1: key value)
  strcat(mysql_result, " values(");
  for (int i = 0; i < key_num; ++i)
  {
    sprintf(mysql_result, "%s%lu,", mysql_result, keys[i]);
  }

  ///// Generate Ob and Mysql sql statement(value part2: column value)
  for (int i = 0; i < col_num && 0 == err; ++i)
  {
    int64_t value = 0;
    const char* column_name = schema_->get_column_name(0, i);
    if (!strcmp(column_name, TIMESTAMP_COL_NAME))
    {
      value = tbsys::CTimeUtil::getTime();
    }
    else
    {
      int64_t tmp_value = 0;
      value_gen_.next_int_value(tmp_value);
      value = tmp_value;
    }

    int column_type = schema_->get_column_type(0, i);
    if (column_type == ObIntType)
    {
      sprintf(obsql_result, "%s%ld", obsql_result, value);
    }
    else if (column_type == ObVarcharType)
    {
      sprintf(obsql_result, "%s'%ld'", obsql_result, value);
    }
    else
    {
      TBSYS_LOG(WARN, "invalid column type, column_type=%d", column_type);
      err = OB_ERROR;
    }

    sprintf(mysql_result, "%s%ld", mysql_result, value);

    if (i != col_num - 1)
    {
      strcat(obsql_result, ",");
      strcat(mysql_result, ",");
    }
    else
    {
      strcat(obsql_result, ")");
      strcat(mysql_result, ")");
    }
  }
  return err;
}


