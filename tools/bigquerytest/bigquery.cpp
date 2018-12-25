/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: bigquery.cpp,v 0.1 2012/04/01 11:31:02 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "bigquery.h"

Bigquery::Bigquery()
{
  table_name_[0] = '\0';
  groupby_num_ = 0;
  orderby_num_ = 0;
  limit_ = 0;
  offset_ = 0;
  filter_num_ = 0;
  select_num_ = 0;
  having_num_ = 0;
  is_distinct_ = false;
  set_limit_ = false;
}

Bigquery::~Bigquery()
{
}

void Bigquery::set_table_name(const char* table_name)
{
  assert(NULL != table_name);
  strcpy(table_name_, table_name);
}

char* Bigquery::get_table_name()
{
  return table_name_;
}

void Bigquery::add_groupby_column(const char* column_name)
{
  assert(NULL != column_name);
  assert(groupby_num_ < MAX_COLUMN_NUM);

  strcpy(groupby_columns_[groupby_num_], column_name);
  ++groupby_num_;
}

void Bigquery::add_orderby_column(const char* column_name, bool is_asc)
{
  assert(NULL != column_name);
  assert(orderby_num_ < MAX_COLUMN_NUM);

  strcpy(orderby_columns_[orderby_num_].column_name, column_name);
  orderby_columns_[orderby_num_].type = is_asc ? ORDERBY_ASC : ORDERBY_DEC;
  ++orderby_num_;
}

void Bigquery::add_select_column(const char* column_name)
{
  assert(NULL != column_name);
  assert(select_num_ < MAX_COLUMN_NUM);

  strcpy(select_columns_[select_num_], column_name);
  ++select_num_;
}

void Bigquery::add_having_filter(const char* column_name, int64_t type, int64_t col_val)
{
  assert(NULL != column_name);
  assert(having_num_ < MAX_COLUMN_NUM);

  strcpy(having_columns_[having_num_].column_name, column_name);
  having_columns_[having_num_].type = type;
  having_columns_[having_num_].col_val.set_int(col_val);
  ++having_num_;
}

void Bigquery::add_filter(const char* column_name, int64_t type, int64_t col_val)
{
  assert(NULL != column_name);
  assert(filter_num_ < MAX_COLUMN_NUM);

  strcpy(filter_columns_[filter_num_].column_name, column_name);
  filter_columns_[filter_num_].type = type;
  filter_columns_[filter_num_].col_val.set_int(col_val);
  ++filter_num_;
}

void Bigquery::set_limit(int64_t limit, int64_t offset)
{
  set_limit_ = true;
  limit_ = limit;
  offset_ = offset;
}

void Bigquery::set_distinct(bool is_distinct)
{
  is_distinct_ = is_distinct;
}

char* Bigquery::to_sql()
{
  sql_[0] = '\0';
  // select (distinct) col1, col2 from table
  strcat(sql_, "select ");
  if (is_distinct_)
  {
    strcat(sql_, "distinct ");
  }
  for (int64_t i = 0; i < select_num_; ++i)
  {
    strcat(sql_, select_columns_[i]);
    if (i != select_num_ - 1)
    {
      strcat(sql_, ",");
    }
    else
    {
      strcat(sql_, " ");
    }
  }

  strcat(sql_, "from ");
  strcat(sql_, table_name_);
  strcat(sql_, " ");
  
  // where
  if (filter_num_ > 0)
  {
    strcat(sql_, "where /*+rowkey,prefix:8,suffix:8*/ ");
    for (int64_t i = 0; i < filter_num_; ++i)
    {
      strcat(sql_, filter_columns_[i].column_name);
      strcat(sql_, translate_op(filter_columns_[i].type));
      strcat(sql_, translate_obj(filter_columns_[i].col_val));
      if (i != filter_num_ - 1)
      {
        strcat(sql_, " and ");
      }
    }
    strcat(sql_, " ");
  }

  // groupby
  if (groupby_num_ > 0)
  {
    strcat(sql_, "group by ");
    for (int64_t i = 0; i < groupby_num_; ++i)
    {
      strcat(sql_, groupby_columns_[i]);
      if (i != groupby_num_ - 1)
      {
        strcat(sql_, ",");
      }
    }
    strcat(sql_, " ");
  }

  // having
  if (having_num_ > 0)
  {
    strcat(sql_, "having ");
    for (int64_t i = 0; i < having_num_; ++i)
    {
      strcat(sql_, having_columns_[i].column_name);
      strcat(sql_, translate_op(having_columns_[i].type));
      strcat(sql_, translate_obj(having_columns_[i].col_val));
      if (i != having_num_ - 1)
      {
        strcat(sql_, " and");
      }
    }
    strcat(sql_, " ");
  }

  // orderby
  if (orderby_num_ > 0)
  {
    strcat(sql_, "order by ");
    for (int64_t i = 0; i < orderby_num_; ++i)
    {
      strcat(sql_, orderby_columns_[i].column_name);
      if (orderby_columns_[i].type == ORDERBY_DEC)
      {
        strcat(sql_, " desc");
      }
      if (i != orderby_num_ - 1)
      {
        strcat(sql_, ",");
      }
    }
    strcat(sql_, " ");
  }

  // limit
  if (set_limit_)
  {
    strcat(sql_, "limit ");
    strcat(sql_, translate_int(limit_));
    strcat(sql_, " ");
    strcat(sql_, translate_int(offset_));
  }
  
  return sql_;
}

char* Bigquery::translate_op(int64_t type)
{
  tmp_buf_[0] = '\0';
  switch (type)
  {
    case FILTER_EQ:
      strcpy(tmp_buf_, "=");
      break;
    case FILTER_LT:
      strcpy(tmp_buf_, "<");
      break;
    case FILTER_GT:
      strcpy(tmp_buf_, ">");
      break;
    case FILTER_LE:
      strcpy(tmp_buf_, "<=");
      break;
    case FILTER_GE:
      strcpy(tmp_buf_, ">=");
      break;
    default:
      TBSYS_LOG(WARN, "invalid type, type=%ld", type);
      assert(false); // impossible
      break;
  }
  return tmp_buf_;
}

char* Bigquery::translate_obj(const ObObj& obj)
{
  int64_t type = obj.get_type();
  if (type == ObIntType)
  {
    int64_t int_val = 0;
    obj.get_int(int_val);
    return translate_int(int_val);
  }
  else if (type == ObVarcharType)
  {
    ObString str_val;
    obj.get_varchar(str_val);
    return translate_str(str_val);
  }
  else
  {
    TBSYS_LOG(WARN, "invalid obj type, type=%ld", type);
    assert(false);
  }

  return NULL;
}

char* Bigquery::translate_int(int64_t int_val)
{
  sprintf(tmp_buf_, "%ld", int_val);
  return tmp_buf_;
}

char* Bigquery::translate_str(const ObString& str)
{
  sprintf(tmp_buf_, "%.*s", str.length(), str.ptr());
  return tmp_buf_;
}

