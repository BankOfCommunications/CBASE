/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_sqltest_schema.cpp,v 0.1 2012/02/21 15:55:54 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "sqltest_schema.h"

SqlTestSchema::SqlTestSchema()
{
  is_init_ = false;
  num_tables_ = 0;
  memset(num_keys_, 0x00, sizeof(num_keys_));
  memset(num_columns_, 0x00, sizeof(num_columns_));
}

SqlTestSchema::~SqlTestSchema()
{
}

ObSchemaManagerV2& SqlTestSchema::get_schema_mgr()
{
  return schema_manager_;
}

int SqlTestSchema::set_keys(int table_idx, int num_keys, const char** keys)
{
  int err = OB_SUCCESS;

  if (NULL == keys || num_keys <= 0)
  {
    TBSYS_LOG(WARN, "invalid param");
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    num_keys_[table_idx] = num_keys;
    for (int i = 0; i < num_keys; ++i)
    {
      strcpy(keys_[table_idx][i], keys[i]);
    }
  }

  return err;
}

int SqlTestSchema::init()
{
  int err = OB_SUCCESS;

  if (is_init_)
  {
    TBSYS_LOG(WARN, "init twice");
    err = OB_ERROR;
  }
  else
  {
    err = init_table_schema();
  }

  if (OB_SUCCESS == err)
  {
    is_init_ = true;
  }
  return err;
}

int SqlTestSchema::init_table_schema()
{
  int err = OB_SUCCESS;

  const ObTableSchema* schema = NULL;
  int64_t table_size = schema_manager_.get_table_count();
  num_tables_ = table_size;
  int table_idx = 0;

  for (schema = schema_manager_.table_begin();
      OB_SUCCESS == err && schema != schema_manager_.table_end() && schema != NULL;
      schema++)
  {
    sprintf(tables_[table_idx], "%s", schema->get_table_name());
    err = init_column_schema(schema->get_table_id(), table_idx);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to init column schema, table_id=%lu, err=%d",
          schema->get_table_id(), err);
    }
    else
    {
      ++table_idx;
    }
  }

  return err;
}

int SqlTestSchema::init_column_schema(const uint64_t table_id, const int table_idx)
{
  int err = OB_SUCCESS;
  const ObColumnSchemaV2* columns = NULL;
  int32_t column_size = 0;

  columns = schema_manager_.get_table_schema(table_id, column_size);
  num_columns_[table_idx] = 0;
  for (int column_idx = 0; column_idx < column_size; ++column_idx)
  {
    if (0 != strcmp(columns[column_idx].get_name(), "prefix") &&
        0 != strcmp(columns[column_idx].get_name(), "suffix"))
    {
      sprintf(columns_[table_idx][num_columns_[table_idx]], "%s", columns[column_idx].get_name());
      column_types_[table_idx][num_columns_[table_idx]] = columns[column_idx].get_type();
      ++num_columns_[table_idx];
    }
  }
  
  return err;
}
 
const int SqlTestSchema::get_table_count() const
{
  if (!is_init_)
  {
    TBSYS_LOG(WARN, "not init");
  }

  return num_tables_;
}

const int SqlTestSchema::get_key_count(int table_idx) const
{
  if (!is_init_)
  {
    TBSYS_LOG(WARN, "not init");
  }
  else if (table_idx < 0 || table_idx >= num_tables_)
  {
    TBSYS_LOG(WARN, "invalid param, table_idx=%d, num_tables_=%d", table_idx, num_tables_);
  }

  return num_keys_[table_idx];
}

const int SqlTestSchema::get_column_count(int table_idx) const
{
  if (!is_init_)
  {
    TBSYS_LOG(WARN, "not init");
  }
  else if (table_idx < 0 || table_idx >= num_tables_)
  {
    TBSYS_LOG(WARN, "invalid param, table_idx=%d, num_tables_=%d", table_idx, num_tables_);
  }

  return num_columns_[table_idx];
}

const char* SqlTestSchema::get_table_name(int table_idx) const
{
  if (!is_init_)
  {
    TBSYS_LOG(WARN, "not init");
  }
  else if (table_idx < 0 || table_idx >= num_tables_)
  {
    TBSYS_LOG(WARN, "invalid param, table_idx=%d, num_tables_=%d", table_idx, num_tables_);
  }

  return tables_[table_idx];
}

const char* SqlTestSchema::get_column_name(int table_idx, int column_idx) const
{
  if (!is_init_)
  {
    TBSYS_LOG(WARN, "not init");
  }
  else if (table_idx < 0 || table_idx >= num_tables_)
  {
    TBSYS_LOG(WARN, "invalid param, table_idx=%d, num_tables_=%d", table_idx, num_tables_);
  }
  else if (column_idx < 0 || column_idx >= num_columns_[table_idx])
  {
    TBSYS_LOG(WARN, "invalid param, column_idx=%d, num_columns_=%d", column_idx, num_columns_[table_idx]);
  }

  return columns_[table_idx][column_idx];
}

const int SqlTestSchema::get_column_type(int table_idx, int column_idx) const
{
  assert(is_init_ == true);
  assert(table_idx >= 0 && table_idx < num_tables_);
  assert(column_idx >= 0 && column_idx < num_columns_[table_idx]);

  return column_types_[table_idx][column_idx];
}

const char* SqlTestSchema::get_random_column_name(int table_idx) const
{
  if (!is_init_)
  {
    TBSYS_LOG(WARN, "not init");
  }
  else if (table_idx < 0 || table_idx >= num_tables_)
  {
    TBSYS_LOG(WARN, "invalid param, table_idx=%d, num_tables_=%d", table_idx, num_tables_);
  }

  int rand_val = rand() % num_columns_[table_idx];
  return columns_[table_idx][rand_val];
}

const char* SqlTestSchema::get_key_name(int table_idx, int key_idx) const
{
  if (!is_init_)
  {
    TBSYS_LOG(WARN, "not init");
  }
  else if (table_idx < 0 || table_idx >= num_tables_)
  {
    TBSYS_LOG(WARN, "invalid param, table_idx=%d, num_tables_=%d", table_idx, num_tables_);
  }
  else if (key_idx < 0 || key_idx >= num_keys_[table_idx])
  {
    TBSYS_LOG(WARN, "invalid param, key_idx=%d, num_keys_=%d", key_idx, num_keys_[table_idx]);
  }

  return keys_[table_idx][key_idx];
}

