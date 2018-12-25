/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: bigquery_writer.cpp,v 0.1 2012/03/31 16:49:36 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "bigquery_writer.h"

static const char* BIGQUERY_TABLE = "bigquery_table";
static const char* COLUMN_PREFIX = "col";

BigqueryWriter::BigqueryWriter()
{
  mysql_client_ = NULL;
}

BigqueryWriter::~BigqueryWriter()
{
  mysql_client_ = NULL;
}

int BigqueryWriter::init(MysqlClient& mysql_client, PrefixInfo& prefix_info)
{
  int err = 0;

  mysql_client_ = &mysql_client;
  prefix_info_ = &prefix_info;

  return err;
}

int BigqueryWriter::write_data(uint64_t prefix)
{
  int err = 0;

  int64_t row_num = 0;
  int64_t col_arr[16];
  char rule_data[1024];
  ValueRule rule;

  err = prefix_info_->set_read_write_status(prefix, BIGQUERY_UPDATING);
  if (READ_WRITE_CONFLICT != err && 0 != err)
  {
    TBSYS_LOG(WARN, "failed to set update flag to true, prefix=%lu, err=%d", prefix, err);
  }

  if (0 == err)
  {
    err = value_gen_.generate_value_rule(prefix, rule);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to generate value rule, prefix=%lu, err=%d",
          prefix, err);
    }
    else
    {
      err = rule.serialize(rule_data, sizeof(rule_data));
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to serialize rule, rule_data=%p, rule_len=%ld, err=%d",
            rule_data, sizeof(rule_data), err);
      }
    }
  }

  if (0 == err)
  {
    row_num = rule.get_row_num();
    err = prefix_info_->set_rule_and_row_num(prefix, rule_data, row_num);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to set rule or row num, prefix=%lu, rule_data=%s, row_num=%ld, err=%d",
          prefix, rule_data, row_num, err);
    }
  }

  if (0 == err)
  {
    const int64_t NUM_PER_MUTATION = 100;
    //const int64_t NUM_PER_MUTATION = 1;
    int64_t NUM_BATCH = (row_num + NUM_PER_MUTATION - 1) / NUM_PER_MUTATION;
    for (int64_t batch = 0; batch < NUM_BATCH; ++batch)
    {
      int64_t batch_row_num = (batch == NUM_BATCH-1) ? row_num % NUM_PER_MUTATION : NUM_PER_MUTATION;
      if (0 == batch_row_num)
      {
        batch_row_num = NUM_PER_MUTATION;
      }
      //TBSYS_LOG(INFO, "batch=%ld, batch_row_num=%ld, row_num=%ld", batch, batch_row_num, row_num);
      char stmt_str[10240];
      char tmp_str[10240];
      MYSQL_BIND bind_ary[10240];
      int64_t col_arr_store[10240];
      int64_t bind_ary_size = 0;
      memset(bind_ary, 0x00, sizeof(bind_ary));

      snprintf(stmt_str, sizeof(stmt_str), "replace into %s(prefix, suffix", BIGQUERY_TABLE);
      for (int64_t i = 0; i < rule.get_rule_num(); ++i)
      {
        snprintf(tmp_str, sizeof(tmp_str), ", %s%ld", COLUMN_PREFIX, i + 1);
        strcat(stmt_str, tmp_str);
      }
      strcat(stmt_str, ") values");
      for (int64_t i = 0; i < batch_row_num; ++i)
      {
        strcat(stmt_str, "(?,?");
        for (int64_t j = 0; j < rule.get_rule_num(); ++j)
        {
          strcat(stmt_str, ",?");
        }
        strcat(stmt_str, ")");
        if (i != batch_row_num - 1)
        {
          strcat(stmt_str, ",");
        }
      }

      err = mysql_client_->stmt_prepare(stmt_str, strlen(stmt_str));
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to prepare, err=%d", err);
      }

      for (uint64_t i = batch * NUM_PER_MUTATION; i < (uint64_t) batch * NUM_PER_MUTATION + batch_row_num && 0 == err; ++i)
      {
        int64_t ret_size = 0;
        rule.get_col_values(i, col_arr, sizeof(col_arr) / sizeof(col_arr[0]), ret_size);
        err = write_data(prefix, i, col_arr, ret_size, bind_ary, bind_ary_size, col_arr_store);
        if (0 != err)
        {
          TBSYS_LOG(WARN, "failed to write data, prefix=%lu, suffix=%lu, err=%d",
              prefix, i, err);
        }
      }

      if (0 == err)
      {
        err = mysql_client_->stmt_bind_param(bind_ary, bind_ary_size);
        if (0 != err)
        {
          TBSYS_LOG(WARN, "failed to bind param, bind=%p, len=%ld", bind_ary, bind_ary_size);
        }
      }

      if (0 == err)
      {
        err = commit();
        if (0 != err)
        {
          TBSYS_LOG(WARN, "failed to commit, err=%d", err);
        }
      }
    }
  }

  TBSYS_LOG(INFO, "[write_data] rule=%s, err=%d", rule.to_string(), err);
  
  if (0 == err)
  {
    err = prefix_info_->set_read_write_status(prefix, 0);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to reset flag, prefix=%lu, err=%d", prefix, err);
    }
  }
  else if (READ_WRITE_CONFLICT == err)
  {
    err = OB_SUCCESS;
  }

  return err;
}

int BigqueryWriter::write_data(uint64_t prefix, uint64_t suffix, int64_t* col_ary, int64_t arr_len,
    MYSQL_BIND* bind_ary, int64_t& bind_ary_size, int64_t* col_arr_store)
{
  int err = 0;
  assert(col_ary != NULL && arr_len > 0);
  assert(mysql_client_ != NULL);
  //MYSQL_BIND bind[128];

  MYSQL_BIND * bind = bind_ary + bind_ary_size;
  int64_t* store = col_arr_store + bind_ary_size;
  //memset(bind, 0x00, 128 * sizeof(bind[0]));
  bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
  store[0] = prefix;
  bind[0].buffer = (void*) (store);
  bind[0].buffer_length = 8;
  bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
  store[1] = suffix;
  bind[1].buffer = (void*) (store + 1);
  bind[1].buffer_length = 8;
  for (int64_t i = 0; 0 == err && i < arr_len; ++i)
  {
    bind[i+2].buffer_type = MYSQL_TYPE_LONGLONG;
    //bind[i+2].buffer = (void*) (col_ary+i);
    store[i+2] = col_ary[i];
    bind[i+2].buffer = (void*) (store + i + 2);
    bind[i+2].buffer_length = 8;
  }

  bind_ary_size += (arr_len + 2);

  /*
  if (0 == err)
  {
    err = mysql_client_->stmt_bind_param(bind, arr_len + 2);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to bind param, bind=%p, len=%ld", bind, arr_len + 2);
    }
  }
  */

  return err;
}

void BigqueryWriter::translate_uint64(char* rowkey, uint64_t value)
{
  assert(rowkey != NULL);
  for (int i = 7; i >=0; --i)
  {
    *(rowkey + i) = value % 256;
    value /= 256;
  }
}

int BigqueryWriter::commit()
{
  int err = 0;
  assert(NULL != mysql_client_);
  err = mysql_client_->stmt_execute();
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to execute, err=%d", err);
  }
  return err;
}


