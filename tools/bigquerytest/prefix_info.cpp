/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: error_record.cpp,v 0.1 2012/02/24 10:58:32 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "prefix_info.h"
static const uint64_t INVALID_PREFIX = INT64_MAX;
static const char* PREFIX_STATUS_TABLE_NAME = "bigquery_prefix_rule";
static const int64_t PREFIX_BITS = 48;

PrefixInfo::PrefixInfo()
{
  ob_client_ = NULL;
  pthread_rwlock_init(&w_lock_, NULL);
}

PrefixInfo::~PrefixInfo()
{
  pthread_rwlock_destroy(&w_lock_);
  // empty
}

int PrefixInfo::init(MysqlClient& ob_client)
{
  assert(NULL == ob_client_);

  ob_client_ = &ob_client;

  return 0;
}

int PrefixInfo::set_read_write_status(uint64_t prefix, int status)
{
  int err = 0;

  if (status == 0)
  {
    err = set_status(prefix, 0);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to set status, prefix=%lu, err=%d", prefix, err);
    }
  }
  else
  {
    pthread_rwlock_wrlock(&w_lock_);
    int ori_status = 0;
    err = get_status(prefix, ori_status);

    //TBSYS_LOG(INFO, "prefix=%lu, ori_status=%d", prefix, ori_status);

    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to get ori status, prefix=%lu, err=%d", prefix, err);
    }
    else
    {
      if (ori_status != 0)
      {
        err = READ_WRITE_CONFLICT;
      }
      else
      {
        err = set_status(prefix, status);
        if (0 != err)
        {
          TBSYS_LOG(WARN, "failed to set status, prefix=%lu, status=%d, err=%d", prefix, status, err);
        }
      }
    }

    pthread_rwlock_unlock(&w_lock_);
  }
  return err;
}

int PrefixInfo::set_status(uint64_t prefix, int status)
{
  int err = 0;
  assert(NULL != ob_client_);

  char ob_sql[2048];
  sprintf(ob_sql, "replace into %s(prefix, stat) values(%lu, %d)",
      PREFIX_STATUS_TABLE_NAME, prefix, status);
  err = ob_client_->exec_update(ob_sql);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to update ob, sql=%s, err=%d", ob_sql, err);
  }

  return err;
}

int PrefixInfo::get_status(uint64_t prefix, int& status)
{
  int err = 0;

  assert(NULL != ob_client_);

  char ob_sql[2048];
  sprintf(ob_sql, "select stat from %s where prefix=%lu",
      PREFIX_STATUS_TABLE_NAME, prefix);
  Array array;
  err = ob_client_->exec_query(ob_sql, array);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to query ob, sql=%s, err=%d", ob_sql, err);
  }
  else
  {
    int row_num = array.get_row_num();
    assert(row_num <= 1);
    if (row_num == 0)
    {
      status = 0;
      //err = SQLTEST_OB_NOT_EXIST;
      err = 0;
    }
    else
    {
      char* value = NULL;
      int type = 0;
      array.get_value(0, 0, value, type);
      assert(NULL != value);
      status = atoi(value);
    }
  }

  return err;
}

int PrefixInfo::set_rule(uint64_t prefix, const char* rule_data)
{
  int err = 0;
  assert(NULL != ob_client_);

  char ob_sql[2048];
  sprintf(ob_sql, "replace into %s(prefix, rule_data) values(%lu, '%s')", PREFIX_STATUS_TABLE_NAME, prefix, rule_data);
  err = ob_client_->exec_update(ob_sql);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to update ob, sql=%s, err=%d", ob_sql, err);
  }

  return err;
}

int PrefixInfo::set_rule_and_row_num(uint64_t prefix, char* rule_data, uint64_t row_num)
{
  int err = 0;
  assert(NULL != ob_client_);

  char ob_sql[2048];
  sprintf(ob_sql, "replace into %s(prefix, rule_data, row_num) values(%lu, '%s', %lu)",
      PREFIX_STATUS_TABLE_NAME, prefix, rule_data, row_num);
  err = ob_client_->exec_update(ob_sql);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to update ob, sql=%s, err=%d", ob_sql, err);
  }
  return err;
}

int PrefixInfo::get_rule(uint64_t prefix, char* rule_data, int64_t rule_size)
{
  int err = 0;

  assert(NULL != ob_client_);

  char ob_sql[2048];
  sprintf(ob_sql, "select rule_data from %s where prefix=%lu",
      PREFIX_STATUS_TABLE_NAME, prefix);
  Array array;
  err = ob_client_->exec_query(ob_sql, array);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to query ob, sql=%s, err=%d", ob_sql, err);
  }
  else
  {
    int row_num = array.get_row_num();
    assert(row_num <= 1);
    if (row_num == 0)
    {
      err = 0;
    }
    else
    {
      char* value = NULL;
      int type = 0;
      array.get_value(0, 0, value, type);
      assert(NULL != value);
      assert((int64_t) strlen(value) < rule_size);
      strcpy(rule_data, value);
    }
  }

  return err;
}

int PrefixInfo::set_row_num(uint64_t prefix, uint64_t row_num)
{
  int err = 0;
  assert(NULL != ob_client_);

  char ob_sql[2048];
  sprintf(ob_sql, "replace into %s(prefix, row_num) values(%lu, %lu)",
      PREFIX_STATUS_TABLE_NAME, prefix, row_num);
  err = ob_client_->exec_update(ob_sql);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to update ob, sql=%s, err=%d", ob_sql, err);
  }

  return err;
}

int PrefixInfo::get_row_num(uint64_t prefix, uint64_t& res_val)
{
  int err = 0;
  assert(NULL != ob_client_);

  char ob_sql[2048];
  sprintf(ob_sql, "select row_num from %s where prefix=%lu",
      PREFIX_STATUS_TABLE_NAME, prefix);
  Array array;
  err = ob_client_->exec_query(ob_sql, array);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to query ob, sql=%s, err=%d", ob_sql, err);
  }
  else
  {
    int row_num = array.get_row_num();
    assert(row_num <= 1);
    if (row_num == 0)
    {
      res_val = 0;
      err = 0;
    }
    else
    {
      char* value = NULL;
      int type = 0;
      array.get_value(0, 0, value, type);
      assert(NULL != value);
      res_val = atol(value);
    }
  }

  return err;
}

int PrefixInfo::get_max_prefix(uint64_t client_id, uint64_t& prefix)
{
  int err = 0;
  err = get_row_num((INVALID_PREFIX  >> (64 - PREFIX_BITS)) | (client_id << PREFIX_BITS), prefix);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to get max prefix, key=%lu, err=%d",
        INVALID_PREFIX, err);
  }

  //TBSYS_LOG(INFO, "[get_max_prefix] prefix=%lu, err=%d", prefix, err);
  return err;
}

int PrefixInfo::set_max_prefix(uint64_t client_id, uint64_t prefix)
{
  int err = 0;
  err = set_row_num((INVALID_PREFIX >> (64 - PREFIX_BITS))| (client_id << PREFIX_BITS), prefix);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to set max prefix, key=%lu, err=%d",
        INVALID_PREFIX, err);
  }
  //TBSYS_LOG(INFO, "[set_max_prefix] prefix=%lu, err=%d", prefix, err);
  return err;
}

