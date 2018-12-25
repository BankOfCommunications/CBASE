/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: write_worker.cpp,v 0.1 2012/02/27 10:55:04 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "write_worker.h"

WriteWorker::WriteWorker()
{
  sql_builder_ = NULL;
  key_gen_ = NULL;
  sqltest_dao_ = NULL;
}

WriteWorker::~WriteWorker()
{
}

int WriteWorker::init(SqlBuilder& sql_builder, KeyGenerator& key_gen, SqlTestDao& sqltest_dao)
{
  assert(sql_builder_ == NULL && key_gen_ == NULL && sqltest_dao_ == NULL);
  sql_builder_ = &sql_builder;
  key_gen_ = &key_gen;
  sqltest_dao_ = &sqltest_dao;
  return 0;
}

void WriteWorker::run(CThread* thread, void* arg)
{
  int err = 0;
  uint64_t prefix = 0;
  uint64_t suffix = 0;
  static const int BUF_SIZE = 2048;
  char obsql_buf[BUF_SIZE];
  char mysql_buf[BUF_SIZE];

  if (NULL != key_gen_ && NULL != sql_builder_ && NULL != sqltest_dao_)
  {
    while (!_stop && 0 == err)
    {
      key_gen_->get_next_key(prefix, suffix, false);
      //sql_builder_->set_keys(prefix, suffix);
      uint64_t keys[2] = {prefix, suffix};
      err = sql_builder_->generate_write_sql(obsql_buf, mysql_buf, sizeof(keys), keys);
      if (0 != err)
      {
        TBSYS_LOG(ERROR, "failed to generate write sql, err=%d", err);
      }
      else
      {
        err = sqltest_dao_->exec_update(obsql_buf, mysql_buf, prefix);
        if (0 != err)
        {
          TBSYS_LOG(ERROR, "failed to exec update, obsql=%s, mysql=%s, prefix=%lu, err=%d",
              obsql_buf, mysql_buf, prefix, err);
        }
      }
    }
  }
}

