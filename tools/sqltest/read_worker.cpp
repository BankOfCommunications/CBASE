/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: read_worker.cpp,v 0.1 2012/02/27 11:21:53 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "read_worker.h"

ReadWorker::ReadWorker()
{
  sql_builder_ = NULL;
  key_gen_ = NULL;
  sqltest_dao_ = NULL;
}

ReadWorker::~ReadWorker()
{
}

int ReadWorker::init(SqlBuilder& sql_builder, KeyGenerator& key_gen, SqlTestDao& sqltest_dao, SqlTestPattern& sqltest_pattern)
{
  assert(sql_builder_ == NULL && key_gen_ == NULL && sqltest_dao_ == NULL && sqltest_pattern_ == NULL);
  sql_builder_ = &sql_builder;
  key_gen_ = &key_gen;
  sqltest_dao_ = &sqltest_dao;
  sqltest_pattern_ = &sqltest_pattern;
  return 0;
}

void ReadWorker::run(CThread* thread, void* arg)
{
  int err = 0;
  uint64_t prefix = 0;
  uint64_t suffix = 0;
  static const int BUF_SIZE = 2048;
  char sql_buf[BUF_SIZE];
  char pattern[BUF_SIZE];

  if (NULL != key_gen_ && NULL != sql_builder_ && NULL != sqltest_dao_)
  {
    while (!_stop && 0 == err)
    {
      key_gen_->get_next_key(prefix, suffix, true);
      //sql_builder_->set_keys(prefix, suffix);
      uint64_t keys[2] = {prefix, suffix};
      sqltest_pattern_->get_next_pattern(pattern);
      err = sql_builder_->generate_read_sql(pattern, sql_buf, sizeof(sql_buf), sizeof(keys), keys);
      if (0 != err)
      {
        TBSYS_LOG(ERROR, "failed to generate read sql, err=%d", err);
      }
      else
      {
        err = sqltest_dao_->exec_query(sql_buf, sql_buf, prefix);
        if (0 != err)
        {
          TBSYS_LOG(ERROR, "failed to exec query, obsql=%s, mysql=%s, prefix=%lu, err=%d",
              sql_buf, sql_buf, prefix, err);
        }
      }
    }
  }
}

