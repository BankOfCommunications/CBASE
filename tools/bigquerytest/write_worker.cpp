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
  key_gen_ = NULL;
}

WriteWorker::~WriteWorker()
{
}

int WriteWorker::init(KeyGenerator& key_gen, MysqlClient& ob_client, PrefixInfo& prefix_info)
{
  key_gen_ = &key_gen;
  int err = writer_.init(ob_client, prefix_info);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to init writer, err=%d", err);
  }
  return err;
}

void WriteWorker::run(CThread* thread, void* arg)
{
  int err = 0;
  uint64_t prefix = 0;

  if (NULL != key_gen_)
  {
    TBSYS_LOG(INFO, "start write worker");
    while (!_stop && 0 == err)
    {
      key_gen_->get_next_key(prefix, false);
      err = writer_.write_data(prefix);
      if (0 != err)
      {
        TBSYS_LOG(ERROR, "failed to write data, prefix=%lu, err=%d", prefix, err);
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "key_gen_ is NULL");
  }
}

