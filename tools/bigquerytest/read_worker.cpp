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
  key_gen_ = NULL;
}

ReadWorker::~ReadWorker()
{
  // empty
}

int ReadWorker::init(KeyGenerator& key_gen, MysqlClient& ob_client, PrefixInfo& prefix_info)
{
  key_gen_ = &key_gen;

  int err = reader_.init(ob_client, prefix_info);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to init bigquery reader, err=%d", err);
  }
  return err;
}

void ReadWorker::run(CThread* thread, void* arg)
{
  int err = 0;
  uint64_t prefix = 0;

  if (NULL != key_gen_)
  {
    TBSYS_LOG(INFO, "start read worker");
    while (!_stop && 0 == err)
    {
      err = key_gen_->get_next_key(prefix, true);
      if (OB_NEED_RETRY == err)
      {
        err = 0;
        sleep(1);
      }
      else
      {
        err = reader_.read_data(prefix);
        if (0 != err)
        {
          TBSYS_LOG(ERROR, "failed to read data, prefix=%lu, err=%d", prefix, err);
        }
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "key_gen_ is NULL");
  }
}

