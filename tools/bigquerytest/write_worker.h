/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: write_worker.h,v 0.1 2012/02/27 10:52:28 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_WRITE_WORKER_H__
#define __OCEANBASE_WRITE_WORKER_H__

#include <tbsys.h>
#include "key_generator.h"
#include "bigquery_writer.h"

class WriteWorker : public tbsys::CDefaultRunnable
{
  public:
    WriteWorker();
    ~WriteWorker();

    int init(KeyGenerator& key_gen, MysqlClient& ob_client, PrefixInfo& prefix_info);

  public:
    virtual void run(tbsys::CThread* thread, void* arg);

  private:
    KeyGenerator* key_gen_;
    BigqueryWriter writer_;
};


#endif //__WRITE_WORKER_H__

