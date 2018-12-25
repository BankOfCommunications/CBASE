/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_producer.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_CLIENT_PRODUCER_H_
#define LOAD_CLIENT_PRODUCER_H_

#include "tbsys.h"
#include "load_param.h"
#include "common/ob_server.h"
#include "common/ob_fifo_stream.h"
#include "load_producer.h"

namespace oceanbase
{
  namespace tools
  {
    struct QueryInfo;
    class LoadRpc;
    class LoadFilter;
    class LoadManager;
    // from rpc server for get request 
    class LoadClientProducer:public LoadProducer 
    {
    public:
      LoadClientProducer();
      virtual ~LoadClientProducer();
    public:
      void init(const SourceParam & param, LoadRpc * rpc, LoadFilter * filter, LoadManager * manager);
      virtual void run(tbsys::CThread* thread, void* arg);
    private:
      static const int64_t TIMEOUT = 1000000L;
      int get_request(const common::ObServer & server, const int64_t count);
      int push_request(const QueryInfo & query);
    private:
      int64_t last_time_;
      int64_t std_time_;
      LoadRpc * rpc_;
    };
  }
}

#endif // LOAD_CLIENT_PRODUCER_H_
