/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_runner.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_RUNNER_H_
#define LOAD_RUNNER_H_

#include "load_rpc.h"
#include "load_define.h"
#include "load_client.h"
#include "load_watcher.h"
#include "load_filter.h"
#include "load_producer.h"
#include "load_consumer.h"
#include "load_manager.h"

namespace oceanbase
{
  namespace tools
  {
    struct MeterParam;
    class LoadRunner
    {
    public:
      LoadRunner();
      virtual ~LoadRunner();
    public:
      int start(const MeterParam & param, const bool read_only = false);
      void wait();
      void stop();
    private:
      LoadManager manager_;
      LoadWatcher watcher_;
      int32_t producer_count_;
      LoadFilter filter_;
      LoadProducer producer_;
      int32_t consumer_count_;
      LoadConsumer consumer_;
    };
  }
}

#endif // LOAD_RUNNER_H_

