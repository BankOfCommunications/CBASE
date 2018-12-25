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

#ifndef LOAD_PRODUCER_H_
#define LOAD_PRODUCER_H_

#include "tbsys.h"
#include "load_param.h"
#include "common/ob_fifo_stream.h"

namespace oceanbase
{
  namespace tools
  {
    struct QueryInfo;
    class LoadFilter;
    class LoadManager;
    class LoadProducer:public tbsys::CDefaultRunnable
    {
    public:
      LoadProducer();
      LoadProducer(const SourceParam & param, LoadFilter * filter, LoadManager * manager);
      virtual ~LoadProducer();
    public:
      virtual void run(tbsys::CThread* thread, void* arg);
    public:
      void init(const SourceParam & param, LoadFilter * filter, LoadManager * manager, const bool read_only);
      static void destroy_request(QueryInfo & query);
      int load_file(const char * file, const common::ObServer & server, int64_t & count);
    protected:
      int read_request(FILE * file, common::FIFOPacket & header, char buffer[],
          const uint32_t max_len, uint32_t & data_len);
      int construct_request(const common::FIFOPacket & header, const char buffer[],
          const int64_t data_len, QueryInfo & query);
    protected:
      LoadFilter * filter_;
      SourceParam param_;
      LoadManager * manager_;
      bool read_only_;
    };
  }
}

#endif // LOAD_PRODUCER_H_
