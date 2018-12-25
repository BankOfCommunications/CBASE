/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_service_ms_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SCHEMA_SERVICE_MS_PROVIDER_H
#define _OB_SCHEMA_SERVICE_MS_PROVIDER_H 1

#include "common/roottable/ob_ms_provider.h"
#include "ob_chunk_server_manager.h"

namespace oceanbase
{
  namespace rootserver
  {
    // thread-safe ms provider for schema_service
    class ObSchemaServiceMsProvider: public common::ObMsProvider
    {
      public:
        ObSchemaServiceMsProvider(const ObChunkServerManager &server_manager);
        virtual ~ObSchemaServiceMsProvider();
        int get_ms(const common::ObScanParam &scan_param, const int64_t retry_num, common::ObServer &ms);
        //add pangtianze [Paxos] 20170420:b
        int reset_when_master_swtich();
        int reset(const std::vector<ObServer> &ms_list);
        //add:e
      private:
        //add pangtianze [Paxos] 20170420:b
        void reset_ms_retry();
        //add:e
        bool did_need_reset();
        int reset();
        void update_ms_retry(const common::ObServer &ms);
        int get_ms(common::ObServer &ms)
        {
          UNUSED(ms);
          return common::OB_NOT_IMPLEMENT;
        }
      private:
        static const int64_t MAX_SERVER_COUNT = common::OB_TABLET_MAX_REPLICA_COUNT;
        static const int64_t MAX_MS_RETRY = 3;
        struct MsAndRetry
        {
          common::ObServer ms_;
          int64_t retry_count_;
          MsAndRetry()
          {
            retry_count_ = MAX_MS_RETRY;
          }
        };
        DISALLOW_COPY_AND_ASSIGN(ObSchemaServiceMsProvider);
        // data members
        const ObChunkServerManager &server_manager_;
        MsAndRetry ms_carray_[MAX_SERVER_COUNT];
        int64_t count_;
        tbsys::CRWLock rwlock_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_SCHEMA_SERVICE_MS_PROVIDER_H */

