/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ups_rpc_proxy.h,v 0.1 2011/06/30 11:10:55 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_UPDATESERVER_OB_UPS_RPC_PROXY_H__
#define __OCEANBASE_UPDATESERVER_OB_UPS_RPC_PROXY_H__

#include "common/ob_schema_manager.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_cache.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsMergerRpcProxy : public mergeserver::ObMergerRpcProxy
    {
      public:
        ObUpsMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
            const common::ObServer& root_server, const common::ObServer& merge_server,
            ObUpsTableMgr& table_mgr, ObUpsCache& ups_cache);

        ~ObUpsMergerRpcProxy();

        int init(mergeserver::ObMergerRpcStub* rpc_stub, common::ObMergerSchemaManager* schema,
            mergeserver::ObMergerLocationCacheProxy* cache);

      public:
        virtual int cs_get(const common::ObGetParam& get_param, mergeserver::ObMergerTabletLocation& addr,
            common::ObScanner& scanner, common::ObIterator*& it_out);
        virtual int ups_get(const mergeserver::ObMergerTabletLocation& addr,
            const common::ObGetParam& get_param, common::ObScanner& scanner);

      private:
        static const int64_t DEFAULT_TIMEOUT = 10 * 1000L * 1000L; // 10s

      private:
        ObUpsTableMgr& table_mgr_;
        ObUpsCache& ups_cache_;
    };
  }
}


#endif //__OB_UPS_RPC_PROXY_H__

