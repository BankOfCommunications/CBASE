/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_ms_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_MS_PROVIDER_H
#define _OB_ROOT_MS_PROVIDER_H 1

#include "common/roottable/ob_ms_provider.h"
#include "ob_chunk_server_manager.h"
#include "ob_root_server_config.h"
#include "ob_root_rpc_stub.h"
namespace oceanbase
{
  namespace rootserver
  {
    // thread safe simple ronud-robin merge server provider
    class ObRootMsProvider:public common::ObMsProvider
    {
    public:
      ObRootMsProvider(ObChunkServerManager & server_manager);
      virtual ~ObRootMsProvider();
      void init(ObRootServerConfig & config, ObRootRpcStub & rpc_stub
                //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                , ObRootServer2 &root_server
                //add:e
                );
    public:
      int get_ms(common::ObServer & server);
      int get_ms(common::ObServer & server, const bool query_master_cluster);
      // not implement this abstract interface
      int get_ms(const common::ObScanParam & param, int64_t retry_num, common::ObServer& server)
      {
        UNUSED(param);
        UNUSED(retry_num);
        UNUSED(server);
        return common::OB_NOT_IMPLEMENT;
      }
    private:
      // get master cluster merge server
      int get_ms(const common::ObServer & master_rs, common::ObServer & server);
    private:
      bool init_;
      ObChunkServerManager & server_manager_;
      ObRootServerConfig *config_;
      ObRootRpcStub *rpc_stub_;
      //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
      ObRootServer2 *root_server_;
      //add:e
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_MS_PROVIDER_H */

