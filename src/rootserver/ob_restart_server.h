/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_restart_server.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_RESTART_SERVER_H
#define _OB_RESTART_SERVER_H

#include "ob_chunk_server_manager.h"
#include "ob_root_table2.h"
#include "ob_root_log_worker.h"
#include "ob_root_rpc_stub.h"
#include "common/ob_server.h"
#include "common/ob_define.h"
#include "ob_restart_server.h"
#include "ob_root_server_config.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootTable2;
    class ObRestartServer
    {
    public:
      ObRestartServer();
      int restart_servers();

      inline void set_root_config(ObRootServerConfig* config)
      {
        config_ = config;
      }
      inline void set_server_manager(ObChunkServerManager* server_manager)
      {
        server_manager_ = server_manager;
      }
      inline void set_root_table2(ObRootTable2* root_table)
      {
        root_table_ = root_table;
      }
      inline void set_root_rpc_stub(ObRootRpcStub* rpc_stub)
      {
        rpc_stub_ = rpc_stub;
      }
      inline void set_root_log_worker(ObRootLogWorker* log_worker)
      {
        log_worker_ = log_worker;
      }
      inline void set_root_table_build_mutex(tbsys::CThreadMutex* root_table_build_mutex)
      {
        root_table_build_mutex_ = root_table_build_mutex;
      }
      inline void set_server_manager_rwlock(tbsys::CRWLock* server_manager_rwlock)
      {
        server_manager_rwlock_ = server_manager_rwlock;
      }
      inline void set_root_table_rwlock(tbsys::CRWLock* root_table_rwlock)
      {
        root_table_rwlock_ = root_table_rwlock;
      }

    private:
      void shutdown_cs(ObServerStatus* it);
      bool check_inner_stat();
      void find_can_restart_server(int32_t expected_replicas_num);

    private:
      ObRootServerConfig* config_;
      ObChunkServerManager* server_manager_;
      ObRootTable2* root_table_;
      ObRootRpcStub* rpc_stub_;
      ObRootLogWorker* log_worker_;
      tbsys::CThreadMutex* root_table_build_mutex_;
      tbsys::CRWLock* server_manager_rwlock_;
      tbsys::CRWLock* root_table_rwlock_;
    };
  }
}

#endif /* _OB_RESTART_SERVER_H */


