/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_FETCH_LSYNC_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_FETCH_LSYNC_H_

#include "tbsys.h"
#include "common/ob_server.h"
#include "ob_ups_rpc_stub.h"
//#include "mock_ups_rpc_stub.h"
#include "ob_commit_log_receiver.h"

namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
    {
      //forward decleration
      class TestObUpsFetchLsync_test_init_Test;
    }
  }
  namespace updateserver
  {
    class ObUpsFetchLsync : public tbsys::CDefaultRunnable
    {
      friend class tests::updateserver::TestObUpsFetchLsync_test_init_Test;
      static const int64_t TIMEOUT_DELTA = 10000;
    public:
      ObUpsFetchLsync();
      virtual ~ObUpsFetchLsync();
    public:
      /// @brief 初始化
      /// @param [in] lsync_server Master地址
      /// @param [in] log_id 起始日志ID
      /// @param [in] log_seq 起始日志序号
      /// @param [in] rpc_stub RPC类
      virtual int init(const common::ObServer &lsync_server, const uint64_t log_id,
                       const uint64_t log_seq, //ObUpsRpcStub *rpc_stub,
                       ObUpsRpcStub *rpc_stub,
                       ObCommitLogReceiver *clog_receiver,
                       const int64_t fetch_timeout, common::ObRoleMgr *role_mgr);

      virtual void run(tbsys::CThread* thread, void* arg);

      inline uint64_t get_log_id() {return log_id_;}
      inline uint64_t get_log_seq() {return log_seq_;}

      void set_lsync_server(const common::ObServer &lsync_server)
      {
        server_lock_.wrlock();
        lsync_server_ = lsync_server;
        server_lock_.unlock();
      };

      common::ObServer get_lsync_server() const
      {
        common::ObServer ret;
        server_lock_.rdlock();
        ret = lsync_server_;
        server_lock_.unlock();
        return ret;
      };

    protected:
      //ObUpsRpcStub *rpc_stub_;
      ObUpsRpcStub *rpc_stub_;
      ObCommitLogReceiver *clog_receiver_;
      common::ObRoleMgr *role_mgr_;
      uint64_t log_id_;
      uint64_t log_seq_;
      int64_t fetch_timeout_;
      common::ObServer lsync_server_;
      mutable common::SpinRWLock server_lock_;
      bool is_initialized_;
    };
  } // end namespace updateserver
} // end namespace oceanbase


#endif // OCEANBASE_UPDATESERVER_OB_UPS_FETCH_LSYNC_H_

