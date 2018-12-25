/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_schema_manager.h,v 0.1 2010/09/17 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_RPC_PROXY_H_
#define OB_MERGER_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_server.h"
#include "common/ob_ups_info.h"
#include "common/ob_transaction.h"
#include "sql/ob_physical_plan.h"
#include "sql/ob_ups_result.h"

namespace oceanbase
{
  namespace common
  {
    class ObMutator;
    class ObScanParam;
    class ObGeneralRpcStub;
    class ObClientConfig;
    class ObiRole;
  }

  namespace mergeserver
  {
    // this class is the outer interface class based on rpc stub class,
    // which is the real network interactor,
    // it encapsulates all interface for data getting operation,
    // that sometime need rpc calling to get the right version data.
    // if u want to add a new interactive interface,
    // please at first add the real detail at rpc stub class
    class ObMergerRpcProxy
    {
    public:
      //
      ObMergerRpcProxy(const common::ObServerType type = common::MERGE_SERVER);
      // construct func
      // param  @retry_times rpc call retry times when error occured
      //        @timeout every rpc call timeout
      //        @root_server root server addr for some interface
      //        @mege_server merge server local host addr for input
      ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
          const common::ObServer & root_server, const common::ObServer & merge_server,
          const common::ObServerType type = common::MERGE_SERVER);

      virtual ~ObMergerRpcProxy();

    public:
      // param  @rpc_buff rpc send and response buff
      //        @rpc_frame client manger for network interaction
      int init(common::ObGeneralRpcStub *rpc_stub);

      // set retry times and timeout
      int set_rpc_param(const int64_t retry_times, const int64_t timeout);

      // set min fetch update server list interval
      void set_min_interval(const int64_t interval);

      // fetch update server list
      int fetch_update_server_list(int32_t & count);

      // some get func as temperary interface
      const common::ObGeneralRpcStub *get_rpc_stub() const
      {
        return rpc_stub_;
      }
      int64_t get_rpc_timeout(void) const
      {
        return rpc_timeout_;
      }

      const common::ObServer & get_root_server(void) const
      {
        return root_server_;
      }
      void set_root_server(const common::ObServer & server)
      {
        root_server_ = server;
      }
    public:
      int get_bloom_filter(const common::ObServer &server, const common::ObNewRange &range, const int64_t tablet_version, const int64_t bf_version, ObString &bf_buffer) const;
      int get_ups_log_seq(const common::ObServer &ups, const int64_t timeout, int64_t & log_seq);
    public:
      // retry interval time
      static const int64_t RETRY_INTERVAL_TIME = 20; // 20 ms usleep

      // least fetch schema time interval
      static const int64_t LEAST_FETCH_SCHEMA_INTERVAL = 1000 * 1000; // 1s

      // mutate to update server and fetch return data
      // param  @mutate_param mutate param
      // param  @has_data has data for scanner deserialize
      //        @scanner for return value
      virtual int ups_mutate(const common::ObMutator & mutate_param, const bool has_data,
          common::ObScanner & scanner);


      // execute the plan on ups
      int ups_plan_execute(int64_t timeout, const sql::ObPhysicalPlan &plan, sql::ObUpsResult &result);
      int ups_start_trans(const common::ObTransReq &req, common::ObTransID &trans_id);
      int ups_end_trans(const common::ObEndTransReq &req);

      // fetch new frozen master update server version
      int get_last_frozen_memtable_version(int64_t & frozen_version);

      // get master update server
      int get_master_ups_old(const bool renew, common::ObServer & server);
      // WARNING: be cautious of this interface
      // set master update server if using vip for protocol compatible for cs merge
      void set_master_ups(const common::ObServer & server);

      // get master update server
      // may fail if some server not up. the proxy user should retry
      int get_master_ups(const bool renew, common::ObServer & server);

      // kill session/query on mergeserver
      int kill_session(const uint32_t ip, const int32_t session_id, const bool is_query);

      // output scanner result for debug
      static void output(common::ObScanner & result);

    protected:
      // check inner stat
      bool check_inner_stat(void) const;

    private:
      // check scan param
      // param  @scan_param scan parameter
      static bool check_scan_param(const common::ObScanParam & scan_param);

      // check range param
      // param  @range_param range parameter
      static bool check_range_param(const common::ObNewRange & range_param);

    private:
      // find master update server from server list
      void find_master_ups(const common::ObUpsList & list, common::ObServer & master);

      // check and modify update server list
      void modify_ups_list(common::ObUpsList & list);

      // check the server is ok to select for read
      // not in blacklist and read percentage is gt 0
      bool check_server(const int32_t index);

      // get rootserver of master master cluster
      int get_master_obi_rs(const common::ObServer &rootserver, common::ObServer &master_obi_rs);

      int get_inst_master_ups(const common::ObServer &root_server, common::ObServer &ups_master);

      inline bool check_need_retry_ups(const int rc);
    private:
      /// max len
      static const int64_t MAX_RANGE_LEN = 128;
      static const int64_t MAX_ROWKEY_LEN = 8;
    private:
      bool init_;                                   // rpc proxy init stat
      int64_t rpc_timeout_;                         // rpc call timeout
      int64_t rpc_retry_times_;                     // rpc retry times
      char max_range_[MAX_RANGE_LEN];               // for debug print range string
      common::ObServer root_server_;                // root server addr
      common::ObServer merge_server_;               // merge server addr
      // update server list
      int64_t min_fetch_interval_;                  // min fetch update server interval
      int64_t fetch_ups_timestamp_;                 // last fetch update server timestamp
      common::ObServer master_update_server_;       // update server vip addr
      tbsys::CRWLock ups_list_lock_;                // lock for update server list
      tbsys::CThreadMutex update_lock_;             // lock for fetch update server info
       // master master update server
      int64_t min_fetch_mm_ups_interval_;           // min fetch update server interval
      int64_t max_fetch_mm_ups_interval_;           // min fetch update server interval
      int64_t fetch_mm_ups_timestamp_;              // last fetch update server timestamp
      common::ObServer master_master_ups_;          // update server vip addr
      tbsys::CRWLock mm_ups_list_lock_;             // lock for update server list
      tbsys::CThreadMutex mm_update_lock_;          // lock for fetch update server info
      int64_t call_get_master_ups_times_;           // prevent redudant update server list
      common::ObServerType server_type_;            // server type for different load balance
      uint64_t cur_finger_print_;                   // server list finger print
      common::ObUpsList update_server_list_;        // update server list for read
      const common::ObGeneralRpcStub *rpc_stub_;    // rpc stub bottom module
    };

    inline bool ObMergerRpcProxy::check_need_retry_ups(const int rc)
    {
      bool need_retry = false;
      //mod wangdonghui [ups_replication_cannot_server_time] 20170425 :b
      //if (OB_NOT_MASTER == rc)
      if (OB_NOT_MASTER == rc || OB_RESPONSE_TIME_OUT == rc)
      //mod :e
      {
        need_retry = true;
      }
      return need_retry;
    }
  }
}



#endif // OB_MERGER_RPC_PROXY_H_
