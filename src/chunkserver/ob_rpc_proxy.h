/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_rpc_proxy.h for rpc among chunk server, update server and
 * root server.
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_RPC_PROXY_H_
#define OCEANBASE_CHUNKSERVER_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_range2.h"
#include "common/ob_server.h"
#include "common/ob_iterator.h"
#include "ob_read_ups_balance.h"
#include "ob_ups_blacklist.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "common/ob_general_rpc_stub.h"
#include "ob_sql_rpc_stub.h"
//add zhaoqiong [Schema Manager] 20150327:b
#include "common/ob_schema_service_impl.h"
//add:e

namespace oceanbase
{
  namespace common
  {
    class ObGetParam;
    class ObScanner;
    class ObNewRange;
    class ObScanParam;
    class ObSchemaManagerV2;
    class ColumnFilter;
    class ObMergerSchemaManager;
    class ObGeneralRpcStub;
  }

  namespace chunkserver
  {

    // this class is the outer interface class based on rpc stub class,
    // which is the real network interactor,
    // it encapsulates all interface for data getting operation,
    // that sometime need rpc calling to get the right version data.
    // if u want to add a new interactive interface,
    // please at first add the real detail at rpc stub class
    class ObMergerRpcProxy : public ObSqlUpsRpcProxy
    {
    public:
      //
      ObMergerRpcProxy();
      // construct func
      // param  @retry_times rpc call retry times when error occured
      //        @timeout every rpc call timeout
      //        @root_server root server addr for some interface
      //        @update_server update server addr for some interface
      ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
          const common::ObServer & root_server);

      virtual ~ObMergerRpcProxy();

    public:
      // param  @rpc_buff rpc send and response buff
      //        @rpc_frame client manger for network interaction
      int init(common::ObGeneralRpcStub * rpc_stub, ObSqlRpcStub * sql_rpc_stub, common::ObMergerSchemaManager * schema
               //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
               , const int32_t cluster_id
               //add:e
               );
      //add zhaoqiong [Schema Manager] 20150327:b
      int set_schema_service(common::ObSchemaServiceImpl* schema_service);
      common::ObSchemaServiceImpl* get_schema_service(){return schema_service_;}
      //add:e
      // set retry times and timeout
      int set_rpc_param(const int64_t retry_times, const int64_t timeout);

      // set black list params
      int set_blacklist_param(const int64_t timeout, const int64_t fail_count);

      static const int64_t LOCAL_NEWEST = 0;    // local cation newest version

      // get the scheam data according to the timetamp, in some cases depend on the timestamp value
      // if timestamp is LOCAL_NEWEST, it meanse only get the local latest version
      // otherwise, it means that at first find in the local versions, if not exist then need rpc
      // waring: after using manager, u must release this schema version for washout
      // param  @timeout every rpc call timeout
      //        @manager the real schema pointer returned
      int get_schema(const uint64_t table_id, const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

      //      virtual int check_incremental_data_range(
      //          int64_t table_id, ObVersionRange &version, ObVersionRange &new_range); /*add zhaoqiong [Truncate Table]:20160318*/
            virtual int check_incremental_data_range(
                int64_t table_id, ObVersionRange &version, bool & is_truncated); /*add zhaoqiong [Truncate Table]:20170519*/

      // fetch new schema if find new version
      int fetch_schema_version(int64_t & timestamp);

      // fetch update server list
      int fetch_update_server_list(int32_t & count);

      // waring: release schema after using for dec the manager reference count
      //        @manager the real schema pointer returned
      int release_schema(const common::ObSchemaManagerV2 * manager);

      // get master update server
      int get_update_server(const bool renew, common::ObServer & server, bool need_master = true);

      int get_master_master_update_server(const bool renew, ObServer & master_master_ups);

      // some get func as temperary interface
      const common::ObGeneralRpcStub * get_rpc_stub() const
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
      //add pangtianze [Paxos rs_election] 20150716:b
      void set_root_server(const common::ObServer &server)
      {
        root_server_ = server;
      }
      //add:e

      //add:e
    public:
      // retry interval time
      static const int64_t RETRY_INTERVAL_TIME = 20000; // 20 ms usleep

      // least fetch schema time interval
      static const int64_t LEAST_FETCH_SCHEMA_INTERVAL = 1000 * 1000; // 1s

      // get data from one chunk server, which is positioned according to the param
      // param  @get_param get param
      //        @list tablet location list as output
      //        @scanner return result
      virtual int cs_get(const common::ObGetParam & get_param,
                         common::ObScanner & scanner, common::ObIterator *it_out[],int64_t& it_size);

      // scan data from one chunk server, which is positioned according to the param
      // the outer class cellstream can give the whole data according to scan range param
      // param  @scan_param scan param
      //        @list tablet location list as output
      //        @scanner return result

      virtual int cs_scan(const common::ObScanParam & scan_param,
                          common::ObScanner & scanner, common::ObIterator *it_out[],
                          int64_t& it_size);


      // lock and check whether need fetch new schema
      // param  @timestamp new schema timestamp
      //        @manager the new schema pointer
      int fetch_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

      // get frozen time from update server
      // param  @frozen_version frozen version to query
      //        @frozen_time returned forzen time
      int get_frozen_time(const int64_t frozen_version, int64_t& forzen_time);

      // get frozen schema from update server
      // param  @frozen_version frozen version to query
      //        @schema returned forzen schema
      int get_frozen_schema(const int64_t frozen_version, common::ObSchemaManagerV2& schema);

      // output scanner result for debug
      static void output(common::ObScanner & result);

      // output new scan result for debug, not implement yet
      static void output(common::ObNewScanner & result);

      // get data from update server
      // param  @get_param get param
      //        @scanner return result
      virtual int ups_get(const common::ObGetParam & get_param,
                          common::ObScanner & scanner,
                          const common::ObServerType server_type = common::MERGE_SERVER,
                          const int64_t time_out = 0);

      // scan data from update server
      // param  @scan_param get param
      //        @scanner return result
      virtual int ups_scan(const common::ObScanParam & scan_param,
                           common::ObScanner & scanner,
                           const common::ObServerType server_type = common::MERGE_SERVER,
                           const int64_t time_out = 0);

      //add wenghaixing [secondary index static_index_build.cs_scan]20150324
      virtual int cs_cs_scan(const common::ObScanParam & scan_param,
                             common::ObServer chunkserver,
                             common::ObScanner & scanner,
                             const common::ObServerType server_type = common::MERGE_SERVER,
                             const int64_t time_out = 0);
      //add e

      // get data from update server
      // param  @get_param get param
      //        @scanner return result
      virtual int sql_ups_get(const common::ObGetParam & get_param,
                          common::ObNewScanner & scanner,
                          const int64_t time_out = 0);

      // scan data from update server
      // param  @scan_param get param
      //        @scanner return result
      virtual int sql_ups_scan(const common::ObScanParam & scan_param,
                           common::ObNewScanner & scanner,
                           const int64_t time_out = 0);

    private:
      // get data from update server
      // param  @get_param get param
      //        @scanner return result
      template<class T, class RpcT>
      int ups_get_(RpcT *rpc_stub,
                          const common::ObGetParam & get_param,
                          T & scanner,
                          const common::ObServerType server_type = common::MERGE_SERVER,
                          const int64_t time_out = 0);

      // scan data from update server
      // param  @scan_param get param
      //        @scanner return result
      template<class T, class RpcT>
      int ups_scan_(RpcT *rpc_stub,
                           const common::ObScanParam & scan_param,
                           T & scanner,
                           const common::ObServerType server_type = common::MERGE_SERVER,
                           const int64_t time_out = 0);

      //add wenghaixing [secondary index static_index_build.cs_scan]20150324
      template<class T, class RpcT>
      int cs_scan_(RpcT *rpc_stub,
                   const common::ObScanParam & scan_param,
                   const common::ObServer chunkserver,
                   T & scanner,
                   const common::ObServerType server_type = common::MERGE_SERVER,
                   const int64_t time_out = 0);
      template<class T, class RpcT>
      int cs_cs_scan_(RpcT *rpc_stub,
                          const common::ObScanParam & scan_param,
                          const common::ObServer chunkserver,
                          T & scanner,
                          const int64_t time_out = 0);
      //add e

      // get data from master update server
      // param  @get_param get param
      //        @scanner return result
      template<class T, class RpcT>
      int master_ups_get(RpcT *rpc_stub,
                         const common::ObGetParam & get_param,
                         T & scanner,
                         const int64_t time_out = 0);

      // get data from update server list
      // param  @get_param get param
      //        @scanner return result
      template<class T, class RpcT>
      int slave_ups_get(RpcT *rpc_stub,
                        const common::ObGetParam & get_param,
                        T & scanner,
                        const common::ObServerType server_type,
                        const int64_t time_out = 0);

      // scan data from master update server
      // param  @get_param get param
      //        @scanner return result
      template<class T, class RpcT>
      int master_ups_scan(RpcT *rpc_stub,
                          const common::ObScanParam & scan_param,
                          T & scanner,
                          const int64_t time_out = 0);

      // scan data from update server list
      // param  @get_param get param
      //        @scanner return result
      template<class T, class RpcT>
      int slave_ups_scan(RpcT *rpc_stub,
                         const common::ObScanParam & scan_param,
                         T & scanner,
                         const common::ObServerType server_type,
                         const int64_t time_out = 0);

      // find master update server from server list
      void update_ups_info(const common::ObUpsList & list);

      // check and modify update server list
      void modify_ups_list(common::ObUpsList & list);

      // check the server is ok to select for read
      // not in blacklist and read percentage is gt 0
      bool check_server(const int32_t index, const common::ObServerType server_type);

      // for CS consistency read
      int get_inst_master_ups(const common::ObServer &root_server, common::ObServer &ups_master);
      int get_master_obi_rs(const common::ObServer &rootserver, common::ObServer &master_obi_rs);

    private:
      // check inner stat
      bool check_inner_stat(void) const;

      // check scan param
      // param  @scan_param scan parameter
      static bool check_scan_param(const common::ObScanParam & scan_param);

      // check range param
      // param  @range_param range parameter
      static bool check_range_param(const common::ObNewRange & range_param);

      // get new schema through root server rpc call
      // param  @timestamp old schema timestamp
      //        @manager the new schema pointer
      int get_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

      // check if need retry ups when got a error code
      inline bool check_need_retry_ups(const int rc);

      //add wenghaixing [secondary index static_index_build.cs_scan]20150324
      inline bool check_need_retry_cs(const int rc);
      //add e

      /// max len
      static const int64_t MAX_RANGE_LEN = 128;
      static const int64_t MAX_ROWKEY_LEN = 8;

    private:
      bool init_;                                   // rpc proxy init stat
      int64_t rpc_timeout_;                         // rpc call timeout
      int64_t rpc_retry_times_;                     // rpc retry times
      char max_range_[MAX_RANGE_LEN];               // for debug print range string
      common::ObServer root_server_;                // root server addr

      // update server
      int64_t min_fetch_interval_;                  // min fetch update server interval
      int64_t fetch_ups_timestamp_;                 // last fetch update server timestamp
      common::ObServer update_server_;              // update server addr
      common::ObServer inconsistency_update_server_;// inconsistency update server
      tbsys::CThreadMutex update_lock_;             // lock for fetch update server info
      const common::ObGeneralRpcStub * rpc_stub_;            // rpc stub bottom module
      const ObSqlRpcStub * sql_rpc_stub_;
      common::ObMergerSchemaManager * schema_manager_;      // merge server schema cache
      int64_t fetch_schema_timestamp_;              // last fetch schema from root timestamp
      tbsys::CThreadMutex schema_lock_;             // lock for update schema manager

      // update server list
      tbsys::CRWLock ups_list_lock_;                // lock for update server list
      uint64_t cur_finger_print_;                   // server list finger print
      int64_t fail_count_threshold_;                // pull to black list threshold times
      int64_t black_list_timeout_;                  // black list timeout for alive
      ObUpsBlackList black_list_;                   // black list of update server
      ObUpsBlackList ups_black_list_for_merge_;     // black list of update server
      common::ObUpsList update_server_list_;        // update server list for read
      // get master master ups for consistency read
      int64_t fetch_mm_ups_timestamp_;              // last fetch update server timestamp
      tbsys::CRWLock mm_ups_list_lock_;             // lock for update server list
      tbsys::CThreadMutex mm_update_lock_;          // lock for fetch update server info
      common::ObServer master_master_ups_;          // update server vip addr
      //add zhaoqiong [Schema Manager] 20150327:b
      oceanbase::common::ObSchemaServiceImpl* schema_service_;
      //add:e
      //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      int32_t cluster_id_;
      //add:e
    };

    inline bool ObMergerRpcProxy::check_need_retry_ups(const int rc)
    {
      bool need_retry = false;
      if (OB_NOT_MASTER == rc || OB_RESPONSE_TIME_OUT == rc)
      {
        need_retry = true;
      }
      return need_retry;
    }

    //add wenghaixing [secondary index static_index_build.cs_scan]20150324
    inline bool ObMergerRpcProxy::check_need_retry_cs(const int rc)
    {
      bool need_retry = false;
      if(OB_RESPONSE_TIME_OUT == rc)
      {
        need_retry =true;
      }
      return need_retry;
    }
    //add e
  }
}

#endif // OCEANBASE_CHUNKSERVER_RPC_PROXY_H_
