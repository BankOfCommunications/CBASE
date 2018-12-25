#ifndef OCEANBASE_MERGESERVER_SERVICE_H_
#define OCEANBASE_MERGESERVER_SERVICE_H_

#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_obi_role.h"
#include "common/thread_buffer.h"
#include "common/ob_session_mgr.h"
#include "common/nb_accessor/ob_nb_accessor.h"
#include "common/ob_config_manager.h"
#include "common/ob_version.h"
#include "ob_ms_schema_task.h"
#include "ob_ms_monitor_task.h"
#include "ob_ms_service_monitor.h"
#include "ob_ms_lease_task.h"
#include "ob_ms_ups_task.h"
#include "ob_ms_sql_proxy.h"
#include "ob_query_cache.h"
#include "easy_io_struct.h"
#include "ob_merge_server_config.h"
#include "common/ob_privilege_manager.h"
#include "common/ob_statistics.h"
#include "ob_get_privilege_task.h"
//add zhaoqiong [Schema Manager] 20150327:b
#include "common/ob_common_rpc_stub.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_schema_service_impl.h"
//add:e

namespace oceanbase
{
  namespace common
  {
    class ObMergerSchemaManager;
    class ObTabletLocationCache;
    class ObTabletLocationCacheProxy;
    class ObGeneralRpcStub;
  }
  namespace mergeserver
  {
    class ObMergeServer;
    class ObMergerRpcProxy;
    class ObMergerRootRpcProxy;
    class ObMergerAsyncRpcStub;
    class ObMergerSchemaProxy;
    static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
    //mod chujiajia [Paxos rs_election] 20151110:b
    //static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
    static const int64_t RETRY_INTERVAL_TIME = 100 * 1000; // usleep 100 ms
    //mod:e
    class ObMergeServerService: public ObVersionProvider
    {
      public:
        ObMergeServerService();
        ~ObMergeServerService();

      public:
        int initialize(ObMergeServer* merge_server);
        int start();
        int destroy();
        bool check_instance_role(const bool read_master) const;

      public:
        /// extend lease valid time = sys.cur_timestamp + delay
        void extend_lease(const int64_t delay);

        /// check lease expired
        bool check_lease(void) const;

        /// register to root server
        //mod pangtianze [Paxos rs_election] 20170228
        //int register_root_server(void);
        int register_root_server(bool is_first = false);
        //mod:e

        sql::ObSQLSessionMgr* get_sql_session_mgr() const;
        void set_sql_session_mgr(sql::ObSQLSessionMgr* mgr);
        void set_sql_id_mgr(sql::ObSQLIdMgr *mgr) {sql_id_mgr_ = mgr;};
        /* reload config after update local configuration succ */
        int reload_config();

        int do_request(
          const int64_t receive_time,
          const int32_t packet_code,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        //int get_agent(ObMergeJoinAgent *&agent);
        void handle_failed_request(const int64_t timeout, const int32_t packet_code);

        mergeserver::ObMergerRpcProxy  *get_rpc_proxy() const {return rpc_proxy_;}
        mergeserver::ObMergerRootRpcProxy * get_root_rpc() const {return root_rpc_;}
        mergeserver::ObMergerAsyncRpcStub   *get_async_rpc() const {return async_rpc_;}
        common::ObMergerSchemaManager *get_schema_mgr() const {return schema_mgr_;}
        common::ObTabletLocationCacheProxy *get_cache_proxy() const {return cache_proxy_;}
        common::ObStatManager *get_stat_manager() const { return service_monitor_; }

        const common::ObVersion get_frozen_version() const
        {
          return frozen_version_;
        }

        ObMergeServerConfig& get_config();
        const ObMergeServerConfig& get_config() const;
      private:
        // lease init 10s
        static const int64_t DEFAULT_LEASE_TIME = 10 * 1000 * 1000L;

        // warning: fetch schema interval can not be too long
        // because of the heartbeat handle will block tbnet thread
        static const int64_t FETCH_SCHEMA_INTERVAL = 30 * 1000;

        // list sessions
        int ms_list_sessions(
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer);
        // list sessions
        int ms_kill_session(
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer);
        // kill sql session
        int ms_sql_kill_session(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us
          );
        //add pangtianze [Paxos rs_switch] 20170208:b
        //force re-regist
        int ms_force_regist(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        //add:e
        //add pangtianze [Paxos rs_switch] 20170301:b
        int ms_refresh_rs_list(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        //add:e
        // heartbeat
        int ms_heartbeat(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // clear cache
        int ms_clear(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // monitor stat
        int ms_stat(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // reload conf
        int ms_reload_config(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // get query
        int ms_get(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // scan query
        int ms_scan(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        int ms_sql_scan(
          const int64_t start_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int ms_accept_schema(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        //add zhaoqiong [Schema Manager] 20150327:b
        int ms_accept_schema_mutator(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        //add:e

        int send_sql_response(
          easy_request_t* req,
          common::ObDataBuffer& out_buffer,
          ObSQLResultSet &result,
          int32_t channel_id,
          int64_t timeout_us);

        int ms_sql_execute(
          const int64_t start_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int do_timeouted_req(
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer);

        // mutate update
        int ms_mutate(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        // change log level
        int ms_change_log_level(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int ms_set_config(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int ms_get_config(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          easy_request_t* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

      private:
        ObConfigManager& get_config_mgr();

        // init server properties
        int init_ms_properties_();

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMergeServerService);
        ObMergeServer* merge_server_;
        bool inited_;
        // is registered or not
        volatile bool registered_;
        // lease timeout time
        int64_t lease_expired_time_;
        // instance role type
        common::ObiRole instance_role_;

      private:
        static const uint64_t MAX_INNER_TABLE_COUNT = 32;
        static const uint64_t MAX_ROOT_SERVER_ACCESS_COUNT = 32;
        static const int64_t REQUEST_EVENT_QUEUE_SIZE = 8192;
        //
        int64_t frozen_version_;
        ObMergerRpcProxy  *rpc_proxy_;
        oceanbase::common::ObGeneralRpcStub  *rpc_stub_;
        ObMsSQLProxy sql_proxy_;
        ObMergerAsyncRpcStub   *async_rpc_;
        common::ObMergerSchemaManager *schema_mgr_;
        ObMergerSchemaProxy *schema_proxy_;
        oceanbase::common::nb_accessor::ObNbAccessor *nb_accessor_;
        ObMergerRootRpcProxy * root_rpc_;
        ObMergerUpsTask fetch_ups_task_;
        ObMergerSchemaTask fetch_schema_task_;
        ObMergerLeaseTask check_lease_task_;
        ObMergerMonitorTask monitor_task_;
        ObTabletLocationCache *location_cache_;
        common::ObTabletLocationCacheProxy *cache_proxy_;
        ObMergerServiceMonitor *service_monitor_;
        oceanbase::common::ObSessionManager session_mgr_;
        sql::ObSQLSessionMgr *sql_session_mgr_;
        ObQueryCache* query_cache_;
        common::ObPrivilegeManager *privilege_mgr_;
        ObGetPrivilegeTask update_privilege_task_;
        sql::ObSQLIdMgr *sql_id_mgr_;
        //add zhaoqiong [Schema Manager] 20150327:b
        oceanbase::common::ObLocalMs ms_;
        oceanbase::common::ObCommonRpcStub *common_rpc_;
        oceanbase::common::ObScanHelperImpl *scan_helper_;
        common::ObSchemaServiceImpl *schema_service_;
        //add:e
        //add chujiajia [Paxos rs_election] 21051110:b
        common::ObSpinLock register_lock_;
        //add:e
    };

    inline void ObMergeServerService::extend_lease(const int64_t delay)
    {
      lease_expired_time_ = tbsys::CTimeUtil::getTime() + delay;
    }

    inline bool ObMergeServerService::check_lease(void) const
    {
      return tbsys::CTimeUtil::getTime() <= lease_expired_time_;
    }

    inline sql::ObSQLSessionMgr* ObMergeServerService::get_sql_session_mgr() const
    {
      return sql_session_mgr_;
    }

    inline void ObMergeServerService::set_sql_session_mgr(sql::ObSQLSessionMgr* mgr)
    {
      sql_session_mgr_ = mgr;
    }

  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_SERVICE_H_ */
