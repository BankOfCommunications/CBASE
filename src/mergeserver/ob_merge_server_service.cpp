#include <sys/syscall.h>
#include <unistd.h>
#include "ob_merge_server_service.h"
#include "ob_merge_server.h"
#include "common/ob_define.h"
#include "common/utility.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mod_define.h"
#include "common/ob_mutator.h"
#include "common/ob_result.h"
#include "common/ob_row_desc.h"
#include "common/ob_action_flag.h"
#include "common/ob_trace_log.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_schema_manager.h"
#include "common/ob_new_scanner.h"
#include "sql/ob_sql_result_set.h"
#include "ob_ms_rpc_proxy.h"
#include "common/ob_general_rpc_stub.h"
#include "ob_ms_async_rpc.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_get_request.h"
#include "ob_mutator_param_decoder.h"
#include "ob_ms_service_monitor.h"
#include "ob_ms_schema_proxy.h"
#include "ob_ms_scanner_encoder.h"
#include "common/location/ob_tablet_location_cache.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "ob_ms_service_monitor.h"
#include "ob_read_param_decoder.h"
#include "ob_ms_tsi.h"
#include "ob_ms_scan_param.h"
#include "ob_ms_scan_request.h"

#define __ms_debug__
#include "common/debug.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver
  {
    static const int32_t MAX_ERROR_MSG_LEN = 256;
    ObMergeServerService::ObMergeServerService()
    {
      merge_server_ = NULL;
      inited_ = false;
      registered_ = false;

      rpc_proxy_ = NULL;
      rpc_stub_ = NULL;
      async_rpc_ = NULL;

      schema_mgr_ = NULL;
      schema_proxy_ = NULL;
      root_rpc_ = NULL;

      location_cache_ = NULL;
      cache_proxy_ = NULL;
      service_monitor_ = NULL;
      sql_session_mgr_ = NULL;

      nb_accessor_ = NULL;
      query_cache_ = NULL;
      privilege_mgr_ = NULL;
      frozen_version_ = OB_INVALID_VERSION;

      lease_expired_time_ = tbsys::CTimeUtil::getTime() + DEFAULT_LEASE_TIME;
      sql_id_mgr_ = NULL;
      //add zhaoqiong [Schema Manager] 20150327:b
      scan_helper_ = NULL;
      schema_service_ = NULL;
      //add:e
    }

    ObMergeServerService::~ObMergeServerService()
    {
      destroy();
      if (NULL != service_monitor_)
      {
        delete service_monitor_;
        service_monitor_ = NULL;
      }
    }

    int ObMergeServerService::start()
    {
      return init_ms_properties_();
    }

    ObMergeServerConfig& ObMergeServerService::get_config()
    {
      return merge_server_->get_config();
    }

    const ObMergeServerConfig& ObMergeServerService::get_config() const
    {
      return merge_server_->get_config();
    }

    ObConfigManager& ObMergeServerService::get_config_mgr()
    {
      return merge_server_->get_config_mgr();
    }
    int ObMergeServerService::initialize(ObMergeServer* merge_server)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      merge_server_ = merge_server;
      if (OB_SUCCESS !=
          (ret = get_config_mgr().init(
            merge_server_->get_self(),
            merge_server_->get_client_manager(),
            merge_server_->get_timer())))
      {
        TBSYS_LOG(ERROR, "init error, ret: [%d]", ret);
      }
      return ret;
    }
    //mod pangtianze [Paxos rs_election] 20170228:b
    //int ObMergeServerService::register_root_server()
    int ObMergeServerService::register_root_server(bool is_first)
    //mod:e
    {
      common::ObSpinLockGuard guard(register_lock_);
      int err = OB_SUCCESS;
      int32_t status = 0; // obsolete, any value
      registered_ = false;
      int64_t cluster_id = 0;
      char server_version[OB_SERVER_VERSION_LENGTH] = "";
      get_package_and_svn(server_version, sizeof(server_version));

      //add pangtianze [Paxos rs_election] 20150707:b
      common::ObServer current_rs = merge_server_->get_root_server();
      merge_server_->set_new_root_server(current_rs);
      merge_server_->get_rs_mgr().set_master_rs(current_rs);
      merge_server_->get_rs_mgr().init_current_idx();
      //add:e

      while (!merge_server_->is_stoped())
      {
        err = rpc_stub_->register_merge_server(
          merge_server_->get_config().network_timeout,
          merge_server_->get_root_server(),
          merge_server_->get_self(),
          (int32_t)merge_server_->get_config().obmysql_port,
          merge_server_->get_config().lms, status,
          cluster_id,
          server_version);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to register merge server to root server! "
                    "root_server: [%s]",
                    to_cstring(merge_server_->get_root_server()));
          //add bingo [Paxos ms/cs cluster_id] 20170401:b
          if(OB_CLUSTER_ID_ERROR == err){
            TBSYS_LOG(WARN, "register self to rootserver failed, stop self, err=%d", err);
            merge_server_->stop();
          }
          //add:e
          //add pangtianze [Paxos rs_election] 20170228:b
          ///will regist to next rs
          if (!is_first)
          {
            //not first, try next different rs
            merge_server_->get_rs_mgr().next_rs();
            current_rs = merge_server_->get_rs_mgr().get_current_rs();
            merge_server_->set_new_root_server(current_rs);
            TBSYS_LOG(INFO, "will regist to next rootserver=%s", merge_server_->get_root_server().to_cstring());
          }
          else
          {
              //first regist, try the same rs
          }
          //add:e
          usleep(RETRY_INTERVAL_TIME);
        }
        else
        {
          registered_ = true;
          lease_expired_time_ = tbsys::CTimeUtil::getTime() + DEFAULT_LEASE_TIME;
          //del lbzhong [Paxos balance] 20160614:b
          //merge_server_->get_config().cluster_id = cluster_id;
          //de:e
          //add pangtianze [Paxos rs_election] 20170228:b
          ///set master rs info after regist succ
          if (!is_first)
          {
            merge_server_->set_root_server(current_rs);
          }
          merge_server_->get_rs_mgr().set_master_rs(current_rs);
          TBSYS_LOG(INFO, "register self to rootserver successed, master rs=%s", current_rs.to_cstring());
          //add:e
          break;
        }
      }
      return err;
    }
    int ObMergeServerService::reload_config()
    {
      int ret = OB_SUCCESS;
      if ((NULL == merge_server_ ) || (NULL == rpc_proxy_))
      {
        TBSYS_LOG(ERROR, "Check merge server failed. server[%p], rpc[%p]", merge_server_, rpc_proxy_);
        ret = OB_NOT_INIT;
      }

      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_->set_rpc_param(merge_server_->get_config().retry_times,
                                        merge_server_->get_config().network_timeout);
      }

      if (OB_SUCCESS == ret)
      {
        location_cache_->set_timeout(merge_server_->get_config().vtable_location_cache_timeout, true);
        location_cache_->set_timeout(merge_server_->get_config().location_cache_timeout, false);
        ret = location_cache_->set_mem_size(merge_server_->get_config().location_cache_size);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t count = 0;
        for (int32_t i = 0; i <= merge_server_->get_config().retry_times; ++i)
        {
          ret = rpc_proxy_->fetch_update_server_list(count);
          if (OB_SUCCESS == ret)
          {
            TBSYS_LOG(INFO, "Fetch update server list succ. count[%d]", count);
            break;
          }
          usleep(RETRY_INTERVAL_TIME);
        }
      }

      if (OB_SUCCESS == ret)
      {
        merge_server_->get_timer().destroy();
        ret = merge_server_->get_timer().init();
      }

      if (OB_SUCCESS == ret)
      {
        ObChunkServerTaskDispatcher::get_instance()->set_factor(merge_server_->get_config().use_new_balance_method);
      }

      if (OB_SUCCESS == ret)
      {
        ret = merge_server_->get_timer().schedule(check_lease_task_,
                                                  merge_server_->get_config().lease_check_interval, true);
      }

      if (OB_SUCCESS == ret)
      {
        ret = merge_server_->get_timer().schedule(monitor_task_,
                                                  merge_server_->get_config().monitor_interval, true);
      }

      return ret;
    }

    // check instance role is right for read master
    bool ObMergeServerService::check_instance_role(const bool read_master) const
    {
      bool result = true;
      if ((true == read_master) && (instance_role_.get_role() != ObiRole::MASTER))
      {
        result = false;
      }
      return result;
    }

    int ObMergeServerService::init_ms_properties_()
    {
      int err  = OB_SUCCESS;
      ObSchemaManagerV2 *newest_schema_mgr = NULL;
      if (OB_SUCCESS == err)
      {
        newest_schema_mgr = new(std::nothrow)ObSchemaManagerV2;
        if (NULL == newest_schema_mgr)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == err)
      {
        rpc_stub_ = new(std::nothrow)ObGeneralRpcStub();
        if (NULL == rpc_stub_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = rpc_stub_->init(merge_server_->get_rpc_buffer(),
            &(merge_server_->get_client_manager()));
          if (OB_SUCCESS == err)
          {
            //mod pangtianze [Paxos rs_election] 20170228:b
            //err = register_root_server();
            err = register_root_server(true); ///first regist
            //mod:e
            if (err != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "server register to root failed:ret[%d]", err);
            }
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        async_rpc_ = new(std::nothrow)ObMergerAsyncRpcStub();
        if (NULL == async_rpc_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = async_rpc_->init(merge_server_->get_rpc_buffer(),
            &(merge_server_->get_client_manager()));
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "init async rpc failed:ret[%d]", err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        root_rpc_ = new (std::nothrow) ObMergerRootRpcProxy(merge_server_->get_config().retry_times,
          merge_server_->get_config().network_timeout, merge_server_->get_root_server());
        if (NULL == root_rpc_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = root_rpc_->init(rpc_stub_);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "root rpc proxy init failed:ret[%d]", err);
          }
        }
      }

      // fetch system root table schema
      if (OB_SUCCESS == err)
      {
        schema_mgr_ = new(std::nothrow)ObMergerSchemaManager;
        if (NULL == schema_mgr_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          // fetch core table schema
          for (int32_t i = 0; i <= merge_server_->get_config().retry_times; ++i)
          {
            err = rpc_stub_->fetch_schema(merge_server_->get_config().network_timeout,
              merge_server_->get_root_server(), 0, true, *newest_schema_mgr);
            if (OB_SUCCESS == err)
            {
              TBSYS_LOG(DEBUG, "fetch core table schema success");
              schema_mgr_->init(true, *newest_schema_mgr);
              break;
            }
            else
            {
              TBSYS_LOG(DEBUG, "fetch core table schema failed, ret=%d", err);
            }
            usleep(RETRY_INTERVAL_TIME);
          }
        }
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      if (OB_SUCCESS == err)
      {
        common_rpc_ = new(std::nothrow)ObCommonRpcStub();
        if (NULL == common_rpc_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_SUCCESS != (err = common_rpc_->init(&(merge_server_->get_client_manager()))))
        {
          TBSYS_LOG(ERROR,"common_rpc init error");
        }
        else if (NULL == (scan_helper_ = new(std::nothrow)ObScanHelperImpl))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (schema_service_ = new(std::nothrow)ObSchemaServiceImpl))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          ms_.set_ms(merge_server_->get_self());
          scan_helper_->set_rpc_stub(common_rpc_);
          scan_helper_->set_scan_timeout(merge_server_->get_config().network_timeout);
          scan_helper_->set_mutate_timeout(merge_server_->get_config().network_timeout);
          scan_helper_->set_scan_retry_times(1);
          scan_helper_->set_ms_provider(&ms_);
          if (OB_SUCCESS != (err = schema_service_->init(scan_helper_, false)))
          {
            TBSYS_LOG(ERROR,"init schema_service_ error");
          }
        }
      }
      //add:e
      if (OB_SUCCESS == err)
      {
        //mod zhaoqiong [Schema Manager] 20150327:b
        //schema_proxy_ = new(std::nothrow)ObMergerSchemaProxy(root_rpc_, schema_mgr_);
        schema_proxy_ = new(std::nothrow)ObMergerSchemaProxy(root_rpc_, schema_mgr_, schema_service_);
        //mod:e
        if (NULL == schema_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == err)
      {
        rpc_proxy_ = new(std::nothrow)ObMergerRpcProxy(merge_server_->get_config().retry_times,
          merge_server_->get_config().network_timeout, merge_server_->get_root_server(),
          merge_server_->get_self());
        if (NULL == rpc_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          merge_server_->get_bloom_filter_task_queue_thread().init(rpc_proxy_);
        }
      }

      if (OB_SUCCESS == err)
      {
        location_cache_ = new(std::nothrow)ObTabletLocationCache;
        if (NULL == location_cache_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = location_cache_->init(merge_server_->get_config().location_cache_size,
            1024, merge_server_->get_config().location_cache_timeout,
            merge_server_->get_config().vtable_location_cache_timeout);
        }
      }

      if (OB_SUCCESS == err)
      {
        service_monitor_ = new(std::nothrow)ObMergerServiceMonitor(0);
        if (NULL == service_monitor_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          ObServer sql_server;
          if (!sql_server.set_ipv4_addr(merge_server_->get_self().get_ipv4(), (int32_t)merge_server_->get_config().obmysql_port))
          {
            TBSYS_LOG(WARN, "set sql server port for stat manager failed");
            err = OB_ERROR;
          }
          else
          {
            service_monitor_->init(sql_server);
            ObStatSingleton::init(service_monitor_);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        cache_proxy_ = new(std::nothrow)common::ObTabletLocationCacheProxy(merge_server_->get_self(),
          root_rpc_, location_cache_);
        if (NULL == cache_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          // init lock holder for concurrent inner table access
          err = cache_proxy_->init(MAX_INNER_TABLE_COUNT, MAX_ROOT_SERVER_ACCESS_COUNT);
        }
      }

      if (OB_SUCCESS == err)
      {
        //if (merge_server_->get_config().query_cache_size >= 1)
        //{
        //  query_cache_ = new(std::nothrow)ObQueryCache();
        //  if (NULL == query_cache_)
        //  {
        //    err = OB_ALLOCATE_MEMORY_FAILED;
        //  }
        //  else
        //  {
        //    err = query_cache_->init(merge_server_->get_config().query_cache_size);
        //    if (OB_SUCCESS != err)
        //    {
        //      TBSYS_LOG(WARN, "initialize query cache failed:ret[%d]", err);
        //    }
        //  }
        //}
      }
      if (OB_SUCCESS == err)
      {
        err = rpc_proxy_->init(rpc_stub_);
      }
      if (OB_SUCCESS == err)
      {
        privilege_mgr_ = merge_server_->get_privilege_manager();
      }

      if (OB_SUCCESS == err)
      {
        int32_t count = 0;
        for (int32_t i = 0; i <= merge_server_->get_config().retry_times; ++i)
        {
          err = rpc_proxy_->fetch_update_server_list(count);
          if (OB_SUCCESS == err)
          {
            TBSYS_LOG(INFO, "fetch update server list succ:count[%d]", count);
            break;
          }
          usleep(RETRY_INTERVAL_TIME);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = merge_server_->get_timer().init();
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "timer init failed:ret[%d]", err);
        }
      }

      // lease check timer task
      if (OB_SUCCESS == err)
      {
        check_lease_task_.init(this);
        err = merge_server_->get_timer().schedule(check_lease_task_,
          merge_server_->get_config().lease_check_interval, true);
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "%s", "lease check timer schedule succ");
        }
        else
        {
          TBSYS_LOG(ERROR, "lease check timer schedule failed:ret[%d]", err);
        }
      }

      // init rebalance method
      if (OB_SUCCESS == err)
      {
        ObChunkServerTaskDispatcher::get_instance()->set_factor(merge_server_->get_config().use_new_balance_method);
      }

      // monitor timer task
      if (OB_SUCCESS == err)
      {
        err = monitor_task_.init(merge_server_);
        if (OB_SUCCESS == err)
        {
          err = merge_server_->get_timer().schedule(monitor_task_,
            merge_server_->get_config().monitor_interval, true);
          if (OB_SUCCESS == err)
          {
            TBSYS_LOG(INFO, "%s", "monitor timer schedule succ");
          }
          else
          {
            TBSYS_LOG(ERROR, "monitor timer schedule failed:ret[%d]", err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        sql_proxy_.set_env(rpc_proxy_, root_rpc_,
                           async_rpc_, schema_mgr_, cache_proxy_, this);
      }
      if (OB_SUCCESS == err)
      {
        merge_server_->get_bloom_filter_task_queue_thread().start();
      }

      if (OB_SUCCESS != err)
      {
        if (rpc_proxy_)
        {
          delete rpc_proxy_;
          rpc_proxy_ = NULL;
        }

        if (rpc_stub_)
        {
          delete rpc_stub_;
          rpc_stub_ = NULL;
        }

        if (schema_mgr_)
        {
          delete schema_mgr_;
          schema_mgr_ = NULL;
        }

        if (schema_proxy_)
        {
          delete schema_proxy_;
          schema_proxy_ = NULL;
        }

        if (location_cache_)
        {
          delete location_cache_;
          location_cache_ = NULL;
        }

        if (cache_proxy_)
        {
          delete cache_proxy_;
          cache_proxy_ = NULL;
        }

        if (query_cache_)
        {
          delete query_cache_;
          query_cache_ = NULL;
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        if (common_rpc_)
        {
          delete common_rpc_;
          common_rpc_ = NULL;
        }

        if (scan_helper_)
        {
          delete scan_helper_;
          scan_helper_ = NULL;
        }

        if (schema_service_)
        {
          delete schema_service_;
          schema_service_ = NULL;
        }
        //add:e
      }

      if (newest_schema_mgr)
      {
        delete newest_schema_mgr;
        newest_schema_mgr = NULL;
      }

      if (OB_SUCCESS == err)
      {
        inited_ = true;
      }
      else
      {
        TBSYS_LOG(ERROR, "service init error, ret: [%d]", err);
      }
      __debug_init__();
      return err;
    }

    int ObMergeServerService::destroy()
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        inited_ = false;
        TBSYS_LOG(WARN, "stop mergeserver timer");
        merge_server_->get_timer().destroy();
        merge_server_ = NULL;
        TBSYS_LOG(WARN, "destroy rpc proxy etc.");
        delete rpc_proxy_;
        rpc_proxy_ = NULL;
        delete rpc_stub_;
        rpc_stub_ = NULL;
        delete schema_mgr_;
        schema_mgr_ = NULL;
        delete schema_proxy_;
        schema_proxy_ = NULL;
        delete location_cache_;
        location_cache_ = NULL;
        delete cache_proxy_;
        cache_proxy_ = NULL;
        //add zhaoqiong [Schema Manager] 20150327:b
        delete common_rpc_;
        common_rpc_ = NULL;
        delete scan_helper_;
        scan_helper_ = NULL;
        delete schema_service_;
        schema_service_ = NULL;
        //add:e

        if (NULL != query_cache_)
        {
          delete query_cache_;
          query_cache_ = NULL;
        }
        TBSYS_LOG(WARN, "service stoped");
      }
      else
      {
        rc = OB_NOT_INIT;
      }
      return rc;
    }

    void ObMergeServerService::handle_failed_request(const int64_t timeout, const int32_t packet_code)
    {
      if (!inited_)  //|| !registered_)
      {
        TBSYS_LOG(WARN, "%s", "merge server has not inited or registered");
      }
      else
      {
        // no need deserialize the packet to get the table id
        switch (packet_code)
        {
        case OB_SCAN_REQUEST:
        case OB_GET_REQUEST:
          OB_STAT_INC(MERGESERVER, NB_GET_OP_COUNT);
          OB_STAT_INC(MERGESERVER, NB_SCAN_OP_TIME, timeout);
          break;
        default:
          TBSYS_LOG(WARN, "handle overflow or timeout packet not include statistic info:packet[%d]", packet_code);
        }
      }
    }

    int ObMergeServerService::do_request(
      const int64_t receive_time,
      const int32_t packet_code,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      int rc = OB_SUCCESS;
      if (!inited_)  //|| !registered_)
      {
        TBSYS_LOG(WARN, "%s", "merge server has not inited or registered");
        rc = OB_NOT_INIT;
      }

      if (rc == OB_SUCCESS)
      {
        UNUSED(timeout_us);
        switch (packet_code)
        {
        case OB_LIST_SESSIONS_REQUEST:
          rc = ms_list_sessions(version, channel_id, req, in_buffer, out_buffer);
          break;
        case OB_KILL_SESSION_REQUEST:
          rc = ms_kill_session(version, channel_id, req, in_buffer, out_buffer);
          break;
        case OB_REQUIRE_HEARTBEAT:
          rc = ms_heartbeat(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        //add pangtianze [Paxos rs_switch] 20170208:b
        case OB_FORCE_SERVER_REGIST:
          rc = ms_force_regist(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        //add:e
        //add pangtianze [Paxos rs_switch] 20170301:b
        case OB_REFRESH_RS_LIST:
          rc = ms_refresh_rs_list(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        //add:e
        case OB_SQL_EXECUTE:
          rc = ms_sql_execute(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_SWITCH_SCHEMA:
          rc = ms_accept_schema(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        //add zhaoqiong [Schema Manager] 20150327:b
        case OB_SWITCH_SCHEMA_MUTATOR:
          rc = ms_accept_schema_mutator(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        //add:e
        case OB_SCAN_REQUEST:
          rc = ms_scan(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_SQL_SCAN_REQUEST:
          rc = ms_sql_scan(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_GET_REQUEST:
          rc = ms_get(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_WRITE:
        case OB_MS_MUTATE:
          rc = ms_mutate(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_FETCH_STATS:
          rc = ms_stat(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_CLEAR_REQUEST:
          rc = ms_clear(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_UPS_RELOAD_CONF:
          rc = ms_reload_config(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_CHANGE_LOG_LEVEL:
          rc = ms_change_log_level(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_SET_CONFIG:
          rc = ms_set_config(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_GET_CONFIG:
          rc = ms_get_config(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        case OB_SQL_KILL_SESSION:
          rc = ms_sql_kill_session(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_us);
          break;
        default:
          TBSYS_LOG(WARN, "check packet type failed:type[%d]", packet_code);
          rc = OB_ERROR;
        }
      }
      return rc;
    }

    int ObMergeServerService::ms_list_sessions(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      const int32_t MS_LIST_SESSIONS_VERSION = 1;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      UNUSED(in_buffer);
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(req);
      const int64_t buf_size = 16*1024;
      char *buf = (char*)ob_malloc(buf_size, ObModIds::OB_MS_COMMON);
      ObObj result;
      int64_t pos = 0;
      ObString str;
      result.set_varchar(str);
      if (NULL == buf)
      {
        TBSYS_LOG(WARN,"fail to allocate memory for session infos");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      if (OB_SUCCESS == err)
      {
        session_mgr_.get_sessions(buf,buf_size,pos);
        str.assign(buf,static_cast<int32_t>(pos));
        result.set_varchar(str);
      }
      int32_t send_err  = OB_SUCCESS;
      if (OB_SUCCESS != (err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position())))
      {
        TBSYS_LOG(WARN,"fail to serialize result code [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (err = result.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position()))))
      {
        TBSYS_LOG(WARN,"fail to serialize result msg [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (send_err =  merge_server_->send_response(OB_LIST_SESSIONS_RESPONSE,
        MS_LIST_SESSIONS_VERSION, out_buffer, req, channel_id))))
      {
        TBSYS_LOG(WARN,"fail to send list sessions response [send_err:%d]", send_err);
      }
      ob_free(buf);
      easy_request_wakeup(req);
      return send_err;
    }

    int ObMergeServerService::ms_kill_session(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      const int32_t MS_KILL_SESSION_VERSION = 1;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      UNUSED(out_buffer);
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(req);
      int64_t session_id = 0;
      ObObj obj;
      if (OB_SUCCESS != (err = obj.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position())))
      {
        TBSYS_LOG(WARN,"fail to get session id from request [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.get_int(session_id))))
      {
        TBSYS_LOG(WARN,"fail to get session id from reqeust [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && (session_id <= 0))
      {
        TBSYS_LOG(WARN,"invalid aguemnt [session_id:%ld]", session_id);
      }
      if (OB_SUCCESS == err)
      {
        session_mgr_.kill_session(static_cast<uint64_t>(session_id));
      }
      int32_t send_err  = OB_SUCCESS;
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO, "kill session [session_id:%ld]", session_id);
        send_err = merge_server_->send_response(OB_KILL_SESSION_RESPONSE, MS_KILL_SESSION_VERSION,
          out_buffer, req, channel_id);
      }
      easy_request_wakeup(req);
      return send_err;
    }

    int ObMergeServerService::ms_sql_kill_session(
      const int64_t receive_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us
      )
    {
      ObResultCode rc;
      const int32_t MS_SQL_KILL_SESSION_VERSION = 1;
      int32_t &err = rc.result_code_;
      UNUSED(receive_time);
      UNUSED(timeout_us);
      UNUSED(out_buffer);
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(req);
      int32_t session_id = 0;
      bool is_query = false;

      if (OB_SUCCESS != (err = serialization::decode_vi32(in_buffer.get_data(),
                                                          in_buffer.get_capacity(),
                                                          in_buffer.get_position(),
                                                          &session_id)))
      {
        TBSYS_LOG(WARN,"fail to decode session id from request [err:%d]", err);
      }

      if (OB_SUCCESS == err)
      {
        err = serialization::decode_bool(in_buffer.get_data(), in_buffer.get_capacity(),
                                         in_buffer.get_position(), &is_query);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to decode is_kill_query from request [err:%d]", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = sql_session_mgr_->kill_session(static_cast<uint32_t>(session_id), is_query);
      }
      int32_t send_err  = OB_SUCCESS;
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_SQL_KILL_SESSION_RESPONSE, MS_SQL_KILL_SESSION_VERSION,
          out_buffer, req, channel_id);
      }
      return send_err;
    }
    //add pangtianze [Paxos rs_switch] 20170208:b
    //force re-regist
    int ObMergeServerService::ms_force_regist(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
        UNUSED(channel_id);
        UNUSED(start_time);
        UNUSED(req);
        UNUSED(in_buffer);
        UNUSED(out_buffer);
        UNUSED(timeout_us);
        int ret = OB_SUCCESS;
        const int32_t MS_FORCE_REGIST_VERSION = 1;
        ObServer rs_master;
        if (MS_FORCE_REGIST_VERSION != version)
        {
          TBSYS_LOG(WARN, "version not equal. version=%d, MS_FORCE_REGIST_VERSION=%d", version, MS_FORCE_REGIST_VERSION);
          ret = OB_ERROR_FUNC_VERSION;
        }
        else if (OB_SUCCESS != (ret = rs_master.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize rootserver. err=%d", ret);
        }
        else
        {
          ///0. set master rootserver
          merge_server_->set_root_server(rs_master); //mod pangtianze 20170706
          TBSYS_LOG(INFO, "ms set new master rootserver[%s] after election", rs_master.to_cstring());
          ///1. make ms lease timeout
          lease_expired_time_ = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "reset lease, then will re-regist, lease=%ld", lease_expired_time_);
          ///2. check lease right now
          merge_server_->get_timer().schedule(check_lease_task_, 0, false);
          //add pangtianze [Paxos rs_switch] 20170424
          ///3. clear ms tablet location cache
          if (NULL != location_cache_)
          {
            ret = location_cache_->clear();
            TBSYS_LOG(INFO, "clear tablet location cache:ret[%d]", ret);
          }
          //add:e
        }
        // no response
        easy_request_wakeup(req);
        return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_switch] 20170301:b
    int ObMergeServerService::ms_refresh_rs_list(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
        UNUSED(start_time);
        UNUSED(timeout_us);
        UNUSED(channel_id);
        UNUSED(out_buffer);

        const int32_t MY_VERSION = 1;
        int ret = OB_SUCCESS;

        if (version != MY_VERSION)
        {
          ret = OB_ERROR_FUNC_VERSION;
          TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);

        }
        else
        {
          int32_t servers_count = 0;       
          if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position(), &servers_count)))
          {
            TBSYS_LOG(WARN, "failed to deserialize server_count. err=%d", ret);
          }
          if (OB_SUCCESS == ret)
          {
            if (servers_count > 0) //count cannot be 0
            {
                common::ObServer servers[servers_count];
                for (int32_t i = 0; i < servers_count; i++)
                {
                  if (OB_SUCCESS != (ret = servers[i].deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position())))
                  {
                     TBSYS_LOG(WARN, "failed to deserialize rootserver. err=%d", ret);
                     break;
                  }
                }
                //mod chujiajia[Paxos rs_election]20151021:b
                if(OB_SUCCESS == ret)
                {
                  if (OB_SUCCESS != (ret = merge_server_->get_rs_mgr().update_all_rs(servers, servers_count)))
                  {
                    TBSYS_LOG(WARN, "failed update all rootservers. err=%d", ret);
                  }
                }
                //mod:e
            }
            else
            {
                ret = OB_INVALID_VALUE;
            }
            //mod:e
          }
        }

        easy_request_wakeup(req);
        return ret;
    }
    //add:e
    int ObMergeServerService::ms_heartbeat(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      static const int32_t MS_HEARTBEAT_LEASE_SCHEMA_VERSION = 3;
      static const int32_t MS_HEARTBEAT_PRIVILEGE_VERSION = 4;
      static const int32_t MS_HEARTBEAT_CONFIG_VERSION = 4;

      FILL_TRACE_LOG("step 1. start process heartbeat");
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      UNUSED(channel_id);
      UNUSED(start_time);
      UNUSED(req);
      UNUSED(out_buffer);
      UNUSED(timeout_us);

      if (version >= MS_HEARTBEAT_LEASE_SCHEMA_VERSION)
      {
        int64_t lease_duration = 0;
        err = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &lease_duration);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "parse heartbeat input lease_duration param failed:ret[%d]", err);
        }
        else
        {
          if (lease_duration <= 0)
          {
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "check lease duration failed:duration[%ld]", lease_duration);
          }
        }
        //TODO frozen version
        int64_t frozen_mem_version = -1;
        if (OB_SUCCESS == err)
        {
          err = serialization::decode_vi64(in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(), &frozen_mem_version);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "parse heartbeat frozen memory version failed:ret[%d]", err);
          }
          else
          {
            frozen_version_ = frozen_mem_version;
          }
        }
        int64_t local_version = schema_mgr_->get_latest_version();
        int64_t schema_version = 0;
        if (OB_SUCCESS == err)
        {
          err = serialization::decode_vi64(in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(), &schema_version);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "parse heartbeat schema version failed:ret[%d]", err);
          }
          else if ((local_version > schema_version) && (schema_version != 0))
          {
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "check schema local version gt than new version:"
                "local[%ld], new[%ld]", local_version, schema_version);
          }
        }

        FILL_TRACE_LOG("step 2. decode heartbeat:lease[%ld], local[%ld], version[%ld]",
            lease_duration, local_version, schema_version);

        if (OB_SUCCESS == err)
        {
          err = root_rpc_->async_heartbeat(merge_server_->get_self(), (int32_t)merge_server_->get_config().obmysql_port,
              merge_server_->get_config().lms);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "heartbeat to root server failed:ret[%d]", err);
          }
          else
          {
            extend_lease(lease_duration);
            TBSYS_LOG(DEBUG, "%s", "heartbeat to root server succ");
          }
        }

        // fetch new schema in a temp timer task
        if ((OB_SUCCESS == err) && (local_version < schema_version) && (schema_version > 0))
        {
          fetch_schema_task_.init(schema_proxy_, schema_mgr_);
          fetch_schema_task_.set_version(local_version, schema_version);
          srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
          err = merge_server_->get_timer().schedule(fetch_schema_task_,
              random() % FETCH_SCHEMA_INTERVAL, false);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "schedule fetch new schema task failed:version[%ld], ret[%d]",
                schema_version, err);
          }
        }

        // Role
        if (OB_SUCCESS == err)
        {
          if (OB_SUCCESS != (err = instance_role_.deserialize(in_buffer.get_data(),
                  in_buffer.get_capacity(), in_buffer.get_position())))
          {
            TBSYS_LOG(WARN, "parse heartbeat input role param failed:ret[%d]", err);
          }
        }
      }
      if (version >= MS_HEARTBEAT_PRIVILEGE_VERSION)
      {
        int64_t local_privilege_version = merge_server_->get_privilege_manager()->get_newest_privilege_version();
        int64_t privilege_version = 0;
        if (OB_SUCCESS == err)
        {
          err = serialization::decode_vi64(in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(), &privilege_version);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "parse heartbeat privilege version failed:ret[%d]", err);
          }
          /* for slave ms, it's weird */
          /* else if (local_privilege_version > privilege_version) */
          /* { */
          /*   err = OB_ERR_OLDER_PRIVILEGE_VERSION; */
          /*   TBSYS_LOG(ERROR, "check privilege local version gt than new version:" */
          /*       "local[%ld], incoming[%ld], ret=%d", local_privilege_version, privilege_version, err); */
          /* } */
          else if (local_privilege_version < privilege_version)
          {
            TBSYS_LOG(INFO, "local_privilege_version=%ld, incoming privilege_version=%ld",
                local_privilege_version, privilege_version);
            update_privilege_task_.init(privilege_mgr_, &sql_proxy_, privilege_version);
            err = merge_server_->get_timer().schedule(update_privilege_task_, 0, false);
            if (err != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "schedule update privilege task failed, version=%ld, ret=%d", privilege_version, err);
            }
          }
        }
      }
      if (version >= MS_HEARTBEAT_CONFIG_VERSION)
      {
        int64_t config_version;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                       in_buffer.get_capacity(),
                                                       in_buffer.get_position(),
                                                       &config_version);
          if (OB_SUCCESS != (rc.result_code_))
          {
            TBSYS_LOG(ERROR, "parse heartbeat config version failed: ret[%d]",
                      rc.result_code_);
          }
          else if (OB_SUCCESS !=
                   (rc.result_code_ = get_config_mgr().got_version(config_version)))
          {
            TBSYS_LOG(WARN, "Process config failed, ret: [%d]", rc.result_code_);
          }
        }
      }

      FILL_TRACE_LOG("step 3. process heartbeat finish:ret[%d]", err);
      CLEAR_TRACE_LOG();
      return err;
    }

    int ObMergeServerService::ms_clear(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start clear tablet location cache");
      const int32_t MS_CLEAR_VERSION = 1;
      UNUSED(start_time);
      UNUSED(in_buffer);
      UNUSED(version);
      UNUSED(timeout_us);
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      if (NULL != location_cache_)
      {
        err = location_cache_->clear();
        TBSYS_LOG(INFO, "clear tablet location cache:ret[%d]", err);
      }
      else
      {
        err = OB_ERROR;
      }

      int32_t send_err  = OB_SUCCESS;
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_CLEAR_RESPONSE, MS_CLEAR_VERSION,
          out_buffer, req, channel_id);
      }

      FILL_TRACE_LOG("step 2. process clear cache finish:ret[%d]", err);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      return send_err;
    }

    int ObMergeServerService::ms_stat(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start monitor stat");
      const int32_t MS_MONITOR_VERSION = 1;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      int32_t send_err  = OB_SUCCESS;
      UNUSED(in_buffer);
      UNUSED(start_time);
      UNUSED(version);
      UNUSED(timeout_us);
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        if (NULL != service_monitor_)
        {
          err = service_monitor_->serialize(out_buffer.get_data(),out_buffer.get_capacity(),
            out_buffer.get_position());
        }
        else
        {
          err = OB_NO_MONITOR_DATA;
        }
      }

      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_FETCH_STATS_RESPONSE, MS_MONITOR_VERSION,
          out_buffer, req, channel_id);
      }

      FILL_TRACE_LOG("step 2. process monitor stat finish:ret[%d]", err);
      CLEAR_TRACE_LOG();
      return send_err;
    }

    int ObMergeServerService::ms_get(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      int64_t req_timeout_us = timeout_us;
      if ((0 >= timeout_us) || (timeout_us > merge_server_->get_config().max_req_process_time))
      {
        req_timeout_us = merge_server_->get_config().max_req_process_time;
        TBSYS_LOG(DEBUG, "reset timeoutus %ld", req_timeout_us);
      }
      int32_t request_cid = channel_id;
      const int32_t MS_GET_VERSION = 1;
      ObResultCode rc;
      char err_msg[MAX_ERROR_MSG_LEN]="";
      rc.message_.assign(err_msg, sizeof(err_msg));
      int32_t &err = rc.result_code_;
      int32_t &err_code = rc.result_code_;
      int32_t send_err  = OB_SUCCESS;
      err = OB_SUCCESS;
      ObGetParam *get_param_with_name = GET_TSI_MULT(ObGetParam,  TSI_MS_GET_PARAM_WITH_NAME_1);
      ObGetParam *org_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_ORG_GET_PARAM_1);
      ObGetParam *decoded_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_DECODED_GET_PARAM_1);
      ObReadParam *decoded_read_param = NULL;
      const ObSchemaManagerV2 *schema_mgr = NULL;
      const ObSchemaManagerV2 *user_schema_mgr = NULL;
      ObScanner *result_scanner = GET_TSI_MULT(ObScanner, TSI_MS_SCANNER_1);
      ObMergerGetRequest * get_event = GET_TSI_ARGS(ObMergerGetRequest, TSI_MS_GET_EVENT_1,
          cache_proxy_, async_rpc_);
      int64_t timeout_time = start_time + req_timeout_us;
      easy_connection_t* conn = req->ms->c;
      if (OB_SUCCESS == err && MS_GET_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == err &&
        (NULL == org_get_param || NULL == decoded_get_param || NULL == result_scanner || NULL == get_param_with_name))
      {
        TBSYS_LOG(WARN,"fail to allocate memory for request");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS == err)
      {
        decoded_read_param = dynamic_cast<ObReadParam *>(decoded_get_param);
        org_get_param->reset();
        get_param_with_name->reset();
        decoded_get_param->reset();
        result_scanner->reset();
        if ((!get_event->inited())
          && ( OB_SUCCESS != (err = get_event->init(REQUEST_EVENT_QUEUE_SIZE,ObModIds::OB_MS_REQUEST_EVENT, &session_mgr_))))
        {
          TBSYS_LOG(WARN,"fail to init ObMergerGetEvent [err:%d]", err);
        }
      }
      /// decode request
      if (OB_SUCCESS == err)
      {
        FILL_TRACE_LOG("step 1. start serve ms_get [request_event_id:%lu]", get_event->get_request_id());
        // get the local newest user schema, only fetch local cached schema, no rpc.
        //rpc_proxy_->fetch_new_user_schema(ObMergerRpcProxy::LOCAL_NEWEST, &user_schema_mgr);
        user_schema_mgr = schema_mgr_->get_user_schema(0);
        // decode get param set user schema for compatible RowkeyInfo
        // scan system table will never be used.
        org_get_param->set_compatible_schema(user_schema_mgr);

        err = org_get_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to parse ObGetParam [err:%d]",err);
        }
        else
        {
          ObReadParam * org_read_param = NULL;
          org_read_param = dynamic_cast<ObReadParam*>(org_get_param);
          if (NULL != decoded_read_param && NULL != org_read_param)
          {
            *decoded_read_param = *org_read_param;
          }
          else
          {
            TBSYS_LOG(WARN, "fail to cast ObGetParam type to ObReadParam type. decoded_read_param=%p, org_read_param=%p",
                decoded_read_param, org_read_param);
            err = OB_ERROR;
          }
        }
        // user_schema_mgr can be release after scan param decoded.
        if (NULL != user_schema_mgr)
        {
          schema_proxy_->release_schema(user_schema_mgr);
        }
      }
      if (0 >= org_get_param->get_cell_size())
      {
        TBSYS_LOG(WARN,"get param cell size error, [org_get_param:%ld]", org_get_param->get_cell_size());
        err = OB_INVALID_ARGUMENT;
      }

      /// get local newest sys or user table schema
      if (OB_SUCCESS == err)
      {
        err =  schema_proxy_->get_schema((*org_get_param)[0]->table_name_, ObMergerSchemaProxy::LOCAL_NEWEST, &schema_mgr);
        if ((OB_SUCCESS != err) || (NULL == schema_mgr))
        {
          TBSYS_LOG(WARN, "fail to get the latest schema, unexpected error:table[%.*s], ret[%d]",
              (*org_get_param)[0]->table_name_.length(), (*org_get_param)[0]->table_name_.ptr(), err);
          err = OB_ERR_UNEXPECTED;
        }
        else if (org_get_param->is_binary_rowkey_format())
        {
          result_scanner->set_binary_rowkey_format(true);
          result_scanner->set_compatible_schema(schema_mgr);
        }
      }

      bool is_cache_hit = false;
      uint64_t session_id = 0;
      ObCellInfo* first_cell = NULL;
      ObString first_table_name;
      bool got_all_result = false;
      int64_t net_session_id = 0;
      int64_t packet_cnt = 0;
      ObPacketQueueThread &queue_thread = merge_server_->get_default_task_queue_thread();
      ObPacket* next_request = NULL;

      if (OB_SUCCESS == err)
      {
        first_cell = org_get_param->operator [](0);
        if (NULL != first_cell)
        {
          first_table_name = first_cell->table_name_;
        }
      }

      if ((OB_SUCCESS == err) && NULL != query_cache_
          && org_get_param->get_is_result_cached()
          && schema_mgr->get_table_query_cache_expire_time(first_table_name) > 0)
      {
        ObString result_key;
        result_key.assign_ptr(in_buffer.get_data(), static_cast<int32_t>(in_buffer.get_position()));
        if (OB_SUCCESS == query_cache_->get(result_key, out_buffer))
        {
          is_cache_hit = true;
        }
      }

      if (OB_SUCCESS == err && is_cache_hit)
      {
        // query cache hit
        if ((OB_SUCCESS == send_err) &&
          (OB_SUCCESS != (send_err = merge_server_->send_response(OB_GET_RESPONSE, MS_GET_VERSION,out_buffer,
          req,request_cid, 0))))
        {
          TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
        }
        if (OB_SUCCESS == send_err)
        {
          packet_cnt ++;
        }
        FILL_TRACE_LOG("step 2. query cache hit, send_response:send_err[%d], code[%d], packet_cnt[%ld]",
          send_err, err, packet_cnt);
      }
      else
      {
        //query cache miss
        if (OB_SUCCESS == err)
        {
          err = ob_decode_get_param(*org_get_param,*schema_mgr,*decoded_get_param, *get_param_with_name, &rc);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to decode get param [err:%d]", err);
          }
        }
        if ((OB_SUCCESS == err)
            && (OB_SUCCESS != (err = session_mgr_.session_begin(*decoded_get_param,
                               convert_addr_to_server(conn->addr),session_id,
                               syscall(SYS_gettid), static_cast<pid_t>(pthread_self())))))
        {
          TBSYS_LOG(WARN,"fail to create session in ObSessionManager [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          get_event->set_session_id(static_cast<uint32_t>(session_id));
        }

        FILL_TRACE_LOG("step 2. finish parse the schema for ms_get:err[%d],get_cell_size[%ld]", err,
          decoded_get_param->get_cell_size());

        if (OB_SUCCESS == err)
        {
          get_event->set_timeout_percent((int32_t)merge_server_->get_config().timeout_percent);
          err = get_event->set_request_param(*decoded_get_param, req_timeout_us,
                                             merge_server_->get_config().max_parellel_count,
                                             merge_server_->get_config().max_get_rows_per_subreq,
                                             merge_server_->get_config().reserve_get_param_count);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "get request set param failed:err[%d]", err);
          }
          else
          {
            err = get_event->wait(timeout_time - tbsys::CTimeUtil::getTime());
            if (err != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "get request wait failed:err[%d]", err);
            }
          }
        }
        FILL_TRACE_LOG("step 3. wait response succ");

        /// prepare result
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = get_event->fill_result(*result_scanner,
            *get_param_with_name,got_all_result))))
        {
          TBSYS_LOG(WARN, "fail to faill result [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (!got_all_result))
        {
          net_session_id = queue_thread.generate_session_id();
          FILL_TRACE_LOG("step 3.x generator session id:session[%ld]", net_session_id);
        }
        do
        {
          if ((OB_SUCCESS == err) && (!got_all_result) && (packet_cnt > 0))
          {
            result_scanner->reset();
            if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = get_event->fill_result(*result_scanner,
                *get_param_with_name,got_all_result))))
            {
              TBSYS_LOG(WARN,"fail to fill result [err:%d]", err);
            }
          }
          FILL_TRACE_LOG("step 4.x. finish fill next result scanner:err[%d]", send_err);
          out_buffer.get_position() = 0;
          /// always send error code
          if ((OB_SUCCESS != (send_err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(),
              out_buffer.get_position()))))
          {
            //
            request_cid = next_request->get_channel_id();
            TBSYS_LOG(WARN,"fail to serialize result code [err:%d]", send_err);
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
            && (OB_SUCCESS !=(send_err = result_scanner->serialize(out_buffer.get_data(),out_buffer.get_capacity(),
            out_buffer.get_position()))))
          {
            TBSYS_LOG(WARN,"fail to serialize result scanner [err:%d]", send_err);
          }
          FILL_TRACE_LOG("step 4.x. finish serialize scanner:err[%d]", send_err);
          if ((OB_SUCCESS == err) && !got_all_result && (OB_SUCCESS != queue_thread.prepare_for_next_request(net_session_id)))
          {
            TBSYS_LOG(WARN,"fail to prepare network session [err:%d]", err);
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
              && got_all_result && 0 == packet_cnt && NULL != query_cache_
              && schema_mgr->get_table_query_cache_expire_time(first_table_name) > 0)
          {
            ObString result_key;
            ObQueryCacheValue result_value;
            result_key.assign_ptr(in_buffer.get_data(), static_cast<int32_t>(in_buffer.get_capacity()));
            result_value.expire_time_ =
              schema_mgr->get_table_query_cache_expire_time(first_table_name)
              + tbsys::CTimeUtil::getTime();
            result_value.size_ = out_buffer.get_position();
            result_value.buf_ = out_buffer.get_data();
            err = query_cache_->put(result_key, result_value);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to put get result into query cache:err[%d]", err);
              err = OB_SUCCESS;
            }
          }
          FILL_TRACE_LOG("step 4.x. prepare for next request:send_err[%d], code[%d], packet_cnt[%ld]",
              send_err, err_code, packet_cnt);
          if ((OB_SUCCESS == send_err) &&
            (OB_SUCCESS != (send_err = merge_server_->send_response(OB_GET_RESPONSE, MS_GET_VERSION,out_buffer,
            req,request_cid, (got_all_result?0:net_session_id)))))
          {
            TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
          }
          if (OB_SUCCESS == send_err)
          {
            packet_cnt ++;
          }
          FILL_TRACE_LOG("step 5.x. send_response:send_err[%d], code[%d], packet_cnt[%ld]",  send_err, err_code, packet_cnt);
          if ((OB_SUCCESS == err) && (!got_all_result) && (OB_SUCCESS == send_err))
          {
            err = queue_thread.wait_for_next_request(net_session_id,next_request,timeout_time - tbsys::CTimeUtil::getTime());
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to wait next request from session [err:%d,net_session_id:%ld]", err, net_session_id);
            }
            else
            {
              request_cid = next_request->get_channel_id();
            }
          }
          FILL_TRACE_LOG("step 5.x. wait next request succ:code[%d], packet_cnt[%ld]", err_code, packet_cnt);
        }while ((!got_all_result) && (OB_SUCCESS == send_err) && (OB_SUCCESS == err));
      }

      int64_t end_time = tbsys::CTimeUtil::getTime();
      if (OB_SUCCESS == err)
      {
        if (end_time - start_time > merge_server_->get_config().slow_query_threshold)
        {
          const char *session_info = NULL;
          if (NULL == (session_info = session_mgr_.get_session(session_id)))
          {
            TBSYS_LOG(WARN,"get empty session info");
          }
          else
          {
            TBSYS_LOG(WARN, "slow query:%s; used_time:%ld; result_size:%ld", session_info, end_time - start_time,
                result_scanner->get_size());
          }
        }
      }

      // check process timeout
      if (end_time > start_time + req_timeout_us)
      {
        err_code = OB_PROCESS_TIMEOUT;
        TBSYS_LOG(WARN, "check process get task timeout:start[%ld], end[%ld], timeout[%ld], used[%ld]",
          start_time, end_time, req_timeout_us, end_time - start_time);
      }

      /// inc monitor counter
      if (NULL != service_monitor_)
      {
        int64_t table_id = 0;
        if ((*decoded_get_param)[0] != NULL)
        {
          table_id = (*decoded_get_param)[0]->table_id_;
        }
        OB_STAT_TABLE_INC(MERGESERVER, table_id, NB_GET_OP_COUNT);
        OB_STAT_TABLE_INC(MERGESERVER, table_id, NB_GET_OP_TIME, end_time - start_time);
      }
      FILL_TRACE_LOG("step 5. at last send scanner reponse for ms_get:send_err[%d], code[%d]",
            send_err, err_code);

      if (NULL != schema_mgr)
      {
        schema_proxy_->release_schema(schema_mgr);
      }
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      if (NULL != get_event)
      {
        get_event->reset();
      }
      if (session_id > 0)
      {
        session_mgr_.session_end(session_id);
      }
      if (net_session_id > 0)
      {
        queue_thread.destroy_session(net_session_id);
      }
      return send_err;
    }

    int ObMergeServerService::ms_mutate(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start serve ms_mutate [peer:%s]", get_peer_ip(req));
      int64_t req_timeout_us = timeout_us;
      if ((0 >= timeout_us) || (timeout_us > merge_server_->get_config().max_req_process_time))
      {
        req_timeout_us = merge_server_->get_config().max_req_process_time;
        TBSYS_LOG(DEBUG, "reset timeoutus %ld", req_timeout_us);
      }
      const int32_t MS_MUTATE_VERSION = 1;
      int32_t send_err  = OB_SUCCESS;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      err = OB_SUCCESS;
      const ObSchemaManagerV2 *schema_mgr = NULL;
      ObGetParam *org_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_ORG_GET_PARAM_1);
      ObGetParam *decoded_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_DECODED_GET_PARAM_1);
      ObMutator *org_mutator_param = GET_TSI_MULT(ObMutator, TSI_MS_ORG_MUTATOR_1);
      ObMutator *decoded_mutator_param = GET_TSI_MULT(ObMutator, TSI_MS_DECODED_MUTATOR_1);
      ObScanner *ups_result = GET_TSI_MULT(ObScanner, TSI_MS_UPS_SCANNER_1);
      ObScanner *result_scanner = GET_TSI_MULT(ObScanner, TSI_MS_SCANNER_1);
      if (OB_SUCCESS == err && MS_MUTATE_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == err &&
        (NULL == org_get_param || NULL == decoded_get_param || NULL == org_mutator_param
        || NULL == decoded_mutator_param || NULL == ups_result || NULL == result_scanner))
      {
        TBSYS_LOG(WARN,"fail to allocate memory for request");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        org_mutator_param->reset();
        decoded_mutator_param->reset();
        org_get_param->reset();
        decoded_get_param->reset();
        ups_result->clear();
        result_scanner->clear();
      }
      /// decode request
      if (OB_SUCCESS == err)
      {
        err = org_mutator_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to parse ObMutator [err:%d]",err);
        }
      }
      /// get local newest schema
      if (OB_SUCCESS == err)
      {
        err = schema_proxy_->get_schema(org_mutator_param->get_first_table_name(),
            ObMergerSchemaProxy::LOCAL_NEWEST, &schema_mgr);
        if ((OB_SUCCESS != err) || (NULL == schema_mgr))
        {
          TBSYS_LOG(WARN, "fail to get the latest schema, unexpected error:table[%.*s], ret[%d]",
              org_mutator_param->get_first_table_name().length(), org_mutator_param->get_first_table_name().ptr(), err);
          err = OB_ERR_UNEXPECTED;
        }
      }
      /// decode and check mutator param
      if (OB_SUCCESS == err)
      {
        err = ObMutatorParamDecoder::decode(*org_mutator_param, *schema_mgr, *decoded_mutator_param,
          *org_get_param, *decoded_get_param);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to decode mutator param [err:%d]", err);
        }
      }
      FILL_TRACE_LOG("step 2. finish parse the schema for ms_mutate:err[%d]", err);
      TBSYS_LOG(DEBUG, "need prefetch data:prefetch_cell[%ld], return_cell[%ld]",
        decoded_get_param->get_cell_size(), org_get_param->get_cell_size());
      /// do request for get static data
      FILL_TRACE_LOG("step 3. no need to get prefetch data for mutate:prefetch[%ld], return[%ld], err[%d]",
        decoded_get_param->get_cell_size(), org_get_param->get_cell_size(), err);
      int64_t end_time = tbsys::CTimeUtil::getTime();
      if (end_time > start_time + req_timeout_us)
      {
        err = OB_PROCESS_TIMEOUT;
        TBSYS_LOG(WARN, "check process mutate task timeout:start[%ld], end[%ld], timeout[%ld], used[%ld]",
          start_time, end_time, req_timeout_us, end_time - start_time);
      }
      /// send ups request
      if (OB_SUCCESS == err)
      {
        err = rpc_proxy_->ups_mutate(*decoded_mutator_param, org_get_param->get_cell_size() > 0, *ups_result);
      }
      FILL_TRACE_LOG("step 4. ups mutate finished:err[%d]", err);
      /// encode id to names
      if ((OB_SUCCESS == err) && (org_get_param->get_cell_size() > 0))
      {
        err = ObMergerScannerEncoder::encode(*org_get_param, *ups_result, *result_scanner);
      }
      FILL_TRACE_LOG("step 5. convert mutate scanner id to name finished:err[%d]", err);
      int err_code = rc.result_code_;
      /// always send error code
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err && !result_scanner->is_empty())
      {
        err = result_scanner->serialize(out_buffer.get_data(),out_buffer.get_capacity(),
          out_buffer.get_position());
      }
      FILL_TRACE_LOG("step 6. finish serialize the scanner result for ms_mutate:err[%d]", err);
      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_MS_MUTATE_RESPONSE, MS_MUTATE_VERSION,out_buffer,
          req,channel_id);
      }
      // check process timeout
      end_time = tbsys::CTimeUtil::getTime();
      if (end_time > start_time + req_timeout_us)
      {
        err_code = OB_PROCESS_TIMEOUT;
        TBSYS_LOG(WARN, "check process mutate task timeout:start[%ld], end[%ld], timeout[%ld], used[%ld]",
          start_time, end_time, req_timeout_us, end_time - start_time);
      }
      FILL_TRACE_LOG("step 7. at last send scanner reponse for ms_mutate:send_err[%d], code[%d]",
        send_err, err_code);
      if (NULL != schema_mgr)
      {
        schema_proxy_->release_schema(schema_mgr);
      }
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      return send_err;
    }

    int ObMergeServerService::ms_accept_schema(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      UNUSED(start_time);
      UNUSED(timeout_us);
      const int32_t OB_MS_ACCEPT_SCHEMA_VERSION = 1;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int &err = rc.result_code_;
      ObSchemaManagerV2 *schema = NULL;
      int ret = OB_SUCCESS;
      int64_t schema_version = 0;

      if (version != OB_MS_ACCEPT_SCHEMA_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == err)
      {
        if (NULL == (schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_MS_SERVICE_FUNC)))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "fail to new ObSchemaManagerV2");
        }
      }

      if (OB_SUCCESS == err)
      {
        if (NULL == schema_mgr_)
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(WARN, "schema_mgr_ is null");
        }
      }

      if (OB_SUCCESS == err)
      {
        err = schema->deserialize(
              in_buffer.get_data(), in_buffer.get_capacity(),
              in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to deserialize schema:err[%d]", err);
        }
        else
        {
          schema_version = schema->get_version();
        }
      }

      if (OB_SUCCESS == err)
      {
        if (schema_version <= schema_mgr_->get_latest_version())
        {
          TBSYS_LOG(WARN, "schema version too old. old=%ld, latest=%ld",
              schema_version, schema_mgr_->get_latest_version());
          err = OB_OLD_SCHEMA_VERSION;
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        else if (!schema->is_completion())
        {
          TBSYS_LOG(INFO, "schema is not completion");
        }
        //add:e
        else if (OB_SUCCESS != (err = schema_mgr_->add_schema(*schema)))
        {
          TBSYS_LOG(WARN, "fail to add schema :err[%d]", err);
        }
      }
      //del zhaoqiong [Schema Manager] 20150327:b
      //OB_DELETE(ObSchemaManagerV2, ObModIds::OB_MS_SERVICE_FUNC, schema);
      //del:e

      ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fail to serialize: ret[%d]", ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = merge_server_->send_response(
            OB_SWITCH_SCHEMA_RESPONSE,
            OB_MS_ACCEPT_SCHEMA_VERSION,
            out_buffer, req, channel_id);
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      if (OB_SUCCESS == err && OB_SUCCESS == ret && !schema->is_completion())
      {
        int64_t timeout_us = merge_server_->get_config().network_timeout;
        ret = schema->fetch_schema_followed(*rpc_stub_,timeout_us*2,merge_server_->get_root_server(),schema->get_version());
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = schema_mgr_->add_schema(*schema)))
          {
            TBSYS_LOG(WARN, "fail to add schema :err[%d]", ret);
          }
        }
      }

      OB_DELETE(ObSchemaManagerV2, ObModIds::OB_MS_SERVICE_FUNC, schema);
      //add:e
      return ret;
    }
    //add zhaoqiong [Schema Manager] 20150327:b
    int ObMergeServerService::ms_accept_schema_mutator(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      UNUSED(start_time);
      UNUSED(timeout_us);
      const int32_t OB_MS_ACCEPT_SCHEMA_VERSION = 1;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int &err = rc.result_code_;
      ObSchemaManagerV2 *schema = NULL;
      ObSchemaMutator *schema_mutator = NULL;
      int ret = OB_SUCCESS;

      if (version != OB_MS_ACCEPT_SCHEMA_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == schema_mgr_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "schema_mgr_ is null");
      }
      else if (NULL == (schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_MS_SERVICE_FUNC)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "fail to new ObSchemaManagerV2");
      }
      else if (NULL == (schema_mutator = OB_NEW(ObSchemaMutator, ObModIds::OB_MS_SERVICE_FUNC)))
      {
          err = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "fail to new ObSchemaMutator");
      }
      else if (OB_SUCCESS != (err= schema_mutator->deserialize(in_buffer.get_data(),
                                 in_buffer.get_capacity(),in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "fail to deserialize schema:err[%d]", err);
      }
      else if (schema_mutator->get_refresh_schema())
      {
        //ignore, wait heartbeat task fetch full schema
        TBSYS_LOG(WARN, "schema_mutator contain refresh schema, ignore it");
      }
      else if (schema_mutator->get_end_version() <= schema_mgr_->get_latest_version())
      {
        TBSYS_LOG(WARN, "schema version too old. old=%ld, latest=%ld",
            schema_mutator->get_end_version(), schema_mgr_->get_latest_version());
        err = OB_OLD_SCHEMA_VERSION;
      }
      else
      {
        const ObSchemaManagerV2 *tmp = schema_mgr_->get_user_schema(0);
        if (tmp == NULL)
        {
              TBSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]",
                  tmp, schema_mgr_->get_latest_version());
              err = OB_INNER_STAT_ERROR;
        }
        else
        {
          schema->copy_without_sort(*tmp);
          if (OB_SUCCESS != (err = schema_mgr_->release_schema(tmp)))
          {
            TBSYS_LOG(WARN, "can not release schema ret[%d]", err);
          }
          else if (OB_SUCCESS != (err = schema->apply_schema_mutator(*schema_mutator)))
          {
            TBSYS_LOG(WARN, "apply_schema_mutator fail(mutator version[%ld->%ld])",
                      schema_mutator->get_start_version(), schema_mutator->get_end_version());
          }
          else if (OB_SUCCESS != (err = schema_mgr_->add_schema(*schema)))
          {
            TBSYS_LOG(WARN, "fail to add schema :err[%d]", err);
          }
        }        
      }

      OB_DELETE(ObSchemaMutator, ObModIds::OB_MS_SERVICE_FUNC, schema_mutator);
      OB_DELETE(ObSchemaManagerV2, ObModIds::OB_MS_SERVICE_FUNC, schema);

      ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fail to serialize: ret[%d]", ret);
      }
      else
      {
        ret = merge_server_->send_response(
            OB_SWITCH_SCHEMA_MUTATOR_RESPONSE,
            OB_MS_ACCEPT_SCHEMA_VERSION,
            out_buffer, req, channel_id);
      }

      return ret;
    }
    //add:e

    int ObMergeServerService::send_sql_response(
      easy_request_t* req,
      common::ObDataBuffer& out_buffer,
      ObSQLResultSet &result,
      int32_t channel_id,
      int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      ObPacket* next_request = NULL;
      bool fullfilled = false;
      ObPacketQueueThread &queue_thread =
        merge_server_->get_default_task_queue_thread();
	  //add peiouya [TIMEOUT_BUG_FIXED] 20141204:b
	  int64_t timeout_timestamp = tbsys::CTimeUtil::getTime() + timeout_us;
	  //add 20141204:e
      do
      {
        out_buffer.get_position() = 0;
        if (OB_SUCCESS == ret && OB_SUCCESS !=
            (ret = result.serialize(out_buffer.get_data(),
                                    out_buffer.get_capacity(),
                                    out_buffer.get_position())))
        {
          TBSYS_LOG(ERROR, "serilize result buffer error, ret = [%d]", ret);
          break;
        }
        result.get_fullfilled(fullfilled);
        if (result.is_select() && !fullfilled)
        {
          if (0 == session_id)
          {
            /* need a session id since data isn't fullfilled and
             * it is the first packet.
             */
            session_id = queue_thread.generate_session_id();
          }
          /* data isn't fullfilled so set a wait object */
          ret = queue_thread.prepare_for_next_request(session_id);
        }
        /* serialize result code successfully, if only a fullfilled
           packet session_id will been set to 0. */
        if (OB_SUCCESS == ret
            && OB_SUCCESS != (ret = merge_server_->send_response(
                                OB_SQL_EXECUTE_RESPONSE, 1, out_buffer,
                                req, response_cid, session_id)))
        {
          TBSYS_LOG(ERROR, "send response error! ret = [%d]", ret);
          break;
        }
        else if (result.is_select() && !fullfilled)
        {
          /* The last packet sent wasn't fullfulled, wait for next
           * request of client.
           */
		  //mod peiouya [TIMEOUT_BUG_FIXED] 20141204:b
		  //del b
          /*ret = queue_thread.wait_for_next_request(
            session_id, next_request,
            timeout_us - tbsys::CTimeUtil::getTime());
		  */
		  //del e
		  ret = queue_thread.wait_for_next_request(
            session_id, next_request,
            timeout_timestamp - tbsys::CTimeUtil::getTime());
		  //mod 20141204:e
          if (OB_NET_SESSION_END == ret)
          {
            /* merge server end this session.
             *
             * Cleanup resources when received a end session packet
             * althought there are data remained should been sent.
             */
            req = next_request->get_request();
            ret = OB_SUCCESS;
            easy_request_wakeup(req);
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout,"
                      " ret=%d", ret);
            break;
          }
          else
          {
            /* receive a `get_next_packet' packet successfully */
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
          }
        }
        else
        {
          /* no data should been sent, so break. */
          break;
        }
      } while (true);
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }
      return ret;
    }

    int ObMergeServerService::ms_sql_execute(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      UNUSED(start_time);
      UNUSED(version);
      int ret = OB_SUCCESS;
      ObString query_str;
      if (OB_SUCCESS !=
          (ret = query_str.deserialize(in_buffer.get_data(),
                                       in_buffer.get_capacity(),
                                       in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "fail to deserialize query string.");
      }
      else
      {
        TBSYS_LOG(TRACE, "internal query: [%.*s], length: [%d]",
                  query_str.length(), query_str.ptr(), query_str.length());
      }

      ObSQLResultSet rs;
      sql::ObSqlContext context;
      int64_t schema_version = 0;
      ObSQLSessionInfo session;
      int retcode = OB_SUCCESS;

      if (OB_SUCCESS == ret)
      {
        //add tianz [EXPORT_TOOL] 20141120:b
		ObString name = ObString::make_string(OB_QUERY_TIMEOUT_PARAM);
        ObObj type;
        type.set_type(ObIntType);
        ObObj value;
        value.set_int(timeout_us);
        TBSYS_LOG(INFO, "DEBUG:set query_timeout is %ld", timeout_us);
		//add 20141120:e
        if (OB_SUCCESS !=
            (ret = sql_proxy_.init_sql_env(context, schema_version,
                                           rs, session)))
        {
          TBSYS_LOG(WARN, "init sql env error.");
        }
		//add tianz [EXPORT_TOOL] 20141120:b
        else if (OB_SUCCESS != (ret = session.load_system_variable(name, type, value)))
        {
          TBSYS_LOG(ERROR, "load system variable %.*s failed, ret=%d", name.length(), name.ptr(), ret);
        }
		//add 20141120:e
        else
        {
          if (OB_SUCCESS != (ret = sql_proxy_.execute(query_str, rs,
                                                      context, schema_version)))
          {
            TBSYS_LOG(WARN, "execute sql failed. ret = [%d] sql = [%.*s]",
                      ret, query_str.length(), query_str.ptr());
          }
          else if (OB_SUCCESS != (ret = rs.open()))
          {
            TBSYS_LOG(ERROR, "open sql result set error, ret = [%d]", ret);
          }
          else
          {
            if (OB_SUCCESS != (retcode = send_sql_response(req, out_buffer, rs,
                                                       channel_id, timeout_us)))
            {
              TBSYS_LOG(ERROR, "send sql result error, sql: [%.*s], ret: [%d]",
                        query_str.length(), query_str.ptr(), retcode);
              ret = retcode;
            }
          }
          sql_proxy_.cleanup_sql_env(context, rs);
        }
      }
	  //mod peiouya [TIMEOUT_BUG_FIXED] 20141204:b
	  //if (OB_SUCCESS != ret)
      if (OB_SUCCESS != ret && OB_RESPONSE_TIME_OUT != ret)
	  //mod 20141204:e
      {
        /* if rs errno is not set.. */
        if (rs.get_errno() == OB_SUCCESS)
        {
          rs.set_errno(ret);
          rs.set_sqlstr(query_str);
        }
        if (OB_SUCCESS != (
              ret = rs.serialize(out_buffer.get_data(),
                                 out_buffer.get_capacity(),
                                 out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serilize result set error, ret: [%d]", ret);
        }
        else if (OB_SUCCESS != (
                   retcode = merge_server_->send_response(
                     OB_SQL_EXECUTE_RESPONSE, 1, out_buffer, req, channel_id)))
        {
          TBSYS_LOG(WARN, "send response error, ret: [%d]", retcode);
        }
      }
      return retcode;
    }

    int ObMergeServerService::ms_scan(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start serve ms_scan [peer:%s]", get_peer_ip(req));
      int64_t req_timeout_us = timeout_us;
      if ((0 >= timeout_us) || (timeout_us > merge_server_->get_config().max_req_process_time))
      {
        req_timeout_us = merge_server_->get_config().max_req_process_time;
        TBSYS_LOG(DEBUG, "reset timeoutus %ld", req_timeout_us);
      }
      int32_t request_cid = channel_id;
      const int32_t MS_SCAN_VERSION = 1;
      ObResultCode rc;
      char err_msg[MAX_ERROR_MSG_LEN]="";
      rc.message_.assign(err_msg, sizeof(err_msg));
      int32_t &err = rc.result_code_;
      int32_t send_err  = OB_SUCCESS;
      err = OB_SUCCESS;
      ObScanParam *org_scan_param = GET_TSI_MULT(ObScanParam, TSI_MS_ORG_SCAN_PARAM_1);
      ObScanner tablet_location_obscanner;
      ObScanParam *decoded_scan_param = GET_TSI_MULT(ObScanParam, TSI_MS_DECODED_SCAN_PARAM_1);
      ObReadParam * decoded_read_param = NULL;
      ObMergerScanParam *ms_scan_param = GET_TSI_MULT(ObMergerScanParam, TSI_MS_MS_SCAN_PARAM_1);
      ObScanner *result_scanner = GET_TSI_MULT(ObScanner, TSI_MS_SCANNER_1);
      ObMergerScanRequest *scan_event = GET_TSI_ARGS(ObMergerScanRequest, TSI_MS_SCAN_EVENT_1, cache_proxy_, async_rpc_);
      const ObSchemaManagerV2 *schema_mgr = NULL;
      const ObSchemaManagerV2 *user_schema_mgr = NULL;
      int64_t timeout_time = start_time + req_timeout_us;
      easy_connection_t* conn = req->ms->c;
      if (OB_SUCCESS == err && MS_SCAN_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == err &&
        ((NULL == org_scan_param)
        || (NULL == decoded_scan_param)
        || (NULL == result_scanner)
        || (NULL == ms_scan_param)
        || (NULL == scan_event)))
      {
        TBSYS_LOG(WARN,"fail to allocate memory for request");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS == err)
      {
        decoded_read_param = dynamic_cast<ObReadParam *>(decoded_scan_param);
        org_scan_param->set_location_info(tablet_location_obscanner);
        org_scan_param->reset();
        decoded_scan_param->reset();
        result_scanner->reset();
        ms_scan_param->reset();
        if ((!scan_event->inited())
          && ( OB_SUCCESS != (err = scan_event->init(REQUEST_EVENT_QUEUE_SIZE,ObModIds::OB_MS_REQUEST_EVENT, &session_mgr_))))
        {
          TBSYS_LOG(WARN,"fail to init ObMergerScanEvent [err:%d]", err);
        }
      }

      /// decode request
      if (OB_SUCCESS == err)
      {
        FILL_TRACE_LOG("step 1. start serve ms_scan [request_event_id:%lu]", scan_event->get_request_id());

        // get the local newest user schema, only fetch local cached schema, no rpc.
        //rpc_proxy_->fetch_new_user_schema(ObMergerRpcProxy::LOCAL_NEWEST, &user_schema_mgr);
        user_schema_mgr = schema_mgr_->get_user_schema(0);
        // decode scan param set user schema for compatible RowkeyInfo
        // scan system table will never be used.
        org_scan_param->set_compatible_schema(user_schema_mgr);

        err = org_scan_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to deserialize ObScanParam [err:%d]",err);
          snprintf(rc.message_.ptr(), rc.message_.length(), "fail to deserialize ObScanParam [err:%d]", err);
        }
        else
        {
          ObReadParam * org_read_param = NULL;
          decoded_scan_param->set_location_info(*(org_scan_param->get_location_info()));
          org_read_param = dynamic_cast<ObReadParam*>(org_scan_param);
          if (NULL != decoded_read_param && NULL != org_read_param)
          {
            *decoded_read_param = *org_read_param;
          }
          else
          {
            TBSYS_LOG(WARN, "fail to cast ObScanParam type to ObReadParam type. decoded_read_param=%p, org_read_param=%p",
                decoded_read_param, org_read_param);
            err = OB_ERROR;
          }
        }
        // user_schema_mgr can be release after scan param decoded.
        if (NULL != user_schema_mgr)
        {
          schema_proxy_->release_schema(user_schema_mgr);
        }
      }

      /// get local newest schema
      if (OB_SUCCESS == err)
      {
        err = schema_proxy_->get_schema(org_scan_param->get_table_name(), ObMergerSchemaProxy::LOCAL_NEWEST, &schema_mgr);
        if ((OB_SUCCESS != err) || (NULL == schema_mgr))
        {
          TBSYS_LOG(WARN, "fail to get the latest schema, unexpected error:table[%.*s], ret[%d]",
              org_scan_param->get_table_name().length(), org_scan_param->get_table_name().ptr(), err);
          err = OB_ERR_UNEXPECTED;
        }
        else if (org_scan_param->is_binary_rowkey_format())
        {
          result_scanner->set_binary_rowkey_format(true);
          result_scanner->set_compatible_schema(schema_mgr);
        }
      }

      bool is_cache_hit = false;
      uint64_t session_id = 0;
      bool got_all_result = false;
      int64_t net_session_id = 0;
      int64_t packet_cnt = 0;
      ObPacketQueueThread &queue_thread = merge_server_->get_default_task_queue_thread();
      ObPacket* next_request = NULL;
      int & err_code = rc.result_code_;

      if ((OB_SUCCESS == err) && NULL != query_cache_
          && org_scan_param->get_is_result_cached()
          && schema_mgr->get_table_query_cache_expire_time(org_scan_param->get_table_name()) > 0)
      {
        ObString result_key;
        result_key.assign_ptr(in_buffer.get_data(), static_cast<int32_t>(in_buffer.get_position()));
        if (OB_SUCCESS == query_cache_->get(result_key, out_buffer))
        {
          is_cache_hit = true;
        }
      }

      if (OB_SUCCESS == err && is_cache_hit)
      {
        // query cache hit
        if ((OB_SUCCESS != (send_err = merge_server_->send_response(OB_SCAN_RESPONSE,
          MS_SCAN_VERSION,out_buffer,req,request_cid, 0))))
        {
          TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
        }
        if (OB_SUCCESS == send_err)
        {
          packet_cnt ++;
        }
        FILL_TRACE_LOG("step 2. query cache hit, send_response:send_err[%d], code[%d], packet_cnt[%ld]",
          send_err, err, packet_cnt);
      }
      else
      {
        //query cache miss
        /// decode and check scan param
        if (OB_SUCCESS == err)
        {
          err = ob_decode_scan_param(*org_scan_param, *schema_mgr, *decoded_scan_param, &rc);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to decode scan param [err:%d]", err);
          }
        }

        if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (err = session_mgr_.session_begin(*org_scan_param,
           convert_addr_to_server(conn->addr),session_id,
           syscall(SYS_gettid), static_cast<pid_t>(pthread_self())))))
        {
          TBSYS_LOG(WARN,"fail to create session in SessionManager [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          scan_event->set_session_id(static_cast<uint32_t>(session_id));
        }

        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = ms_scan_param->set_param(*decoded_scan_param))))
        {
          TBSYS_LOG(WARN,"fail to set ObMergerScanParam [err:%d]", err);
        }
        FILL_TRACE_LOG("step 2. finish parse the schema for ms_scan:err[%d]", err);

        /// do request
        if (OB_SUCCESS == err)
        {
          scan_event->set_timeout_percent((int32_t)merge_server_->get_config().timeout_percent);
          const ObNewRange *range = ms_scan_param->get_ms_param()->get_range();
          if (range->is_whole_range())
          {
            err = scan_event->set_request_param(*ms_scan_param,
                                                merge_server_->get_config().intermediate_buffer_size,
                                                req_timeout_us, 1);
          }
          else
          {
            err = scan_event->set_request_param(*ms_scan_param,
                                                merge_server_->get_config().intermediate_buffer_size,
                                                req_timeout_us, merge_server_->get_config().max_parellel_count);
          }
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "scan request set request param failed [err:%d]", err);
          }
        }

        /// process result
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_event->wait(timeout_time - tbsys::CTimeUtil::getTime()))))
        {
          TBSYS_LOG(WARN, "scan request wait failed [err:%d]", err);
        }

        if (OB_SUCCESS == err)
        {
          FILL_TRACE_LOG("mem_size_used[%ld]", scan_event->get_mem_size_used());
        }

        /// prepare result
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_event->fill_result(*result_scanner,*org_scan_param,
          got_all_result))))
        {
          TBSYS_LOG(WARN, "fail to faill result [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (!got_all_result))
        {
          net_session_id = queue_thread.generate_session_id();
        }
        do
        {
          if ((OB_SUCCESS == err) && (!got_all_result) && (packet_cnt > 0))
          {
            result_scanner->reset();
            if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_event->fill_result(*result_scanner,*org_scan_param,
              got_all_result))))
            {
              TBSYS_LOG(WARN, "fail to fill result [err:%d]", err);
            }
          }
          if ((OB_SUCCESS == err) && !got_all_result && (OB_SUCCESS != queue_thread.prepare_for_next_request(net_session_id)))
          {
            TBSYS_LOG(WARN, "fail to prepare network session [err:%d]", err);
          }
          out_buffer.get_position() = 0;
          /// always send error code
          if ((OB_SUCCESS != (send_err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
            out_buffer.get_position()))))
          {
            TBSYS_LOG(WARN,"fail to serialize result code [err:%d]", send_err);
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
            && (OB_SUCCESS !=(send_err = result_scanner->serialize(out_buffer.get_data(),out_buffer.get_capacity(),
            out_buffer.get_position()))))
          {
            TBSYS_LOG(WARN,"fail to serialize result scanner [err:%d]", send_err);
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
              && got_all_result && 0 == packet_cnt && NULL != query_cache_
              && schema_mgr->get_table_query_cache_expire_time(org_scan_param->get_table_name()) > 0)
          {
            ObString result_key;
            ObQueryCacheValue result_value;
            result_key.assign_ptr(in_buffer.get_data(),  static_cast<int32_t>(in_buffer.get_capacity()));
            result_value.expire_time_ =
              schema_mgr->get_table_query_cache_expire_time(org_scan_param->get_table_name())
              + tbsys::CTimeUtil::getTime();
            result_value.size_ = out_buffer.get_position();
            result_value.buf_ = out_buffer.get_data();
            err = query_cache_->put(result_key, result_value);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to put scan result into query cache:err[%d]", err);
              err = OB_SUCCESS;
            }
          }
          FILL_TRACE_LOG("step 4.x. finish serialize scanner:err[%d]", send_err);
          if ((OB_SUCCESS == send_err) &&
            (OB_SUCCESS != (send_err = merge_server_->send_response(OB_SCAN_RESPONSE, MS_SCAN_VERSION,out_buffer,
            req,request_cid, (got_all_result?0:net_session_id)))))
          {
            TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
          }
          if (OB_SUCCESS == send_err)
          {
            packet_cnt ++;
          }
          FILL_TRACE_LOG("step 5.x. send_response:send_err[%d], code[%d], packet_cnt[%ld]",  send_err, err_code, packet_cnt);
          if ((OB_SUCCESS == err) && (!got_all_result) && (OB_SUCCESS == send_err))
          {
            err = queue_thread.wait_for_next_request(net_session_id,next_request,timeout_time - tbsys::CTimeUtil::getTime());
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to wait next request from session [err:%d,net_session_id:%ld]", err, net_session_id);
            }
            else
            {
              request_cid = next_request->get_channel_id();
              //add hongchen [UNLIMIT_TABLE] 20161031:b
              //here must reset req with the next_request, because requst of the send is changed
              req         = next_request->get_request();
              //add hongchen [UNLIMIT_TABLE] 20161031:e
            }
          }
        }while ((!got_all_result) && (OB_SUCCESS == send_err) && (OB_SUCCESS == err));
      }

      int64_t end_time = tbsys::CTimeUtil::getTime();
      if (OB_SUCCESS == err)
      {
        if (end_time - start_time > merge_server_->get_config().slow_query_threshold)
        {
          const char *session_info = NULL;
          if (NULL == (session_info = session_mgr_.get_session(session_id)))
          {
            TBSYS_LOG(WARN, "get empty session info");
          }
          else
          {
            TBSYS_LOG(WARN, "slow query:%s; used_time:%ld; result_size:%ld", session_info, end_time - start_time,
                result_scanner->get_size());
          }
        }
      }

      // check process timeout
      if (end_time > start_time + req_timeout_us)
      {
        err_code = OB_PROCESS_TIMEOUT;
        TBSYS_LOG(WARN, "check process scan task timeout:start[%ld], end[%ld], timeout[%ld], used[%ld]",
          start_time, end_time, req_timeout_us, end_time - start_time);
      }

      /// inc monitor counter
      if (NULL != service_monitor_ )
      {
        uint64_t table_id = decoded_scan_param->get_table_id();
        OB_STAT_TABLE_INC(MERGESERVER, table_id, NB_SCAN_OP_COUNT);
        OB_STAT_TABLE_INC(MERGESERVER, table_id, NB_SCAN_OP_TIME, end_time - start_time);
      }
      if (NULL != schema_mgr)
      {
        schema_proxy_->release_schema(schema_mgr);
      }

      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      if (NULL != scan_event)
      {
        scan_event->reset();
      }
      ObMSSchemaDecoderAssis *schema_assis = GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1);
      if (NULL != schema_assis && !is_cache_hit)
      {
        schema_assis->clear();
      }
      if (session_id > 0)
      {
        session_mgr_.session_end(session_id);
      }
      if (net_session_id > 0)
      {
        queue_thread.destroy_session(net_session_id);
      }
      return send_err;
    }

    int ObMergeServerService::ms_sql_scan(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      UNUSED(start_time);
      UNUSED(channel_id);
      UNUSED(req);
      UNUSED(timeout_us);
      const int32_t RS_SCAN_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      uint64_t  table_id = 0;
      ObNewScanner *new_scanner = GET_TSI_MULT(ObNewScanner, TSI_MS_NEW_SCANNER_1);
      sql::ObSqlScanParam *sql_scan_param_ptr = GET_TSI_MULT(sql::ObSqlScanParam,
                                                             TSI_MS_SQL_SCAN_PARAM_1);
      if (version != RS_SCAN_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == new_scanner || NULL == sql_scan_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local scan_param or new scanner, "
                  "new_scanner=%p, sql_scan_param_ptr=%p", new_scanner, sql_scan_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new_scanner->reuse();
        sql_scan_param_ptr->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = sql_scan_param_ptr->deserialize(
          in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_sql_scan input scan param error.");
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        table_id = sql_scan_param_ptr->get_table_id();
        if (OB_ALL_SERVER_STAT_TID == table_id)
        {
          /// update memory usage statistics
          OB_STAT_SET(MERGESERVER, MS_MEMORY_LIMIT, ob_get_memory_size_limit());
          OB_STAT_SET(MERGESERVER, MS_MEMORY_TOTAL, ob_get_memory_size_handled());
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_PARSER, ob_get_mod_memory_usage(ObModIds::OB_SQL_PARSER));
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_TRANSFORMER, ob_get_mod_memory_usage(ObModIds::OB_SQL_TRANSFORMER));
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_PS_PLAN, ob_get_mod_memory_usage(ObModIds::OB_SQL_PS_TRANS)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_RESULT_SET_DYN)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_PHY_PLAN)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_PS_STORE_PHYSICALPLAN)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_PS_STORE_OPERATORS)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_PS_STORE_ITEM));
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_RPC_REQUEST, ob_get_mod_memory_usage(ObModIds::OB_SQL_RPC_REQUEST));
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_ARRAY, ob_get_mod_memory_usage(ObModIds::OB_SQL_ARRAY));
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_EXPR, ob_get_mod_memory_usage(ObModIds::OB_SQL_EXPR));
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_ROW_STORE, ob_get_mod_memory_usage(ObModIds::OB_SQL_ROW_STORE));
          OB_STAT_SET(MERGESERVER, MS_SQL_MU_SESSION,
                      ob_get_mod_memory_usage(ObModIds::OB_SQL_SESSION)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_SESSION_POOL)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_SESSION_SBLOCK)
                      + ob_get_mod_memory_usage(ObModIds::OB_SQL_SESSION_HASHMAP));

          new_scanner->set_range(*sql_scan_param_ptr->get_range());
          rc.result_code_ = service_monitor_->get_scanner(*new_scanner);
        }
        else if (OB_ALL_SERVER_SESSION_TID == table_id)
        {
          new_scanner->set_range(*sql_scan_param_ptr->get_range());
          rc.result_code_ = sql_session_mgr_->get_scanner(*new_scanner);
        }
        else if (OB_ALL_STATEMENT_TID == table_id)
        {
          new_scanner->set_range(*sql_scan_param_ptr->get_range());
          rc.result_code_ = sql_id_mgr_->get_scanner(merge_server_->get_self(), *new_scanner);
        }
        if(OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "open query service fail:err[%d]", rc.result_code_);
        }
      }

      out_buffer.get_position() = 0;
      int serialize_ret = rc.serialize(out_buffer.get_data(),
                                       out_buffer.get_capacity(),
                                       out_buffer.get_position());
      if (OB_SUCCESS != serialize_ret)
      {
        TBSYS_LOG(ERROR, "serialize result code object failed.");
      }

      // if scan return success , we can return scanner.
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        serialize_ret = new_scanner->serialize(out_buffer.get_data(),
                                               out_buffer.get_capacity(),
                                               out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize ObScanner failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        merge_server_->send_response(OB_SQL_SCAN_RESPONSE, RS_SCAN_VERSION, out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObMergeServerService::ms_reload_config(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      const int32_t MS_RELOAD_VERSION = 1;
      UNUSED(start_time);
      UNUSED(in_buffer);
      UNUSED(version);
      UNUSED(timeout_us);
      ObResultCode rc;
      int &ret = rc.result_code_;

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = get_config_mgr().reload_config()))
        {
          TBSYS_LOG(WARN, "reload configuration failed, ret: [%d]", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "reload configuration succeffully.");
        }

        int32_t send_err  = OB_SUCCESS;
        ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS == ret)
        {
          send_err = merge_server_->send_response(OB_UPS_RELOAD_CONF_RESPONSE, MS_RELOAD_VERSION,
                                                  out_buffer, req, channel_id);
        }
        if (OB_SUCCESS != send_err)
        {
          TBSYS_LOG(WARN, "Send reload config response failed, ret: [%d]", send_err);
        }
      }

      return ret;
    }

    int ObMergeServerService::ms_change_log_level(
      const int64_t receive_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(receive_time);
      UNUSED(version);
      UNUSED(timeout_us);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position(), &log_level)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if ((TBSYS_LOG_LEVEL_ERROR <= log_level) && (TBSYS_LOG_LEVEL_DEBUG >= log_level))
        {
          TBSYS_LOG(INFO, "change log level. From: %d, To: %d", TBSYS_LOGGER._level, log_level);
          TBSYS_LOGGER._level = log_level;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          result.result_code_ = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS != (ret = result.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
            out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = merge_server_->send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buffer, req, channel_id);
        }
      }
      return ret;
    }

    int ObMergeServerService::ms_set_config(
      const int64_t receive_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      UNUSED(version);
      UNUSED(timeout_us);
      UNUSED(receive_time);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      common::ObString config_str;
      if (OB_SUCCESS != (ret = config_str.deserialize(
                           in_buffer.get_data(),
                           in_buffer.get_capacity(), in_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "Deserialize config string failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = get_config().add_extra_config(config_str.ptr(), true)))
      {
        TBSYS_LOG(ERROR, "Set config failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = get_config_mgr().reload_config()))
      {
        TBSYS_LOG(ERROR, "Reload config failed! ret: [%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "Set config successfully! str: [%s]", config_str.ptr());
        get_config().print();
      }

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buffer.get_data(),
                                             out_buffer.get_capacity(),
                                             out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = merge_server_->send_response(OB_SET_CONFIG_RESPONSE,
                                                  MY_VERSION, out_buffer, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    int ObMergeServerService::ms_get_config(
      const int64_t receive_time,
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      UNUSED(version);
      UNUSED(timeout_us);
      UNUSED(receive_time);
      UNUSED(in_buffer);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;

      if (NULL == merge_server_)
      {
        ret = OB_NOT_INIT;
      }

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buffer.get_data(),
                                             out_buffer.get_capacity(),
                                             out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS == res.result_code_ &&
               OB_SUCCESS !=
               (ret = merge_server_->get_config().serialize(out_buffer.get_data(),
                                                            out_buffer.get_capacity(),
                                                            out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize configuration fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = merge_server_->send_response(OB_GET_CONFIG_RESPONSE,
                                                  MY_VERSION, out_buffer, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }
  } /* mergeserver */
} /* oceanbase */
