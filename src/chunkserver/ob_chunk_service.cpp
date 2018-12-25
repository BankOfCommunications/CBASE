/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include "ob_chunk_service.h"
#include "ob_chunk_server.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range2.h"
#include "common/ob_result.h"
#include "common/file_directory_utils.h"
#include "common/ob_trace_log.h"
#include "common/ob_schema_manager.h"
#include "common/ob_version.h"
#include "common/ob_profile_log.h"
#include "sql/ob_sql_scan_param.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_tablet.h"
#include "ob_chunk_server_main.h"
#include "ob_query_service.h"
#include "ob_sql_query_service.h"
#include "ob_tablet_service.h"
#include "ob_sql_rpc_stub.h"
#include "common/ob_common_stat.h"
#include "ob_tablet_writer.h"
#include "ob_tablet_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::compactsstable;

namespace oceanbase
{
  namespace chunkserver
  {
    ObChunkService::ObChunkService()
    : chunk_server_(NULL), inited_(false),
      service_started_(false), in_register_process_(false),
      service_expired_time_(0),
      migrate_task_count_(0), disk_checker_(this), lease_checker_(this),
      merge_task_(this),
      //add wenghaixing [secondary index static_index_build] 20150302
      index_task_(this),index_check_task_(this),
      //add e
      fetch_ups_task_(this), report_tablet_task_(this),
      del_tablet_rwlock_(),
      build_index_version_(OB_INVALID_VERSION)//add liumz, [secondary index version management]20160413
    {
    }

    ObChunkService::~ObChunkService()
    {
      destroy();
    }

    /**
     * use ObChunkService after initialized.
     */
    int ObChunkService::initialize(ObChunkServer* chunk_server)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == chunk_server)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = timer_.init()))
      {
        TBSYS_LOG(ERROR, "init timer fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = ms_list_task_.init(
                                //mod pangtianze [Paxos rs_election] 20150708:b
                                //chunk_server->get_config().get_root_server(),
                                chunk_server->get_root_server(),
                                //mod:e
                                &chunk_server->get_client_manager(),
                                false)))
      {
        TBSYS_LOG(ERROR, "init ms list failt, ret: [%d]", ret);
      }
      else if (OB_SUCCESS !=
               (ret = chunk_server->get_config_mgr().init(
                 ms_list_task_, chunk_server->get_client_manager(), timer_)))
      {
        TBSYS_LOG(ERROR, "init chunk server config error, ret: [%d]", ret);
      }
      else
      {
        //add zhaoqiong [Schema Manager] 20150327:b
        //init schema_service_
        common_rpc_ = new(std::nothrow)ObCommonRpcStub();
        if (NULL == common_rpc_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_SUCCESS != (ret = common_rpc_->init(&(chunk_server->get_client_manager()))))
        {
          TBSYS_LOG(ERROR,"common_rpc init error");
        }
        else if (NULL == (scan_helper_ = new(std::nothrow)ObScanHelperImpl))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (schema_service_ = new(std::nothrow)ObSchemaServiceImpl))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          ms_.init(&ms_list_task_);
          scan_helper_->set_rpc_stub(common_rpc_);
          scan_helper_->set_scan_timeout(chunk_server->get_config().network_timeout);
          scan_helper_->set_mutate_timeout(chunk_server->get_config().network_timeout);
          scan_helper_->set_scan_retry_times(1);
          scan_helper_->set_ms_provider(&ms_);
          if (OB_SUCCESS != (ret = schema_service_->init(scan_helper_, false)))
          {
            TBSYS_LOG(ERROR,"init schema_service_ error");
          }
        }
      }
      if (OB_SUCCESS != ret)
      {
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
      }
      else
      {
        //add:e
        chunk_server_ = chunk_server;
        inited_ = true;
      }

      return ret;
    }

    /*
     * stop service, before chunkserver stop working thread.
     */
    int ObChunkService::destroy()
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        inited_ = false;
        timer_.destroy();
        service_started_ = false;
        in_register_process_ = false;
        chunk_server_ = NULL;
        //add zhaoqiong [Schema Manager] 20150327:b
        delete common_rpc_;
        common_rpc_ = NULL;
        delete scan_helper_;
        scan_helper_ = NULL;
        delete schema_service_;
        schema_service_ = NULL;
        //add:e
      }
      else
      {
        rc = OB_NOT_INIT;
      }

      return rc;
    }

    /**
     * ChunkServer must fetch schema from RootServer first.
     * then provide service.
     */
    int ObChunkService::start()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }
      else
      {
        rc = chunk_server_->init_merge_join_rpc();
        if (rc != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "init merge join rpc failed.");
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        else if (OB_SUCCESS != (rc = chunk_server_->get_rpc_proxy()->set_schema_service(schema_service_)))
        {
          TBSYS_LOG(ERROR, "rpc_proxy set_schema_service failed");
        }
        //add:e
      }

      if (OB_SUCCESS == rc)
      {
        rc = check_compress_lib(chunk_server_->get_config().check_compress_lib);
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "the compress lib in the list is not exist: rc=[%d]", rc);
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = load_tablets();
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "load local tablets error, rc=%d", rc);
        }
      }
      if (OB_SUCCESS == rc)
      {
        //mod pangtianze [Paxos rs_election] 20170228
        //rc = register_self();
        rc = register_self(true); ///first regist to rs
        //mod:e
      }
      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(lease_checker_,
            chunk_server_->get_config().lease_check_interval, false);
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(disk_checker_, chunk_server_->get_config().disk_check_interval, false);
      }

      if (OB_SUCCESS == rc)
      {
        //for the sake of simple,just update the stat per second
        rc = timer_.schedule(stat_updater_,1000000,true);
      }
      //add wenghaixing [secondary index static_index_build]20150321
      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(index_check_task_,10000000,true);
      }
      //add e

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(fetch_ups_task_,
          chunk_server_->get_config().fetch_ups_interval, false);
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(time_update_duty_, TimeUpdateDuty::SCHEDULE_PERIOD, true);
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(ms_list_task_, MsList::SCHEDULE_PERIOD, true);
      }

      if (OB_SUCCESS == rc)
      {
        ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
        rc = tablet_manager.start_merge_thread();
        if (rc != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"start merge thread failed.");
        }

        int64_t compactsstable_cache_size = chunk_server_->get_config().compactsstable_cache_size;
        if ((OB_SUCCESS == rc) && (compactsstable_cache_size > 0))
        {
          if ((rc = tablet_manager.start_cache_thread()) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"start cache thread failed");
          }
        }

        int64_t bypass_loader_threads = chunk_server_->get_config().bypass_sstable_loader_thread_num;
        if (OB_SUCCESS == rc && bypass_loader_threads > 0)
        {
          if (OB_SUCCESS != (rc = tablet_manager.start_bypass_loader_thread()))
          {
            TBSYS_LOG(ERROR, "start bypass sstable loader threads failed, ret=%d", rc);
          }
        }
        //add wenghaixing [secondary index static_index_build]20150302
        if(OB_SUCCESS == rc)
        {
          ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
          rc = tablet_manager.start_index_work();
        }
        //add e
      }

      return rc;
    }

    int ObChunkService::check_compress_lib(const char* compress_name_buf)
    {
      int rc = OB_SUCCESS;
      char temp_buf[OB_MAX_VARCHAR_LENGTH];
      ObCompressor* compressor = NULL;
      char* ptr = NULL;

      if (NULL == compress_name_buf)
      {
        TBSYS_LOG(ERROR, "compress name buf is NULL");
        rc = OB_INVALID_ARGUMENT;
      }
      else if (static_cast<int64_t>(strlen(compress_name_buf)) >= OB_MAX_VARCHAR_LENGTH)
      {
        TBSYS_LOG(ERROR, "compress name length >= OB_MAX_VARCHAR_LENGTH: compress name length=[%d]", static_cast<int>(strlen(compress_name_buf)));
        rc = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(temp_buf, compress_name_buf, strlen(compress_name_buf) + 1);
      }

      if (OB_SUCCESS == rc)
      {
        char* save_ptr = NULL;
        ptr = strtok_r(temp_buf, ":", &save_ptr);
        while (NULL != ptr)
        {
          if (NULL == (compressor = create_compressor(ptr)))
          {
            TBSYS_LOG(ERROR, "create compressor error: ptr=[%s]", ptr);
            rc = OB_ERROR;
            break;
          }
          else
          {
            destroy_compressor(compressor);
            TBSYS_LOG(INFO, "create compressor success: ptr=[%s]", ptr);
            ptr = strtok_r(NULL, ":", &save_ptr);
          }
        }
      }

      return rc;
    }

    int ObChunkService::load_tablets()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      // load tablets;
      if (OB_SUCCESS == rc)
      {
        int32_t size = 0;
        const int32_t *disk_no_array = tablet_manager.get_disk_manager().get_disk_no_array(size);
        if (disk_no_array != NULL && size > 0)
        {
          rc = tablet_manager.load_tablets(disk_no_array, size);
        }
        else
        {
          rc = OB_ERROR;
          TBSYS_LOG(ERROR, "get disk no array failed.");
        }
      }

      return rc;
    }
    //mod pangtianze [Paxos rs_election] 20170228:b
    //int ObChunkService::register_self_busy_wait(int32_t &status)
    int ObChunkService::register_self_busy_wait(int32_t &status, bool is_first)
    //mod:e
    {
      int rc = OB_SUCCESS;
      status = 0;
      char server_version[OB_SERVER_VERSION_LENGTH] = "";
      get_package_and_svn(server_version, sizeof(server_version));
      int64_t cluster_id = 0;
      //add pangtianze [Paxos rs_election] 20150707:b
      common::ObServer current_rs = chunk_server_->get_root_server();
      rs_mgr_.set_master_rs(current_rs);
      rs_mgr_.init_current_idx();
      chunk_server_->set_all_root_server(current_rs);
      //add:e
      while (inited_)
      {
        rc = CS_RPC_CALL_RS(register_server, chunk_server_->get_self(), false,
                            status,
                            cluster_id,
                            server_version);
        //mod pangtianze [Paxos rs_election] 20172014:b
        /*if (OB_SUCCESS == rc) break;
        if (OB_RESPONSE_TIME_OUT != rc && OB_NOT_INIT != rc)
        {
          TBSYS_LOG(ERROR, "register self to rootserver failed, rc=%d", rc);
          break;
        }*/
        if (OB_SUCCESS == rc)
        {
          TBSYS_LOG(INFO, "register self to rootserver successed, master rs=%s", chunk_server_->get_root_server().to_cstring());
          break;
        }
        else
        {
           TBSYS_LOG(WARN, "register self to rootserver failed, rc=%d", rc);
           //add bingo [Paxos ms/cs cluster_id] 20170401:b
           if(OB_CLUSTER_ID_ERROR == rc){
             TBSYS_LOG(WARN, "register self to rootserver failed, stop self, err=%d", rc);
             chunk_server_->stop();
           }
           //add:e
           if (!is_first)
           {
              //not first regist, so will use next rs
              current_rs = rs_mgr_.next_rs();
              chunk_server_->set_all_root_server(current_rs);
              TBSYS_LOG(INFO, "will regist to next rootserver=%s", current_rs.to_cstring());
           }
           else
           {
               //first regist, so will use the same rs
           }
        }
        //mod:e
        //mod chujiajia [Paxos rs_election] 20151113:b
        //usleep(static_cast<useconds_t>(chunk_server_->get_config().network_timeout));
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME));
        //mod:e
      }
      //del lbzhong [Paxos balance] 20160614:b
      /*
      if (OB_SUCCESS == rc)
      {
        chunk_server_->get_config().cluster_id = cluster_id;
      }
      */
      //del:e
      return rc;
    }

    int ObChunkService::report_tablets_busy_wait()
    {
      int rc = OB_SUCCESS;
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      while (inited_ && (service_started_ || (in_register_process_ && !service_started_)))
      {
        rc = tablet_manager.report_tablets();
        if (OB_SUCCESS == rc) break;
        if (OB_RESPONSE_TIME_OUT != rc && OB_CS_EAGAIN != rc)
        {
          TBSYS_LOG(ERROR, "report tablets to rootserver failed, rc=%d", rc);
          break;
        }
        usleep(static_cast<useconds_t>(chunk_server_->get_config().network_timeout));
      }
      return rc;
    }

    int ObChunkService::fetch_schema_busy_wait(ObSchemaManagerV2 *schema)
    {
      int rc = OB_SUCCESS;
      if (NULL == schema)
      {
        TBSYS_LOG(ERROR,"invalid argument,sceham is null");
        rc = OB_INVALID_ARGUMENT;
      }
      else
      {
        while (inited_)
        {
          CS_RPC_CALL_RS(fetch_schema, 0, false, *schema);
          if (OB_SUCCESS == rc) break;
          if (OB_RESPONSE_TIME_OUT != rc)
          {
            TBSYS_LOG(ERROR, "report tablets to rootserver failed, rc=%d", rc);
            break;
          }
          usleep(static_cast<useconds_t>(chunk_server_->get_config().network_timeout));
        }
      }
      return rc;
    }
    //mod pangtianze [Paxos rs_election] 20170228:b
    //int ObChunkService::register_self()
    int ObChunkService::register_self(bool is_first)
    //mod:e
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot register_self.");
        rc = OB_NOT_INIT;
      }

      if (in_register_process_)
      {
        TBSYS_LOG(ERROR, "another thread is registering.");
        rc = OB_ERROR;
      }
      else
      {
        in_register_process_ = true;
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      //const ObChunkServerParam & param = chunk_server_->get_config();
      //ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
      int32_t status = 0;
      // register self to rootserver until success.
      if (OB_SUCCESS == rc)
      {
        //mod pangtianze [Paxos rs_election] 20170228
        //rc = register_self_busy_wait(status);
        rc = register_self_busy_wait(status, is_first);
        //mod:e
      }

      if (OB_SUCCESS == rc)
      {
        // TODO init lease 10s for first startup.
        service_expired_time_ = tbsys::CTimeUtil::getTime() + 10000000;
        int64_t current_data_version = tablet_manager.get_serving_data_version();
        if (0 == status)
        {
          TBSYS_LOG(INFO, "system startup on first time, wait rootserver start new schema,"
              "current data version=%ld", current_data_version);
          // start chunkserver on very first time, do nothing, wait rootserver
          // launch the start_new_schema process.
          //service_started_ = true;
        }
        else
        {
          TBSYS_LOG(INFO, "chunk service start, current data version: %ld", current_data_version);
          rc = report_tablets_busy_wait();
          if (OB_SUCCESS == rc)
          {
            tablet_manager.report_capacity_info();
            service_started_ = true;
          }
        }
      }

      in_register_process_ = false;

      return rc;
    }
    /*
     * after initialize() & start(), then can handle the request.
     */
    int ObChunkService::do_request(
        const int64_t receive_time,
        const int32_t packet_code,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        rc = OB_NOT_INIT;
      }

      if (OB_SUCCESS == rc)
      {
        if (!service_started_
            && packet_code != OB_START_MERGE
            && packet_code != OB_REQUIRE_HEARTBEAT
            && packet_code != OB_RS_REQUEST_REPORT_TABLET)
        {
          TBSYS_LOG(ERROR, "service not started, only accept "
              "start schema message, heatbeat, or force report tablet from rootserver.");
          rc = OB_CS_SERVICE_NOT_STARTED;
        }
      }
      if (OB_SUCCESS == rc)
      {
        //check lease valid.
        if (!is_valid_lease()
            && (!in_register_process_)
            && packet_code != OB_REQUIRE_HEARTBEAT)
        {
          // TODO re-register self??
          TBSYS_LOG(WARN, "lease expired, wait timer schedule re-register self to rootserver.");
        }
      }

      if (OB_SUCCESS != rc)
      {
        TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d",
            packet_code, rc);
        common::ObResultCode result;
        result.result_code_ = rc;
        // send response.
        int serialize_ret = result.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
        }
        else
        {
          chunk_server_->send_response(
              packet_code + 1, version,
              out_buffer, req, channel_id);
        }
      }
      else
      {
        switch(packet_code)
        {
          case OB_GET_REQUEST:
            rc = cs_get(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_SQL_GET_REQUEST:
            rc = cs_sql_get(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_CS_MIGRATE:
            rc = cs_fetch_data(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_FETCH_DATA:
            rc = cs_tablet_read(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_SQL_SCAN_REQUEST:
            rc = cs_sql_scan(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_SCAN_REQUEST:
            rc = cs_scan(receive_time, version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_BATCH_GET_REQUEST:
            rc = cs_batch_get(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_DROP_OLD_TABLETS:
            rc = cs_drop_old_tablets(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_REQUIRE_HEARTBEAT:
            rc = cs_heart_beat(version, channel_id, req, in_buffer, out_buffer);
            break;
          //add pangtianze [Paxos rs_switch] 20170208:b
          case OB_FORCE_SERVER_REGIST:
            rc = cs_force_regist(version, channel_id, req, in_buffer, out_buffer);
            break;
          //add:e
          //add pangtianze [Paxos rs_election] 20170301:b
          case OB_REFRESH_RS_LIST:
            rc = cs_refresh_rs_list(version, channel_id, req, in_buffer, out_buffer);
            break;
          //add:e
          //add wenghaixing [secondary index] 20141110
          case OB_CS_CREATE_INDEX_SIGNAL:
            rc =cs_static_index(version, channel_id, req, in_buffer, out_buffer);
            TBSYS_LOG(INFO,"This CS has recieved a signal for index creating");
            break;
          //add e
          //add wenghaixing [secondary index static_index_build.exceptional_handle] 20150409
          case OB_WHIPPING_WOK:
            rc = cs_recieve_wok(version, channel_id, req, in_buffer, out_buffer);
            TBSYS_LOG(INFO,"This CS has recieved a wok!");
            break;
          //add e
          case OB_MIGRATE_OVER:
            rc = cs_load_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_DELETE_TABLETS:
            rc = cs_delete_tablets(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_CREATE_TABLE:
            rc = cs_create_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_SWITCH_SCHEMA:
            rc = cs_accept_schema(version, channel_id, req, in_buffer, out_buffer);
            break;
          //add zhaoqiong [Schema Manager] 20150327:b
          case OB_SWITCH_SCHEMA_MUTATOR:
            rc = cs_accept_schema_mutator(version, channel_id, req, in_buffer, out_buffer);
            break;
          //add:e
          case OB_CS_GET_MIGRATE_DEST_LOC:
            rc = cs_get_migrate_dest_loc(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_DUMP_TABLET_IMAGE:
            rc = cs_dump_tablet_image(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_FETCH_STATS:
            rc = cs_fetch_stats(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_START_GC:
            rc = cs_start_gc(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_UPS_RELOAD_CONF:
            rc = cs_reload_conf(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SHOW_PARAM:
            rc = cs_show_param(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_STOP_SERVER:
            rc = cs_stop_server(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CHANGE_LOG_LEVEL:
            rc = cs_change_log_level(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_RS_REQUEST_REPORT_TABLET:
            rc = cs_force_to_report_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_CHECK_TABLET:
            rc = cs_check_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_MERGE_TABLETS:
            rc = cs_merge_tablets(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_SEND_FILE_REQUEST:
            rc = cs_send_file(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SYNC_ALL_IMAGES:
            rc = cs_sync_all_images(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_LOAD_BYPASS_SSTABLE:
            rc = cs_load_bypass_sstables(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_DELETE_TABLE:
            rc = cs_delete_table(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_FETCH_SSTABLE_DIST:
            rc = cs_fetch_sstable_dist(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_SET_CONFIG:
            rc = cs_set_config(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_GET_CONFIG:
            rc = cs_get_config(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_FETCH_BLOOM_FILTER:
            rc = cs_get_bloom_filter(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_CS_INSTALL_DISK:
            rc = cs_install_disk(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_UNINSTALL_DISK:
            rc = cs_uninstall_disk(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SHOW_DISK:
            rc = cs_show_disk(version, channel_id, req, in_buffer, out_buffer);
            break;
          default:
            rc = OB_ERROR;
            TBSYS_LOG(WARN, "not support packet code[%d]", packet_code);
            break;
        }
      }
      return rc;
    }

    int ObChunkService::cs_send_file(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      return chunk_server_->get_file_service().handle_send_file_request(
          version, channel_id, req, in_buffer, out_buffer);
    }

    int ObChunkService::cs_batch_get(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      // TODO  not implement yet.
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(req);
      UNUSED(in_buffer);
      UNUSED(out_buffer);
      return OB_SUCCESS;
    }

    int ObChunkService::get_sql_query_service(ObSqlQueryService *&service)
    {
      int ret = OB_SUCCESS;
      service = GET_TSI_ARGS(ObTabletService, TSI_CS_TABLET_SERVICE_1, *chunk_server_);
      if (NULL == service)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "get thread specific ObSqlQueryService fail:ret[%d]", ret);
      }

      return ret;
    }

    int ObChunkService::get_query_service(ObQueryService *&service)
    {
      int ret = OB_SUCCESS;
      service =  GET_TSI_ARGS(ObQueryService, TSI_CS_QUERY_SERVICE_1, *chunk_server_);
      if (NULL == service)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      return ret;
    }

    int ObChunkService::cs_get(
        const int64_t start_time,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      const int32_t CS_GET_VERSION = 1;
      uint64_t table_id = OB_INVALID_ID;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObQueryService* query_service = NULL;
      ObScanner* scanner = GET_TSI_MULT(ObScanner, TSI_CS_SCANNER_1);
      ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_CS_GET_PARAM_1);
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      int64_t ups_data_version = 0;
      bool is_fullfilled = true;
      int64_t fullfilled_num = 0;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread =
        chunk_server_->get_default_task_queue_thread();
      easy_addr_t addr = get_easy_addr(req);
      FILL_TRACE_LOG("start cs_get");

      if (version != CS_GET_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == scanner || NULL == get_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local get_param or scanner, "
            "scanner=%p, get_param_ptr=%p", scanner, get_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        rc.result_code_ = get_query_service(query_service);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->reset();
        rc.result_code_ = get_param_ptr->deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());

        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_get input param error.");
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        if (get_param_ptr->get_cell_size() <= 0)
        {
          TBSYS_LOG(WARN, "invalid param, cell_size=%ld",get_param_ptr->get_cell_size());
          rc.result_code_ = OB_INVALID_ARGUMENT;
        }
        else if (NULL == get_param_ptr->get_row_index() || get_param_ptr->get_row_size() <= 0)
        {
          TBSYS_LOG(WARN, "invalid get param, row_index=%p, row_size=%ld",
              get_param_ptr->get_row_index(), get_param_ptr->get_row_size());
          rc.result_code_ = OB_INVALID_ARGUMENT;
        }
        else
        {
          // FIXME: the count is not very accurate,we just inc the get count of the first table
          table_id = (*get_param_ptr)[0]->table_id_;
          OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_COUNT);
          rc.result_code_ = query_service->get(*get_param_ptr, *scanner, timeout_time);
        }
      }

      FILL_TRACE_LOG("finish get, ret=%d,", rc.result_code_);

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
        if (!is_fullfilled)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      do
      {
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
        }

        // send response. return result code anyway.
        out_buffer.get_position() = 0;
        int serialize_ret = rc.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
          break;
        }

        // if get return success, we can return the scanner.
        if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
        {
          serialize_ret = scanner->serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position());
          if (OB_SUCCESS != serialize_ret)
          {
            TBSYS_LOG(ERROR, "serialize ObScanner failed.");
            break;
          }
          ups_data_version = scanner->get_data_version();
        }

        if (OB_SUCCESS == serialize_ret)
        {
          OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_BYTES, out_buffer.get_position());
          chunk_server_->send_response(
              OB_GET_RESPONSE, CS_GET_VERSION,
              out_buffer, req, response_cid, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled)
        {
          scanner->reset();
          rc.result_code_ = queue_thread.wait_for_next_request(
            session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d",
              rc.result_code_);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            rc.result_code_ = query_service->fill_get_data(*get_param_ptr, *scanner);
            scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          }
        }
        else
        {
          //error happen or fullfilled
          break;
        }
      } while (true);

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_TIME, consume_time);

      if (NULL != get_param_ptr
          && consume_time >= chunk_server_->get_config().slow_query_warn_time)
      {
        TBSYS_LOG(WARN, "slow get: table_id:%lu, "
            "row size=%ld, peer=%s, rc=%d, scanner size=%ld,  consume=%ld",
            table_id,  get_param_ptr->get_row_size(),
                  inet_ntoa_r(addr),
                  rc.result_code_, scanner->get_size(), consume_time);
      }

      FILL_TRACE_LOG("send get response, packet_cnt=%ld, session_id=%ld, ret=%d",
        packet_cnt, session_id, rc.result_code_);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      bool release_table = true;
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      ObTabletManager::ObGetThreadContext*& get_context = tablet_manager.get_cur_thread_get_contex();
      if (get_context != NULL)
      {
        int64_t tablet_count = get_context->tablets_count_;
        for (int64_t i=0; i< tablet_count; ++i)
        {
          if ((get_context->tablets_[i] != NULL) &&
              (check_update_data(*get_context->tablets_[i],ups_data_version,release_table) != OB_SUCCESS))
          {
            TBSYS_LOG(WARN,"Problem of check frozen data of this tablet"); //just warn
          }
          else if (!release_table)
          {
            get_context->tablets_[i] = NULL; //do not release
          }
          else
          {
            //have new frozen table
          }
        }
      }

      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }
      query_service->end_get();
      reset_query_thread_local_buffer();
      chunk_server_->get_tablet_manager().end_get();

      return rc.result_code_;
    }

    int ObChunkService::reset_internal_status(bool release_table /*=true*/)
    {
      int ret = OB_SUCCESS;

      /**
       * if this function fails, it means that some critical problems
       * happen, don't kill chunk server here, just output some error
       * info, then we can restart this chunk server manualy.
       */
      ret = chunk_server_->get_tablet_manager().end_scan(release_table);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to end scan to release resources");
      }

      ret = reset_query_thread_local_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to reset query thread local buffer");
      }

      ret = wait_aio_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to wait aio buffer free");
      }

      return ret;
    }

    int ObChunkService::cs_show_disk(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      const int32_t CS_SHOW_DISK_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_SHOW_DISK_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
        ObDiskManager & disk_manager = tablet_manager.get_disk_manager();

        int32_t disk_num = 0;
        int32_t serialize_ret = 0;
        const int32_t* disk_no_array = disk_manager.get_disk_no_array(disk_num);

        if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serialize the rc failed, ret:%d", serialize_ret);
        }

        if (OB_SUCCESS == serialize_ret)
        {
          serialize_ret = serialization::encode_i32(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), disk_num);
        }

        if (OB_SUCCESS == serialize_ret && NULL != disk_no_array)
        {
          for (int32_t i = 0; i < disk_num; i++)
          {
            if (OB_SUCCESS == serialize_ret)
            {
              serialize_ret = serialization::encode_i32(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), disk_no_array[i]);
            }

            if (OB_SUCCESS == serialize_ret)
            {
              serialize_ret = serialization::encode_bool(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(),
                  disk_manager.is_disk_avail(disk_no_array[i]));
            }
          }
        }

        if (OB_SUCCESS == serialize_ret)
        {
          chunk_server_->send_response(
              OB_CS_SHOW_DISK_RESPONSE,
              CS_SHOW_DISK_VERSION,
              out_buffer, req, channel_id);
        }
        else
        {
          TBSYS_LOG(WARN, "serialize failed, ret:%d", serialize_ret);
        }
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_install_disk(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      common::ObResultCode rc;
      const int32_t CS_INSTALL_DISK_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if(version != CS_INSTALL_DISK_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if(chunk_server_->get_tablet_manager().is_disk_maintain())
      {
        TBSYS_LOG(WARN, "disk is maintaining");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot do disk maintain");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int32_t disk_no = -1;
      char mount_path[OB_MAX_FILE_NAME_LENGTH];
      memset(mount_path, 0, sizeof(mount_path));

      ObString mount_path_str(static_cast<common::ObString::obstr_size_t>(OB_MAX_FILE_NAME_LENGTH),
          static_cast<common::ObString::obstr_size_t>(OB_MAX_FILE_NAME_LENGTH), mount_path);

      if(OB_SUCCESS != rc.result_code_)
      {
      }
      else if (OB_SUCCESS != (rc.result_code_ = mount_path_str.deserialize(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "decode mount path failed");
      }
      else
      {
        rc.result_code_ = chunk_server_->get_tablet_manager().install_disk(mount_path, disk_no);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "cs_load_disk rc.serialize error, ret:%d", serialize_ret);
      }
      else if (OB_SUCCESS != (serialize_ret = serialization::encode_vi32(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position(), disk_no)))
      {
        TBSYS_LOG(WARN, "cs_load_disk mount disk_no:%d serialize failed, ret:%d",
            disk_no, serialize_ret);
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_INSTALL_DISK_RESPONSE,
            CS_INSTALL_DISK_VERSION,
            out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_uninstall_disk(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      common::ObResultCode rc;
      const int32_t CS_UNINSTALL_DISK_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if(version != CS_UNINSTALL_DISK_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if(chunk_server_->get_tablet_manager().is_disk_maintain())
      {
        TBSYS_LOG(WARN, "disk is maintaining");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot do disk maintain");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int32_t disk_no = -1;

      if(OB_SUCCESS != rc.result_code_)
      {
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(), &disk_no)))
      {
        TBSYS_LOG(WARN, "parse disk maintain param error.");
      }
      else
      {
        rc.result_code_ = chunk_server_->get_tablet_manager().uninstall_disk(disk_no);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "cs_load_disk rc.serialize error, ret:%d", serialize_ret);
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_UNINSTALL_DISK_RESPONSE,
            CS_UNINSTALL_DISK_VERSION,
            out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_sql_scan(
        const int64_t start_time,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      sql::ObSqlScanParam *sql_scan_param_ptr = GET_TSI_MULT(sql::ObSqlScanParam, TSI_CS_SQL_SCAN_PARAM_1);
      FILL_TRACE_LOG("start_cs_sql_scan");
      return cs_sql_read(start_time, version, channel_id, req, in_buffer, out_buffer, timeout_time, sql_scan_param_ptr);
    }

    int ObChunkService::cs_sql_get(
        const int64_t start_time,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      sql::ObSqlGetParam *sql_get_param_ptr = GET_TSI_MULT(sql::ObSqlGetParam, TSI_CS_SQL_GET_PARAM_1);
      FILL_TRACE_LOG("start_cs_sql_get");
      return cs_sql_read(start_time, version, channel_id, req, in_buffer, out_buffer, timeout_time, sql_get_param_ptr);
    }

    int ObChunkService::cs_sql_read(
        const int64_t start_time,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time,
        sql::ObSqlReadParam *sql_read_param_ptr)
    {
      const int32_t CS_SCAN_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      uint64_t  table_id = OB_INVALID_ID; //for stat
      bool is_scan = true; //for stat
      ObNewScanner* new_scanner = GET_TSI_MULT(ObNewScanner, TSI_CS_NEW_SCANNER_1);
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      bool is_fullfilled = true;
      int64_t fullfilled_num = 0;
      int64_t ups_data_version = 0;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread =
        chunk_server_->get_default_task_queue_thread();
      ObSqlQueryService *sql_query_service = NULL;
      easy_addr_t addr = get_easy_addr(req);
      bool force_trace_log = REACH_TIME_INTERVAL(100*1000LL);
      SET_FORCE_TRACE_LOG(force_trace_log);

      INIT_PROFILE_LOG_TIMER();

      if (version != CS_SCAN_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == new_scanner || NULL == sql_read_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local scan_param or new scanner, "
            "new_scanner=%p, sql_read_param_ptr=%p", new_scanner, sql_read_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new_scanner->reuse();
        sql_read_param_ptr->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = sql_read_param_ptr->deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_sql_scan input scan param error. ret=%d", rc.result_code_);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        const ObSqlScanParam *sql_scan_param_ptr = dynamic_cast<const ObSqlScanParam *>(sql_read_param_ptr);
        is_scan = (NULL != sql_scan_param_ptr);
        table_id = sql_read_param_ptr->get_table_id();
        FILL_TRACE_LOG("deserialize_param_done, is_scan=%d,table_id=%ld", is_scan, table_id);
        if (table_id == OB_ALL_SERVER_STAT_TID)
        {
          if (NULL == sql_scan_param_ptr)
          {
            rc.result_code_ = OB_NOT_SUPPORTED;
            TBSYS_LOG(WARN, "get method is not supported for all server stat table ");
          }
          else
          {
            is_last_packet = true;
            new_scanner->set_range(*sql_scan_param_ptr->get_range());
            ObStatSingleton::get_instance()->get_scanner(*new_scanner);
          }
          FILL_TRACE_LOG("get_server_stat_done");
        }
        else
        {
          if (OB_SUCCESS == rc.result_code_)
          {
            if(OB_SUCCESS != (rc.result_code_ = get_sql_query_service(sql_query_service)))
            {
              TBSYS_LOG(WARN, "get sql query service fail:ret[%d]", rc.result_code_);
            }
            else
            {
              sql_query_service->reset();
            }
          }

          if (OB_SUCCESS == rc.result_code_)
          {
            sql_query_service->set_timeout_us(timeout_time);
            table_id = sql_read_param_ptr->get_table_id();

            if (is_scan)
            {
              OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_COUNT);
            }
            else
            {
              OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_COUNT);
            }

            PROFILE_LOG_TIME(DEBUG, "begin open sql_query_service, rc=%d, table_id=%ld", rc.result_code_, table_id);
            rc.result_code_ = sql_query_service->open(*sql_read_param_ptr);
            PROFILE_LOG(DEBUG, "open sql_query_service complete, rc=%d", rc.result_code_);

            if(OB_SUCCESS != rc.result_code_)
            {
              TBSYS_LOG(WARN, "open query service fail:err[%d]", rc.result_code_);
            }
            FILL_TRACE_LOG("open sql_query_service done rc=%d", rc.result_code_);
          }

          if(OB_SUCCESS == rc.result_code_)
          {
            rc.result_code_ = sql_query_service->fill_scan_data(*new_scanner);
            if (OB_ITER_END == rc.result_code_)
            {
              is_last_packet = true;
              rc.result_code_ = OB_SUCCESS;
            }
            if (OB_SUCCESS != rc.result_code_)
            {
              TBSYS_LOG(WARN, "failed to fill_scan_data:err[%d]", rc.result_code_);
            }
            FILL_TRACE_LOG("first fill_data_done, is_last_packet=%d, rc=%d", is_last_packet, rc.result_code_);
          }
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        new_scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
        if (!is_fullfilled && !is_last_packet)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      PROFILE_LOG_TIME(DEBUG, "first fill scan data, #row=%ld,  is_last_packet=%d, "
          "is_fullfilled=%ld, ret=%d, session_id=%ld.", fullfilled_num,
          is_last_packet, is_fullfilled, rc.result_code_, session_id);

      do
      {
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
        }
        // send response.
        out_buffer.get_position() = 0;
        int serialize_ret = rc.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
          break ;
        }

        // if scan return success , we can return scanner.
        if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
        {
          serialize_ret = new_scanner->serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position());
          ups_data_version = new_scanner->get_data_version();
          if (OB_SUCCESS != serialize_ret)
          {
            TBSYS_LOG(ERROR, "serialize ObScanner failed.");
            break;
          }
        }

        if (OB_SUCCESS == serialize_ret)
        {
          if (is_scan)
          {
            OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_BYTES, out_buffer.get_position());
          }
          else
          {
            OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_BYTES, out_buffer.get_position());
          }
          chunk_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_SQL_SCAN_RESPONSE, CS_SCAN_VERSION,
              out_buffer, req, response_cid, session_id);
          PROFILE_LOG_TIME(DEBUG, "send response, #packet=%ld, is_last_packet=%d, session_id=%ld",
              packet_cnt, is_last_packet, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)
        {
          new_scanner->reuse();
          rc.result_code_ = queue_thread.wait_for_next_request(
            session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            //merge server end this session
            rc.result_code_ = OB_SUCCESS;
            if (NULL != next_request)
            {
              req = next_request->get_request();
              easy_request_wakeup(req);
            }
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d",
              rc.result_code_);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
            rc.result_code_ = sql_query_service->fill_scan_data(*new_scanner);
            if (OB_ITER_END == rc.result_code_)
            {
              /**
               * the last packet is not always with true fullfilled flag,
               * maybe there is not enough memory to query the normal scan
               * request, we just return part of result, user scan the next
               * data if necessary. it's order to be compatible with 0.2
               * version.
               */
              is_last_packet = true;
              rc.result_code_ = OB_SUCCESS;
            }
            new_scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
            PROFILE_LOG_TIME(DEBUG, "next fill scan data, #row=%ld,  is_last_packet=%d, "
                "is_fullfilled=%ld, ret=%d, session_id=%ld.", fullfilled_num,
                is_last_packet, is_fullfilled, rc.result_code_, session_id);
          }
        }
        else
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      FILL_TRACE_LOG("send response, packet_cnt=%ld, session_id=%ld, io stat: %s, ret=%d",
          packet_cnt, session_id, get_io_stat_str(), rc.result_code_);

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      if (is_scan)
      {
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_TIME, consume_time);
      }
      else
      {
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_TIME, consume_time);
      }

      //if ups have frozen mem table,get it
      bool release_tablet = true;

      if (OB_SUCCESS == rc.result_code_)
      {
        ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
        ObTablet*& tablet = tablet_manager.get_cur_thread_scan_tablet();
        if (tablet != NULL && check_update_data(*tablet,ups_data_version,release_tablet) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"check update data failed"); //just warn
        }
      }

      FILL_TRACE_LOG("send response, packet_cnt=%ld, session_id=%ld, ret=%d",
        packet_cnt, session_id, rc.result_code_);

      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      if(NULL != sql_query_service)
      {
        sql_query_service->close();
      }
      reset_internal_status(release_tablet);

      FILL_TRACE_LOG("end");
      if (consume_time >= chunk_server_->get_config().slow_query_warn_time)
      {
        if (is_scan)
        {
          ObSqlScanParam *sql_scan_param_ptr = reinterpret_cast<ObSqlScanParam *>(sql_read_param_ptr);
          TBSYS_LOG(WARN, "slow sql scan: table_id:%lu, scan_range=%s, data version=%ld, read mode=%d, peer=%s, rc=%d, scanner size=%ld, io_stat:%s consume=%ld",
            sql_scan_param_ptr->get_table_id(),  to_cstring(*(sql_scan_param_ptr->get_range())),
            sql_scan_param_ptr->get_data_version(), sql_scan_param_ptr->get_read_mode(),
            inet_ntoa_r(addr), rc.result_code_, new_scanner->get_size(), get_io_stat_str(), consume_time);
        }
        else
        {
          ObSqlGetParam *sql_get_param_ptr = reinterpret_cast<ObSqlGetParam *>(sql_read_param_ptr);
          TBSYS_LOG(WARN, "slow sql get: table_id:%lu, row size=%ld, peer=%s, rc=%d, scanner size=%ld, io_stat:%s consume=%ld",
            sql_get_param_ptr->get_table_id(), sql_get_param_ptr->get_row_size(),
            inet_ntoa_r(addr), rc.result_code_, new_scanner->get_size(), get_io_stat_str(), consume_time);
        }
        if (force_trace_log)
        {
          FORCE_PRINT_TRACE_LOG();
        }
        else
        {
          PRINT_TRACE_LOG();
        }
      }
      else
      {
        PRINT_TRACE_LOG();
      }
      sql_read_param_ptr->reset();

      return rc.result_code_;
    }

    int ObChunkService::cs_scan(
        const int64_t start_time,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      const int32_t CS_SCAN_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      uint64_t  table_id = OB_INVALID_ID; //for stat
      ObQueryService* query_service = NULL;
      ObScanner* scanner = GET_TSI_MULT(ObScanner, TSI_CS_SCANNER_1);
      common::ObScanParam *scan_param_ptr = GET_TSI_MULT(ObScanParam, TSI_CS_SCAN_PARAM_1);
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      bool is_fullfilled = true;
      int64_t fullfilled_num = 0;
      int64_t ups_data_version = 0;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread =
        chunk_server_->get_default_task_queue_thread();
      easy_addr_t addr = get_easy_addr(req);
      char sql[1024] = "";
      int64_t pos = 0;

      FILL_TRACE_LOG("start cs_scan");

      if (version != CS_SCAN_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == scanner || NULL == scan_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local scan_param or scanner, "
            "scanner=%p, scan_param_ptr=%p", scanner, scan_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        rc.result_code_ = get_query_service(query_service);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->reset();
        rc.result_code_ = scan_param_ptr->deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_scan input scan param error.");
        }
        else
        {
          // force every client scan request use preread mode.
          scan_param_ptr->set_read_mode(ScanFlag::ASYNCREAD);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        table_id = scan_param_ptr->get_table_id();
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_COUNT);
        rc.result_code_ = query_service->scan(*scan_param_ptr, *scanner, timeout_time);
        if (OB_ITER_END == rc.result_code_)
        {
          is_last_packet = true;
          rc.result_code_ = OB_SUCCESS;
        }
      }

      FILL_TRACE_LOG("finish scan, sql=[%s], version_range=%s, "
                     "scan_size=%ld, scan_direction=%d, ret=%d,",
        (NULL != scan_param_ptr) ? (scan_param_ptr->to_str(sql, sizeof(sql), pos),sql) : sql,
        (NULL != scan_param_ptr) ? range2str(scan_param_ptr->get_version_range()) : "",
        (NULL != scan_param_ptr) ? (GET_SCAN_SIZE(scan_param_ptr->get_scan_size())) : 0,
        (NULL != scan_param_ptr) ? scan_param_ptr->get_scan_direction() : 0,
        rc.result_code_);

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
        if (!is_fullfilled && !is_last_packet)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      do
      {
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
        }
        // send response.
        out_buffer.get_position() = 0;
        int serialize_ret = rc.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
          break ;
        }

        // if scan return success , we can return scanner.
        if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
        {
          serialize_ret = scanner->serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position());
          ups_data_version = scanner->get_data_version();
          if (OB_SUCCESS != serialize_ret)
          {
            TBSYS_LOG(ERROR, "serialize ObScanner failed.");
            break;
          }
        }
        if (OB_SUCCESS == serialize_ret)
        {
          OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_BYTES, out_buffer.get_position());
          chunk_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_SCAN_RESPONSE, CS_SCAN_VERSION,
              out_buffer, req, response_cid, session_id);
          packet_cnt++;
        }
        //modify wenghaixing [secondary index static_index_build ]20150814
        //if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)old code
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet && !scan_param_ptr->if_need_fake())
        //modify e
        {
          scanner->reset();
          rc.result_code_ = queue_thread.wait_for_next_request(
            session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            //merge server end this session
            req = next_request->get_request();
            rc.result_code_ = OB_SUCCESS;
            easy_request_wakeup(req);
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d",
              rc.result_code_);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
            rc.result_code_ = query_service->fill_scan_data(*scanner);
            if (OB_ITER_END == rc.result_code_)
            {
              /**
               * the last packet is not always with true fullfilled flag,
               * maybe there is not enough memory to query the normal scan
               * request, we just return part of result, user scan the next
               * data if necessary. it's order to be compatible with 0.2
               * version.
               */
              is_last_packet = true;
              rc.result_code_ = OB_SUCCESS;
            }
            scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          }
        }
        else
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_TIME, consume_time);
      //if ups have frozen mem table,get it
      bool release_tablet = true;

      if (OB_SUCCESS == rc.result_code_)
      {
        ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
        ObTablet*& tablet = tablet_manager.get_cur_thread_scan_tablet();
        if (tablet != NULL && check_update_data(*tablet,ups_data_version,release_tablet) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"check update data failed"); //just warn
        }
      }

      if (NULL != scan_param_ptr
          && consume_time >= chunk_server_->get_config().slow_query_warn_time)
      {
        pos = 0;
        scan_param_ptr->to_str(sql, sizeof(sql), pos);
        TBSYS_LOG(WARN,
            "slow scan:[%s], version_range=%s, scan size=%ld, scan_direction=%d, "
            "read mode=%d, peer=%s, rc=%d, scanner size=%ld, row_num=%ld, "
            "cell_num=%ld, io_stat: %s, consume=%ld",
            sql, range2str(scan_param_ptr->get_version_range()),
            GET_SCAN_SIZE(scan_param_ptr->get_scan_size()),
            scan_param_ptr->get_scan_direction(),
            scan_param_ptr->get_read_mode(),
                  inet_ntoa_r(addr),
                  rc.result_code_, scanner->get_size(),
                  scanner->get_row_num(), scanner->get_cell_num(),
                  get_io_stat_str(), consume_time);
      }

      FILL_TRACE_LOG("send response, packet_cnt=%ld, session_id=%ld, read mode=%d, "
                     "io stat: %s, ret=%d",
        packet_cnt, session_id, (NULL != scan_param_ptr) ? scan_param_ptr->get_read_mode() : 1,
        get_io_stat_str(), rc.result_code_);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }
      query_service->end_scan();
      reset_internal_status(release_tablet);

      return rc.result_code_;
    }

    int ObChunkService::cs_drop_old_tablets(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DROP_OLD_TABLES_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      //char msg_buff[24];
      //rc.message_.assign(msg_buff, 24);
      if (version != CS_DROP_OLD_TABLES_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t memtable_frozen_version = 0;

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_vi64(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &memtable_frozen_version);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse drop_old_tablets input memtable_frozen_version param error.");
        }
      }

      TBSYS_LOG(INFO, "drop_old_tablets: memtable_frozen_version:%ld", memtable_frozen_version);

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_DROP_OLD_TABLETS_RESPONSE,
            CS_DROP_OLD_TABLES_VERSION,
            out_buffer, req, channel_id);
      }


      // call tablet_manager_ drop tablets.
      //ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      //rc.result_code_ = tablet_manager.drop_tablets(memtable_frozen_version);

      return rc.result_code_;
    }

    //add wenghaixing [secondary index static_index_build.exceptional_handle] 20150409
    int ObChunkService::cs_recieve_wok(const int32_t version, const int32_t channel_id, easy_request_t *req, ObDataBuffer &in_buffer, ObDataBuffer &out_buffer)
    {
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      BlackList list;
      UNUSED(channel_id);
     // UNUSED(req);
      //UNUSED(out_buffer);
      UNUSED(version);
      int MY_VERSION = 1;
      if(NULL == chunk_server_)
      {
        rc.result_code_ = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "chunk server pointer can not be null");
      }
      if(OB_SUCCESS == rc.result_code_)
      {
        if(OB_SUCCESS != (rc.result_code_ = list.deserialize(in_buffer.get_data(),in_buffer.get_capacity(),in_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "desirialize black list failed, ret = %d", rc.result_code_);
        }
        else
        {
          ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
          rc.result_code_ = tablet_manager.get_index_worker().push_wok(list);
        }
      }
      if(OB_SUCCESS ==
         rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position()))
      {
        chunk_server_->send_response(OB_WHIPPING_WOK_RESPONSE, MY_VERSION, out_buffer, req, channel_id);
      }
      return rc.result_code_;
    }
    //add e
//add wenghaixing [secondary index static_index_build] 20150318
    int ObChunkService::cs_static_index(const int32_t version, const int32_t channel_id, easy_request_t *req, ObDataBuffer &in_buffer, ObDataBuffer &out_buffer)
    {
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      UNUSED(channel_id);
      UNUSED(req);
      UNUSED(out_buffer);
      UNUSED(version);
      //UNUSED(in_buffer);
      uint64_t index_id = OB_INVALID_ID;
      int64_t hist_width = 0;
      //int64_t pos = 0;
      /*uint64_t need_index_table_list[OB_MAX_TABLE_NUMBER];
      int64_t table_list_count;
      if(OB_SUCCESS!=(rc.result_code_=gen_index_table_list(table_list_count,need_index_table_list)))
      {
        TBSYS_LOG(ERROR,"failed to get index table list");
      }
      else if(OB_SUCCESS!=(rc.result_code_=gen_static_index(table_list_count,need_index_table_list)))
      {
        TBSYS_LOG(ERROR,"failed to get static index");
      }
      */
      int64_t wait_time = 5000000;
      if(OB_SUCCESS != (rc.result_code_ = serialization::decode_i64(in_buffer.get_data(),in_buffer.get_capacity(),in_buffer.get_position(),reinterpret_cast<int64_t *>(&index_id))))
      {
        TBSYS_LOG(ERROR,"seriliazation error,cannot decode index_tid to build index!ret[%d]",rc.result_code_);
      }
      else if(OB_SUCCESS != (rc.result_code_ = serialization::decode_i64(in_buffer.get_data(),in_buffer.get_capacity(),in_buffer.get_position(),&hist_width)))
      {
        TBSYS_LOG(ERROR,"seriliazation error,cannot decode hist_width to build index!ret[%d]",rc.result_code_);
      }
      else if(!index_task_.is_scheduled())
      {
        index_task_.set_schedule_idx_tid(index_id);
        index_task_.set_hist_width(hist_width);
        TBSYS_LOG(INFO,"index_task: set_schedule_tid[%ld],hist_width[%ld]",index_id,hist_width);
        if(OB_SUCCESS != (rc.result_code_ = (timer_.schedule(index_task_, wait_time, false)))) //async
        {
          TBSYS_LOG(WARN, "cannot schedule index_task_ after wait %ld us", wait_time);
        }
        else
        {
          TBSYS_LOG(INFO, "launch a new index process after wait %ld us", wait_time);
          index_task_.set_scheduled();
        }
      }
      return rc.result_code_;
    }

    int ObChunkService::gen_index_table_list(int64_t &count, uint64_t *list)
    {
        int ret=OB_SUCCESS;
        count=0;
        ObMergerSchemaManager* tablet_manager=chunk_server_->get_schema_manager();
        int64_t version=tablet_manager->get_latest_version();
        const ObSchemaManagerV2 * schema =tablet_manager->get_user_schema(version);
        const ObTableSchema* table_schema=NULL;
        uint64_t tbl_tid=OB_INVALID_ID;
        if(schema==NULL)
        {
            ret=OB_SCHEMA_ERROR;
            TBSYS_LOG(ERROR,"get user schema error!");
        }
        else
        {
            const hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>* hash=schema->get_index_hash();
            hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>::const_iterator iter=hash->begin();
            for (;iter != hash->end(); ++iter)
            {
                IndexList il=iter->second;
                for(int64_t i=0;i<il.get_count();i++){
                     il.get_idx_id(i,tbl_tid);
                     table_schema =schema->get_table_schema(tbl_tid);
                     if(table_schema==NULL)
                     {
                        ret=OB_SCHEMA_ERROR;
                        TBSYS_LOG(ERROR,"failed to get table schema");
                     }
                     else if(table_schema->get_index_helper().status==NOT_AVALIBALE)
                     {
                        list[count]=table_schema->get_table_id();

                     }

                     count++;
                }
            }
        }
        return ret;
    }

    int ObChunkService::gen_static_index(int64_t count, uint64_t *list){
        int ret=OB_SUCCESS;
        uint64_t tid=OB_INVALID_ID;
        //uint64_t cid=OB_INVALID_ID;
        int64_t max_column_id,rowkey_column=0;
        ObTabletScan tmp_table_scan;
        ObSSTableScan ob_sstable_scan;
        ObSqlScanParam ob_sql_scan_param;
        sstable::ObSSTableScanParam sstable_scan_param;
        ObNewRange range;
        ObArray<uint64_t> basic_columns;
        ObMergerSchemaManager* tablet_manager=chunk_server_->get_schema_manager();
        int64_t version=tablet_manager->get_latest_version();
        const ObSchemaManagerV2 * schema =tablet_manager->get_user_schema(version);
        ObTabletManager &tablet_mgr = chunk_server_->get_tablet_manager();
        ScanContext sc;
        tablet_mgr.build_scan_context(sc);
        const ObRow *cur_row = NULL;
        const ObRowkey *rowkey=NULL;
        const ObColumnSchemaV2* col_schema=NULL;
        char str[1024];
        for(int64_t i=0;i<count;i++)
        {
            tid=list[i];
            basic_columns.clear();
           // UNUSED(tid);

            const ObTableSchema* table_schema=schema->get_table_schema(tid);
            if(NULL==table_schema)
            {
                TBSYS_LOG(ERROR,"get table schema failed,tid[%ld]!",tid);
                ret=OB_SCHEMA_ERROR;
            }
            else
            {
                /*rowkey_column=table_schema->get_rowkey_info().get_size();
                ObObj start_rowkey[rowkey_column];
                ObObj end_rowkey[rowkey_column];
                for(int64_t j=0;j<rowkey_column;j++)
                {
                    if(OB_SUCCESS!=(ret=table_schema->get_rowkey_info().get_column_id(j,cid)))
                    {
                        TBSYS_LOG(ERROR,"get column schema failed,cid[%ld]",cid);
                        ret=OB_SCHEMA_ERROR;
                    }
                    else if(OB_SUCCESS)
                }*/
                range.table_id_=table_schema->get_index_helper().tbl_tid;
                range.start_key_.set_min_row();
                range.end_key_.set_max_row();
                ob_sql_scan_param.set_range(range);
                ob_sql_scan_param.set_is_result_cached(false);
                max_column_id=table_schema->get_max_column_id();
                rowkey_column=table_schema->get_rowkey_info().get_size();
                uint64_t cid;
                for(int64_t j=0;j<rowkey_column;j++)
                {
                    if(OB_SUCCESS!=(ret=table_schema->get_rowkey_info().get_column_id(j,cid)))
                    {
                        TBSYS_LOG(ERROR,"get column schema failed,cid[%ld]",cid);
                        ret=OB_SCHEMA_ERROR;
                    }
                    else
                    {
                        basic_columns.push_back(cid);
                    }
                }
                for(int64_t j=OB_APP_MIN_COLUMN_ID;j<=max_column_id;j++)
                {
                    if(table_schema->get_rowkey_info().is_rowkey_column(j)||NULL==(col_schema=schema->get_column_schema(tid,j)))
                    {
                    }
                    else
                    {
                        basic_columns.push_back(col_schema->get_id());
                        //TBSYS_LOG(ERROR,"test::whx column id[%ld]",col_schema->get_id());
                    }
                }

                if(OB_SUCCESS==ret)
                {
                 if(OB_SUCCESS!=(ret=tmp_table_scan.build_sstable_scan_param_pub(basic_columns,ob_sql_scan_param,sstable_scan_param)))
                 {
                     TBSYS_LOG(ERROR,"failed to build sstable scan param");
                 }
                 else if(OB_SUCCESS!=(ret=ob_sstable_scan.open_scan_context(sstable_scan_param,sc)))
                 {
                     TBSYS_LOG(ERROR,"failed to open scan context");
                 }
                 else if(OB_SUCCESS!=(ret=ob_sstable_scan.open()))
                 {
                     TBSYS_LOG(ERROR,"failed to open ObSsTableScan!");
                 }
                 else
                 {
                     while(ret==OB_SUCCESS)
                     {
                         ret=ob_sstable_scan.get_next_row(rowkey,cur_row);
                         memset(str,0,1024);
                         cur_row->to_string(str,1024);
                        // if(ret==OB_SUCCESS)TBSYS_LOG(INFO,"test::whx get_next_row from index,%s",str);
                     }
                     if(ret==OB_ITER_END)
                     {
                         ret=OB_SUCCESS;
                     }
                 }
                }
            }
    }
        return ret;
    }
    //add e
    //add wenghaixing [secondary index static_index_build.heartbeat]20150528
    int ObChunkService::handle_index_beat(IndexBeat beat)
    {
      int ret = OB_SUCCESS;
      int64_t wait_time = 5000000;
      if(OB_INVALID_ID == beat.idx_tid || ERROR == beat.status || 0 == beat.hist_width)
      {
        //NORMAL STATUS, DO NOTHING AT THIS TIME
        //TODO, CS在建最后一个索引时offline, 然后又online, need release resources
        //TODO, 考虑RS重启情况 //TODO, RS重启不能stop_mission,最后一张索引建完,需要stop_mission
        if(0 != beat.start_time && !index_task_.get_round_end())
        {
          index_task_.try_stop_mission(beat.idx_tid);
        }
      }
      else if(OB_INVALID_ID != beat.idx_tid && INDEX_INIT == beat.status)
      {
        index_task_.set_schedule_time(beat.idx_tid, beat.start_time);//set schedule time every time if need
        TBSYS_LOG(DEBUG, "test::whx index index_task_ schedule tid[%ld],boolean[%d]", index_task_.get_schedule_idx_tid(),index_task_.is_scheduled());
        if(!index_task_.is_scheduled()&& index_task_.get_round_end())
        {
          if(OB_SUCCESS == index_task_.set_schedule_idx_tid(beat.idx_tid))//idx_tid相同,不会进入以下分支
          {
            index_task_.set_schedule_time(beat.idx_tid, beat.start_time);//set schedule time firstly
            index_task_.set_hist_width(beat.hist_width);
            TBSYS_LOG(INFO,"index_task: set_schedule_tid[%ld],hist_width[%ld]",beat.idx_tid,beat.hist_width);
            if(OB_SUCCESS != (ret = (timer_.schedule(index_task_, wait_time, false)))) //async
            {
              TBSYS_LOG(WARN, "cannot schedule index_task_ after wait %ld us", wait_time);
            }
            else
            {
              TBSYS_LOG(INFO, "launch a new index process after wait %ld us", wait_time);
              index_task_.set_scheduled();
            }
          }
        }
        else if(!index_task_.get_round_end())
        {
          index_task_.try_stop_mission(beat.idx_tid);
          //TBSYS_LOG(ERROR, "test::whx try stop mission");
        }
      }
      if(OB_UNLIKELY(NULL == chunk_server_))
      {
        TBSYS_LOG(ERROR,"shuold not be here, null chunk ");
      }
      else
      {
        chunk_server_->get_tablet_manager().set_beat_tid(beat.idx_tid);
      }
      return ret;
    }
    //add e
    //add liumz, [secondary index version management]20160413:b
    int ObChunkService::handle_index_beat_v2(IndexBeat beat, int64_t frozen_version, int64_t build_index_version, bool is_last_finished)
    {
      int ret = OB_SUCCESS;
      int64_t wait_time = 5000000;
      TBSYS_LOG(DEBUG, "beat.idx_tid[%lu], beat.status[%d], beat.hist_width[%ld], beat.start_time[%ld].", beat.idx_tid, beat.status, beat.hist_width, beat.start_time);
      TBSYS_LOG(DEBUG, "is_merge_done[%d], frozen_version[%ld], build_index_version[%ld], building_version[%ld], data_version[%ld]",
                chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_done(),
                frozen_version, build_index_version, get_build_index_version(),
                chunk_server_->get_tablet_manager().get_serving_data_version());
      if ((OB_INVALID_VERSION != build_index_version) //valid
          || (OB_INVALID_VERSION == build_index_version //1.rs restart
              && OB_INVALID_VERSION != get_build_index_version() //2.building index
              && frozen_version == get_build_index_version() //3.version not change
              )
          )
      {
        if (build_index_version > get_build_index_version()
            && OB_INVALID_VERSION != build_index_version
            && OB_INVALID_VERSION != get_build_index_version())
        {
          //建索引版本变化,终止当前任务,reset_build_index_version()
          if(!index_task_.get_round_end())
          {
            //mod liumz, [force stop mission]20160620:b
            //index_task_.try_stop_mission(beat.idx_tid);
            TBSYS_LOG(DEBUG, "test::liumz force stop mission");
            index_task_.try_stop_mission(OB_INVALID_ID);
            index_task_.set_schedule_idx_tid(OB_INVALID_ID);
            //mod:e
          }
          set_build_index_version(OB_INVALID_VERSION);
        }
        /*
         * 大前提： build_index_version == chunk_server_->get_tablet_manager().get_serving_data_version()
         *        && OB_INVALID_VERSION != build_index_version
         * 1. 首次建索引 OB_INVALID_VERSION == get_build_index_version()
         * 2. 后续建索引 build_index_version == get_build_index_version()
         */
        else if (chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_done()
                 && OB_INVALID_VERSION != build_index_version
                 && build_index_version == chunk_server_->get_tablet_manager().get_serving_data_version()
                 && (build_index_version == get_build_index_version()
                     || OB_INVALID_VERSION == get_build_index_version()))
        {
          if(OB_INVALID_ID == beat.idx_tid || ERROR == beat.status || 0 == beat.hist_width)
          {
            //NORMAL STATUS, DO NOTHING AT THIS TIME
            //TODO, CS在建最后一个索引时offline, 然后又online, need release resources
            //TODO, 考虑RS重启情况
            //TODO, RS重启不能stop_mission,最后一张索引建完,需要stop_mission
            if(is_last_finished && !index_task_.get_round_end())
            {
              //mod liumz, [force stop mission]20160620:b
              //index_task_.try_stop_mission(beat.idx_tid);
              TBSYS_LOG(DEBUG, "test::liumz force stop mission");
              index_task_.try_stop_mission(OB_INVALID_ID);
              index_task_.set_schedule_idx_tid(OB_INVALID_ID);
              //mod:e
              set_build_index_version(OB_INVALID_VERSION);
            }
          }
          else if(OB_INVALID_ID != beat.idx_tid && INDEX_INIT == beat.status)
          {
            set_build_index_version(build_index_version);
            index_task_.set_schedule_time(beat.idx_tid, beat.start_time);//set schedule time every time if need
            TBSYS_LOG(DEBUG, "test::whx index index_task_ schedule tid[%ld],is_scheduled[%d],round_end[%d]", index_task_.get_schedule_idx_tid(),index_task_.is_scheduled(),index_task_.get_round_end());
            if(!index_task_.is_scheduled()&& index_task_.get_round_end())
            {
              if(OB_SUCCESS == index_task_.set_schedule_idx_tid(beat.idx_tid))//idx_tid相同,不会进入以下分支
              {
                index_task_.set_schedule_time(beat.idx_tid, beat.start_time);//set schedule time firstly
                index_task_.set_hist_width(beat.hist_width);
                TBSYS_LOG(INFO,"index_task: set_schedule_tid[%ld],hist_width[%ld]",beat.idx_tid,beat.hist_width);
                //add liumz, [clean old sstable firstly]20160624:b
                if(OB_SUCCESS != cs_delete_global_index(beat.idx_tid))
                {
                  TBSYS_LOG(WARN,"cs delete global index failed, idx_tid[%ld]",beat.idx_tid);
                }
                //add:e
                if(OB_SUCCESS != (ret = (timer_.schedule(index_task_, wait_time, false)))) //async
                {
                  TBSYS_LOG(WARN, "cannot schedule index_task_ after wait %ld us", wait_time);
                }
                else
                {
                  TBSYS_LOG(INFO, "launch a new index process after wait %ld us", wait_time);
                  index_task_.set_scheduled();
                }
              }
            }
            else if(!index_task_.get_round_end())
            {
              TBSYS_LOG(DEBUG, "test::whx try stop mission");
              index_task_.try_stop_mission(beat.idx_tid);
            }
          }
        }
      }
      else
      {
        //其他情况,终止当前任务,reset_build_index_version()
        if(!index_task_.get_round_end())
        {
          //mod liumz, [force stop mission]20160620:b
          //index_task_.try_stop_mission(beat.idx_tid);
          TBSYS_LOG(DEBUG, "test::liumz force stop mission");
          index_task_.try_stop_mission(OB_INVALID_ID);
          index_task_.set_schedule_idx_tid(OB_INVALID_ID);
          //mod:e
        }
        set_build_index_version(OB_INVALID_VERSION);
      }

      if(OB_UNLIKELY(NULL == chunk_server_))
      {
        TBSYS_LOG(ERROR,"shuold not be here, null chunk ");
      }
      else
      {
        chunk_server_->get_tablet_manager().set_beat_tid(beat.idx_tid);
      }
      return ret;
    }
    //add:e

    //add pangtianze [Paxos rs_switch] 20170208:b
    int ObChunkService::cs_force_regist(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
        UNUSED(channel_id);
        UNUSED(in_buffer);
        UNUSED(out_buffer);
        int ret = OB_SUCCESS;
        const int32_t CS_FORCE_REGIST_VERSION = 1;
        ObServer rs_master;
        if (CS_FORCE_REGIST_VERSION != version)
        {
          TBSYS_LOG(WARN, "version not equal. version=%d, CS_FORCE_REGIST_VERSION=%d", version, CS_FORCE_REGIST_VERSION);
          ret = OB_ERROR_FUNC_VERSION;
        }
        else if (OB_SUCCESS != (ret = rs_master.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "failed to deserialize rootserver. err=%d", ret);
        }
        else
        {
          ///set master rootserver
          chunk_server_->set_all_root_server(rs_master);
          TBSYS_LOG(INFO, "cs set new master rootserver[%s] after election", rs_master.to_cstring());

          ///make cs lease timeout
          service_expired_time_ = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "reset lease, then will re-regist, lease=%ld", service_expired_time_);

          ///check lease right now
          timer_.schedule(lease_checker_, 0, false);
        }

        // no response
        easy_request_wakeup(req);
        return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20170301:b
    int ObChunkService::cs_refresh_rs_list(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
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
                  if (OB_SUCCESS != (ret = rs_mgr_.update_all_rs(servers, servers_count)))
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
        // no response
        easy_request_wakeup(req);
        return ret;
    }
    //add:e

    /*
     * int cs_heart_beat(const int64_t lease_duration);
     */
    int ObChunkService::cs_heart_beat(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_HEART_BEAT_VERSION = 2;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      //char msg_buff[24];
      //rc.message_.assign(msg_buff, 24);
      UNUSED(channel_id);
      UNUSED(req);
      UNUSED(out_buffer);
      //if (version != CS_HEART_BEAT_VERSION)
      //{
      //  rc.result_code_ = OB_ERROR_FUNC_VERSION;
      //}

      // send heartbeat request to root_server first
      //add wenghaixing [secondary index static_index_build.heart_beat]20150528
      IndexBeat beat;
      //add e
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = CS_RPC_CALL_RS(heartbeat_server, chunk_server_->get_self(), OB_CHUNKSERVER);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "failed to async_heartbeat, ret=%d", rc.result_code_);
        }
      }

      // ignore the returned code of async_heartbeat
      int64_t lease_duration = 0;
      rc.result_code_ = common::serialization::decode_vi64(
          in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position(), &lease_duration);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(ERROR, "parse cs_heart_beat input lease_duration param error.");
      }
      else
      {
        service_expired_time_ = tbsys::CTimeUtil::getTime() + lease_duration;
        TBSYS_LOG(DEBUG, "cs_heart_beat: lease_duration=%ld", lease_duration);
      }

      TBSYS_LOG(DEBUG,"cs_heart_beat,version:%d,CS_HEART_BEAT_VERSION:%d",version,CS_HEART_BEAT_VERSION);

      if (version >= CS_HEART_BEAT_VERSION)
      {
        int64_t frozen_version = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_vi64(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&frozen_version);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_heart_beat input frozen_version param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: frozen_version=%ld", frozen_version);
          }
        }
        //add liumz, [secondary index version management]20160413:b
        int64_t build_index_version = OB_INVALID_VERSION;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_vi64(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&build_index_version);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_heart_beat input build_index_version param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: build_index_version=%ld", build_index_version);
          }
        }
        bool is_last_finished = false;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_bool(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&is_last_finished);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_heart_beat input is_last_finished param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: is_last_finished=%d", is_last_finished);
          }
        }
        if(OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = beat.deserialize(in_buffer.get_data(),
                                             in_buffer.get_capacity(),
                                             in_buffer.get_position()
                                             );
          if (OB_SUCCESS != (rc.result_code_))
          {
            TBSYS_LOG(ERROR, "parse heartbeat index beat failed: ret[%d]",
                      rc.result_code_);
          }
          else
          {
            rc.result_code_ = handle_index_beat_v2(beat, frozen_version, build_index_version, is_last_finished);
          }
        }
        //add:e

        if (OB_SUCCESS == rc.result_code_ && service_started_
            && chunk_server_->get_config_mgr().get_current_version() > 1)
        {
          int64_t wait_time = 0;
          ObTabletManager &tablet_manager = chunk_server_->get_tablet_manager();
          if ( frozen_version > tablet_manager.get_serving_data_version()
              && tablet_manager.get_serving_data_version() > 0
               && OB_INVALID_VERSION == build_index_version && OB_INVALID_VERSION == get_build_index_version() && index_task_.get_round_end()/*add liumz, [secondary index version management]20160413*/)
          {
            if (frozen_version > merge_task_.get_last_frozen_version())
            {
              TBSYS_LOG(INFO,"pending a new frozen version need merge:%ld,last:%ld",
                  frozen_version, merge_task_.get_last_frozen_version() );
              merge_task_.set_frozen_version(frozen_version);
            }
            if (!merge_task_.is_scheduled()
                && tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version))
            {
              srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
              int64_t merge_delay_interval = chunk_server_->get_config().merge_delay_interval;
              if (merge_delay_interval > 0)
              {
                wait_time = random() % merge_delay_interval;
              }
              // wait one more minute for ensure slave updateservers sync frozen version.
              wait_time += chunk_server_->get_config().merge_delay_for_lsync;
              if (OB_SUCCESS != (timer_.schedule(merge_task_, wait_time, false)))  //async
              {
                TBSYS_LOG(WARN, "cannot schedule merge_task_ after wait %ld us.", wait_time);
              }
              else
              {
                TBSYS_LOG(INFO, "launch a new merge process after wait %ld us.", wait_time);
                merge_task_.set_scheduled();
              }
            }
          }
        }
      }

      if (version > CS_HEART_BEAT_VERSION)
      {
        int64_t local_version = 0;
        int64_t schema_version = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                       in_buffer.get_capacity(),
                                                       in_buffer.get_position(),
                                                       &schema_version);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse heartbeat schema version failed:ret[%d]",
                      rc.result_code_);
          }
        }

        // fetch new schema in a temp timer task
        if ((OB_SUCCESS == rc.result_code_) && service_started_)
        {
          local_version = chunk_server_->get_schema_manager()->get_latest_version();
          if ((local_version > schema_version) && (schema_version != 0))
          {
            rc.result_code_ = OB_ERROR;
            TBSYS_LOG(ERROR, "check schema local version gt than new version:"
                "local[%ld], new[%ld]", local_version, schema_version);
          }
          else if (!fetch_schema_task_.is_scheduled()
                   && local_version < schema_version)
          {
            fetch_schema_task_.init(chunk_server_->get_rpc_proxy(),
                                    chunk_server_->get_schema_manager());
            fetch_schema_task_.set_version(local_version, schema_version);
            srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
            timer_.schedule(fetch_schema_task_,
                            random() % FETCH_SCHEMA_INTERVAL, false);
            fetch_schema_task_.set_scheduled();
          }
        }
      }

      if (version > CS_HEART_BEAT_VERSION)
      {
        int64_t config_version;
        ObConfigManager &config_mgr = chunk_server_->get_config_mgr();
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
                   (rc.result_code_ = config_mgr.got_version(config_version)))
          {
            TBSYS_LOG(WARN, "Process config failed, ret: [%d]", rc.result_code_);
          }
        }
      }
      /*
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_HEARTBEAT_RESPONSE,
            CS_HEART_BEAT_VERSION,
            out_buffer, connection, channel_id);
      }
      */

      //del liumz, [secondary index version management]20160413:b
      /*
      //add wenghaixing [secondary index static_index_build.heart_beat]20150528
      if(OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = beat.deserialize(in_buffer.get_data(),
                                           in_buffer.get_capacity(),
                                           in_buffer.get_position()
                                           );
        if (OB_SUCCESS != (rc.result_code_))
        {
          TBSYS_LOG(ERROR, "parse heartbeat index beat failed: ret[%d]",
                    rc.result_code_);
        }
        else
        {
          rc.result_code_ = handle_index_beat(beat);
          //TBSYS_LOG(ERROR, "test::whx index beat tid[%ld]", beat.idx_tid);
        }
      }
      //add e
      */
      //del:e
      return rc.result_code_;
    }

    int ObChunkService::cs_accept_schema(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_ACCEPT_SCHEMA_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int &err = rc.result_code_;
      ObSchemaManagerV2 *schema = NULL;
      ObMergerSchemaManager *schema_manager = NULL;
      int64_t schema_version = 0;
      int ret = OB_SUCCESS;

      if (version != CS_ACCEPT_SCHEMA_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == err)
      {
        if (NULL == (schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_CS_SERVICE_FUNC)))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "fail to new ObSchemaManagerV2");
        }
      }

      if (OB_SUCCESS == err)
      {
        if (NULL == chunk_server_ || NULL == (schema_manager = chunk_server_->get_schema_manager()))
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(WARN, "not init:chunk_server_[%p], schema_manager[%p]", chunk_server_, schema_manager);
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
        if (schema_version <= schema_manager->get_latest_version())
        {
          TBSYS_LOG(WARN, "schema version too old. old=%ld, latest=%ld",
              schema_version, schema_manager->get_latest_version());
          err = OB_OLD_SCHEMA_VERSION;
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        else if (!schema->is_completion())
        {
          TBSYS_LOG(INFO, "schema is not completion");
        }
        //add:e
        else if (OB_SUCCESS != (err = schema_manager->add_schema(*schema)))
        {
          TBSYS_LOG(WARN, "fail to add schema :err[%d]", err);
        }
      }
      //del zhaoqiong [Schema Manager] 20150327:b
      //OB_DELETE(ObSchemaManagerV2, ObModIds::OB_CS_SERVICE_FUNC, schema);
      //del:e

      ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fail to serialize: ret[%d]", ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = chunk_server_->send_response(
            OB_SWITCH_SCHEMA_RESPONSE,
            CS_ACCEPT_SCHEMA_VERSION,
            out_buffer, req, channel_id);
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      if (OB_SUCCESS == ret && OB_SUCCESS == err && !schema->is_completion())
      {
        int64_t timeout_us = chunk_server_->get_config().network_timeout;
        ret = schema->fetch_schema_followed(chunk_server_->get_rpc_stub(),timeout_us*2,chunk_server_->get_root_server(),schema->get_version());
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = schema_manager->add_schema(*schema)))
          {
            TBSYS_LOG(WARN, "fail to add schema :err[%d]", ret);
          }
        }
      }

      OB_DELETE(ObSchemaManagerV2, ObModIds::OB_CS_SERVICE_FUNC, schema);
      //add:e
      return ret;
    }
    //add zhaoqiong [Schema Manager] 20150327:b
    int ObChunkService::cs_accept_schema_mutator(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_ACCEPT_SCHEMA_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int &err = rc.result_code_;
      ObSchemaManagerV2 *schema = NULL;
      ObSchemaMutator *schema_mutator = NULL;
      ObMergerSchemaManager *schema_manager = NULL;
      int ret = OB_SUCCESS;

      if (version != CS_ACCEPT_SCHEMA_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == chunk_server_ || NULL == (schema_manager = chunk_server_->get_schema_manager()))
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "not init:chunk_server_[%p], schema_manager[%p]", chunk_server_, schema_manager);
      }
      else if (NULL == (schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_CS_SERVICE_FUNC)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "fail to new ObSchemaManagerV2");
      }
      else if (NULL == (schema_mutator = OB_NEW(ObSchemaMutator, ObModIds::OB_CS_SERVICE_FUNC)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "fail to new ObSchemaMutator");
      }
      else if (OB_SUCCESS != (err = schema_mutator->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "fail to deserialize schema mutator:err[%d]", err);
      }
      else if (schema_mutator->get_refresh_schema())
      {
        //ignore, wait heartbeat task get full schema later
        TBSYS_LOG(WARN, "schema_mutator contain refresh schema, ignore it");
      }
      else if (schema_mutator->get_end_version() <= schema_manager->get_latest_version())
      {
        TBSYS_LOG(WARN, "schema version too old. old=%ld, latest=%ld",
             schema_mutator->get_end_version(), schema_manager->get_latest_version());
        err = OB_OLD_SCHEMA_VERSION;
      }
      else
      {
        //copy local schema, apply mutator, then assign new schema
        const ObSchemaManagerV2 *tmp = schema_manager->get_user_schema(0);
        if (tmp == NULL)
        {
          TBSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]",
                    tmp, schema_manager->get_latest_version());
          err = OB_INNER_STAT_ERROR;
        }
        else
        {
          schema->copy_without_sort(*tmp);
          if (OB_SUCCESS != (err = schema_manager->release_schema(tmp)))
          {
            TBSYS_LOG(WARN, "can not release schema, ret[%d]", err);
          }
          else if (OB_SUCCESS != (err = schema->apply_schema_mutator(*schema_mutator)))
          {
            TBSYS_LOG(WARN, "apply_schema_mutator fail(mutator version[%ld->%ld])",
                      schema_mutator->get_start_version(), schema_mutator->get_end_version());
          }
          else if (OB_SUCCESS != (err = schema_manager->add_schema(*schema)))
          {
            TBSYS_LOG(WARN, "fail to add schema :err[%d]", err);
          }
          else
          {
            TBSYS_LOG(INFO, "add schema success(mutator version[%ld->%ld])",
                     schema_mutator->get_start_version(), schema_mutator->get_end_version());
          }
        }
      }
      OB_DELETE(ObSchemaManagerV2, ObModIds::OB_CS_SERVICE_FUNC, schema);
      OB_DELETE(ObSchemaMutator, ObModIds::OB_CS_SERVICE_FUNC, schema_mutator);

      ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fail to serialize: ret[%d]", ret);
      }
      else
      {
        ret = chunk_server_->send_response(
            OB_SWITCH_SCHEMA_MUTATOR_RESPONSE,
            CS_ACCEPT_SCHEMA_VERSION,
            out_buffer, req, channel_id);
      }
      return ret;
    }
    //add:e

    int ObChunkService::cs_create_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_CREATE_TABLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      if (version != CS_CREATE_TABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
          TBSYS_LOG(WARN, "load bypass sstable is running, cannot create tablet");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      ObNewRange range;
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_create_tablet input range param error.");
        }
      }

      TBSYS_LOG(INFO, "cs_create_tablet, dump input range %s", to_cstring(range));

      // get last frozen memtable version for update
      int64_t last_frozen_memtable_version = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &last_frozen_memtable_version);
        /*
        ObServer update_server;
        rc.result_code_ = chunk_server_->get_rs_rpc_stub().get_update_server(update_server);
        ObRootServerRpcStub update_stub;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = update_stub.init(update_server, &chunk_server_->get_client_manager());
        }
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = update_stub.get_last_frozen_memtable_version(last_frozen_memtable_version);
        }
        */
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(DEBUG, "create tablet, last_frozen_memtable_version=%ld",
            last_frozen_memtable_version);
        rc.result_code_ = chunk_server_->get_tablet_manager().create_tablet(
            range, last_frozen_memtable_version);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_create_tablet rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_CREATE_TABLE_RESPONSE,
            CS_CREATE_TABLE_VERSION,
            out_buffer, req, channel_id);
      }


      // call tablet_manager_ drop tablets.

      return rc.result_code_;
    }


    int ObChunkService::cs_load_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_LOAD_TABLET_VERSION = 2;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      if (version <= 0 || version > CS_LOAD_TABLET_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_config().merge_migrate_concurrency
               && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate in.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot migrate in");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      ObNewRange range;
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
      int64_t num_file = 0;
      //deserialize ObRange
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet range param error.");
        }
        else
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(DEBUG,"cs_load_tablet dump range <%s>", range_buf);
        }
      }

      int32_t dest_disk_no = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi32(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &dest_disk_no);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse dest_disk_no range param error.");
        }
        else
        {
          TBSYS_LOG(INFO, "cs_load_tablet dest_disk_no=%d ", dest_disk_no);
        }
      }

      // deserialize tablet_version;
      int64_t tablet_version = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &tablet_version);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet tablet_version param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet tablet_version = %ld ", tablet_version);
        }
      }

      uint64_t crc_sum = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), (int64_t*)(&crc_sum));
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet crc_sum param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet crc_sum = %lu ", crc_sum);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(),&num_file);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet number of sstable  param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet num_file = %ld ", num_file);
        }
      }

      char (*path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
      char * path_buf = NULL;
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      if (OB_SUCCESS == rc.result_code_ && num_file > 0)
      {
        path_buf = static_cast<char*>(ob_malloc(num_file*OB_MAX_FILE_NAME_LENGTH, ObModIds::OB_CS_COMMON));
        if ( NULL == path_buf )
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for path array.");
          rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          path = new(path_buf)char[num_file][OB_MAX_FILE_NAME_LENGTH];
        }

        int64_t len = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          for( int64_t idx =0; idx < num_file; idx++)
          {
            if(NULL == common::serialization::decode_vstr(in_buffer.get_data(),
                  in_buffer.get_capacity(), in_buffer.get_position(),
                  path[idx], OB_MAX_FILE_NAME_LENGTH, &len))
            {
              rc.result_code_ = OB_ERROR;
              TBSYS_LOG(ERROR, "parse cs_load_tablet dest_path param error.");
              break;
            }
            else
            {
              TBSYS_LOG(INFO, "parse cs_load_tablet dest_path [%ld] = %s", idx, path[idx]);
            }
          }
        }
      }

      // deserialize tablet_seq_num
      int64_t tablet_seq_num = 0;
      if (OB_SUCCESS == rc.result_code_ && version  > 1)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &tablet_seq_num);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet tablet_seq_num param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet tablet_seq_num = %ld ", tablet_seq_num);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.dest_load_tablet(range, path, num_file,
          tablet_version, tablet_seq_num, dest_disk_no, crc_sum);
        if (OB_SUCCESS != rc.result_code_ && OB_CS_MIGRATE_IN_EXIST != rc.result_code_)
        {
          TBSYS_LOG(WARN, "ObTabletManager::dest_load_tablet error, rc=%d", rc.result_code_);
        }
      }

      if ( NULL != path_buf )
      {
        ob_free(path_buf);
      }

      //send response to src chunkserver
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_MIGRATE_RESPONSE,
            CS_LOAD_TABLET_VERSION,
            out_buffer, req, channel_id);
      }
      return rc.result_code_;
    }

    int ObChunkService::cs_delete_tablets(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DELETE_TABLETS_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletReportInfoList *delete_tablet_list = NULL;
      bool is_force = false;

      if (version != CS_DELETE_TABLETS_VERSION
          && version != CS_DELETE_TABLETS_VERSION + 1)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot remove tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (NULL == (delete_tablet_list = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1)))
      {
        TBSYS_LOG(ERROR, "cannot get ObTabletReportInfoList object.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        delete_tablet_list->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = delete_tablet_list->deserialize(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_delete_tablets tablet info list param error.");
        }
      }

      if (version > CS_DELETE_TABLETS_VERSION)
      {
        if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
        {
          TBSYS_LOG(WARN, "load bypass sstable is running, cannot delete tablets");
          rc.result_code_ = OB_CS_EAGAIN;
        }
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_bool(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&is_force);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_delete_tablets input is_force param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: is_force=%d", is_force);
          }
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
        ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();

        char range_buf[OB_RANGE_STR_BUFSIZ];
        int64_t size = delete_tablet_list->get_tablet_size();
        int64_t version = 0;
        const ObTabletReportInfo* const tablet_info_array = delete_tablet_list->get_tablet();
        ObTablet *src_tablet = NULL;
        int32_t disk_no = -1;

        for (int64_t i = 0; i < size ;  ++i)
        {
          version = tablet_info_array[i].tablet_location_.tablet_version_;
          tablet_info_array[i].tablet_info_.range_.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          rc.result_code_ = tablet_image.acquire_tablet(
              tablet_info_array[i].tablet_info_.range_,
              ObMultiVersionTabletImage::SCAN_FORWARD, version, src_tablet);

          if (OB_SUCCESS == rc.result_code_ && NULL != src_tablet
              && src_tablet->get_data_version() == version
              && tablet_info_array[i].tablet_info_.range_.compare_with_startkey2(src_tablet->get_range()) == 0
              && tablet_info_array[i].tablet_info_.range_.compare_with_endkey2(src_tablet->get_range()) == 0)
          {
            rc.result_code_ = tablet_image.remove_tablet(
              tablet_info_array[i].tablet_info_.range_, version, disk_no);
            if (OB_SUCCESS != rc.result_code_)
            {
              TBSYS_LOG(WARN, "failed to remove tablet from tablet image, "
                              "version=%ld, disk=%d, range=%s",
                src_tablet->get_data_version(), src_tablet->get_disk_no(), range_buf);
            }
            else
            {
              TBSYS_LOG(INFO, "delete tablet, version=%ld, disk=%d, range=<%s>",
                  src_tablet->get_data_version(), src_tablet->get_disk_no(), range_buf);
              src_tablet->set_merged();
              src_tablet->set_removed();
            }
          }
          else
          {
            TBSYS_LOG(INFO, "cannot find tablet, version=%ld, range=<%s>, local range=<%s>",
                version, range_buf, NULL != src_tablet ? to_cstring(src_tablet->get_range()) : "nil");
          }

          if (NULL != src_tablet)
          {
            tablet_image.release_tablet(src_tablet);
          }
        }

      }

      //send response to src chunkserver
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DELETE_TABLETS_RESPONSE,
            CS_DELETE_TABLETS_VERSION,
            out_buffer, req, channel_id);
      }
      return rc.result_code_;
    }

    int ObChunkService::cs_merge_tablets(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_MERGE_TABLETS_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletReportInfoList *merge_tablet_list = NULL;

      if (version != CS_MERGE_TABLETS_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_reported())
      {
        TBSYS_LOG(WARN, "merge running, cannot merge tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstables is running, cannot merge tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (NULL == (merge_tablet_list = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1)))
      {
        TBSYS_LOG(ERROR, "cannot get ObTabletReportInfoList object.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        merge_tablet_list->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = merge_tablet_list->deserialize(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_merge_tablets tablet info list param error.");
        }
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "merge_tablets rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_MERGE_TABLETS_RESPONSE,
            CS_MERGE_TABLETS_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = chunk_server_->get_tablet_manager().merge_multi_tablets(*merge_tablet_list);
      }

      bool is_merge_succ = (OB_SUCCESS == rc.result_code_);
      //report merge tablets info to root server
      rc.result_code_ = CS_RPC_CALL_RS(merge_tablets_over, *merge_tablet_list, is_merge_succ);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "report merge tablets over error, is_merge_succ=%d, seq_num=%ld",
            is_merge_succ, is_merge_succ ? merge_tablet_list->get_tablet()[0].tablet_location_.tablet_seq_ : -1);
      }
      else if (is_merge_succ)
      {
        TBSYS_LOG(INFO, "merge tablets <%s> over and success, seq_num=%ld",
            to_cstring(merge_tablet_list->get_tablet()[0].tablet_info_.range_),
            merge_tablet_list->get_tablet()[0].tablet_location_.tablet_seq_);
      }

      return rc.result_code_;
    }


    int ObChunkService::cs_get_migrate_dest_loc(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_GET_MIGRATE_DEST_LOC_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_GET_MIGRATE_DEST_LOC_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_config().merge_migrate_concurrency
          && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped())
          && (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped()))
      {
        TBSYS_LOG(WARN, "merge or load bypass sstables running, cannot migrate in.");
        rc.result_code_ = OB_CS_EAGAIN;
      }


      int64_t occupy_size = 0;
      //deserialize occupy_size
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &occupy_size);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_get_migrate_dest_loc occupy_size param error.");
        }
        else
        {
          TBSYS_LOG(INFO, "cs_get_migrate_dest_loc occupy_size =%ld", occupy_size);
        }
      }

      int32_t disk_no = 0;
      char dest_directory[OB_MAX_FILE_NAME_LENGTH];

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      disk_no = tablet_manager.get_disk_manager().get_disk_for_migrate();
      if (disk_no <= 0)
      {
        TBSYS_LOG(ERROR, "get wrong disk no =%d", disk_no);
        rc.result_code_ = OB_ERROR;
      }
      else
      {
        rc.result_code_ = get_sstable_directory(disk_no, dest_directory, OB_MAX_FILE_NAME_LENGTH);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return disk_no & dest_directory.
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        serialize_ret = serialization::encode_vi32(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position(), disk_no);
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize disk_no failed.");
        }
      }

      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        ObString dest_string(OB_MAX_FILE_NAME_LENGTH,
            static_cast<int32_t>(strlen(dest_directory)), dest_directory);
        serialize_ret = dest_string.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize dest_directory failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_GET_MIGRATE_DEST_LOC_RESPONSE,
            CS_GET_MIGRATE_DEST_LOC_VERSION,
            out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_dump_tablet_image(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DUMP_TABLET_IMAGE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      int32_t index = 0;
      int32_t disk_no = 0;

      char *dump_buf = NULL;
      const int64_t dump_size = OB_MAX_PACKET_LENGTH - 1024;

      int64_t pos = 0;
      ObTabletImage * tablet_image = NULL;

      if (version != CS_DUMP_TABLET_IMAGE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &index)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image index param error.");
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &disk_no)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image disk_no param error.");
      }
      else if (disk_no <= 0)
      {
        TBSYS_LOG(WARN, "cs_dump_tablet_image input param error, "
            "disk_no=%d", disk_no);
        rc.result_code_ = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (dump_buf = static_cast<char*>(ob_malloc(dump_size, ObModIds::OB_CS_COMMON))))
      {
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory for serialization failed.");
      }
      else if (OB_SUCCESS != (rc.result_code_ =
            chunk_server_->get_tablet_manager().get_serving_tablet_image().
            serialize(index, disk_no, dump_buf, dump_size, pos)))
      {
        TBSYS_LOG(WARN, "serialize tablet image failed. disk_no=%d", disk_no);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return dump_buf
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        ObString return_dump_obj(static_cast<int32_t>(pos), static_cast<int32_t>(pos), dump_buf);
        serialize_ret = return_dump_obj.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize return_dump_obj failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DUMP_TABLET_IMAGE_RESPONSE,
            CS_DUMP_TABLET_IMAGE_VERSION,
            out_buffer, req, channel_id);
      }

      if (NULL != dump_buf)
      {
        ob_free(dump_buf);
        dump_buf = NULL;
      }
      if (NULL != tablet_image)
      {
        delete(tablet_image);
        tablet_image = NULL;
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_fetch_stats(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      const int32_t CS_FETCH_STATS_VERSION = 1;
      int ret = OB_SUCCESS;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_FETCH_STATS_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return dump_buf
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == ret)
      {
        ret = chunk_server_->get_stat_manager().serialize(
            out_buffer.get_data(),out_buffer.get_capacity(),out_buffer.get_position());
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_FETCH_STATS_RESPONSE,
            CS_FETCH_STATS_VERSION,
            out_buffer, req, channel_id);
      }
      return ret;
    }

    int ObChunkService::cs_start_gc(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_START_GC_VERSION = 1;
      int ret = OB_SUCCESS;
      int64_t recycle_version = 0;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_START_GC_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == rc.result_code_ )
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &recycle_version);
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_START_GC_VERSION,
            out_buffer, req, channel_id);
      }
      chunk_server_->get_tablet_manager().start_gc(recycle_version);
      return ret;
    }

    int ObChunkService::cs_check_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t DEFAULT_VERSION = 1;
      int ret = OB_SUCCESS;
      int64_t table_id = 0;
      ObNewRange range;
      ObTablet* tablet = NULL;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != DEFAULT_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      //if (OB_SUCCESS == ret && OB_SUCCESS == rc.result_code_ )
      else if (OB_SUCCESS !=
          (rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
             in_buffer.get_capacity(), in_buffer.get_position(), &table_id)))
      {
        TBSYS_LOG(WARN, "deserialize table id error. pos=%ld, cap=%ld",
            in_buffer.get_position(), in_buffer.get_capacity());
        rc.result_code_ = OB_INVALID_ARGUMENT;
      }
      else
      {
        range.table_id_ = table_id;
        range.start_key_.set_min_row();
        range.end_key_.set_max_row();
        rc.result_code_ = chunk_server_->get_tablet_manager().get_serving_tablet_image().acquire_tablet(
            range, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
        if (NULL != tablet)
        {
          chunk_server_->get_tablet_manager().get_serving_tablet_image().release_tablet(tablet);
        }
      }

      if (OB_SUCCESS != (ret = rc.serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      else
      {
        chunk_server_->send_response(
            OB_CS_CHECK_TABLET_RESPONSE,
            DEFAULT_VERSION,
            out_buffer, req, channel_id);
      }
      return ret;
    }

    int ObChunkService::cs_reload_conf(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      int ret = OB_SUCCESS;
      const int32_t CS_RELOAD_CONF_VERSION = 1;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_RELOAD_CONF_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == chunk_server_)
      {
        rc.result_code_ = OB_NOT_INIT;
      }
      else
      {
        rc.result_code_ = chunk_server_->get_config_mgr().reload_config();
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      else
      {
        chunk_server_->send_response(OB_UPS_RELOAD_CONF_RESPONSE,
                                     CS_RELOAD_CONF_VERSION, out_buffer, req, channel_id);
      }

      return ret;
    }

    int ObChunkService::cs_show_param(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SHOW_PARAM_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_SHOW_PARAM_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_SHOW_PARAM_VERSION,
            out_buffer, req, channel_id);
      }
      chunk_server_->get_config().print();
      ob_print_mod_memory_usage();
      return ret;
    }

    int ObChunkService::cs_stop_server(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      //int64_t server_id = chunk_server_->get_root_server().get_ipv4_server_id();
      int64_t peer_id = convert_addr_to_server(req->ms->c->addr);

      /*
      if (server_id != peer_id)
      {
        TBSYS_LOG(WARN, "*stop server* WARNNING coz packet from unrecongnized address "
                  "which is [%lld], should be [%lld] as rootserver.", peer_id, server_id);
        // comment follow line not to strict packet from rs.
        // rc.result_code_ = OB_ERROR;
      }
      */

      int32_t restart = 0;
      rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_capacity(),
                                                  in_buffer.get_position(), &restart);

      //int64_t pos = 0;
      //rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_position(), pos, &restart);

      if (restart != 0)
      {
        BaseMain::set_restart_flag();
        TBSYS_LOG(INFO, "receive *restart server* packet from: [%ld]", peer_id);
      }
      else
      {
        TBSYS_LOG(INFO, "receive *stop server* packet from: [%ld]", peer_id);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
          OB_STOP_SERVER_RESPONSE,
          version,
          out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {

        chunk_server_->stop();
      }
      return rc.result_code_;
    }

    int ObChunkService::cs_force_to_report_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      UNUSED(channel_id);
      UNUSED(req);
      UNUSED(in_buffer);
      UNUSED(out_buffer);
      static const int MY_VERSION = 1;
      TBSYS_LOG(INFO, "cs receive force_report_tablet to rs. maybe have some network trouble");
      if (MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(WARN, "force to report tablet verion not equal. my_version=%d, receive_version=%d", MY_VERSION, version);
      }
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      if (OB_SUCCESS == ret)
      {
        if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade()
            || 0 != atomic_compare_exchange(&scan_tablet_image_count_, 1, 0))
        {
          TBSYS_LOG(WARN, "someone else is reporting. give up this process");
        }
        else
        {
          ret = report_tablets_busy_wait();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to report tablets. err = %d", ret);
          }
          else
          {
            tablet_manager.report_capacity_info();
          }
          atomic_exchange(&scan_tablet_image_count_, 0);
        }
      }
      easy_request_wakeup(req);
      return ret;
    }

    int ObChunkService::cs_change_log_level(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buffer.get_data(),
                                                          in_buffer.get_capacity(),
                                                          in_buffer.get_position(),
                                                          &log_level)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if (TBSYS_LOG_LEVEL_ERROR <= log_level
            && TBSYS_LOG_LEVEL_DEBUG >= log_level)
        {
          TBSYS_LOG(INFO, "change log level. From: %d, To: %d", TBSYS_LOGGER._level, log_level);
          TBSYS_LOGGER._level = log_level;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          result.result_code_ = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS != (ret = result.serialize(out_buffer.get_data(),
                                                  out_buffer.get_capacity(),
                                                  out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = chunk_server_->send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buffer, req, channel_id);
        }
      }
      return ret;
    }

    int ObChunkService::cs_sync_all_images(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SYNC_ALL_IMAGES_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      TBSYS_LOG(INFO, "chunkserver start sync all tablet images");
      if (version != CS_SYNC_ALL_IMAGES_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_SYNC_ALL_IMAGES_VERSION,
            out_buffer, req, channel_id);
      }

      if (inited_ && service_started_
          && !tablet_manager.get_chunk_merge().is_pending_in_upgrade())
      {
        rc.result_code_ = tablet_manager.sync_all_tablet_images();
      }
      else
      {
        TBSYS_LOG(WARN, "can't sync tablet images now, please try again later");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      TBSYS_LOG(INFO, "finish sync all tablet images, ret=%d", rc.result_code_);

      return ret;
    }

    int ObChunkService::cs_tablet_read(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      const int32_t CS_FETCH_DATA_VERSION = 1;
      UNUSED(timeout_time);
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletFileReader* tablet_reader = GET_TSI_MULT(ObTabletFileReader, TSI_CS_FETCH_DATA_2);
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      int serialize_ret = OB_SUCCESS;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread =
        chunk_server_->get_default_task_queue_thread();
      reset_query_thread_local_buffer();

      if (version != CS_FETCH_DATA_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == tablet_reader)
      {
        TBSYS_LOG(ERROR, "failed to get thread local migrate_reader, "
            "tablet_reader=%p", tablet_reader);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        tablet_reader->reset();
        tablet_reader->set_tablet_manager(&chunk_server_->get_tablet_manager());
      }

      int16_t local_sstable_version = 0;
      int64_t sequence_num = 0;
      int64_t row_checksum = 0;
      int64_t sstable_type = 0;

      ObDataSourceDesc desc;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = desc.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse data source param error.");
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(INFO, "begin fetch_reader, %s", to_cstring(desc));
        //open sstable scanner
        if(OB_SUCCESS != (rc.result_code_ = tablet_reader->prepare(desc.range_, desc.tablet_version_, static_cast<int16_t>(desc.sstable_version_))))
        {
          TBSYS_LOG(WARN, "prepare tablet reader failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = tablet_reader->open()))
        {
          TBSYS_LOG(WARN, "open tablet reader failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = tablet_reader->get_sstable_version(local_sstable_version)))
        {
          TBSYS_LOG(WARN, "get sstable version, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = tablet_reader->get_sequence_num(sequence_num)))
        {
          TBSYS_LOG(WARN, "get sequence_num failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = tablet_reader->get_row_checksum(row_checksum)))
        {
          TBSYS_LOG(WARN, "get row_checksum failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = tablet_reader->get_sstable_type(sstable_type)))
        {
          TBSYS_LOG(WARN, "get sstable type failed, ret:%d", rc.result_code_);
        }
      }

      out_buffer.get_position() = rc.get_serialize_size();
      if(OB_SUCCESS != (serialize_ret = serialization::encode_i16(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), local_sstable_version)))
      {
        TBSYS_LOG(ERROR, "serialize sstable version object failed, ret:%d", serialize_ret);
      }
      else if(OB_SUCCESS != (serialize_ret = serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), sequence_num)))
      {
        TBSYS_LOG(ERROR, "serialize sequence_num failed, ret:%d", serialize_ret);
      }
      else if(OB_SUCCESS != (serialize_ret = serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), row_checksum)))
      {
        TBSYS_LOG(ERROR, "serialize row_checksum failed, ret:%d", serialize_ret);
      }
      else if(OB_SUCCESS != (serialize_ret = serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), sstable_type)))
      {
        TBSYS_LOG(ERROR, "serialize sstable_type failed, ret:%d", serialize_ret);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_reader->read_next(out_buffer);
      }

      int64_t tmp_pos = 0;
      if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), tmp_pos)))
      {
        TBSYS_LOG(ERROR, "serialize rc code failed, ret:%d", serialize_ret);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        if (tablet_reader->has_new_data())
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      do
      {
        if (OB_SUCCESS == rc.result_code_ &&
            tablet_reader->has_new_data() &&
            !is_last_packet)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to prepare_for_next_request:err[%d]", rc.result_code_);
          }
        }


        if(OB_SUCCESS == serialize_ret)
        {
          // send response.
          chunk_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_CS_FETCH_DATA_RESPONSE, OB_CS_FETCH_DATA,
              out_buffer, req, response_cid, session_id);
          FILL_TRACE_LOG("send response, #packet=%ld, is_last_packet=%d, session_id=%ld",
              packet_cnt, is_last_packet, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == rc.result_code_ && tablet_reader->has_new_data())
        {
          rc.result_code_ = queue_thread.wait_for_next_request(
              session_id, next_request, THE_CHUNK_SERVER.get_config().datasource_timeout);
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            //end this session
            rc.result_code_ = OB_SUCCESS;
            if (NULL != next_request)
            {
              req = next_request->get_request();
              easy_request_wakeup(req);
            }
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d",
                rc.result_code_);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
            out_buffer.get_position() = rc.get_serialize_size();
            rc.result_code_ = tablet_reader->read_next(out_buffer);
            if(!tablet_reader->has_new_data())
            {
              is_last_packet = true;
            }

            int64_t pos = 0;
            if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), pos)))
            {
              TBSYS_LOG(ERROR, "serialize rc code failed, ret:%d", serialize_ret);
              break;
            }
          }
        }
        else
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      int ret = 0;
      if(NULL != tablet_reader && OB_SUCCESS != (ret = tablet_reader->close()))
      {
        TBSYS_LOG(WARN, "tablet reader close fail, ret:%d", ret);
      }

      FILL_TRACE_LOG("send response, packet_cnt=%ld, session_id=%ld, io stat: %s, ret=%d",
          packet_cnt, session_id, get_io_stat_str(), rc.result_code_);

      if(OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "cs_tablet_read finish, rc:%d", rc.result_code_);
      }
      else
      {
        TBSYS_LOG(INFO, "cs_tablet_read finish, rc:%d", rc.result_code_);
      }


      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      reset_internal_status();

      FILL_TRACE_LOG("end");
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      return rc.result_code_;
    }

    int ObChunkService::cs_fetch_data(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_FETCH_DATA_VERSION = 3;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int32_t is_inc_migrate_task_count = 0;
      tbsys::CRLockGuard guard(del_tablet_rwlock_);

      if (version != CS_FETCH_DATA_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObDataSourceDesc desc;
      CharArena allocator;
      ObTabletFileWriter* tablet_writer = GET_TSI_MULT(ObTabletFileWriter, TSI_CS_FETCH_DATA_1);

      if (NULL == tablet_writer)
      {
        TBSYS_LOG(ERROR, "failed to get thread local tablet_writer, "
            "tablet_writer=%p", tablet_writer);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        tablet_writer->reset();
      }


      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = desc.deserialize(allocator, in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse data source param error.");
        }
        //add tianz[cs_admin migrate tablet]20141125:b
        if (!desc.dst_server_.is_valid())
        {
          desc.dst_server_ = chunk_server_->get_self();
        }
        //add 20141125 e
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fetch_data rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_MIGRATE_OVER_RESPONSE,
            CS_FETCH_DATA_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(INFO, "begin migrate data %s", to_cstring(desc));
      }

      if (!chunk_server_->get_config().merge_migrate_concurrency
          && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot migrate");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int64_t max_migrate_task_count = chunk_server_->get_config().max_migrate_task_count;
      if (OB_SUCCESS == rc.result_code_ )
      {
        uint32_t old_migrate_task_count = migrate_task_count_;
        while(old_migrate_task_count < max_migrate_task_count)
        {
          uint32_t tmp = atomic_compare_exchange(&migrate_task_count_, old_migrate_task_count+1, old_migrate_task_count);
          if (tmp == old_migrate_task_count)
          {
            is_inc_migrate_task_count = 1;
            break;
          }
          old_migrate_task_count = migrate_task_count_;
        }

        if (0 == is_inc_migrate_task_count)
        {
          TBSYS_LOG(WARN, "current migrate task count = %u, exceeded max = %ld",
              old_migrate_task_count, max_migrate_task_count);
          rc.result_code_ = OB_CS_EAGAIN;
        }
        else
        {
          TBSYS_LOG(INFO, "current migrate task count = %u", migrate_task_count_);
        }
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();
      int64_t tablet_version = 0;

      if (OB_SUCCESS == rc.result_code_ )
      {
        int16_t remote_sstable_version = 0;
        int16_t local_sstable_version = 0;
        int64_t old_tablet_version = 0;
        bool migrate_locked = true;

        out_buffer.get_position() = 0;
        ObTabletRemoteReader remote_reader(out_buffer, tablet_manager);
        local_sstable_version = static_cast<int16_t>(THE_CHUNK_SERVER.get_config().merge_write_sstable_version);
        int64_t sequence_num = 0;
        int64_t row_checksum = 0;
        int64_t sstable_type = 0;
        desc.sstable_version_ = local_sstable_version;
        tablet_version = desc.tablet_version_;

        //check is in pending in upgrade
        if (!tablet_manager.get_chunk_merge().get_migrate_lock().try_rdlock())
        {
          TBSYS_LOG(WARN, "daily merge is pending in upgrade");
          migrate_locked = false;
          rc.result_code_ = OB_CS_EAGAIN;
        }
        //remote reader prepare, send first request, and get the remote_sstable_version of the src server
        else if(OB_SUCCESS != (rc.result_code_ =
              tablet_manager.check_fetch_tablet_version(desc.range_, tablet_version, old_tablet_version)))
        {
          TBSYS_LOG(WARN, "tablet range:%s data_versin:%ld check failed, ret:%d",
              to_cstring(desc.range_), tablet_version, rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = remote_reader.prepare(desc,
                remote_sstable_version, sequence_num, row_checksum, sstable_type)))
        {
          TBSYS_LOG(WARN, "prepare remote reader failed ret:%d", rc.result_code_);
        }
        else if(local_sstable_version != remote_sstable_version)
        {
          rc.result_code_ = OB_SSTABLE_VERSION_UNEQUAL;
          TBSYS_LOG(ERROR, "sstable version not equal local_sstable_version:%d remote_sstable_version:%d",
              local_sstable_version, remote_sstable_version);
        }
        else if(OB_SUCCESS != (rc.result_code_ = remote_reader.open()))
        {
          TBSYS_LOG(WARN, "remote reader open failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ =
              tablet_writer->prepare(desc.range_, tablet_version, sequence_num, row_checksum, &tablet_manager)))
        {
          TBSYS_LOG(WARN, "remote writer prepare failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = tablet_writer->open()))
        {
          TBSYS_LOG(WARN, "remote writer open failed, ret:%d", rc.result_code_);
        }
        else
        {
          rc.result_code_ = pipe_buffer(tablet_manager, remote_reader, *tablet_writer);

          int ret = OB_SUCCESS;
          if(OB_SUCCESS != (ret = remote_reader.close()))
          {
            TBSYS_LOG(WARN, "remote reader close failed, ret:%d", ret);
          }

          if(OB_SUCCESS == rc.result_code_)
          {
            if(OB_SUCCESS != (rc.result_code_ = tablet_writer->close()))
            {
              TBSYS_LOG(WARN, "tablet writer close failed, ret:%d", rc.result_code_);
            }
          }

        }

        if(OB_SUCCESS == rc.result_code_ && old_tablet_version > 0)
        {
          //set old tablet merged, not need to merge
          ObTablet* old_tablet = NULL;
          if(OB_SUCCESS == tablet_image.acquire_tablet(desc.range_,
                ObMultiVersionTabletImage::SCAN_FORWARD, old_tablet_version, old_tablet) &&
              old_tablet != NULL &&
              old_tablet->get_data_version() == old_tablet_version &&
              old_tablet->get_range().equal2(desc.range_))
          {
            old_tablet->set_merged();
            tablet_image.write(old_tablet->get_data_version(), old_tablet->get_disk_no());
          }
          else
          {
            TBSYS_LOG(WARN, "not found old tablet:%s old_tablet_version:%ld, maybe deleted",
                to_cstring(desc.range_), old_tablet_version);
          }

          if(NULL != old_tablet)
          {
            tablet_image.release_tablet(old_tablet);
            old_tablet = NULL;
          }
        }

        int ret = OB_SUCCESS;
        if(OB_SUCCESS != (ret = tablet_writer->cleanup()))
        {
          TBSYS_LOG(WARN, "tablet writer cleanup failed, ret:%d", ret);
        }

        if(migrate_locked)
        {
          //unlock
          tablet_manager.get_chunk_merge().get_migrate_lock().unlock();
        }
      }

      /**
       * it's better to decrease the migrate task counter before
       * calling migrate_over(), because rootserver keeps the same
       * migrate task counter of each chunkserver, if migrate_over()
       * returned, rootserver has decreased the migrate task counter
       * and send a new migrate task to this chunkserver immediately,
       * if this chunkserver hasn't decreased the migrate task counter
       * at this time, the chunkserver will return -1010 error. if
       * rootserver doesn't send migrate message successfully, it
       * doesn't increase the migrate task counter, so it sends
       * migrate message continiously and it receives a lot of error
       * -1010.
       */
      if (0 != is_inc_migrate_task_count)
      {
        atomic_dec(&migrate_task_count_);
      }


      //report migrate info to root server
      desc.tablet_version_ = tablet_version;
      if(OB_CS_MIGRATE_IN_EXIST == rc.result_code_)
      {
        rc.result_code_ = OB_SUCCESS;
      }
      int64_t occupy_size = 0;
      int64_t check_sum = 0;
      uint64_t row_checksum = 0;
      int64_t row_count = 0;
      ObTablet* tablet = NULL;
      if(OB_SUCCESS == rc.result_code_)
      {
        if(OB_SUCCESS == tablet_image.acquire_tablet_all_version(desc.range_,
              ObMultiVersionTabletImage::SCAN_FORWARD,
              ObMultiVersionTabletImage::FROM_NEWEST_INDEX, 0, tablet) &&
            NULL != tablet &&
            tablet->get_data_version() == desc.tablet_version_)
        {
          occupy_size = tablet->get_occupy_size();
          check_sum = tablet->get_checksum();
          row_checksum = tablet->get_row_checksum();
          row_count = tablet->get_row_count();
        }
        else
        {
          TBSYS_LOG(WARN, "add tablet succ, but is deleted, range:%s tablet_version:%ld",
              to_cstring(desc.range_), desc.tablet_version_);
          rc.result_code_ = OB_ERROR;
        }

        if(NULL != tablet)
        {
          tablet_image.release_tablet(tablet);
          tablet = NULL;
        }
      }

      int ret = OB_SUCCESS;
      ret = CS_RPC_CALL_RS(migrate_over, rc, desc, occupy_size, check_sum, row_checksum, row_count);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "report migrate over error, rc.code=%d", ret);
        if(OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = ret;
        }
      }
      else
      {
        TBSYS_LOG(INFO,"report migrate tablet <%s> over to rootserver success, ret=%d",
            to_cstring(desc.range_), rc.result_code_);
      }

      if(OB_SUCCESS == rc.result_code_ && false == desc.keep_source_ &&
          ObDataSourceDesc::OCEANBASE_INTERNAL == desc.type_)
      {
        ObTabletReportInfoList del_tablets_list;
        ObTabletReportInfo del_tablets_info;
        del_tablets_info.tablet_info_.range_ = desc.range_;
        del_tablets_info.tablet_location_.chunkserver_ = desc.src_server_;
        del_tablets_info.tablet_location_.tablet_version_ = desc.tablet_version_;
        del_tablets_list.add_tablet(del_tablets_info);

        rc.result_code_= CS_RPC_CALL(delete_tablets, desc.src_server_, del_tablets_list, false);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "send delete tablet <%s> to cs:%s error, rc.code=%d",
              to_cstring(desc.range_), to_cstring(desc.src_server_), rc.result_code_);
        }
        else
        {
          TBSYS_LOG(INFO, "send delete tablet <%s> to cs:%s succ, rc.code=%d",
              to_cstring(desc.range_), to_cstring(desc.src_server_), rc.result_code_);
        }
      }

      if(OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "fetch_data failed rc.code=%d",rc.result_code_);
      }
      else
      {
        TBSYS_LOG(INFO, "fetch_data finish rc.code=%d",rc.result_code_);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_load_bypass_sstables(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_LOAD_BYPASS_SSTABLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      int64_t serving_version = tablet_manager.get_serving_data_version();
      ObTableImportInfoList* table_list =
        GET_TSI_MULT(ObTableImportInfoList, TSI_CS_TABLE_IMPORT_INFO_1);

      if (version != CS_LOAD_BYPASS_SSTABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "failed to allocate memory for import table list info");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (!tablet_manager.get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot load bypass sstables");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!tablet_manager.get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot load bypass "
                        "sstables concurrency");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (OB_SUCCESS != (rc.result_code_ = table_list->deserialize(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize import table list info error, "
                        "load_version=%ld, err=%d",
          table_list->tablet_version_, rc.result_code_);
      }
      else if (serving_version < 0 || table_list->tablet_version_ < 1)
      {
        TBSYS_LOG(WARN, "invalid serving_version=%ld, load_version=%ld",
          serving_version, table_list->tablet_version_);
        rc.result_code_ = OB_ERROR;
      }
      else if (0 != serving_version && table_list->tablet_version_ != serving_version)
      {
        TBSYS_LOG(WARN, "load version is different from local serving verion, "
                        "load_verion=%ld, serving_version=%ld",
          table_list->tablet_version_, serving_version);
        rc.result_code_ = OB_ERROR;
      }

      TBSYS_LOG(INFO, "received load bypass sstable command, ret=%d", rc.result_code_);
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_load_bypass_sstables rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_LOAD_BYPASS_SSTABLE_RESPONSE,
            CS_LOAD_BYPASS_SSTABLE_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.load_bypass_sstables(*table_list);
      }

      bool is_start_load_succ = (OB_SUCCESS == rc.result_code_);
      if (!is_start_load_succ && NULL != table_list
          && table_list->response_rootserver_)
      {
        //report load bypass sstables over info to root server
        rc.result_code_= CS_RPC_CALL_RS(load_bypass_sstables_over,
            chunk_server_->get_self(), *table_list,
            is_start_load_succ);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "report load bypass sstables over error, is_start_load_succ=%d",
              is_start_load_succ);
        }
      }
      TBSYS_LOG(INFO, "start load bypass sstables, load_version=%ld, "
                      "is_response_rootserver=%d, is_start_load_succ=%d, ret=%d",
        table_list->tablet_version_, table_list->response_rootserver_,
        is_start_load_succ, rc.result_code_);

      return rc.result_code_;
    }

    int ObChunkService::cs_delete_table(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DELETE_TABLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      uint64_t table_id = 0;
      const bool is_block = false;
      tbsys::CWLockGuard guard(del_tablet_rwlock_, is_block);
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();

      if (version != CS_DELETE_TABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!guard.acquired())
      {
        rc.result_code_ = OB_CS_EAGAIN;
        TBSYS_LOG(WARN, "migrating tablet, cannot do delete now");
      }
      else if (!tablet_manager.get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot delete table");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!tablet_manager.get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot delete table");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade())
      {
        TBSYS_LOG(WARN, "other thread is scanning tablet images, "
                        "can't iterate it concurrency");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "deserialize table_id error, err=%d", rc.result_code_);
      }

      TBSYS_LOG(INFO, "received delete table command, table_id=%lu, ret=%d",
        table_id, rc.result_code_);
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_load_bypass_sstables rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DELETE_TABLE_RESPONSE,
            CS_DELETE_TABLE_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.delete_table(table_id);
      }

      bool is_delete_succ = (OB_SUCCESS == rc.result_code_);
      //report load bypass sstables over info to root server
      rc.result_code_= CS_RPC_CALL_RS(delete_table_over,
          chunk_server_->get_self(), table_id, is_delete_succ);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "report delete table over error, is_delete_succ=%d",
            is_delete_succ);
      }

      TBSYS_LOG(INFO, "load delete table over, table_id=%lu, "
                      "is_delete_succ=%d, ret=%d",
          table_id, is_delete_succ, rc.result_code_);

      return rc.result_code_;
    }

    //add liumz, [secondary index delete global index sstable]20160621:b
    int ObChunkService::cs_delete_global_index(uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();

      if (!tablet_manager.get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot delete table");
        ret = OB_CS_EAGAIN;
      }
      else if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade())
      {
        TBSYS_LOG(WARN, "other thread is scanning tablet images, "
                        "can't iterate it concurrency");
        ret = OB_CS_EAGAIN;
      }


      TBSYS_LOG(INFO, "received delete table command, table_id=%lu, ret=%d",
        table_id, ret);

      if (OB_SUCCESS == ret)
      {
        ret = tablet_manager.delete_table(table_id);
      }

      bool is_delete_succ = (OB_SUCCESS == ret);

      TBSYS_LOG(INFO, "load delete table over, table_id=%lu, "
                      "is_delete_succ=%d, ret=%d",
          table_id, is_delete_succ, ret);

      return ret;
    }
    //add:e

    int ObChunkService::cs_fetch_sstable_dist(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_FETCH_SSTABLE_DIST_VERSION = 1;
      int ret = OB_SUCCESS;
      int rpc_ret= OB_SUCCESS;
      int64_t table_version = 0;
      int32_t response_cid = channel_id;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      ObPacketQueueThread& queue_thread =
        chunk_server_->get_default_task_queue_thread();
      int64_t session_id = queue_thread.generate_session_id();
      ObPacket* next_request = NULL;
      const int64_t timeout = chunk_server_->get_config().network_timeout;
      ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
      ObMemBuf mem_buf;
      bool is_increased_scan_tablet_image_count = false;
      const int64_t BUF_SIZE = OB_MAX_PACKET_LENGTH - 32*1024; //2MB - 32KB
      int64_t buf_pos = 0;
      char* buf = NULL;
      ObNewRange range;
      ObString sstable_path;
      ObTablet* tablet = NULL;
      int64_t sstable_count = 0;
      char path_buf[OB_MAX_FILE_NAME_LENGTH];

      if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
          || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade()
          || 0 != atomic_compare_exchange(&scan_tablet_image_count_, 1, 0))
      {
        TBSYS_LOG(WARN, "someone else is scanning tablets. give up this process");
        ret = OB_EAGAIN;
      }
      else
      {
        is_increased_scan_tablet_image_count = true;
        if (version != CS_FETCH_SSTABLE_DIST_VERSION)
        {
          ret = OB_ERROR_FUNC_VERSION;
          TBSYS_LOG(WARN, "server version of cs_fetch_sstable_dist is %d, "
              "request version is %d", CS_FETCH_SSTABLE_DIST_VERSION, version);
        }
        else if (OB_SUCCESS != (ret =
              serialization::decode_vi64(in_buffer.get_data(),
                in_buffer.get_capacity(), in_buffer.get_position(), &table_version)))
        {
          TBSYS_LOG(WARN, "failed to decode table_version, cap=%ld pos=%ld ret=%d",
              in_buffer.get_capacity(), in_buffer.get_position(), ret);
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = mem_buf.ensure_space(BUF_SIZE)))
          {
            TBSYS_LOG(WARN, "failed to ensure space, buf size=%ld ret=%d", BUF_SIZE, ret);
          }
          else
          {
            buf = mem_buf.get_buffer();
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (tablet_image.get_serving_version() != table_version)
          {
            TBSYS_LOG(WARN, "serving version[%ld] of this cs is not same as request[%ld]",
                tablet_image.get_serving_version(), table_version);
            ret = OB_INVALID_DATA;
          }
          else if (OB_SUCCESS != (ret = tablet_image.begin_scan_tablets()))
          {
            TBSYS_LOG(WARN, "failed to begin scan tablets, ret=%d", ret);
          }
        }
      }

      do
      {
        if (OB_SUCCESS == ret)
        {
          ret = tablet_image.get_next_tablet(tablet);
          if (OB_ITER_END == ret)
          {
            //do nothig
          }
          else if (OB_SUCCESS == ret && NULL != tablet)
          {
            range = tablet->get_range();
            if (range.table_id_ < common::OB_APP_MIN_TABLE_ID)
            {
              TBSYS_LOG(INFO, "skip internal table %lu", range.table_id_);
              continue;
            }

            if (table_version != tablet->get_data_version())
            {
              ret = OB_INVALID_DATA;
              TBSYS_LOG(WARN, "tablet data version[%ld]  is not same as request[%ld]",
                  tablet->get_data_version(), table_version);
            }
            else
            {
              int64_t sstable_num = tablet->get_sstable_id_list().count();
              if (1 != sstable_num)
              {
                ret = OB_INVALID_DATA;
                TBSYS_LOG(WARN, "one tablet should only has one sstable, recent: %ld range: %s",
                    sstable_num, to_cstring(range));
              }
              else
              {
                const sstable::ObSSTableId& sstable_id = tablet->get_sstable_id_list().at(0);
                ret = get_sstable_path(sstable_id, path_buf, sizeof(path_buf));
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "failed to get sstable path with sstable id %ld range %s, ret=%d",
                      sstable_id.sstable_file_id_, to_cstring(range), ret);
                }
                else
                {
                  sstable_path.assign_ptr(path_buf, static_cast<ObString::obstr_size_t>(strlen(path_buf)));
                }
              }
            }

            if (OB_SUCCESS != (ret = tablet_image.release_tablet(tablet)))
            {
              TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, ret=%d", tablet, ret);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next tablet info, ret=%d", ret);
          }
        }

        if (OB_SUCCESS != ret
            || BUF_SIZE - buf_pos < range.get_serialize_size() + sstable_path.get_serialize_size())
        {
          ObResultCode rc;
          rc.result_code_ = ret;
          out_buffer.get_position() = 0;
          if (OB_SUCCESS != (rpc_ret= rc.serialize(out_buffer.get_data(),
                  out_buffer.get_capacity(), out_buffer.get_position())))
          {
            TBSYS_LOG(WARN, "failed to serialize rc, cap=%ld pos=%ld ret=%d",
                out_buffer.get_capacity(), out_buffer.get_position(), rpc_ret);
            break;
          }

          if (OB_SUCCESS == ret || OB_ITER_END == ret)
          {
            rpc_ret = common::serialization::encode_vi64(
                out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), sstable_count);
            if (OB_SUCCESS != rpc_ret)
            {
              TBSYS_LOG(WARN, "failed to encode sstable count, cap=%ld pos=%ld ret=%d",
                   out_buffer.get_capacity(), out_buffer.get_position(), rpc_ret);
              break;
            }
            else if (out_buffer.get_remain() < buf_pos)
            {
              TBSYS_LOG(WARN, "failed to write tablet info, buf remain %ld, buf_pos %ld",
                  out_buffer.get_remain(), buf_pos);
              rpc_ret = OB_BUF_NOT_ENOUGH;
              break;
            }
            else
            {
              ::memcpy(out_buffer.get_data() + out_buffer.get_position(), buf, buf_pos);
              out_buffer.get_position() += buf_pos;
            }
          }

          rpc_ret = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != rpc_ret)
          {
            TBSYS_LOG(WARN, "failed to prepare next request: session_id=%ld, ret=%d", session_id, rpc_ret);
            break;
          }
          else if (OB_SUCCESS != (rpc_ret =
                chunk_server_->send_response(OB_SUCCESS != ret? OB_NET_SESSION_END: OB_CS_FETCH_SSTABLE_DIST_RESPONSE,
                  CS_FETCH_SSTABLE_DIST_VERSION, out_buffer, req, response_cid, session_id)))
          {
            TBSYS_LOG(WARN, "failed to send response, response_cid=%d, session_id=%ld, ret=%d",
                response_cid, session_id, rpc_ret);
            break;
          }

          if (OB_SUCCESS != ret || OB_SUCCESS != rpc_ret)
          {
            break;
          }

          rpc_ret = queue_thread.wait_for_next_request(session_id, next_request, timeout);
          if (OB_NET_SESSION_END == rpc_ret)
          {
            rpc_ret = OB_SUCCESS;
            TBSYS_LOG(INFO, "client server stop fetching sstable dist");
            break;
          }
          else if (OB_SUCCESS != rpc_ret)
          {
            TBSYS_LOG(WARN, "failed to wait for next request, session_id=%ld timeout=%ld ret=%d",
                session_id, timeout, rpc_ret);
            break;
          }
          response_cid = next_request->get_channel_id();
          req = next_request->get_request();
          sstable_count = 0;
          buf_pos = 0;
        }

        ++sstable_count;
        if (OB_SUCCESS != (ret = range.serialize(buf, BUF_SIZE, buf_pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize range: %s, buf size=%ld buf pos=%ld ret=%d",
              to_cstring(range), BUF_SIZE, buf_pos, ret);
        }
        else if (OB_SUCCESS != (ret = sstable_path.serialize(buf, BUF_SIZE, buf_pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize path: %s, buf size=%ld buf pos=%ld ret=%d",
              path_buf, BUF_SIZE, buf_pos, ret);
        }
      } while(OB_SUCCESS == ret && OB_SUCCESS == rpc_ret);

      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret && OB_SUCCESS != rpc_ret)
      {
        ret = rpc_ret;
      }

      if (is_increased_scan_tablet_image_count)
      {
        int tmp_ret = tablet_image.end_scan_tablets();
        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(WARN, "failed to end scan tablets, ret=%d", tmp_ret);
          if (OB_SUCCESS == ret)
          {
            ret = tmp_ret;
          }
        }
        atomic_exchange(&scan_tablet_image_count_, 0);
      }
      TBSYS_LOG(INFO, "complete to handle fetch_sstable_dist, ret=%d", ret);

      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      return ret;
    }

    int ObChunkService::cs_set_config(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(version);
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
      else if (OB_SUCCESS != (ret = chunk_server_->get_config().add_extra_config(config_str.ptr(), true)))
      {
        TBSYS_LOG(ERROR, "Set config failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = chunk_server_->get_config_mgr().reload_config()))
      {
        TBSYS_LOG(ERROR, "Reload config failed! ret: [%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "Set config successfully! str: [%s]", config_str.ptr());
        chunk_server_->get_config().print();
      }

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buffer.get_data(),
                                             out_buffer.get_capacity(),
                                             out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = chunk_server_->send_response(OB_SET_CONFIG_RESPONSE,
                                                                 MY_VERSION, out_buffer, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    int ObChunkService::cs_get_config(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      UNUSED(version);
      UNUSED(in_buffer);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;

      if (NULL == chunk_server_)
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
               (ret = chunk_server_->get_config().serialize(out_buffer.get_data(),
                                                            out_buffer.get_capacity(),
                                                            out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize configuration fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = chunk_server_->send_response(OB_GET_CONFIG_RESPONSE,
                                                                 MY_VERSION, out_buffer, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    int ObChunkService::cs_get_bloom_filter(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_time)
    {
      UNUSED(version);
      int ret = OB_SUCCESS;
      common::ObResultCode rc;

      const int64_t MAX_BUFSIZ_PER_REQUEST = OB_MAX_PACKET_LENGTH - 65535;
      const int64_t MY_VERSION = 1;

      CharArena allocator;
      ObNewRange range;
      int64_t tablet_version = 0;
      int64_t bf_version = 0;
      ObTablet* tablet = NULL;
      ObSSTableReader* reader = NULL;
      int32_t reader_size = 1;


      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      bool    is_fullfilled = true;
      int64_t current_response_pos = 0;
      int64_t current_response_length = 0;
      int64_t bf_serialize_size = 0;
      char*   bf_buffer = NULL;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread = chunk_server_->get_default_task_queue_thread();
      ObString response_buffer;

      if (OB_SUCCESS != (ret = range.deserialize(allocator,
              in_buffer.get_data(), in_buffer.get_capacity(),
              in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize range error, cap:%ld, pos:%ld",
            in_buffer.get_capacity(), in_buffer.get_position());
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(
              in_buffer.get_data(), in_buffer.get_capacity(),
              in_buffer.get_position(), &tablet_version)))
      {
        TBSYS_LOG(WARN, "deserialize tablet_version error, cap:%ld, pos:%ld",
            in_buffer.get_capacity(), in_buffer.get_position());
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(
              in_buffer.get_data(), in_buffer.get_capacity(),
              in_buffer.get_position(), &bf_version)))
      {
        TBSYS_LOG(WARN, "deserialize tablet_version error, cap:%ld, pos:%ld",
            in_buffer.get_capacity(), in_buffer.get_position());
      }
      else if ( OB_SUCCESS != (ret = chunk_server_->get_tablet_manager().
            get_serving_tablet_image().acquire_tablet(
              range, ObMultiVersionTabletImage::SCAN_FORWARD, tablet_version, tablet))
          || NULL == tablet || tablet->get_data_version() != tablet_version)
      {
        TBSYS_LOG(WARN, "acquire_tablet ret=%d, range:%s, version:%ld, tablet=%p, tv=%ld",
            ret, to_cstring(range), tablet_version, tablet,
            (NULL != tablet) ? tablet->get_data_version() : 0);
        ret = OB_CS_TABLET_NOT_EXIST;
      }
      else if ( range.compare_with_startkey2(tablet->get_range()) != 0
          || range.compare_with_endkey2(tablet->get_range()) != 0)
      {
        TBSYS_LOG(WARN, "input tablet range:<%s> not equal to local range:<%s>",
            to_cstring(range), to_cstring(tablet->get_range()));
        ret = OB_CS_TABLET_NOT_EXIST;
      }
      else if ( OB_SUCCESS != (ret = tablet->find_sstable(range, &reader, reader_size)) )
      {
        TBSYS_LOG(WARN, "get reader ret=%d, range:%s, version:%ld, reader=%p",
            ret, to_cstring(range), tablet_version, reader);
      }
      else if ( NULL == reader || reader_size <= 0 || reader->get_row_count() == 0)
      {
        TBSYS_LOG(INFO, "tablet:%s version:%ld has no sstable.", to_cstring(range), tablet_version);
        ret = OB_CS_EMPTY_TABLET;
      }
      else if ( OB_SUCCESS != (ret = reader->serialize_bloom_filter(
              bf_version, bf_buffer, bf_serialize_size)))
      {
        TBSYS_LOG(WARN, "get NULL bloom_filter range:%s, version:%ld, reader=%p, ret=%d",
            to_cstring(range), tablet_version, reader, ret);
      }

      if (OB_SUCCESS == ret)
      {
        if (bf_serialize_size > MAX_BUFSIZ_PER_REQUEST)
        {
          session_id = queue_thread.generate_session_id();
          current_response_length = MAX_BUFSIZ_PER_REQUEST;
          is_fullfilled = false;
        }
        else
        {
          is_fullfilled = true;
          current_response_length = bf_serialize_size;
        }
      }

      // release tablet when bloom filter serialize done;
      if (NULL != tablet)
      {
        if (NULL != reader
            && (ob_get_mod_memory_usage(ObModIds::OB_BLOOM_FILTER)
             > ObBloomFilterV1::MAX_BLOOM_FILTER_SIZE * 500))
        {
          TBSYS_LOG(INFO, "cache bloom filter memory:%ld too large, destroy current.",
              ob_get_mod_memory_usage(ObModIds::OB_BLOOM_FILTER));
          reader->destroy_bloom_filter();
        }

        if (OB_SUCCESS != (chunk_server_->get_tablet_manager().
              get_serving_tablet_image().release_tablet(tablet)))
        {
          TBSYS_LOG(WARN, "release tablet %p error.", tablet);
        }
      }

      do
      {
        if (OB_SUCCESS == ret && !is_fullfilled)
        {
          ret = queue_thread.prepare_for_next_request(session_id);
        }

        out_buffer.get_position() = 0;
        rc.result_code_ = ret;
        int serialize_ret = rc.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());

        if (OB_SUCCESS == serialize_ret && OB_SUCCESS == ret)
        {
          response_buffer.assign_ptr(bf_buffer + current_response_pos,
              static_cast<int32_t>(current_response_length));
          // serialize reponse buffer and fullfilled flag.
          if (OB_SUCCESS != (serialize_ret = response_buffer.serialize(
                  out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position())))
          {
            TBSYS_LOG(WARN, "serialize response buffer error ret=%d,"
                "buffer size=%d, out buffer cap=%ld, pos=%ld",
                serialize_ret, response_buffer.length(),
                out_buffer.get_capacity(), out_buffer.get_position());
          }
          if (OB_SUCCESS != (serialize_ret = serialization::encode_bool(
                  out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position(), is_fullfilled)))
          {
            TBSYS_LOG(WARN, "serialize fullfilled error ret=%d,"
                "is_fullfilled=%d, out buffer cap=%ld, pos=%ld",
                serialize_ret, is_fullfilled,
                out_buffer.get_capacity(), out_buffer.get_position());
          }
          if (OB_SUCCESS != (serialize_ret = serialization::encode_vi64(
                  out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position(), bf_serialize_size)))
          {
            TBSYS_LOG(WARN, "serialize total size error ret=%d,"
                "bf_serialize_size=%ld, out buffer cap=%ld, pos=%ld",
                serialize_ret, bf_serialize_size,
                out_buffer.get_capacity(), out_buffer.get_position());
          }
        }

        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(WARN, "serialize response failed. ret=%d", serialize_ret);
          break ;
        }
        else
        {
          TBSYS_LOG(INFO, "SEND response::input range=%s, data=%ld, bf=%ld, "
              "session_id=%ld, is_fullfilled=%d, cur_pos=%ld, cur_len=%ld, total_size=%ld",
              to_cstring(range), tablet_version, bf_version, session_id,
              is_fullfilled, current_response_pos, current_response_length, bf_serialize_size);
          chunk_server_->send_response(
              is_fullfilled ? OB_SESSION_END : OB_CS_FETCH_BLOOM_FILTER_RESPONSE,
              MY_VERSION, out_buffer, req, response_cid, session_id);

        }

        if (OB_SUCCESS == ret)
        {

          // send all reponse buffer done.
          if (is_fullfilled) break;

          ret = queue_thread.wait_for_next_request(
              session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_NET_SESSION_END == ret)
          {
            ret = OB_SUCCESS;
            easy_request_wakeup(req);
            break;
          }
          else if (OB_SUCCESS == ret)
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();

            current_response_pos += current_response_length;
            current_response_length = bf_serialize_size - current_response_pos;

            if (current_response_length > MAX_BUFSIZ_PER_REQUEST)
            {
              current_response_length = MAX_BUFSIZ_PER_REQUEST;
              is_fullfilled = false;
            }
            else if (current_response_length <= 0)
            {
              // assume cannot be here! should break earlier cause is_fullfilled == true.
              break;
            }
            else
            {
              is_fullfilled = true;
            }
          }
        }

      } while (OB_SUCCESS == ret);


      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }
      if (NULL != bf_buffer)
      {
        ob_tc_free(bf_buffer, ObModIds::OB_BLOOM_FILTER);
      }


      return ret;
    }

    bool ObChunkService::is_valid_lease()
    {
      int64_t current_time = tbsys::CTimeUtil::getTime();
      return current_time < service_expired_time_;
    }

    bool ObChunkService::have_frozen_table(const ObTablet& tablet,const int64_t ups_data_version) const
    {
      bool ret = false;
      int64_t                     cs_data_version = tablet.get_cache_data_version();
      int64_t                     cs_major        = ObVersion::get_major(cs_data_version);
      int64_t                     ups_major       = ObVersion::get_major(ups_data_version);
      int64_t                     ups_minor       = ObVersion::get_minor(ups_data_version);
      int64_t                     cs_minor        = ObVersion::get_minor(cs_data_version);
      bool                        is_final_minor  = ObVersion::is_final_minor(cs_data_version);

      //这里有两种情况，?种是cs还没有compactsstable,此时ups不可能还有cs_major版本的数据，
      //?要判断ups的minor大于OB_UPS_START_MINOR,比如cs的版本为1且没有compactsstable,那么ups版本?
      //2,2的时候才表明有新的minor freeze
      //另外?种情况是cs已经有了?部份compactsstable,cs_major和ups_major相等，如果minor的版?
      //相差大于1则有新的冻结表，比如cs的版本为(2?1)，ups版本?(2,3)。如果ups_major大于cs_major?
      //且cs_major版本下的?有数据已经拉取完?(is_final_minor为true),说明有新的大版本产生
      //目前限制cs?多只能拥有一个大版本的数据??

      TBSYS_LOG(DEBUG,"cs_version:%ld:%ld,ups_version:%ld:%ld,is_final_minor:%s",
                cs_major,cs_minor,ups_major,ups_minor,is_final_minor ? "Yes" : "No");

      if (cs_major <= 0)
      {
        ret = false; //initialize phase
      }
      else if (0 == cs_minor)
      {
        //cs have no compactsstable and ups major version equal to cs_major + 1,
        //then if usp_minor > 1,there is a frozen table
        if ( (ups_major > (cs_major + 1)) ||
             ((ups_major == (cs_major + 1)) && (ups_minor > OB_UPS_START_MINOR_VERSION)))
        {
          ret = true;
        }
      }
      else
      {
        //there is some compactsstable already
        if ((cs_major == ups_major) && ((ups_minor - cs_minor) > 1))
        {
          ret = true;
        }
        else if (ups_major > cs_major)
        {
          if (is_final_minor)
          {
            TBSYS_LOG(DEBUG,"there is some new frozen table,but we can hold only one major version");
          }
          else
          {
            ret = true;
          }
        }
        else
        {
          //there is no new frozen table
        }
      }

      return ret;
    }

    int ObChunkService::check_update_data(ObTablet& tablet,int64_t ups_data_version,bool& release_tablet)
    {
      int     ret = OB_SUCCESS;
      int64_t compactsstable_cache_size = chunk_server_->get_config().compactsstable_cache_size;
      int64_t usage_size = ob_get_mod_memory_usage(ObModIds::OB_COMPACTSSTABLE_WRITER);
      release_tablet = true;
      if (compactsstable_cache_size > usage_size)
      {
        ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
        if (have_frozen_table(tablet,ups_data_version) &&
            (!merge_task_.is_scheduled() && tablet_manager.get_chunk_merge().is_merge_stoped()) &&
            tablet.compare_and_set_compactsstable_loading())
        {
          if ( (tablet.get_compactsstable_num() > OB_MAX_COMPACTSSTABLE_NUM) ||
               ((ret = tablet_manager.get_cache_thread().push(&tablet,ups_data_version)) != OB_SUCCESS))
          {
            TBSYS_LOG(WARN,"put %p to cache thread failed,ret=%d,compactsstable num:%d",
                      &tablet,ret,tablet.get_compactsstable_num());
            tablet.clear_compactsstable_flag();
          }
          else
          {
            //do not release this tablet
            release_tablet = false;
            TBSYS_LOG(INFO,"push %p to cache thread,ups_data_version:%ld",&tablet,ups_data_version);
          }
        }
      }
      return ret;
    }

    void ObChunkService::LeaseChecker::runTimerTask()
    {
      if (NULL != service_ && service_->inited_)
      {
        if (!service_->is_valid_lease() && !service_->in_register_process_ )
        {
          TBSYS_LOG(INFO, "lease expired, re-register to root_server");
          service_->register_self();
        }


        // reschedule
        service_->timer_.schedule(*this,
            service_->chunk_server_->get_config().lease_check_interval, false);
      }
    }

    void ObChunkService::DiskChecker::runTimerTask()
    {
      ObTabletManager & tablet_manager = service_->chunk_server_->get_tablet_manager();
      int32_t bad_disk = -1;
      int ret = OB_SUCCESS;

      if (OB_IO_ERROR == tablet_manager.get_disk_manager().check_disk(bad_disk))
      {
        TBSYS_LOG(ERROR, "disk:%d failed, will automatically unload", bad_disk);
        int i = 0;

        while (i++ < THE_CHUNK_SERVER.get_config().retry_times)
        {
          ret = tablet_manager.uninstall_disk(bad_disk);
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "unload disk:%d, retry times:%d ret:%d",
                bad_disk, i, ret);
            usleep(static_cast<useconds_t>(THE_CHUNK_SERVER.get_config().network_timeout));
          }
          else
          {
            break;
          }
        }
      }

      // reschedule
      service_->timer_.schedule(*this,
          service_->chunk_server_->get_config().disk_check_interval, false);
    }

    void ObChunkService::StatUpdater::runTimerTask()
    {
      ObStat *stat = NULL;
      OB_STAT_GET(CHUNKSERVER, stat);
      if (NULL == stat)
      {
        TBSYS_LOG(DEBUG,"get stat failed");
      }
      else
      {
        int64_t request_count = stat->get_value(INDEX_META_REQUEST_COUNT);
        current_request_count_ = request_count - pre_request_count_;
        stat->set_value(INDEX_META_REQUEST_COUNT_PER_SECOND,current_request_count_);
        pre_request_count_ = request_count;

        // memory usage stats
        ObMultiVersionTabletImage::ObTabletImageStat image_stat;
        ObTabletManager& manager = THE_CHUNK_SERVER.get_tablet_manager();
        manager.get_serving_tablet_image().get_image_stat(image_stat);
        OB_STAT_SET(CHUNKSERVER, INDEX_CS_SERVING_VERSION, manager.get_serving_data_version());
        OB_STAT_SET(CHUNKSERVER, INDEX_META_OLD_VER_TABLETS_NUM, image_stat.old_ver_tablets_num_);
        OB_STAT_SET(CHUNKSERVER, INDEX_META_OLD_VER_MERGED_TABLETS_NUM, image_stat.old_ver_merged_tablets_num_);
        OB_STAT_SET(CHUNKSERVER, INDEX_META_NEW_VER_TABLETS_NUM, image_stat.new_ver_tablets_num_);
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_NETWORK, ob_get_mod_memory_usage(ObModIds::OB_COMMON_NETWORK));
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_THREAD_BUFFER, ob_get_mod_memory_usage(ObModIds::OB_THREAD_BUFFER));
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_TABLET, ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE));
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_BI_CACHE, manager.get_serving_block_index_cache().get_cache_mem_size());
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_BLOCK_CACHE, manager.get_serving_block_cache().size());
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_BI_CACHE_UNSERVING, manager.get_unserving_block_index_cache().get_cache_mem_size());
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_BLOCK_CACHE_UNSERVING, manager.get_unserving_block_cache().size());
        if (manager.get_join_cache().is_inited())
        {
          OB_STAT_SET(CHUNKSERVER, INDEX_MU_JOIN_CACHE, manager.get_join_cache().get_cache_mem_size());
        }
        if (NULL != manager.get_row_cache())
        {
          OB_STAT_SET(CHUNKSERVER, INDEX_MU_SSTABLE_ROW_CACHE, manager.get_row_cache()->get_cache_mem_size());
        }

        if (run_count_++ % 600 == 0) ob_print_mod_memory_usage();
      }
    }

    void ObChunkService::MergeTask::runTimerTask()
    {
      int err = OB_SUCCESS;
      ObTabletManager & tablet_manager = service_->chunk_server_->get_tablet_manager();
      unset_scheduled();

      if (frozen_version_ <= 0)
      {
        //initialize phase or slave rs switch to master but get frozen version failed
      }
      else if (frozen_version_  < tablet_manager.get_serving_tablet_image().get_newest_version())
      {
        TBSYS_LOG(ERROR,"mem frozen version (%ld) < newest local tablet version (%ld),exit",frozen_version_,
            tablet_manager.get_serving_tablet_image().get_newest_version());
        kill(getpid(),2);
      }
      else if (tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version_))
      {
        err = tablet_manager.merge_tablets(frozen_version_);
        if (err != OB_SUCCESS)
        {
          frozen_version_ = 0; //failed,wait for the next schedule
        }
      }
      else
      {
        tablet_manager.get_chunk_merge().set_newest_frozen_version(frozen_version_);
      }
    }

    int ObChunkService::get_master_master_update_server()
    {
      ObServer tmp_ups;
      return chunk_server_->get_rpc_proxy()->get_master_master_update_server(true, tmp_ups);

    }
    int ObChunkService::fetch_update_server_list()
    {
      int err = OB_SUCCESS;

      if (NULL == chunk_server_->get_rpc_proxy())
      {
        TBSYS_LOG(ERROR, "rpc_proxy_ is NULL");
      }
      else
      {
        int32_t count = 0;
        err = chunk_server_->get_rpc_proxy()->fetch_update_server_list(count);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fetch update server list failed:ret[%d]", err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "fetch update server list succ:count[%d]", count);
        }
      }

      return err;
    }

    //add wenghaixing [secondary index static_index_build]20150302
    void ObChunkService::IndexTask::runTimerTask()
    {
      int err = OB_SUCCESS;
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      unset_scheduled();
      TBSYS_LOG(INFO,"I want to start round to build index!");
      if(tablet_manager.get_index_worker().can_launch_next_round())
      {
        TBSYS_LOG(INFO,"I can launch next round!");
        err = tablet_manager.build_index();
        if(OB_SUCCESS != err)
        {
          TBSYS_LOG(INFO,"wait for next round!");
        }
      }
    }

    int ObChunkService::IndexTask::set_schedule_idx_tid(uint64_t table_id)
    {
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      return tablet_manager.get_index_worker().set_schedule_idx_tid(table_id);
    }

    int ObChunkService::IndexTask::set_schedule_time(uint64_t tid, int64_t start_time)
    {
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      return tablet_manager.get_index_worker().set_schedule_time(tid, start_time);
    }

    void ObChunkService::IndexTask::set_hist_width(int64_t hist_width)
    {
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      tablet_manager.get_index_worker().set_hist_width(hist_width);
    }

    bool ObChunkService::IndexTask::get_round_end()
    {
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      return tablet_manager.get_index_worker().get_round_end();
    }

    uint64_t ObChunkService::IndexTask::get_schedule_idx_tid()
    {
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      return tablet_manager.get_index_worker().get_schedule_idx_tid();
    }

    void ObChunkService::IndexTask::try_stop_mission(uint64_t index_tid)
    {
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      return tablet_manager.get_index_worker().try_stop_mission(index_tid);
    }
    //add e
    //add wenghaixing [secondary idnex static_index_build]20150321
    void ObChunkService::IndexCheckTask::runTimerTask()
    {
      //todo ObIndexWorker的定时自?任务
      //目的是为了防止有遗漏的tablet/range没有处理
      ObTabletManager& tablet_manager = service_->chunk_server_->get_tablet_manager();
      tablet_manager.get_index_worker().check_self();
    }
    //add e

    void ObChunkService::FetchUpsTask::runTimerTask()
    {
      service_->get_master_master_update_server();
      service_->fetch_update_server_list();
      // reschedule fetch updateserver list task with new interval.
      service_->timer_.schedule(*this,
        service_->chunk_server_->get_config().fetch_ups_interval, false);
    }

    int64_t ObChunkService::ReportTabletTask::get_next_wait_useconds()
    {
      if (wait_seconds_ < 1) wait_seconds_ = 1;
      wait_seconds_ <<= 1;
      return wait_seconds_ * 1000000LL;
    }
    void ObChunkService::ReportTabletTask::runTimerTask()
    {
      int ret = OB_SUCCESS;
      ObTabletManager & tablet_manager = service_->chunk_server_->get_tablet_manager();
      if (service_->in_register_process_
          || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
          || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade()
          || 0 != atomic_compare_exchange(&service_->scan_tablet_image_count_, 1, 0))
      {
        TBSYS_LOG(WARN, "someone else is reporting. give up this process");
      }
      else if (tablet_manager.get_serving_data_version() > report_version_)
      {
        // got new data version, current retry round abort.
        TBSYS_LOG(INFO, "service_version(%ld) upgrade, no need report current (%ld)",
            tablet_manager.get_serving_data_version(), report_version_);
      }
      else
      {
        TBSYS_LOG(INFO, "schedule retry report tablets, report version:%ld, wait_seconds_=%ld",
            report_version_, wait_seconds_);
        ret = tablet_manager.report_tablets();
        if (OB_SUCCESS != ret)
        {
          if (OB_RESPONSE_TIME_OUT == ret)
          {
            // reschedule report task;
            service_->timer_.schedule(*this, get_next_wait_useconds(), false);
            TBSYS_LOG(WARN, "report tablets timeout, wait %ld seconds and rereport.", wait_seconds_);
          }
        }
        else
        {
          tablet_manager.report_capacity_info();
        }
        atomic_exchange(&service_->scan_tablet_image_count_, 0);
      }
    }

    void ObChunkService::ReportTabletTask::start_retry_round()
    {
      ObTabletManager & tablet_manager = service_->chunk_server_->get_tablet_manager();
      report_version_ = tablet_manager.get_serving_data_version();
      wait_seconds_ = 1;
    }

    int ObChunkService::schedule_report_tablet()
    {
      // reset retry time interval;
      report_tablet_task_.start_retry_round();
      return timer_.schedule(report_tablet_task_,
          report_tablet_task_.get_next_wait_useconds(), false);
    }

  } // end namespace chunkserver
} // end namespace oceanbase
