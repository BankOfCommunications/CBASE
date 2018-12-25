/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *
 */

#include <stdint.h>
#include <tblog.h>
#include "common/ob_trace_log.h"
#include "common/ob_schema_manager.h"
#include "sstable/ob_sstable_schema.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_main.h"
#include "common/ob_tbnet_callback.h"
#include "ob_chunk_callback.h"
#include "common/ob_config_manager.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_fill_log.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace chunkserver
  {

    ObChunkServer::ObChunkServer(ObChunkServerConfig &config,
                                 ObConfigManager &config_mgr)
      : config_(config), config_mgr_(config_mgr),
        file_service_(), file_client_(), file_client_rpc_buffer_(),
        response_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
        rpc_buffer_(RPC_BUFFER_SIZE)
    {
    }

    ObChunkServer::~ObChunkServer()
    {
    }

    common::ThreadSpecificBuffer::Buffer* ObChunkServer::get_rpc_buffer() const
    {
      return rpc_buffer_.get_buffer();
    }

    common::ThreadSpecificBuffer::Buffer* ObChunkServer::get_response_buffer() const
    {
      return response_buffer_.get_buffer();
    }

    const common::ThreadSpecificBuffer* ObChunkServer::get_thread_specific_rpc_buffer() const
    {
      return &rpc_buffer_;
    }

    const common::ObServer& ObChunkServer::get_self() const
    {
      return self_;
    }

    const common::ObServer ObChunkServer::get_root_server() const
    {
      //mod pangtianze [Paxos rs_election] 20150708:b
      //return config_.get_root_server();
      return root_server_;
      //mod:e
    }
    //add pangtianze [Paxos rs_election] 20150707:b
    void ObChunkServer::set_all_root_server(const common::ObServer &server)
    {
      root_server_ = server;
      rpc_proxy_->set_root_server(server);
      service_.get_ms_list_task().set_rs(server);
      //add pangtianze [Paxos rs_election] 20161124
      ///dump to config file
      char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
      server.ip_to_string(ip_tmp,sizeof(ip_tmp));
      config_.root_server_ip.set_value(ip_tmp);
      config_.root_server_port = server.get_port();
      config_mgr_.dump2file();
      //add:e
    }
    //add:e

    //add huangjianwei [Paxos rs_election] 20160517:b
    void ObChunkServer:: set_root_server(const common::ObServer &server)
    {
      root_server_ = server;
    }
    //add:e
    const common::ObClientManager& ObChunkServer::get_client_manager() const
    {
      return client_manager_;
    }

    ObConfigManager & ObChunkServer::get_config_mgr()
    {
      return config_mgr_;
    }

    const ObChunkServerConfig & ObChunkServer::get_config() const
    {
      return config_;
    }

    ObChunkServerConfig & ObChunkServer::get_config()
    {
      return config_;
    }

    ObChunkServerStatManager & ObChunkServer::get_stat_manager()
    {
      return stat_;
    }

    const ObTabletManager & ObChunkServer::get_tablet_manager() const
    {
      return tablet_manager_;
    }

    ObTabletManager & ObChunkServer::get_tablet_manager()
    {
      return tablet_manager_;
    }

    int ObChunkServer::reload_config()
    {
      int ret = OB_SUCCESS;
      ObTabletManager& tablet_manager = get_tablet_manager();
      const ObChunkServerConfig& config = get_config();

      tablet_manager.get_chunk_merge().set_config_param();
      set_default_queue_size((int)config.task_queue_size);
      set_min_left_time(config.task_left_time);
      tablet_manager.get_serving_block_cache().enlarg_cache_size(config.block_cache_size);
      tablet_manager.get_serving_block_index_cache().enlarg_cache_size(config.block_index_cache_size);
      tablet_manager.get_fileinfo_cache().enlarg_cache_num(config.file_info_cache_num);
      tablet_manager.get_join_cache().enlarg_cache_size(config.block_index_cache_size);
      if (NULL != tablet_manager.get_row_cache())
      {
        tablet_manager.get_row_cache()->enlarg_cache_size(config.sstable_row_cache_size);
      }

      ObMergerRpcProxy* rpc_proxy = get_rpc_proxy();
      if (NULL != rpc_proxy)
      {
        ret = rpc_proxy->set_rpc_param(config.retry_times, config.network_timeout);
        if (OB_SUCCESS == ret)
        {
          ret = rpc_proxy->set_blacklist_param( config.ups_blacklist_timeout, config.ups_fail_count);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "set update server black list param failed:ret=%d", ret);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
        ret = OB_NOT_INIT;
      }
      return ret;
    }

    common::ObGeneralRpcStub & ObChunkServer::get_rpc_stub()
    {
      return rpc_stub_;
    }

    ObMergerRpcProxy* ObChunkServer::get_rpc_proxy()
    {
      return rpc_proxy_;
    }

    ObMergerSchemaManager* ObChunkServer::get_schema_manager()
    {
      return schema_mgr_;
    }

    ObFileClient& ObChunkServer::get_file_client()
    {
      return file_client_;
    }

    ObFileService& ObChunkServer::get_file_service()
    {
      return file_service_;
    }

    int ObChunkServer::set_self(const char* dev_name, const int32_t port)
    {
      int ret = OB_SUCCESS;
      int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      if (0 == ip)
      {
        TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        bool res = self_.set_ipv4_addr(ip, port);
        //add lbzhong [Paxos Cluster.Balance] 20160707:b
        self_.cluster_id_ = (int32_t) config_.cluster_id;
        //add:e
        if (!res)
        {
          TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
              dev_name, port);
          ret = OB_ERROR;
        }
      }
      return ret;
    }
    //add huangjianwei [Paxos rs_election] 20160517:b
    int ObChunkServer::init_rpc()
    {
      int ret = OB_SUCCESS;

      ret = rpc_stub_.init(&rpc_buffer_, &client_manager_);

      if (OB_SUCCESS == ret)
      {
        ret = sql_rpc_stub_.init(&rpc_buffer_, &client_manager_);
      }

      return ret;
    }
    //add:e

    int ObChunkServer::init_merge_join_rpc()
    {
      int ret = OB_SUCCESS;
      ObSchemaManagerV2 *newest_schema_mgr = NULL;
      int64_t retry_times = 0;
      int64_t timeout = 0;

      if (OB_SUCCESS == ret)
      {
        ret = rpc_stub_.init(&rpc_buffer_, &client_manager_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = sql_rpc_stub_.init(&rpc_buffer_, &client_manager_);
      }

      if (OB_SUCCESS == ret)
      {
        newest_schema_mgr = new(std::nothrow)ObSchemaManagerV2;
        if (NULL == newest_schema_mgr)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }


      if (OB_SUCCESS == ret)
      {
        schema_mgr_ = new(std::nothrow)ObMergerSchemaManager;
        if (NULL == schema_mgr_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          timeout = config_.network_timeout;
          int64_t retry_times = 0;
          while (!stoped_)
          {
            // fetch core table schema for startup.
            ret = rpc_stub_.fetch_schema(timeout, get_root_server(),
                                          0, true, *newest_schema_mgr);
            if (OB_SUCCESS == ret || OB_RESPONSE_TIME_OUT != ret) break;
            usleep(RETRY_INTERVAL_TIME);
            TBSYS_LOG(INFO, "retry to fetch core schema:retry_times[%ld]", retry_times ++);
          }
          if (OB_SUCCESS == ret)
          {
            ret = schema_mgr_->init(true, *newest_schema_mgr);
          }
        }

        if (OB_SUCCESS == ret)
        {
          sstable::set_global_sstable_schema_manager(schema_mgr_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        rpc_proxy_ = new(std::nothrow)ObMergerRpcProxy(
          retry_times, timeout, get_root_server());
        if (NULL == rpc_proxy_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_->init(&rpc_stub_, &sql_rpc_stub_, schema_mgr_
                               //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                               , self_.cluster_id_
                               //add:e
                               );
      }

      // set update server black list param
      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_->set_blacklist_param(config_.ups_blacklist_timeout,
                                              config_.ups_fail_count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "set update server black list config failed:ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObServer update_server;
        ret = rpc_proxy_->get_update_server(true, update_server);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t count = 0;
        int64_t retry_times = 0;
        while (!stoped_)
        {
          ret = rpc_proxy_->fetch_update_server_list(count);
          if (OB_SUCCESS == ret)
          {
            TBSYS_LOG(INFO, "fetch update server list succ:count=%d", count);
            break;
          }
          if (OB_RESPONSE_TIME_OUT != ret)
          {
            break;
          }
          usleep(RETRY_INTERVAL_TIME);
          TBSYS_LOG(INFO, "retry to get ups list:retry_times[%ld]", retry_times ++);
        }
      }

      if (OB_SUCCESS != ret)
      {
        if (NULL != rpc_proxy_)
        {
          delete rpc_proxy_;
          rpc_proxy_ = NULL;
        }

        if (NULL != schema_mgr_)
        {
          delete schema_mgr_;
          schema_mgr_ = NULL;
        }
      }

      if (NULL != newest_schema_mgr)
      {
        delete newest_schema_mgr;
        newest_schema_mgr = NULL;
      }

      return ret;
    }

    int ObChunkServer::initialize()
    {
      int ret = OB_SUCCESS;
      // do not handle batch packet.
      // process packet one by one.
      set_batch_process(false);

      // set listen port
      if (OB_SUCCESS == ret)
      {
        ret = set_listen_port((int)config_.port);
      }

      //set call back func
      if (OB_SUCCESS == ret)
      {
        memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
        server_handler_.encode = ObTbnetCallback::encode;
        server_handler_.decode = ObTbnetCallback::decode;
        server_handler_.process = ObChunkCallback::process;
        //server_handler_.batch_process = ObTbnetCallback::batch_process;
        server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
        server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
        server_handler_.user_data = this;
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_dev_name(config_.devname);
        if (OB_SUCCESS == ret)
        {
          ret = set_self(config_.devname, (int32_t)config_.port);
        }
      }
      if (OB_SUCCESS == ret)
      {
        set_self_to_thread_queue(self_);
      }
      //add pangtianze [Paxos rs_election] 20150710:b
      if (ret == OB_SUCCESS)
      {
        ret = init_root_server();
      }
      //add:e
      // task queue and work thread count
      if (OB_SUCCESS == ret)
      {
        ret = set_default_queue_size((int)config_.task_queue_size);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_thread_count((int)config_.task_thread_count);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_io_thread_count((int)config_.io_thread_count);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_min_left_time(config_.task_left_time);
      }

      //TODO  initialize client_manager_ for server remote procedure call.
      if (OB_SUCCESS == ret)
      {
        ret = client_manager_.initialize(eio_, &server_handler_);
      }

      if (OB_SUCCESS == ret)
      {
        //mod zhaoqiong [fixed for Backup]:20150811:b
        //ret = tablet_manager_.init(&config_);
        ret = tablet_manager_.init(&config_, &client_manager_);
        //mod:e
      }

      // server initialize, including start transport,
      // listen port, accept socket data from client
      if (OB_SUCCESS == ret)
      {
        ret = ObSingleServer::initialize();
      }

      if (OB_SUCCESS == ret)
      {
        ret = service_.initialize(this);
      }

      // init file service
      if (OB_SUCCESS == ret)
      {
        int64_t max_migrate_task_count = get_config().max_migrate_task_count;
        ret = file_service_.initialize(this,
            &this->get_default_task_queue_thread(),
            config_.network_timeout,
            static_cast<uint32_t>(max_migrate_task_count));
      }

      if (OB_SUCCESS == ret)
      {
        ret = file_client_.initialize(&file_client_rpc_buffer_,
                                      &client_manager_, config_.migrate_band_limit_per_second);
      }

      stat_.init(get_self());
      ObStatSingleton::init(&stat_);
      return ret;
    }

    int ObChunkServer::start_service()
    {
      TBSYS_LOG(INFO, "start service...");
      // finally, start service, handle the request from client.
      return service_.start();
    }

    void ObChunkServer::wait_for_queue()
    {
      ObSingleServer::wait_for_queue();
    }

    void ObChunkServer::destroy()
    {
      ObSingleServer::destroy();
      TBSYS_LOG(INFO, "single server stoped.");
      service_.destroy();
      TBSYS_LOG(INFO, "destory service_.");
      tablet_manager_.destroy();
      TBSYS_LOG(INFO, "destory tablet_manager_, server exit.");
      //TODO maybe need more destroy
    }
    //add pangtianze [Paxos rs_election] 20150710:b
    int ObChunkServer::init_root_server()
    {
      int ret = OB_SUCCESS;
      bool res = root_server_.set_ipv4_addr(config_.root_server_ip,
                                            (int32_t)config_.root_server_port);
      if (!res)
      {
        TBSYS_LOG(ERROR, "root server address invalid: %s:%s",
                  config_.root_server_ip.str(),
                  config_.root_server_port.str());
        ret = OB_ERROR;
      }
      else
      {
        service_.get_rs_mgr().set_master_rs(root_server_);
      }
      return ret;
    }
    //add:e

    int64_t ObChunkServer::get_process_timeout_time(
      const int64_t receive_time, const int64_t network_timeout)
    {
      int64_t timeout_time  = 0;
      int64_t timeout       = network_timeout;

      if (network_timeout <= 0)
      {
        timeout = config_.network_timeout;
      }

      timeout_time = receive_time + timeout;

      return timeout_time;
    }

    int ObChunkServer::do_request(ObPacket* base_packet)
    {
      int ret = OB_SUCCESS;
      ObPacket* ob_packet = base_packet;
      int32_t packet_code = ob_packet->get_packet_code();
      int32_t version = ob_packet->get_api_version();
      int32_t channel_id = ob_packet->get_channel_id();
      int64_t receive_time = ob_packet->get_receive_ts();
      int64_t network_timeout = ob_packet->get_source_timeout();
      int64_t wait_time = 0;
      easy_request_t* req = ob_packet->get_request();
      ObDataBuffer* in_buffer = ob_packet->get_buffer();
      ThreadSpecificBuffer::Buffer* thread_buffer = response_buffer_.get_buffer();
      if (NULL == req || NULL == req->ms || NULL == req->ms->c)
      {
        TBSYS_LOG(ERROR, "req or req->ms or req->ms->c is NULL, should not reach here");
      }
      else if (NULL == in_buffer || NULL == thread_buffer)
      {
        TBSYS_LOG(ERROR, "in_buffer = %p or out_buffer=%p cannot be NULL.", 
            in_buffer, thread_buffer);
      }
      else
      {
        if (OB_GET_REQUEST == packet_code || OB_SCAN_REQUEST == packet_code
            || OB_SQL_GET_REQUEST == packet_code || OB_SQL_SCAN_REQUEST == packet_code)
        {
          wait_time = tbsys::CTimeUtil::getTime() - receive_time;
          FILL_TRACE_LOG("process request, packet_code=%d, wait_time=%ld",
              packet_code, wait_time);
          PROFILE_LOG(DEBUG, "request from peer=%s, wait_time_in_queue=%ld, packet_code=%d",
                         get_peer_ip(ob_packet->get_request()), wait_time, packet_code);
          PFILL_SET_TRACE_ID(ob_packet->get_trace_id());
          PFILL_SET_PCODE(packet_code);
          PFILL_SET_WAIT_SQL_QUEUE_TIME(wait_time);
          OB_STAT_INC(CHUNKSERVER, INDEX_META_REQUEST_COUNT);
          OB_STAT_INC(CHUNKSERVER, INDEX_META_QUEUE_WAIT_TIME, wait_time);
          OB_STAT_SET(CHUNKSERVER, QUERY_QUEUE_COUNT, get_default_task_queue_thread().size());
          OB_STAT_INC(CHUNKSERVER, QUERY_QUEUE_TIME, wait_time);
        }

        int64_t timeout_time = get_process_timeout_time(receive_time, network_timeout);
        thread_buffer->reset();
        ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
        //TODO read thread stuff multi thread
        TBSYS_LOG(DEBUG, "handle packet, packe code is %d, packet:%p",
            packet_code, ob_packet);
        PFILL_ITEM_START(handle_request_time);
        ret = service_.do_request(receive_time, packet_code,
            version, channel_id, req,
            *in_buffer, out_buffer, timeout_time);
        PFILL_ITEM_END(handle_request_time);
        PFILL_CS_PRINT();
        PFILL_CLEAR_LOG();
      }
      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase

