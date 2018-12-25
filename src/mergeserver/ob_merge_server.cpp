#include "common/ob_define.h"
#include "common/ob_trace_log.h"
#include "ob_merge_server.h"
#include "ob_chunk_server_task_dispatcher.h"
#include <time.h>
#include "ob_merge_callback.h"
#include "common/ob_tbnet_callback.h"
#include "common/utility.h"
#include "common/ob_mod_define.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {
    ObMergeServer::ObMergeServer(ObConfigManager &config_mgr,
                                 ObMergeServerConfig &ms_config)
      : log_count_(0), log_interval_count_(DEFAULT_LOG_INTERVAL),
        config_mgr_(config_mgr), ms_config_(ms_config),
        response_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
        rpc_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
        scan_req_pool_(common::ObModIds::OB_MS_SQL_SCAN_REQ_POOL),
        get_req_pool_(common::ObModIds::OB_MS_SQL_GET_REQ_POOL)
    {
    }

    int ObMergeServer::set_self(const char* dev_name, const int32_t port)
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
        self_.cluster_id_ = (int32_t) ms_config_.cluster_id;
        //add:e
        if (!res)
        {
          TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
                    dev_name, port);
          ret = OB_ERROR;
        }
      }
      if(OB_SUCCESS == ret)
      {
        ObChunkServerTaskDispatcher::get_instance()->set_local_ip(ip);
      }
      return ret;
    }

    int ObMergeServer::set_sql_session_mgr(sql::ObSQLSessionMgr* session_mgr)
    {
      int ret = OB_SUCCESS;
      if (NULL == session_mgr)
      {
        ret = OB_ERROR;
      }
      else
      {
        service_.set_sql_session_mgr(session_mgr);
      }
      return ret;
    }

    bool ObMergeServer::is_stoped() const
    {
      return stoped_;
    }

    int ObMergeServer::start_service()
    {
      return service_.start();
    }

    // reload config
    int ObMergeServer::reload_config()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        ob_set_memory_size_limit(ms_config_.memory_size_limit_percentage
                                 * sysconf(_SC_PHYS_PAGES)
                                 * sysconf(_SC_PAGE_SIZE) / 100);
        log_interval_count_ = ms_config_.log_interval_count;
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_default_queue_size((int32_t)ms_config_.task_queue_size);
        if (OB_SUCCESS == ret)
        {
          ret = set_thread_count((int32_t)ms_config_.task_thread_count);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_min_left_time(ms_config_.task_left_time);
      }

      if (OB_SUCCESS == ret)
      {
        ret = service_.reload_config();
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "enlarge frozen_data_cache_size to: %ld",
            static_cast<int64_t>(ms_config_.frozen_data_cache_size));
        frozen_data_cache_.enlarge_total_size(ms_config_.frozen_data_cache_size);

        TBSYS_LOG(INFO, "enlarge bloom_filter_cache_size to: %ld",
            static_cast<int64_t>(ms_config_.bloom_filter_cache_size));
        insert_cache_.enlarge_total_size(ms_config_.bloom_filter_cache_size);
      }

      return ret;
    }

    int ObMergeServer::initialize()
    {
      int ret = OB_SUCCESS;
      // disable batch process mode
      set_batch_process(false);

      if (ret == OB_SUCCESS)
      {
        // set max memory size limit
        ob_set_memory_size_limit(ms_config_.memory_size_limit_percentage
                                 * sysconf(_SC_PHYS_PAGES)
                                 * sysconf(_SC_PAGE_SIZE) / 100);
      }

      if (ret == OB_SUCCESS)
      {
        memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
        server_handler_.encode = ObTbnetCallback::encode;
        server_handler_.decode = ObTbnetCallback::decode;
        server_handler_.process = ObMergeCallback::process;
        //server_handler_.batch_process
        server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
        server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
        server_handler_.user_data = this;
      }

      if (ret == OB_SUCCESS)
      {
        ret = set_listen_port((int32_t)ms_config_.port);
      }

      if (ret == OB_SUCCESS)
      {
        ret = set_dev_name(ms_config_.devname);
        if (OB_SUCCESS == ret)
        {
          ret = set_self(ms_config_.devname,
                         (int32_t)ms_config_.port);
        }
      }
      if (ret == OB_SUCCESS)
      {
        set_self_to_thread_queue(self_);
      }
      if (ret == OB_SUCCESS)
      {
        ret = init_root_server();
      }

      if (ret == OB_SUCCESS)
      {
        ret = set_default_queue_size((int32_t)ms_config_.task_queue_size);
      }

      if (ret == OB_SUCCESS)
      {
        ret = set_io_thread_count((int32_t)ms_config_.io_thread_count);
      }

      if (ret == OB_SUCCESS)
      {
        ret = set_thread_count((int32_t)ms_config_.task_thread_count);
      }

      if (ret == OB_SUCCESS)
      {
        log_interval_count_ = ms_config_.log_interval_count;
        ret = set_min_left_time(ms_config_.task_left_time);
      }

      if (ret == OB_SUCCESS)
      {
        ret = frozen_data_cache_.init(ms_config_.frozen_data_cache_size);
      }
      if (ret == OB_SUCCESS)
      {
        ret = insert_cache_.init(ms_config_.bloom_filter_cache_size);
      }
      if (ret == OB_SUCCESS)
      {
        ret = scan_req_pool_.init();
      }
      if (ret == OB_SUCCESS)
      {
        ret = get_req_pool_.init();
      }

      if (ret == OB_SUCCESS)
      {
        ret = task_timer_.init();
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_manager_.initialize(eio_, &server_handler_);
      }
      if (ret == OB_SUCCESS)
      {
        ret = ObSingleServer::initialize();
      }

      if (ret == OB_SUCCESS)
      {
        ret = service_.initialize(this);
      }

      return ret;
    }

    void ObMergeServer::destroy()
    {
      TBSYS_LOG(WARN, "stop task timer");
      task_timer_.destroy();
      TBSYS_LOG(WARN, "stop bloomfilter queue thread");
      bloom_filter_queue_thread_.stop();
      bloom_filter_queue_thread_.wait();
      TBSYS_LOG(WARN, "stop single server");
      ObSingleServer::destroy();
      TBSYS_LOG(WARN, "stop mergeserver service");
      service_.destroy();
    }

    int ObMergeServer::init_root_server()
    {
      int ret = OB_SUCCESS;
      bool res = root_server_.set_ipv4_addr(ms_config_.root_server_ip,
                                            (int32_t)ms_config_.root_server_port);
      if (!res)
      {
        TBSYS_LOG(ERROR, "root server address invalid: %s:%s",
                  ms_config_.root_server_ip.str(),
                  ms_config_.root_server_port.str());
        ret = OB_ERROR;
      }
      //add pangtianze [Paxos rs_election] 20150710:b
      else
      {
        rs_mgr_.set_master_rs(root_server_);
      }
      //add:e
      return ret;
    }


    common::ThreadSpecificBuffer* ObMergeServer::get_rpc_buffer()
    {
      return &rpc_buffer_;
    }

    common::ThreadSpecificBuffer::Buffer* ObMergeServer::get_response_buffer() const
    {
      return response_buffer_.get_buffer();
    }

    const common::ObServer& ObMergeServer::get_self() const
    {
      return self_;
    }
    const common::ObServer& ObMergeServer::get_root_server() const
    {
      return root_server_;
    }
    //add pangtianze [Paxos rs_election] 20150707:b
    void ObMergeServer::set_root_server(const common::ObServer &server)
    {
      root_server_ = server;
      service_.get_rpc_proxy()->set_root_server(server);
      service_.get_root_rpc()->set_root_server(server);
      //add pangtianze [Paxos rs_election] 20161124
      //dump to config file
      char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
      server.ip_to_string(ip_tmp,sizeof(ip_tmp));
      ms_config_.root_server_ip.set_value(ip_tmp);
      ms_config_.root_server_port = server.get_port();
      config_mgr_.dump2file();
      //add:e
    }
    //add:e

    //add huangjianwei [Paxos rs_election] 20160513:b
    void ObMergeServer:: set_new_root_server(const common::ObServer &server)
    {
      root_server_ =server;
    }
    //add:e

    ObMergeServerConfig& ObMergeServer::get_config()
    {
      return ms_config_;
    }

    ObConfigManager& ObMergeServer::get_config_mgr()
    {
      return config_mgr_;
    }

    ObTimer& ObMergeServer::get_timer()
    {
      return task_timer_;
    }

    const common::ObClientManager& ObMergeServer::get_client_manager() const
    {
      return client_manager_;
    }

    // overflow packet
    bool ObMergeServer::handle_overflow_packet(ObPacket* base_packet)
    {
      handle_no_response_request(base_packet);
      // must return false
      return false;
    }

    void ObMergeServer::handle_no_response_request(ObPacket * base_packet)
    {
      if (NULL == base_packet)
      {
        TBSYS_LOG(WARN, "packet is illegal, discard.");
      }
      else
      {
        service_.handle_failed_request(base_packet->get_source_timeout(), base_packet->get_packet_code());
      }
    }

    void ObMergeServer::handle_timeout_packet(ObPacket* base_packet)
    {
      handle_no_response_request(base_packet);
      ObSingleServer::handle_timeout_packet(base_packet);
    }
    int ObMergeServer::do_request(common::ObPacket* base_packet)
    {
      int ret = OB_SUCCESS;
      ObPacket* ob_packet = base_packet;
      int32_t packet_code = ob_packet->get_packet_code();
      int32_t version = ob_packet->get_api_version();
      int32_t channel_id = ob_packet->get_channel_id();
      ObDataBuffer* in_buffer = ob_packet->get_buffer();
      ThreadSpecificBuffer::Buffer* thread_buffer = response_buffer_.get_buffer();
      easy_request_t* request = ob_packet->get_request();
      if (NULL == request || NULL == request->ms || NULL == request->ms->c)
      {
        TBSYS_LOG(ERROR, "req or req->ms or req->ms->c is NUll should not reach this");
      }
      else if (NULL == in_buffer || NULL == thread_buffer)
      {
        TBSYS_LOG(ERROR, "in_buffer = %p or out_buffer=%p cannot be NULL.",
            in_buffer, thread_buffer);
      }
      else
      {
        FILL_TRACE_LOG("start handle client=%s request packet wait=%ld",
                       get_peer_ip(base_packet->get_request()),
                       tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts());
        thread_buffer->reset();
        ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
        //TODO read thread stuff multi thread
        ret = service_.do_request(ob_packet->get_receive_ts(), packet_code, version,
            channel_id, request, *in_buffer, out_buffer,
            ob_packet->get_source_timeout() - (tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts()));
      }
      return ret;
    }
    void ObMergeServer::on_ioth_start()
    {
      int64_t affinity_start_cpu = ms_config_.io_thread_start_cpu;
      int64_t affinity_end_cpu = ms_config_.io_thread_end_cpu;
      if (0 <= affinity_start_cpu
          && affinity_start_cpu <= affinity_end_cpu)
      {
        static volatile int64_t cpu = 0;
        int64_t local_cpu = __sync_fetch_and_add(&cpu, 1) % (affinity_end_cpu - affinity_start_cpu + 1) + affinity_start_cpu;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(local_cpu, &cpuset);
        int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        TBSYS_LOG(INFO, "io thread setaffinity tid=%ld ret=%d cpu=%ld start=%ld end=%ld",
                  GETTID(), ret, local_cpu, affinity_start_cpu, affinity_end_cpu);
      }
    }
  } /* mergeserver */
} /* oceanbase */
