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
#include "ob_proxy_server.h"
#include "ob_proxy_server_main.h"
#include "common/ob_tbnet_callback.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_fill_log.h"
#include "ob_proxy_callback.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace proxyserver
  {

    ObProxyServer::ObProxyServer(ObProxyServerConfig &config)
      : config_(config), response_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
        rpc_buffer_(RPC_BUFFER_SIZE)
    {
    }

    ObProxyServer::~ObProxyServer()
    {
    }

    common::ThreadSpecificBuffer::Buffer* ObProxyServer::get_rpc_buffer() const
    {
      return rpc_buffer_.get_buffer();
    }

    common::ThreadSpecificBuffer::Buffer* ObProxyServer::get_response_buffer() const
    {
      return response_buffer_.get_buffer();
    }

    const common::ThreadSpecificBuffer* ObProxyServer::get_thread_specific_rpc_buffer() const
    {
      return &rpc_buffer_;
    }

    const common::ObServer& ObProxyServer::get_self() const
    {
      return self_;
    }

    const ObProxyServerConfig & ObProxyServer::get_config() const
    {
      return config_;
    }

    ObProxyServerConfig & ObProxyServer::get_config()
    {
      return config_;
    }

    int ObProxyServer::set_self(const char* dev_name, const int32_t port)
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
        if (!res)
        {
          TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
              dev_name, port);
          ret = OB_ERROR;
        }
      }
      return ret;
    }


    int ObProxyServer::initialize()
    {
      int ret = OB_SUCCESS;
      // do not handle batch packet.
      // process packet one by one.
      set_batch_process(false);

      // set listen port
      if (OB_SUCCESS == ret)
      {
        ret = set_listen_port((int)config_.get_port());
      }

      if (OB_SUCCESS == ret)
      {
        memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
        server_handler_.encode = ObTbnetCallback::encode;
        server_handler_.decode = ObTbnetCallback::decode;
        server_handler_.process = ObProxyCallback::process;
        //server_handler_.batch_process = ObTbnetCallback::batch_process;
        server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
        server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
        server_handler_.user_data = this;
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_dev_name(config_.get_devname());
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS == ret)
        {
          ret = set_self(config_.get_devname(), (int32_t)config_.get_port());
        }
      }
      if (OB_SUCCESS == ret)
      {
        set_self_to_thread_queue(self_);
      }

      // task queue and work thread count
      if (OB_SUCCESS == ret)
      {
        ret = set_default_queue_size((int)config_.get_task_queue_size());
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_thread_count((int)config_.get_task_thread_count());
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_io_thread_count((int)config_.get_io_thread_count());
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_min_left_time(config_.get_task_left_time());
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

      return ret;
    }

    int ObProxyServer::start_service()
    {
      TBSYS_LOG(INFO, "start service...");
      // finally, start service, handle the request from client.
      return service_.start();
    }

    void ObProxyServer::wait_for_queue()
    {
      ObSingleServer::wait_for_queue();
    }

    void ObProxyServer::destroy()
    {
      ObSingleServer::destroy();
      service_.destroy();
    }

    int64_t ObProxyServer::get_process_timeout_time(
        const int64_t receive_time, const int64_t network_timeout)
    {
      int64_t timeout_time  = 0;
      int64_t timeout       = network_timeout;

      if (network_timeout <= 0)
      {
        timeout = THE_PROXY_SERVER.get_config().get_network_timeout();
      }

      timeout_time = receive_time + timeout;

      return timeout_time;
    }

    int ObProxyServer::do_request(ObPacket* base_packet)
    {
      int ret = OB_SUCCESS;
      ObPacket* ob_packet = base_packet;
      int32_t packet_code = ob_packet->get_packet_code();
      int32_t version = ob_packet->get_api_version();
      int32_t channel_id = ob_packet->get_channel_id();
      int64_t receive_time = ob_packet->get_receive_ts();
      int64_t network_timeout = ob_packet->get_source_timeout();
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
        int64_t timeout_time = get_process_timeout_time(receive_time, network_timeout);
        thread_buffer->reset();
        ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
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

