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

#include "utility.h"
#include "ob_client_manager.h"
#include "easy_io.h"
#include "easy_client.h"
#include "ob_packet.h"
#include "ob_base_server.h"
#include "ob_atomic.h"
#include "ob_tbnet_callback.h"
#include "ob_profile_log.h"
#include "ob_atomic.h"
#include "ob_tsi_factory.h"
#include "ob_trace_id.h"
#include "ob_profile_type.h"
#include "ob_profile_fill_log.h"
namespace oceanbase
{
  namespace common
  {

    ObClientManager::ObClientManager()
      : error_(OB_SUCCESS), inited_(false), dedicate_thread_num_(0), max_request_timeout_(5000000), eio_(NULL), handler_(NULL)
    {
    }

    ObClientManager::~ObClientManager()
    {
      destroy();
    }

    void ObClientManager::destroy()
    {

    }

    int ObClientManager::initialize(easy_io_t* eio, easy_io_handler_pt* handler,
                                    const int64_t max_request_timeout /*=5000000*/)
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(ERROR, "ClientManager already initialized.");
        rc = OB_INIT_TWICE;
      }

      if (OB_SUCCESS == rc)
      {
        max_request_timeout_ = max_request_timeout;
        if (NULL != eio && NULL != handler)
        {
          eio_ = eio;
          handler_ = handler;
        }
        else
        {
          TBSYS_LOG(ERROR, "invalid arguments eio=%p, handler=%p", eio, handler);
          rc = OB_INVALID_ARGUMENT;
        }

      }

      inited_ = (OB_SUCCESS == rc);
      if (!inited_) { destroy(); }

      return rc;
    }

    void ObClientManager::set_error(const int err)
    {
      error_ = err;
      TBSYS_LOG(WARN, "set_err(err=%d)", err);
    }

    int ObClientManager::set_dedicate_thread_num(const int num)
    {
      int err = OB_SUCCESS;
      if (dedicate_thread_num_ > 0)
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "already set dedicate_thread_num, old_value=%d", dedicate_thread_num_);
      }
      else if (NULL == eio_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "eio == NULL");
      }
      else if (eio_->io_thread_pool->thread_count <= num)
      {
        err = OB_SIZE_OVERFLOW;
        TBSYS_LOG(ERROR, "request dedicate_thread_num[%d] >= max_avail_thread_num[%d]", num, eio_->io_thread_pool->thread_count);
      }
      else
      {
        dedicate_thread_num_ = num;
      }
      return err;
    }

    int ObClientManager::post_request(const ObServer& server,
        const int32_t pcode, const int32_t version, const ObDataBuffer& in_buffer) const
    {
      // default packet timeout = 0
      return post_request(server, pcode, version, 0, in_buffer, ObTbnetCallback::default_callback, NULL);
    }

    int ObClientManager::post_request(const ObServer& server, const int32_t pcode, const int32_t version,
        const int64_t timeout, const ObDataBuffer& in_buffer, easy_io_process_pt handler, void* args) const
    {
      if (NULL == handler)
      {
        handler = ObTbnetCallback::default_callback;
      }
      int ret = OB_SUCCESS;
      ret = do_post_request(server, pcode, version, 0, timeout, in_buffer, handler, args);
      return ret;
    }

    int ObClientManager::do_post_request(const ObServer& server,
        const int32_t pcode, const int32_t version,
        const int64_t session_id, const int64_t timeout,
        const ObDataBuffer& in_buffer,
        easy_io_process_pt handler, void* args) const
    {
      int rc = OB_SUCCESS;
      easy_session_t* s = NULL;
      //session will be destroyed on session call back
      //(data buffe size) + (record header size) + (packet size)
      int64_t size = in_buffer.get_position() + OB_RECORD_HEADER_LENGTH + sizeof(ObPacket);
      rc = create_session(server, pcode, version, timeout, in_buffer, size, s);
      if (OB_SUCCESS == rc)
      {
        s->process = handler;
        ObPacket *packet = reinterpret_cast<ObPacket*>(s->r.opacket);
        if (pcode != OB_HEARTBEAT)
        {
          uint32_t *chid = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_CHID_1);
          if (OB_LIKELY(NULL != chid))
          {
            *chid = packet->get_channel_id();
            if (pcode == OB_SQL_SCAN_REQUEST || pcode == OB_SQL_GET_REQUEST)
            {
              PFILL_RPC_START(pcode,(*chid));
            }
          }
        }
        packet->set_session_id(session_id);
        if (NULL != args)
        {
          s->r.user_data = args;
        }
        TBSYS_LOG(DEBUG, "post packet, pcode=%d channel=%u session=%p session_id=%ld",
                  packet->get_packet_code(), packet->get_channel_id(), s, session_id);
        rc = post_session(s);
        if (OB_SUCCESS != rc)
        {
          //session post faild destroy it
          TBSYS_LOG(WARN, "post session faild destroy session s = %p", s);
          easy_session_destroy(s);
          rc = OB_PACKET_NOT_SENT;
        }
      }

      return rc;
    }
    int ObClientManager::post_request_using_dedicate_thread(const ObServer& server, const int32_t pcode, const int32_t version,
                                                            const int64_t timeout, const ObDataBuffer& in_buffer,
                                                            easy_io_process_pt handler, void* args,
                                                            //add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
                                                            const int64_t cmt_log_seq,
                                                            //add:e
                                                            int thread_idx) const
    {
      int rc = OB_SUCCESS;
      int64_t session_id = 0;
      easy_session_t* s = NULL;
      //session will be destroyed on session call back
      //(data buffe size) + (record header size) + (packet size)
      int64_t size = in_buffer.get_position() + OB_RECORD_HEADER_LENGTH + sizeof(ObPacket);
      //mod wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
      rc = create_session(server, pcode, version, timeout, in_buffer, size, s, cmt_log_seq);
      //mod :e
      if (OB_SUCCESS == rc)
      {
        s->process = handler;
        ObPacket *packet = reinterpret_cast<ObPacket*>(s->r.opacket);
        if (pcode != OB_HEARTBEAT)
        {
          uint32_t *chid = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_CHID_1);
          if (OB_LIKELY(NULL != chid))
          {
            *chid = packet->get_channel_id();
            if (pcode == OB_SQL_SCAN_REQUEST || pcode == OB_SQL_GET_REQUEST)
            {
              PFILL_RPC_START(pcode,(*chid));
            }
          }
        }
        packet->set_session_id(session_id);
		//del wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
        //if (NULL != args)
		//del:e
        {
          s->r.user_data = args;
        }
        TBSYS_LOG(DEBUG, "post packet, pcode=%d channel=%u session=%p session_id=%ld",
                  packet->get_packet_code(), packet->get_channel_id(), s, session_id);
        rc = post_session_using_dedicate_thread(s, thread_idx);
        if (OB_SUCCESS != rc)
        {
          //session post faild destroy it
          TBSYS_LOG(WARN, "post session faild destroy session s = %p", s);
          easy_session_destroy(s);
          rc = OB_PACKET_NOT_SENT;
        }
      }

      return rc;
    }

    int ObClientManager::send_request(const ObServer& server, const int32_t pcode, const int32_t version,
        const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer, int64_t& session_id) const
    {
      int rc = OB_SUCCESS;
      rc = do_send_request(server, pcode, version, timeout, in_buffer, out_buffer, session_id);
      return rc;
    }

    int ObClientManager::post_next(const ObServer& server, const int64_t session_id,
        const int64_t timeout, ObDataBuffer& in_buffer, easy_io_process_pt handler, void* args) const
    {
      if (NULL == handler)
      {
        handler = ObTbnetCallback::default_callback;
      }
      return do_post_request(server, OB_SESSION_NEXT_REQUEST, 0, session_id, timeout, in_buffer, handler, args);
    }
    int ObClientManager::post_end_next(const ObServer& server, const int64_t session_id,
                                       const int64_t timeout, ObDataBuffer& in_buffer,
                                       easy_io_process_pt handler, void* args) const
    {
      if (NULL == handler)
      {
        handler = ObTbnetCallback::default_callback;
      }
      return do_post_request(server, OB_SESSION_END, 0, session_id, timeout, in_buffer, handler, args);
    }

    int ObClientManager::get_next(const ObServer& server, const int64_t session_id,
        const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const
    {
      int rc = OB_SUCCESS;

      ObPacket* response = NULL;
      easy_session_t* s = NULL;
      if (OB_SUCCESS == rc)
      {
        int64_t size = in_buffer.get_position() + OB_RECORD_HEADER_LENGTH + sizeof(ObPacket);
        rc = create_session(server, OB_SESSION_NEXT_REQUEST, 0, timeout, in_buffer, size, s);
        if (OB_SUCCESS == rc)
        {
          ObPacket *packet = reinterpret_cast<ObPacket*>(s->r.opacket);
          packet->set_session_id(session_id);
          TBSYS_LOG(DEBUG, "send packet code is %d, session timeout is %f", packet->get_packet_code(),
                    s->timeout);
          response = reinterpret_cast<ObPacket*>(send_session(s));
          if (NULL == response && 1 == s->error)
          {
            TBSYS_LOG(WARN, "send packet to %s failed, easy_session(%p)_dispatch ret:%d",
                      to_cstring(server), s, s->error);
            rc = OB_RPC_SEND_ERROR;
          }
          else if (NULL == response)
          {
            TBSYS_LOG(WARN, "send packet to %s timeout", to_cstring(server));
            rc = OB_RESPONSE_TIME_OUT;
          }
        }
      }

      // deserialize response packet to out_buffer
      if (OB_SUCCESS == rc && NULL != response)
      {
        // copy response's inner_buffer to out_buffer.
        int64_t data_length = response->get_data_length();
        ObDataBuffer* response_buffer = response->get_buffer();
        if (out_buffer.get_remain() < data_length)
        {
          TBSYS_LOG(ERROR, "insufficient memory in out_buffer, remain:%ld, length=%ld",
                    out_buffer.get_remain(), data_length);
          rc = OB_BUF_NOT_ENOUGH;
        }
        else
        {
          memcpy(out_buffer.get_data() + out_buffer.get_position(),
                 response_buffer->get_data() + response_buffer->get_position(),
                 data_length);
          out_buffer.get_position() += data_length;
        }
      }

      if (NULL != s)
      {
        easy_session_destroy(s);
        s = NULL;
      }
      return rc;
    }

    /*
     * send_packet wrappered byte stream %in_buffer as a ObPacket send to remote server
     * and receive the reponse ObPacket translate to %out_buffer
     * @param server send to server
     * @param pcode  packet type
     * @param version packet version
     * @param timeout max wait time
     * @param in_buffer byte stream be sent
     * @param out_buffer response packet byte stream.
     * @return OB_SUCCESS on success or other on failure.
     * response data filled into end of out_buffer,
     * so function return successfully with:
     * response data pointer = out_buffer.get_data() + origin_position
     * response data size = out_buffer.get_position() - origin_position;
     */
    int ObClientManager::send_request(
        const ObServer& server, const int32_t pcode,
        const int32_t version, const int64_t timeout,
        ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const
    {
      int64_t session_id = 0;
      return send_request(server, pcode, version, timeout, in_buffer, out_buffer, session_id);
    }

    /*
     * like the overload function as above.
     * %in_out_buffer position will be reset to start
     * for store response packet's data.
     * response data filled into start of in_out_buffer
     * so function return successfully with:
     * response data  = in_out_buffer.get_data()
     * response data size = in_out_buffer.get_position()
     */
    int ObClientManager::send_request(
        const ObServer& server, const int32_t pcode,
        const int32_t version, const int64_t timeout,
        ObDataBuffer& in_out_buffer) const
    {
      int64_t session_id = 0;
      return send_request(server, pcode, version, timeout, in_out_buffer, session_id);
    }

    int ObClientManager::send_request(const ObServer& server, const int32_t pcode, const int32_t version,
        const int64_t timeout, ObDataBuffer& in_out_buffer, int64_t& session_id) const
    {
      int rc = OB_SUCCESS;
      rc = do_send_request(server, pcode, version, timeout, in_out_buffer, in_out_buffer, session_id);
      return rc;
    }

    int ObClientManager::do_send_request(
        const ObServer& server, const int32_t pcode,
        const int32_t version, const int64_t timeout,
        ObDataBuffer& in_buffer, ObDataBuffer &out_buffer, int64_t &session_id) const
    {
      int rc = OB_SUCCESS;
      ObPacket *response = NULL;
      easy_session_t* s = NULL;
      uint32_t *chid = NULL;
      if (OB_SUCCESS == rc)
      {
        //copy packet into session, session will be destroy when request done or anyother libeasy error
        int64_t size = in_buffer.get_position() + OB_RECORD_HEADER_LENGTH + sizeof(ObPacket);
        rc = create_session(server, pcode, version, timeout, in_buffer, size, s);
        if (OB_SUCCESS == rc)
        {
          ObPacket* packet = reinterpret_cast<ObPacket*>(s->r.opacket);
          chid = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_CHID_1);
          //将chid设置到线程局部中
          *chid = packet->get_channel_id();
          // OB_MS_MUTATE: REPLACE
          // OB_PHY_PLAN_EXECUTE: UPDATE, INSERT
          if (pcode == OB_SQL_SCAN_REQUEST || pcode == OB_SQL_GET_REQUEST
              || pcode == OB_MS_MUTATE || pcode == OB_PHY_PLAN_EXECUTE
              || pcode == OB_START_TRANSACTION || pcode == OB_END_TRANSACTION
              || pcode == OB_NEW_GET_REQUEST || pcode == OB_NEW_SCAN_REQUEST)
          {
            PFILL_RPC_START(pcode,*chid);
          }
          TBSYS_LOG(DEBUG, "send packet, pcode=%d, channel=%u session=%p timeout=%f",
                    packet->get_packet_code(), packet->get_channel_id(), s,
                    s->timeout);
          response = reinterpret_cast<ObPacket*>(send_session(s));
          if (NULL == response && 1 == s->error)
          {
            TBSYS_LOG(WARN, "send packet to %s failed, easy_session(%p)_dispatch ret:%d",
                      to_cstring(server), s, s->error);
            rc = OB_RPC_SEND_ERROR;
          }
          else if (NULL == response)
          {
            TBSYS_LOG(WARN, "send packet to %s timeout", to_cstring(server));
            rc = OB_RESPONSE_TIME_OUT;
          }
        }
      }
      //copy packet buffer from libeasy buffer to output buffer
      //easy_session_destroy will free input packet buffer
      if (OB_SUCCESS == rc && NULL != response)
      {
        int32_t pcode = response->get_packet_code();
        session_id = response->get_session_id();
        if (pcode == OB_SQL_SCAN_RESPONSE || pcode == OB_SQL_GET_RESPONSE 
            || pcode == OB_MS_MUTATE || pcode == OB_COMMIT_END
            || pcode == OB_START_TRANSACTION || pcode == OB_END_TRANSACTION
            || pcode == OB_NEW_GET_REQUEST || pcode == OB_NEW_SCAN_REQUEST)
        {
          if (chid != NULL)
          {
            PFILL_RPC_END(*chid);
          }
        }
        int64_t data_length = response->get_data_length();
        out_buffer.get_position() = 0;
        if (out_buffer.get_remain() < data_length)
        {
          TBSYS_LOG(ERROR, "insufficient memory in out_buffer, remain:%ld, length=%ld",
              out_buffer.get_remain(), data_length);
          rc = OB_BUF_NOT_ENOUGH;
        }
        else
        {
          ObDataBuffer* response_buffer = response->get_buffer();
          memcpy(out_buffer.get_data() + out_buffer.get_position(), response_buffer->get_data() + response_buffer->get_position(), data_length);
          out_buffer.get_position() += data_length;
        }
      }
      if (NULL != s)
      {
        easy_session_destroy(s);
        s = NULL;
      }
      return rc;
    }

    int ObClientManager::create_session(const ObServer& server,
                                        const int32_t pcode, const int32_t version,
                                        const int64_t timeout, const ObDataBuffer& in_buffer,
                                        int64_t size, easy_session_t *& session) const
    {
      int ret = OB_SUCCESS;
      easy_addr_t addr = convert_addr_from_server(&server);
      session = easy_session_create(static_cast<int>(size));
      if (NULL == session)
      {
        TBSYS_LOG(WARN, "create session failed");
        ret = OB_LIBEASY_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        ObPacket* spacket = new(session+1)ObPacket();
        //从线程局部中拿出该请求所对应的trace id
        TraceId *trace_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
        if (NULL == trace_id)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "no memory, alloc trace id failed, ret=%d", ret);
        }
        else
        {
          spacket->set_req_sign(ObPacket::tsi_req_sign());
          ObPacket::tsi_req_sign() = 0;
          /*
          if (trace_id->is_invalid())
          {
            (trace_id->id).seq_ = atomic_inc(&(ObPacket::seq_generator_));
            uint16_t *local_ip_suffix = GET_TSI_MULT(uint16_t, TSI_COMMON_IP_SUFFIX_1);
            (trace_id->id).ip_ = *local_ip_suffix;
            uint16_t *port = GET_TSI_MULT(uint16_t, TSI_COMMON_PORT_1);
            (trace_id->id).port_ = *port;
          }
          */
          //else
          //{
            spacket->set_trace_id(trace_id->uval_);
          //}
          spacket->set_packet_code(pcode);
          //为这个新的packet分配一个channel id
          uint32_t new_chid = atomic_inc(&ObPacket::global_chid);
          spacket->set_channel_id(new_chid);
          spacket->set_api_version(version);
          spacket->set_source_timeout(timeout);
          spacket->set_data(in_buffer);
          ObDataBuffer buffer(reinterpret_cast<char*>(spacket + 1), size - sizeof(ObPacket));
          spacket->serialize(&buffer);
          spacket->set_packet_buffer(buffer.get_data(), buffer.get_position());
          spacket->get_inner_buffer()->get_position() = buffer.get_position();
          session->status = EASY_CONNECT_SEND;//auto connect if there are no connection to addr
          session->r.opacket = spacket;
          session->addr = addr;
          session->thread_ptr = handler_;
          easy_session_set_timeout(session, static_cast<ev_tstamp>(timeout)/1000);
        }
      }
      return ret;
    }
    //add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
    int ObClientManager::create_session(const ObServer& server,
                                        const int32_t pcode, const int32_t version,
                                        const int64_t timeout, const ObDataBuffer& in_buffer,
                                        int64_t size, easy_session_t *& session, const int64_t cmt_log_seq) const
    {
      int ret = OB_SUCCESS;
      easy_addr_t addr = convert_addr_from_server(&server);
      session = easy_session_create(static_cast<int>(size));
      if (NULL == session)
      {
        TBSYS_LOG(WARN, "create session failed");
        ret = OB_LIBEASY_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        ObPacket* spacket = new(session+1)ObPacket();
        //从线程局部中拿出该请求所对应的trace id
        TraceId *trace_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
        if (NULL == trace_id)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "no memory, alloc trace id failed, ret=%d", ret);
        }
        else
        {
          spacket->set_req_sign(ObPacket::tsi_req_sign());
          ObPacket::tsi_req_sign() = 0;
          spacket->set_trace_id(trace_id->uval_);
          spacket->set_packet_code(pcode);
          //为这个新的packet分配一个channel id
          uint32_t new_chid = atomic_inc(&ObPacket::global_chid);
          spacket->set_channel_id(new_chid);
          spacket->set_api_version(version);
          spacket->set_source_timeout(timeout);
          spacket->set_data(in_buffer);
          ObDataBuffer buffer(reinterpret_cast<char*>(spacket + 1), size - sizeof(ObPacket));
          //mod wangjiahao [Paxos ups_replication] 20150609 :b
          spacket->serialize(&buffer, cmt_log_seq);
          //mod :e
          spacket->set_packet_buffer(buffer.get_data(), buffer.get_position());
          spacket->get_inner_buffer()->get_position() = buffer.get_position();
          session->status = EASY_CONNECT_SEND;//auto connect if there are no connection to addr
          session->r.opacket = spacket;
          session->addr = addr;
          session->thread_ptr = handler_;
          easy_session_set_timeout(session, static_cast<ev_tstamp>(timeout)/1000);
        }
      }
      return ret;
    }
    //add :e
    int ObClientManager::post_session_using_dedicate_thread(easy_session_t* s, int thread_idx /* =0 */) const
    {
      int rc = OB_SUCCESS;
      //skip timeout_mesg log
      s->error = 2;
      char buff[OB_SERVER_ADDR_STR_LEN];
      if (dedicate_thread_num_ <= 0)
      {
        rc = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "not set dedicate io thread");
      }
      else if (thread_idx >= dedicate_thread_num_)
      {
        rc = OB_SIZE_OVERFLOW;
        TBSYS_LOG(ERROR, "thread_idx[%d] > dedicate_thread_count[%d]", thread_idx, dedicate_thread_num_);
      }
      else
      {
        (s->addr).cidx = eio_->io_thread_pool->thread_count - dedicate_thread_num_ + thread_idx;
        if (EASY_OK != easy_client_dispatch(eio_, s->addr, s))
        {
          TBSYS_LOG(WARN, "post packet to server:%s faild", easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN));
          rc = OB_RPC_POST_ERROR;
        }
        else
        {
          TBSYS_LOG(DEBUG, "post packet to server:%s", easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN));
        }
      }
      return rc;
    }

    int ObClientManager::post_session(easy_session_t* s) const
    {
      int rc = OB_SUCCESS;
      //skip timeout_mesg log
      s->error = 2;
      char buff[OB_SERVER_ADDR_STR_LEN];
      //使用round robin的方式从IO线程池中选择IO线程
      static uint8_t io_index = 0;
      (s->addr).cidx = (__sync_fetch_and_add(&io_index, 1)) % (eio_->io_thread_pool->thread_count - dedicate_thread_num_);
      if (EASY_OK != easy_client_dispatch(eio_, s->addr, s))
      {
        TBSYS_LOG(WARN, "post packet to server:%s faild", easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN));
        rc = OB_RPC_POST_ERROR;
      }
      else
      {
        TBSYS_LOG(DEBUG, "post packet to server:%s", easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN));
      }
      return rc;
    }

    void* ObClientManager::send_session(easy_session_t* s) const
    {
      int rc = OB_SUCCESS;

      ObPacket *packet = NULL;

      if (OB_SUCCESS == rc)
      {
        //和post_session使用一样的代码是为了区分chunkserver和mergeserver
        //使用round robin的方式从IO线程池中选择IO线程
        static uint8_t io_index = 0;
        (s->addr).cidx = __sync_fetch_and_add(&io_index, 1) % (eio_->io_thread_pool->thread_count - dedicate_thread_num_);
        packet = reinterpret_cast<ObPacket*>(easy_client_send(eio_, s->addr, s));
        char buff[OB_SERVER_ADDR_STR_LEN];
        if (NULL == packet)
        {
          TBSYS_LOG(WARN, "send packet to server:%s faild", easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN));
        }
        else
        {
          TBSYS_LOG(DEBUG, "send packet succ, server=%s response_packet_code=%d",
                    easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN),
                    packet->get_packet_code());
        }
      }
      return packet;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
