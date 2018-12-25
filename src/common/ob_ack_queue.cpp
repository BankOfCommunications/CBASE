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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_ack_queue.h"
#include "ob_result.h"
#include "easy_io.h"
#include "ob_client_manager.h"

namespace oceanbase
{
  namespace common
  {
    struct __ACK_QUEUE_UNIQ_TYPE__ {};
    typedef TypeUniqReg<ObAckQueue, __ACK_QUEUE_UNIQ_TYPE__> TUR;
    // modify wangdonghui [ups_replication] 20170323 :b
    //void ObAckQueue::WaitNode::done(int err)
    void ObAckQueue::WaitNode::done(int64_t& message_residence_time_us, int err)
    // modify:e
    {
      err_ = err;
      receive_time_us_ = tbsys::CTimeUtil::getTime();
      //add wangdonghui [ups_replication] 20170323 :b
      message_residence_time_us_ = message_residence_time_us;
      //add :e
      //add wangjiahao [Paxos ups_replication] 20150731 :b
      if (err == OB_LOG_NOT_SYNC)
      {
        //Slave may flush timeout, or be in status of REPLAYING_LOG because of lacking of too many logs
        TBSYS_LOG(WARN, "wait response: err=%d, %s", err, to_cstring(*this));
      }
      else
      //add :e
      if (OB_SUCCESS != err)
      {
        //mod wangdonghui ERROR->WARN
        TBSYS_LOG(WARN, "wait response: err=%d, %s", err, to_cstring(*this));
      }
      // else if (receive_time_us_ > send_time_us_ + delay_warn_threshold_us_)
      // {
      //   TBSYS_LOG(WARN, "slow response: %s", to_cstring(*this));
      // }
    }

    int64_t ObAckQueue::WaitNode::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "WaitNode: seq=[%ld,%ld], err=%d, server=%s, send_time=%ld, round_time=%ld, timeout=%ld",
                      start_seq_, end_seq_, err_, to_cstring(server_), send_time_us_, receive_time_us_ - send_time_us_, timeout_us_);
      return pos;
    }

    ObAckQueue::ObAckQueue(): callback_(NULL), client_mgr_(NULL), next_acked_seq_(0), lock_(0), seq_is_setted_(false)
    {
      OB_ASSERT(*TUR::value() == NULL);
      *TUR::value() = this;
    }

    ObAckQueue::~ObAckQueue()
    {}

    int ObAckQueue::init(IObAsyncClientCallback* callback, const ObClientManager* client_mgr, int64_t queue_len)
    {
      int err = OB_SUCCESS;
      if (NULL == callback || NULL == client_mgr || queue_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: callback=%p, client_mgr=%p, queue_len=%ld", callback, client_mgr, queue_len);
      }
      else if (OB_SUCCESS != (err = wait_queue_.init(queue_len)))
      {
        TBSYS_LOG(ERROR, "wait_queue.init()=>%d", err);
      }
      else
      {
        callback_ = callback;
        client_mgr_ = client_mgr;
      }
      return err;
    }

    //add wangjiahao [Paxos ups_replication_tmplog] 20150807 :b
    int ObAckQueue::set_next_acked_seq(const int64_t seq)
    {
      int err = OB_SUCCESS;
      int64_t x = 0;
      if (seq_is_setted_)
      {
        TBSYS_LOG(WARN, "next_acked_seq_ is setted already");
        err = OB_INIT_TWICE;
      }
      else
      {
        seq_is_setted_ = true;
        while ((x = next_acked_seq_) < seq && ! __sync_bool_compare_and_swap(&next_acked_seq_, x, seq))
        {
           TBSYS_LOG(WARN, "set new_ack_seq failed");
        }
      }
      return err;
    }
    //add :e

    int64_t ObAckQueue::get_next_acked_seq()
    {
      int err = OB_SUCCESS;
      SeqLockGuard guard(lock_);
      int64_t old_ack_seq = next_acked_seq_;
      int64_t seq = 0;
      WaitNode node;
      //add wangjiahao [Paxos ups_replication] 20150602 :b
      int64_t new_ack_seq = 0;
      int64_t x = 0;
      if (callback_->get_quorum_seq(new_ack_seq) != OB_SUCCESS)
      {
         //not master
      }
      else if ((x = next_acked_seq_) < new_ack_seq && ! __sync_bool_compare_and_swap(&next_acked_seq_, x, new_ack_seq))
      {
         TBSYS_LOG(WARN, "set new_ack_seq failed");
      }
      //add :e
      while(OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = wait_queue_.pop(seq, node))
            && OB_PROCESS_TIMEOUT != err && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "wait_queue.pop()=>%d", err);
        }
        else if (OB_PROCESS_TIMEOUT == err)
        {
          callback_->handle_response(node);
          err = OB_SUCCESS;
        }
        //del wangjiahao [Paxos ups_replication] 20150602 :b
        /*
        if (OB_SUCCESS == err)
        {
          next_acked_seq_ = node.start_seq_;
        }
        */
        //del
      }
      if (old_ack_seq != next_acked_seq_)
      {
        callback_->on_ack(node);
        TBSYS_LOG(DEBUG, "ACK_QUEUE -> next_acked_seq_ = %ld", next_acked_seq_);
      }
      return next_acked_seq_;
    }

    int ObAckQueue::many_post(const ObServer* servers, int64_t n_server, int64_t start_seq, int64_t end_seq,
                                    const int32_t pcode, const int64_t timeout_us, const ObDataBuffer& pkt_buffer, int64_t idx)
    {
      int err = OB_SUCCESS;
      int64_t send_time_us = tbsys::CTimeUtil::getTime();
      ObServer null_server;
      if (NULL == servers || n_server < 0 || start_seq < 0 || end_seq < start_seq || timeout_us <= TIMEOUT_DELTA)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: servers=%p[%ld], seq=[%ld,%ld], timeout=%ld", servers, n_server, start_seq, end_seq, timeout_us);
      }
      for(int64_t i = 0; OB_SUCCESS == err && i < n_server; i++)
      {
        if (OB_SUCCESS != (err = post(servers[i], start_seq, end_seq, send_time_us, pcode, timeout_us - TIMEOUT_DELTA, pkt_buffer, idx)))
        {
          TBSYS_LOG(ERROR, "post(%s)=>%d", to_cstring(servers[i]), err);
        }
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = post(null_server, end_seq, end_seq, send_time_us, pcode, timeout_us -TIMEOUT_DELTA, pkt_buffer, -1)))
      {
        TBSYS_LOG(ERROR, "post(fake)=>%d", err);
      }
      if (OB_SUCCESS == err && n_server <= 0)
      {
        get_next_acked_seq();
      }
      return err;
    }

    int ObAckQueue::post(const ObServer server, int64_t start_seq, int64_t end_seq, int64_t send_time_us,
                           const int32_t pcode, const int64_t timeout_us, const ObDataBuffer& pkt_buffer, int64_t idx)
    {
      int err = OB_SUCCESS;
      int64_t wait_idx = 0;
      WaitNode wait_node;
      if (start_seq < 0 || end_seq < 0 || timeout_us <= TIMEOUT_DELTA)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: servers=%s, seq=[%ld,%ld], timeout=%ld", to_cstring(server), start_seq, end_seq, timeout_us);
      }
      else
      {
        wait_node.start_seq_ = start_seq;
        wait_node.end_seq_ = end_seq;
        wait_node.server_ = server;
        wait_node.send_time_us_ = send_time_us;
        wait_node.timeout_us_ = timeout_us;
        //add wangdonghui [ups_replication] 20170323 :b
        wait_node.message_residence_time_us_ = -1;
        //add :e
      }
      while(OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = wait_queue_.push(wait_idx, wait_node))
            && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "wait_queue_.push()=>%d", err);
        }
        else if (OB_EAGAIN == err)
        {
          get_next_acked_seq();
          err = OB_SUCCESS;
        }
        else
        {
          //post with master commmit_log_id
          if (idx >= 0 && OB_SUCCESS != (err = client_mgr_->post_request_using_dedicate_thread(server, pcode, RPC_VERSION, timeout_us, pkt_buffer,
                                                                                               static_callback<TUR>, (void*)wait_idx,
                                                                                               //wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
                                                                                               (int64_t)next_acked_seq_,
                                                                                               //add:e
                                                                                               (int)idx)))
          {
            TBSYS_LOG(ERROR, "post_request()=>%d", err);
          }
          if (idx < 0 || OB_SUCCESS != err)
          {
            //modify wangdonghui [ups_replication] 20170323 :b
            //wait_queue_.done(wait_idx, wait_node, err);
            int64_t message_residence_time = -1;
            wait_queue_.done(wait_idx, message_residence_time, wait_node, err);
            //mod :e
            callback_->handle_response(wait_node);
          }
          break;
        }
      }
      return err;
    }

    int ObAckQueue::callback(void* data)
    {
      int ret = EASY_OK;
      int err = OB_SUCCESS;
      easy_request_t* r = (easy_request_t*)data;
      if (NULL == r || NULL == r->ms)
      {
        TBSYS_LOG(WARN, "request is null or r->ms is null");
      }
      /*
      else if (NULL == r->user_data)
      {
        TBSYS_LOG(WARN, "request user_data == NULL");
      }
      */
      else
      {
        ObPacket* packet = NULL;
        ObDataBuffer* response_buffer = NULL;
        ObResultCode result_code;
        //add wangdonghui [ups_replication] 20170323 :b
        int64_t message_residence_time_us;
        //add :e
        int64_t pos = 0;
        if (NULL == (packet =(ObPacket*)r->ipacket))
        {
          err = OB_RESPONSE_TIME_OUT;
          TBSYS_LOG(WARN, "receive NULL packet, timeout");
        }
        else if (NULL == (response_buffer = packet->get_buffer()))
        {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "response has NULL buffer");
        }
        else if (OB_SUCCESS != (err = result_code.deserialize(response_buffer->get_data() + response_buffer->get_position(), packet->get_data_length(), pos)))
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d], server=%s", pos, err, inet_ntoa_r(get_easy_addr(r)));
        }
        // add wangdonghui [ups_replication] 20170323 :b
        else if (OB_SUCCESS != (err = serialization::decode_vi64(response_buffer->get_data() + response_buffer->get_position(),
                                                                 packet->get_data_length(), pos, &message_residence_time_us)))
        {
          TBSYS_LOG(ERROR, "deserialize message_residence_time_us failed:pos[%ld], err[%d], server=%s", pos, err, inet_ntoa_r(get_easy_addr(r)));
        }
        //add :e
        else
        {
          err = result_code.result_code_;
        }
        WaitNode node;
        int tmperr = OB_SUCCESS;
        //modify wangdonghui [ups_replication] 20170323 :b
        if (OB_SUCCESS != (tmperr = wait_queue_.done((int64_t)r->user_data, message_residence_time_us, node, err))
            && OB_ALREADY_DONE != tmperr)
        //if (OB_SUCCESS != (tmperr = wait_queue_.done((int64_t)r->user_data, node, err))
        //    && OB_ALREADY_DONE != tmperr)
        //mod :e
        {
          TBSYS_LOG(ERROR, "wait_queue.done()=>%d", tmperr);
        }
        else if (OB_SUCCESS == tmperr)
        {
          callback_->handle_response(node);
          get_next_acked_seq();
        }
      }
      if (NULL != r && NULL != r->ms)
      {
        easy_session_destroy(r->ms);
      }
      return ret;
    }
  }; // end namespace common
}; // end namespace oceanbase
