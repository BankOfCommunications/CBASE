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
#ifndef __OB_COMMON_OB_ACK_QUEUE_H__
#define __OB_COMMON_OB_ACK_QUEUE_H__
#include "tbsys.h"
#include "ob_wait_queue.h"

namespace oceanbase
{
  namespace common
  {
    class IObAsyncClientCallback;
    class ObClientManager;
    class ObAckQueue
    {
      public:
        static const int RPC_VERSION = 1;
        enum { TIMEOUT_DELTA = 10000, DEFAULT_DELAY_WARN_THRESHOLD_US = 50000};
        struct WaitNode
        {
          WaitNode(): start_seq_(-1), end_seq_(-1), err_(OB_SUCCESS), server_(),
                      send_time_us_(0),
                      timeout_us_(0), receive_time_us_(0),
                      message_residence_time_us_(-1)//add wangdonghui [ups_replication] 20170323 :b:e
                      {}
          ~WaitNode() {}
          bool is_timeout() const { return timeout_us_ > 0 && send_time_us_ + timeout_us_ < tbsys::CTimeUtil::getTime(); }
          // modify wangdonghui [ups_replication] 20170323 :b
          //void done(int err);
          void done(int64_t& message_residence_time_us, int err);
          //modify :e
          int64_t get_delay(){ return receive_time_us_ - send_time_us_; }
          int64_t to_string(char* buf, const int64_t len) const;
          int64_t start_seq_;
          int64_t end_seq_;
          int err_;
          ObServer server_;
          int64_t send_time_us_;
          int64_t timeout_us_;
          int64_t receive_time_us_;
          //add wangdonghui [ups_replication] 20170323 :b
          int64_t message_residence_time_us_;
          //add :e
        };
      public:
        ObAckQueue();
        ~ObAckQueue();
        int init(IObAsyncClientCallback* callback, const ObClientManager* client_mgr, int64_t queue_len);
        int64_t get_next_acked_seq();
        //add wangjiahao [Paxos ups_replication_tmplog] 20150807 :b
        int set_next_acked_seq(const int64_t seq);
        //add :e
        int many_post(const ObServer* servers, int64_t n_server, int64_t start_seq, int64_t end_seq,
                            const int32_t pcode, const int64_t timeout_us, const ObDataBuffer& pkt_buffer, int64_t idx=0);
        int post(const ObServer servers, int64_t start_seq, int64_t end_seq, int64_t send_time_us,
                 const int32_t pcode, const int64_t timeout_us, const ObDataBuffer& pkt_buffer, int64_t idx);
        int callback(void* arg);
      private:
        IObAsyncClientCallback* callback_;
        const ObClientManager* client_mgr_;
        ObWaitQueue<WaitNode> wait_queue_;
        volatile int64_t next_acked_seq_;
        volatile uint64_t lock_;
        //add wangjiahao [Paxos ups_replication_tmplog] 20150807 :b
        bool seq_is_setted_;
        //add :e
    };
    class IObAsyncClientCallback
    {
      public:
        IObAsyncClientCallback(){}
        virtual ~IObAsyncClientCallback(){}
        virtual int handle_response(ObAckQueue::WaitNode& node) = 0; // 删除失败的slave
        virtual int on_ack(ObAckQueue::WaitNode& node) = 0; // 唤醒等待线程
        //add wangjiahao [Paxos ups_replication] :b
        virtual int get_quorum_seq(int64_t& quorum_seq) const = 0;
        //add :e
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_ACK_QUEUE_H__ */
