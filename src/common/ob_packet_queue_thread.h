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
 *   yanran <yanran.hfs@taobao.com>
 *   ruohai <ruohai@taobao.com>
 */

#ifndef OCEANBASE_COMMON_OB_PACKET_QUEUE_THREAD_H_
#define OCEANBASE_COMMON_OB_PACKET_QUEUE_THREAD_H_

#include "ob_packet_queue_handler.h"
#include "ob_define.h"
#include "ob_packet.h"
#include "ob_packet_queue.h"
#include "hash/ob_hashmap.h"
#include "ob_trace_id.h"
#include "ob_server.h"
#include "ob_packet_lighty_queue.h"

namespace oceanbase
{
  namespace common
  {
    class ObPacketQueueThread : public tbsys::CDefaultRunnable
    {
      public:
        static const int LIGHTY_QUEUE_SIZE = 1 << 18;
        ObPacketQueueThread(int queue_capacity = LIGHTY_QUEUE_SIZE);

        virtual ~ObPacketQueueThread();

        void setThreadParameter(int threadCount, ObPacketQueueHandler *handler, void *args);

        void stop(bool waitFinish = false);

        bool push(ObPacket *packet, int max_queue_len, bool block = true, bool deep_copy = true);

        //void pushQueue(ObPacketQueue &packetQueue, int maxQueueLen = 0);
        void set_ip_port(const IpPort & ip_port);
        void set_host(const ObServer &host);
        void run(tbsys::CThread *thread, void *arg);

        /*
        ObPacket *head()
        {
          return queue_.head();
        }
        ObPacket *tail()
        {
          return queue_.tail();
        }
        */
        size_t size() const
        {
          return queue_.size();
        }

        void clear();

        /**
         * prepare for wait next request packet.
         * this function for some worker thread want to suspend
         * execute for wait next request from client on same channel.
         * called before send_response avoid next request missed by wait thread.
         */
        int prepare_for_next_request(int64_t session_id);

        /**
         * wait for next request packet from client on same channel.
         * called after send_response, call prepare_for_next_request first.
         * @param session_id 
         * @param next_request packet object
         * @param timeout
         */
        int wait_for_next_request(int64_t session_id, ObPacket* &next_request, const int64_t timeout);

        int destroy_session(int64_t session_id);

        int64_t generate_session_id();

        inline void set_max_wait_thread_count(const uint64_t max_wait_count) 
        { max_waiting_thread_count_ = max_wait_count; }

      protected:
        bool wait_finish_; 
        bool waiting_;
        ObPacketLightyQueue queue_;
        timespec timeout_;
        ObPacketQueueHandler* handler_;
        void* args_;

      private:
        struct WaitObject
        {
          int64_t thread_no_;
          ObPacket* packet_;
          bool session_end_;
          WaitObject() : thread_no_(0), packet_(NULL),session_end_(false) {}
          WaitObject(int64_t no, ObPacket* p) : thread_no_(no), packet_(p),session_end_(false) {}
          void end_session(){ session_end_ = true;}
          bool is_session_end()const{ return session_end_;}
        };

        bool is_next_packet(ObPacket* packet) const;
        bool wakeup_next_thread(ObPacket* packet);
        int64_t get_session_id(ObPacket* packet) const;
        ObPacket* clone_next_packet(ObPacket* packet, int64_t thread_no) const;

        static const int64_t MAX_THREAD_COUNT = 1024;
        static const int64_t MAX_PACKET_SIZE = 2*1024*1024L; // 2M

        tbsys::CThreadCond next_cond_[MAX_THREAD_COUNT];
        hash::ObHashMap<int64_t, WaitObject, hash::SpinReadWriteDefendMode> next_wait_map_;

        volatile uint64_t session_id_;
        volatile uint64_t waiting_thread_count_;
        uint64_t max_waiting_thread_count_;
        IpPort ip_port_;
        ObServer host_;
        char* next_packet_buffer_;
    };

  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_PACKET_QUEUE_THREAD_H_
