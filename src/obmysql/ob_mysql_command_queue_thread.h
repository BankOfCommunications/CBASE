/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_command_queue_thread.h is for what ...
 *
 * Version: ***: ob_mysql_command_queue_thread.h  Fri Jul 27 15:26:38 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_COMMAND_QUEUE_THREAD_H_
#define OB_MYSQL_COMMAND_QUEUE_THREAD_H_

#include "tbsys.h"
#include "ob_mysql_packet_queue_handler.h"
#include "ob_mysql_command_queue.h"
#include "packet/ob_mysql_packet.h"
#include "common/ob_trace_id.h"
#include "common/ob_profile_log.h"
#include "common/ob_server.h"
#include "common/ob_lighty_queue.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLCommandQueueThread : public tbsys::CDefaultRunnable
    {
      public:
        static const int LIGHTY_QUEUE_SIZE  = 1 << 18;
        ObMySQLCommandQueueThread();
        virtual ~ObMySQLCommandQueueThread();

        int init(int queue_capacity = LIGHTY_QUEUE_SIZE);
        void set_ip_port(const common::IpPort & ip_port);
        void setThreadParameter(int threadCount, ObMySQLPacketQueueHandler* handler, void* args);

        void stop(bool waitFinish = false);

        bool push(ObMySQLCommandPacket* packet, int maxQueueLen = 0, bool block = true);
        int get_queue_size()
        {
          return queue_.size();
        }
        /**
         * 线程函数 从队列中取出packet 交给handler处理
         */
        void run(tbsys::CThread* thread, void* arg);
        void set_self_to_thread_queue(const common::ObServer & host);

      private:
        common::LightyQueue queue_;
        //ObMySQLCommandQueue queue_;
        ObMySQLPacketQueueHandler* handler_;
        //tbsys::CThreadCond cond_;
        //tbsys::CThreadCond pushcond_;
        timespec timeout_;
        bool wait_finish_;
        bool waiting_;
        common::IpPort ip_port_;
        common::ObServer host_;
    };
  }// end namespace obmysql
}// end namespace oceanbase
#endif
