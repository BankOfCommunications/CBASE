/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_command_queue.h is for what ...
 *
 * Version: ***: ob_mysql_command_queue.h  Wed Jul 18 10:05:53 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_COMMAND_QUEUE_H_
#define OB_MYSQL_COMMAND_QUEUE_H_

#include "common/ob_define.h"
#include "common/ob_ring_buffer.h"
#include "common/thread_buffer.h"
#include "packet/ob_mysql_command_packet.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLCommandQueue
    {
      friend class ObMySQLCommandQueueThread;
      public:
        static const int64_t THREAD_BUFFER_SIZE = sizeof(ObMySQLCommandPacket) + common::OB_MAX_PACKET_LENGTH;
      public:
        ObMySQLCommandQueue();
        ~ObMySQLCommandQueue();

        int init();

        ObMySQLCommandPacket* pop();
        void push(ObMySQLCommandPacket* packet);
        void clear();
        void free(ObMySQLCommandPacket* packet);

        int size() const;
        bool empty() const;

      private:
        ObMySQLCommandPacket* head_;
        ObMySQLCommandPacket* tail_;
        int size_;
        common::ObRingBuffer ring_buffer_;
        common::ThreadSpecificBuffer* thread_buffer_;
    };
  }
}
#endif
