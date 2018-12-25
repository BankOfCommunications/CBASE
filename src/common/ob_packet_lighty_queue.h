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
 *   lide.wd <lide.wd@alipay.com>
 *     - some work details if you want
 */
#ifndef __OB_COMMON_OB_PACKET_LIGHTY_QUEUE_H__
#define __OB_COMMON_OB_PACKET_LIGHTY_QUEUE_H__

#include <stdint.h>
#include <stddef.h>
#include "ob_define.h"
#include "ob_fifo_allocator.h"
#include "thread_buffer.h"
#include "ob_packet.h"

namespace oceanbase
{
  namespace common
  {
    struct ObPacketLightyQueueItem;
    class ObPacketLightyQueue
    {
      typedef ObPacketLightyQueueItem Item;
      public:
      static const int64_t THREAD_BUFFER_SIZE = sizeof(ObPacket) + OB_MAX_PACKET_LENGTH;
      public:
        ObPacketLightyQueue();
        ~ObPacketLightyQueue();
        static uint64_t get_item_size();
        int init(const uint64_t len, void* buf = NULL);
        int destroy();
        inline int size() const
        {
          return queued_item_;
        }
        int64_t remain() const;
        int push(void* data, const timespec* timeout, bool block, bool deep_copy = true);
        int pop(void*& data, const timespec* timeout);
      public:
        static const int64_t TOTAL_LIMIT = 1024 * 1024 * 1024;//1GB
        static const int64_t HOLD_LIMIT = TOTAL_LIMIT/2;
        static const int64_t PAGE_SIZE = 8 * 1024 * 1024;
      private:
        volatile uint64_t push_ CACHE_ALIGNED;
        volatile uint64_t pop_ CACHE_ALIGNED;
        uint64_t pos_mask_ CACHE_ALIGNED;
        Item* items_;
        void* allocated_;
        int queued_item_;
        FIFOAllocator fifo_allocator_;
        ThreadSpecificBuffer* thread_buffer_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_LIGHTY_QUEUE_H__ */
