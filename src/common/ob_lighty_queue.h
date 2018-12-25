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
#ifndef __OB_COMMON_OB_LIGHTY_QUEUE_H__
#define __OB_COMMON_OB_LIGHTY_QUEUE_H__

#include <stdint.h>
#include <stddef.h>
#include "ob_define.h"
#include "ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    struct LightyQueueItem;
    class LightyQueue
    {
      typedef LightyQueueItem Item;
      public:
        LightyQueue();
        ~LightyQueue();
        static uint64_t get_item_size();
        int init(const uint64_t len, void* buf = NULL,
            uint32_t mod_id = ObModIds::OB_LIGHTY_QUEUE);
        int destroy();
        void reset();
        int size() const;
        int64_t remain() const;
        int push(void* data);
        int push(void* data, const timespec* timeout, bool block);
        int pop(void*& data, const timespec* timeout);
      private:
        volatile uint64_t push_ CACHE_ALIGNED;
        volatile uint64_t pop_ CACHE_ALIGNED;
        uint64_t pos_mask_ CACHE_ALIGNED;
        Item* items_;
        void* allocated_;
        int queued_item_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_LIGHTY_QUEUE_H__ */
