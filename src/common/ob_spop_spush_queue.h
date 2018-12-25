/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_spop_spush_queue.h for single thread pop single thread push queue
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef COMMON_OB_SPOP_SPUSH_QUEUE_H_ 
#define COMMON_OB_SPOP_SPUSH_QUEUE_H_
#include <pthread.h>
#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    class ObSPopSPushQueue
    {
    public:
      ObSPopSPushQueue();
      ~ObSPopSPushQueue();
      int init(const int64_t queue_size, const int32_t mod_id);
      void reset();
      int push(void *msg);
      int pop(const int64_t timeout_us, void *&msg);
      inline uint64_t size()const
      {
        return push_count_ - pop_count_;
      }
    private:
      bool inited_;
      bool     waiter_exist_;
      uint64_t push_count_;
      uint64_t pop_count_;
      int64_t  push_idx_;
      int64_t  pop_idx_;
      int64_t  queue_size_;
      void     **queue_arr_;
      pthread_mutex_t mutex_;
      pthread_cond_t  cond_;
      pthread_mutex_t push_lock_;
    private:
      ObSPopSPushQueue(const ObSPopSPushQueue &);
      ObSPopSPushQueue &operator=(const ObSPopSPushQueue &);
    };
  }
}


#endif /* COMMON_OB_SPOP_SPUSH_QUEUE_H_ */
