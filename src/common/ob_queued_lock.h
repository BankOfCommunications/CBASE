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
#ifndef __OB_COMMON_OB_QUEUED_LOCK_H__
#define __OB_COMMON_OB_QUEUED_LOCK_H__
#include "ob_lighty_queue.h"

namespace oceanbase
{
  namespace common
  {
    // 同时加锁的线程数超过OB_MAX_THREAD_NUM会出现乱序获得锁的情况，但不会出错
    class ObQueuedLock
    {
      public:
        class Guard
        {
          public:
            Guard(ObQueuedLock& lock): lock_(lock) { lock_.lock(ticket_); }
            ~Guard() { lock_.unlock(ticket_); }
          private:
            ObQueuedLock& lock_;
            uint64_t ticket_;
        };
      public:
        ObQueuedLock(): ticket_(0) {
          int syserr = 0;
          int err = OB_SUCCESS;
          for(int64_t i = 0; OB_SUCCESS == err && i < OB_MAX_THREAD_NUM; i++)
          {
            if (0 != (syserr = pthread_mutex_init(mutex_ + i, NULL)))
            {
              err = OB_ERR_SYS;
              TBSYS_LOG(ERROR, "pthread_mutex_init()=>%s", strerror(syserr));
            }
            else if (0 != (syserr = pthread_mutex_lock(mutex_ + i)))
            {
              err = OB_ERR_SYS;
              TBSYS_LOG(ERROR, "pthread_mutex_lock()=>%s", strerror(syserr));
            }
          }
          if (OB_SUCCESS != err)
          {}
          else if (0 != (syserr = pthread_mutex_unlock(mutex_)))
          {
            err = OB_ERR_SYS;
            TBSYS_LOG(ERROR, "pthread_mutex_unlock()=>%s", strerror(syserr));
          }
          OB_ASSERT(OB_SUCCESS == err);
        }
        ~ObQueuedLock() {}
      public:
        int lock(uint64_t& id) {
          int syserr = 0;
          int err = OB_SUCCESS;
          id = __sync_fetch_and_add(&ticket_, 1);
          if (0 != (syserr = pthread_mutex_lock(get_mutex(id))))
          {
            err = OB_ERR_SYS;
            TBSYS_LOG(ERROR, "pthread_mutex_lock()=>%s", strerror(syserr));
          }
          return err;
        }
        int unlock(uint64_t& id) {
          int syserr = 0;
          int err = OB_SUCCESS;
          if (0 != (syserr = pthread_mutex_unlock(get_mutex(id + 1))))
          {
            err = OB_ERR_SYS;
            TBSYS_LOG(ERROR, "pthread_mutex_unlock()=>%s", strerror(syserr));
          }
          return err;
        }
      protected:
        pthread_mutex_t* get_mutex(uint64_t i) { return mutex_ + (i % OB_MAX_THREAD_NUM); }
      private:
        uint64_t ticket_;
        pthread_mutex_t mutex_[OB_MAX_THREAD_NUM];
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_QUEUED_LOCK_H__ */
