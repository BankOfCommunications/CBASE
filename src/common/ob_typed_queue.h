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
#include "ob_lighty_queue.h"
#include "ob_malloc.h"
#include "utility.h"

#ifndef __OB_COMMON_OB_TYPED_QUEUE_H__
#define __OB_COMMON_OB_TYPED_QUEUE_H__
namespace oceanbase
{
  namespace common
  {
    template<typename T>
    class TypedQueue
    {
      public:
        static const int64_t DEFAULT_WAIT_TIME_US = 100 * 1000;
      public:
        TypedQueue(): wait_time_us_(DEFAULT_WAIT_TIME_US), buf_(NULL), free_(), queue_() {}
        ~TypedQueue() {
          if (NULL == buf_)
          {
            ob_free(buf_);
            buf_ = NULL;
          }
        }
      public:
        int init(const int64_t capacity, const int64_t wait_time_us = DEFAULT_WAIT_TIME_US) {
          int err = OB_SUCCESS;
          uint64_t item_size = LightyQueue::get_item_size();
          if (0 >= capacity || !is2n(capacity) || wait_time_us < 0)
          {
            err = OB_INVALID_ARGUMENT;
            TBSYS_LOG(ERROR, "init(capacity=%ld, wait_time_us=%ld):INVALID_ARGUMENT", capacity, wait_time_us);
          }
          else if (NULL != buf_)
          {
            err = OB_INIT_TWICE;
          }
          else if (NULL == (buf_ = (char*)ob_malloc(2 * item_size * capacity + sizeof(T) * capacity,
                                                    ObModIds::OB_LIGHTY_QUEUE)))
          {
            err = OB_ALLOCATE_MEMORY_FAILED;
          }
          else if ((uint64_t)buf_ & 0x7)
          {
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "memory not aligned buf_=%p", buf_);
          }
          else if (OB_SUCCESS != (err = free_.init(capacity, buf_)))
          {
            TBSYS_LOG(ERROR, "free_.init(capacity=%ld)=>%d", capacity, err);
          }
          else if (OB_SUCCESS != (err = queue_.init(capacity, buf_ + item_size * capacity)))
          {
            TBSYS_LOG(ERROR, "queue_.init(capacity=%ld)=>%d", capacity, err);
          }
          for(int64_t i = 0; OB_SUCCESS == err && i < capacity; i++)
          {
            err = free_.push((void*)(buf_ + 2 * item_size * capacity + i * sizeof(T)), 0LL);
          }
          if (OB_SUCCESS != err)
          {
            if (buf_ != NULL)
            {
              ob_free(buf_);
              buf_ = NULL;
            }
          }
          else
          {
            wait_time_us_ = wait_time_us;
            timeout_.tv_sec = wait_time_us/1000000;
            timeout_.tv_nsec = (wait_time_us%1000000) * 1000;
          }
          return err;
        }
        int alloc(T*& p) {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = free_.pop((void*&)p, &timeout_)))
          {}
          else if (NULL == (p = new(p)T()))
          {
            err = OB_INIT_FAIL;
          }
          return err;
        }
        int free(T* p) {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = free_.push((void*)p, &timeout_)))
          {}
          else
          {
            p->~T();
          }
          return err;
        }
        int push(T* p) {
          return queue_.push((void*)p, &timeout_);
        }
        int pop(T*& p) {
          return queue_.pop((void*&)p, &timeout_);
        }
      private:
        int64_t wait_time_us_;
        timespec timeout_;
        char* buf_;
        LightyQueue free_;
        LightyQueue queue_;
    };
  }; // end namespace common
}; // end namespace oceanbase
#endif /* __OB_COMMON_OB_TYPED_QUEUE_H__ */
