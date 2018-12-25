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
#include <sys/syscall.h>
#include <linux/futex.h>
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
#define futex(...) syscall(SYS_futex,__VA_ARGS__)
    static int futex_wake(volatile int* p, int val)
    {
      int err = 0;
      if (0 != futex((int*)p, FUTEX_WAKE_PRIVATE, val, NULL, NULL, 0))
      {
        err = errno;
      }
      return err;
    }

    static int futex_wait(volatile int* p, int val, const timespec* timeout)
    {
      int err = 0;
      if (0 != futex((int*)p, FUTEX_WAIT_PRIVATE, val, timeout, NULL, 0))
      {
        err = errno;
      }
      return err;
    }

    static int dec_if_gt0(volatile int* p)
    {
      int x = 0;
      while((x = *p) > 0 && !__sync_bool_compare_and_swap(p, x, x - 1))
        ;
      return x;
    }

    static int inc_if_le0(volatile int* p)
    {
      int x = 0;
      while((x = *p) <= 0 && !__sync_bool_compare_and_swap(p, x, x + 1))
        ;
      return x;
    }

    struct FCounter
    {
      volatile int32_t val_;
      volatile int32_t n_waiters_;
      FCounter(): val_(0), n_waiters_(0) {}
      ~FCounter() {}
      int32_t inc(const timespec* timeout, bool block)
      {
        int err = 0;
        int32_t val = 0;
        //0->1,-1->0
        //进入这个if分支，push <= pop,有pop线程等着pop，或者槽位空着 
        // < 0: 假唤醒
        if ((val = inc_if_le0(&val_)) <= 0)
        {}
        else if (!block)
        {
          val = __sync_fetch_and_add(&val_, 1);
        }
        else
        {
          // push > pop, pop处理不过来
          __sync_add_and_fetch(&n_waiters_, 1);
          while(true)
          {
            //让它无限等,被pop唤醒
            if (ETIMEDOUT == (err = futex_wait(&val_, val, timeout)))
            {
              val = __sync_fetch_and_add(&val_, 1);
              TBSYS_LOG(WARN, "inc timeout");
              break;
            }
            //被唤醒或者，val_在futex_wait之前被改了，futex_wait直接返回
            if ((val = inc_if_le0(&val_)) <= 0)
            {
              break;
            }
          }
          __sync_add_and_fetch(&n_waiters_, -1);
        }
        return val;
      }

      int32_t dec(const timespec* timeout)
      {
        int err = 0;
        int32_t val = 0;
        if ((val = dec_if_gt0(&val_)) > 0)
        {}
        else
        {
          __sync_add_and_fetch(&n_waiters_, 1);
          while(true)
          {
            if (ETIMEDOUT == (err = futex_wait(&val_, val, timeout)))
            {
              val = __sync_fetch_and_add(&val_, -1);
              break;
            }
            if ((val = dec_if_gt0(&val_)) > 0)
            {
              break;
            }
          }
          __sync_add_and_fetch(&n_waiters_, -1);
        }
        return val;
      }
      void wake_if_needed()
      {
        if (n_waiters_ > 0)
        {
          wake();
        }
      }
      void wake()
      {
        futex_wake(&val_, INT32_MAX);
      }
    };

    struct LightyQueueItem
    {
      FCounter counter_;
      void* volatile data_;
      int push(void* data, const timespec* end_time, bool block)
      {
        int err = 0;
        //返回旧值
        //0->1
        int old_val = 0;
        if ((old_val = counter_.inc(end_time, block)) < 0)
        {
          counter_.wake_if_needed();
          err = OB_EAGAIN;
        }
        else if (old_val > 0)
        {
          counter_.wake_if_needed();
          err = OB_SIZE_OVERFLOW;
        }
        else
        {
          while(!__sync_bool_compare_and_swap(&data_, NULL, data))
            ;
          counter_.wake_if_needed();
        }
        return err;
      }

      int pop(void*& data, const timespec* end_time)
      {
        int err = 0;
        //1->0
        if (counter_.dec(end_time) != 1)
        {
          counter_.wake_if_needed();
          err = OB_EAGAIN;
        }
        else
        {
          counter_.wake_if_needed();
          while(NULL == (data = data_) || !__sync_bool_compare_and_swap(&data_, data, NULL))
            ;
        }
        return err;
      }
    };

    LightyQueue::LightyQueue(): push_(0), pop_(0), pos_mask_(0), items_(NULL), allocated_(NULL), queued_item_(0)
    {}

    LightyQueue::~LightyQueue()
    {
      destroy();
    }

    uint64_t LightyQueue::get_item_size()
    {
      return sizeof(Item);
    }

    int LightyQueue::init(const uint64_t len, void* buf, uint32_t mod_id)
    {
      int err = OB_SUCCESS;
      Item* items = NULL;
      if (0 == len || !is2n(len))
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "init(len=%ld):invalid argument", len);
      }
      else if (NULL != items_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == (items = (Item*)(buf ? buf : (allocated_ = (ob_tc_malloc(get_item_size() * len, mod_id))))))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "init(len=%ld):allocate memory failed.", len);
      }
      else
      {
        pos_mask_ = len - 1;
        memset(items, 0, sizeof(Item) * len);
        items_ = items;
      }
      return err;
    }

    int LightyQueue::destroy()
    {
      int err = OB_SUCCESS;
      if (NULL != allocated_)
      {
        ob_tc_free(allocated_);
        allocated_ = NULL;
      }
      return err;
    }

  int LightyQueue::push(void* data)
  {
    return push(data, NULL, false);
  }
  int LightyQueue::push(void* data, const timespec* timeout, bool block)
  {
    int err = 0;
    while (true)
    {
      uint64_t seq = __sync_fetch_and_add(&push_, 1);
      Item* pi = items_ + (seq & pos_mask_);
      if (0 == (err = pi->push(data, timeout, block)))
      {
        __sync_fetch_and_add(&queued_item_, 1);
        break;
      }
      else if (OB_EAGAIN == err)
      {
        continue;
      }
      else if (OB_SIZE_OVERFLOW == err)
      {
        TBSYS_LOG(WARN, "queue overflow err=%d", err);
        break;
      }
    }
    return err;
  }
  int LightyQueue::pop(void*& data, const struct timespec* timeout)
  {
    int err = 0;
    uint64_t seq = __sync_fetch_and_add(&pop_, 1);
    Item* pi = items_ + (seq & pos_mask_);
    if (0 == (err = pi->pop(data, timeout)))
    {
      __sync_fetch_and_add(&queued_item_, -1);
    }
    return err;
  }

  int64_t LightyQueue::remain() const
  {
    return push_ - pop_;
  }
  int LightyQueue::size() const
  {
    return queued_item_;
  }
  void LightyQueue::reset()
  {
    while (size() > 0)
    {
      void * p;
      pop(p, NULL);
    }
  }
}; // end namespace common
}; // end namespace oceanbase
