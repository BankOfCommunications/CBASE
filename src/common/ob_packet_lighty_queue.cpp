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

#include "ob_packet_lighty_queue.h"
#include "ob_malloc.h"
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
        {
        }
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

    struct ObPacketLightyQueueItem
    {
      FCounter counter_;
      void* volatile data_;
      bool deep_copy_;
      int push(void* data, const timespec* end_time, bool block, bool deep_copy)
      {
        int err = 0;
        //返回旧值
        //0->1
        int old_val = 0;
        //retry
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
          deep_copy_ = deep_copy;
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

    ObPacketLightyQueue::ObPacketLightyQueue(): push_(0), pop_(0), pos_mask_(0), items_(NULL), allocated_(NULL), queued_item_(0)
    {
    }

    ObPacketLightyQueue::~ObPacketLightyQueue()
    {}

    uint64_t ObPacketLightyQueue::get_item_size()
    {
      return sizeof(Item);
    }

    int ObPacketLightyQueue::init(const uint64_t len, void* buf)
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
      else if (NULL == (items = (Item*)(buf ? buf : (allocated_ = (ob_tc_malloc(get_item_size() * len, ObModIds::OB_LIGHTY_QUEUE))))))
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
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = fifo_allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE)))
        {
          TBSYS_LOG(ERROR, "init fifo allocator failed, err=%d", err);
        }
        else
        {
          fifo_allocator_.set_mod_id(ObModIds::OB_PACKET_LIGHTY_QUEUE);
          thread_buffer_ = new ThreadSpecificBuffer(THREAD_BUFFER_SIZE);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "init ObPacketLightyQueue failed, err=%d", err);
      }
      return err;
    }

    int ObPacketLightyQueue::destroy()
    {
      int err = OB_SUCCESS;
      if (NULL != allocated_)
      {
        ob_tc_free(allocated_, ObModIds::OB_LIGHTY_QUEUE);
        allocated_ = NULL;
      }
      if (thread_buffer_ != NULL)
      {
        delete thread_buffer_;
        thread_buffer_ = NULL;
      }
      return err;
    }

  int ObPacketLightyQueue::push(void* data, const timespec* timeout, bool block, bool deep_copy)
  {
    int err = 0;
    while(true)
    {
      uint64_t seq = __sync_fetch_and_add(&push_, 1);
      Item* pi = items_ + (seq & pos_mask_);
      if (OB_UNLIKELY(deep_copy == true))
      {
        ObPacket *packet = reinterpret_cast<ObPacket*>(data);
        ObDataBuffer *buf = packet->get_packet_buffer();
        int64_t size = sizeof(ObPacket) + buf->get_position();
        void *ptr = fifo_allocator_.alloc(size);
        ObPacket *out_packet = reinterpret_cast<ObPacket*>(ptr);
        if (NULL == ptr)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "allocate memory failed, ret=%d", err);
          break;
        }
        else
        {
          memcpy(reinterpret_cast<char*>(ptr), packet, size);
          out_packet->set_packet_buffer(reinterpret_cast<char*>(ptr) + sizeof(ObPacket), buf->get_position());
          if (0 == (err = pi->push(ptr, timeout, block, true)))
          {
            __sync_fetch_and_add(&queued_item_, 1);
            break;
          }
          else if (OB_EAGAIN == err)
          {
            fifo_allocator_.free(ptr);
            //retry
            continue;
          }
          else if (OB_SIZE_OVERFLOW == err)
          {
            fifo_allocator_.free(ptr);
            TBSYS_LOG(WARN, "queue overflow err=%d", err);
            break;
          }
        }
      }
      else
      {
        if (0 == (err = pi->push(data, timeout, block, false)))
        {
          __sync_fetch_and_add(&queued_item_, 1);
          break;
        }
        else if (OB_EAGAIN == err)
        {
          //retry
          continue;
        }
        else if (OB_SIZE_OVERFLOW == err)
        {
          TBSYS_LOG(WARN, "queue overflow err=%d", err);
          break;
        }
      }
    }
    return err;
  }
  int ObPacketLightyQueue::pop(void*& data, const struct timespec* timeout)
  {
    int err = 0;
    uint64_t seq = __sync_fetch_and_add(&pop_, 1);
    Item* pi = items_ + (seq & pos_mask_);
    //add pangtianze [Paxos] 20170614
    if (NULL == pi)
    {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "pi is null");
    }
    //add:e
    else if (0 == (err = pi->pop(data, timeout)))
    {
      if (OB_UNLIKELY(pi->deep_copy_ == true))
      {
        ThreadSpecificBuffer::Buffer* tb = thread_buffer_->get_buffer();
        if (OB_UNLIKELY(NULL == tb))
        {
          TBSYS_LOG(ERROR, "get packet thread buffer failed, return NULL");
        }
        else
        {
          ObPacket *src_packet = reinterpret_cast<ObPacket*>(data);
          int64_t size = sizeof(ObPacket) + src_packet->get_packet_buffer()->get_capacity();
          char *buf = tb->current();
          memcpy(buf, src_packet, size);
          fifo_allocator_.free(data);
          data = reinterpret_cast<void*>(buf);
          ObPacket *out_packet = reinterpret_cast<ObPacket*>(data);
          out_packet->set_packet_buffer(buf + sizeof(ObPacket), src_packet->get_packet_buffer()->get_capacity());
        }
      }
      __sync_fetch_and_add(&queued_item_, -1);
    }
    return err;
  }

  int64_t ObPacketLightyQueue::remain() const
  {
    return push_ - pop_;
  }
}; // end namespace common
}; // end namespace oceanbase
