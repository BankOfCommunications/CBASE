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

#include <errno.h>
#include <stdint.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <time.h>
#include "futex_sem.h"

namespace oceanbase
{
  namespace common
  {
#define futex(...) syscall(SYS_futex,__VA_ARGS__)
    const static int64_t NS_PER_SEC = 1000000000;
    static inline int decrement_if_positive(volatile int* p)
    {
      int x = 0;
      while((x = *p) > 0 && !__sync_bool_compare_and_swap(p, x, x - 1))
        ;
      return x;
    }

    timespec* calc_remain_timespec(timespec* remain, const timespec* end_time)
    {
      timespec now;
      if (NULL == end_time || NULL == remain)
      {}
      else if (0 != clock_gettime(CLOCK_REALTIME, &now))
      {}
      else
      {
        remain->tv_sec = end_time->tv_sec - now.tv_sec;
        remain->tv_nsec = end_time->tv_nsec - now.tv_nsec;
        if (remain->tv_nsec < 0)
        {
          remain->tv_sec--;
          remain->tv_nsec += NS_PER_SEC;
        }
      }
      return remain;
    }

    timespec* calc_abs_time(timespec* ts, const int64_t time_ns)
    {
      if (NULL == ts)
      {}
      else if (0 != (clock_gettime(CLOCK_REALTIME, ts)))
      {}
      else
      {
        ts->tv_nsec += time_ns;
        if (ts->tv_nsec > NS_PER_SEC)
        {
          ts->tv_sec += ts->tv_sec/NS_PER_SEC;
          ts->tv_nsec %= NS_PER_SEC;
        }
      }
      return ts;
    }

    int futex_post(fsem_t* p)
    {
      int err = 0;
      if (__sync_fetch_and_add(&p->val_, 1) >= INT_MAX)
      {
        err = EOVERFLOW;
      }
      else
      {
        __sync_synchronize(); // 这个barrier完全是从glibc中抄过来的，其实可以不要。
        if (p->nwaiters_ > 0)
        {
          futex(&p->val_, FUTEX_WAKE, 1, NULL, NULL, 0);
        }
      }
      return err;
    }

    int futex_wait(fsem_t* p)
    {
      int err = 0;
      if (decrement_if_positive(&p->val_) > 0)
      {}
      else
      {
        __sync_fetch_and_add(&p->nwaiters_, 1);
        while(1)
        {
          if (0 != futex(&p->val_, FUTEX_WAIT, 0, NULL, NULL, 0))
          {
            err = errno;
          }
          if (0 != err && EWOULDBLOCK != err && EINTR != err)
          {
            break;
          }
          if (decrement_if_positive(&p->val_) > 0)
          {
            err = 0;
            break;
          }
        }
        __sync_fetch_and_add(&p->nwaiters_, -1);
      }
      return err;
    }

    int futex_wait(fsem_t* p, const timespec* end_time)
    {
      int err = 0;
      timespec remain;
      if (decrement_if_positive(&p->val_) > 0)
      {}
      else
      {
        __sync_fetch_and_add(&p->nwaiters_, 1);
        while(1)
        {
          calc_remain_timespec(&remain, end_time);
          if (remain.tv_sec < 0)
          {
            err = ETIMEDOUT;
            break;
          }
          if (0 != futex(&p->val_, FUTEX_WAIT, 0, &remain, NULL, 0))
          {
            err = errno;
          }
          if (0 != err && EWOULDBLOCK != err && EINTR != err)
          {
            break;
          }
          if (decrement_if_positive(&p->val_) > 0)
          {
            err = 0;
            break;
          }
        }
        __sync_fetch_and_add(&p->nwaiters_, -1);
      }
      return err;
    }
  }; // end namespace common
}; // end namespace oceanbase
