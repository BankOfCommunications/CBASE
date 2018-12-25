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
#ifndef __OB_COMMON_OB_TSI_UTILS_H__
#define __OB_COMMON_OB_TSI_UTILS_H__
#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    volatile int64_t __next_tid __attribute__((weak));
    inline int64_t itid()
    {
      static __thread int64_t tid = -1;
      return tid < 0? (tid = __sync_fetch_and_add(&__next_tid, 1)): tid;
    }

    struct TCCounter
    {
      struct Item
      {
        uint64_t value_;
      } CACHE_ALIGNED;
      TCCounter(){ memset(items_, 0, sizeof(items_)); }
      ~TCCounter(){}
      int64_t inc(int64_t delta=1) { return __sync_fetch_and_add(&items_[itid() % OB_MAX_THREAD_NUM].value_, delta); }
      int64_t value() const{
        int64_t sum = 0;
        int64_t thread_count = __next_tid;
        int64_t slot_count = (thread_count < OB_MAX_THREAD_NUM)? thread_count: OB_MAX_THREAD_NUM;
        for(int64_t i = 0; i < slot_count; i++)
        {
          sum += items_[i].value_;
        }
        return sum;
      }
      Item items_[OB_MAX_THREAD_NUM];
    };

    struct ExpStat
    {
      ExpStat(const char* metric_name) { memset(this, 0, sizeof(*this)); metric_name_ = metric_name; }
      ~ExpStat(){}
      static int64_t get_idx(const int64_t x){ return x? (64 - __builtin_clzl(x)): 0; }
      int64_t count() const {
        int64_t total_count = 0;
        for(int64_t i = 0; i < 64; i++)
        {
          total_count += count_[i].value();
        }
        return total_count;
      }
      int64_t value() const {
        int64_t total_value = 0;
        for(int64_t i = 0; i < 64; i++)
        {
          total_value += value_[i].value();
        }
        return total_value;
      }
      void add(int64_t x)
      {
        int64_t idx = get_idx(x);
        count_[idx].inc(1);
        value_[idx].inc(x);
      }
      int64_t to_string(char* buf, const int64_t len) const;
      TCCounter count_[64];
      TCCounter value_[64];
      const char* metric_name_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_TSI_UTILS_H__ */
