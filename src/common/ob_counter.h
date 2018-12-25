/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_counter.h for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef COMMON_OB_COUNTER_H_ 
#define COMMON_OB_COUNTER_H_
#include "ob_define.h"
#include "tbsys.h"
#include "threadmutex.h"
#include <pthread.h>
namespace oceanbase
{
  namespace common
  {
    class ObCounter
    {
    public:
      static const int64_t DEFAULT_STATIC_INTERVAL_US = 1000000;
      ObCounter(const int64_t interval_us = DEFAULT_STATIC_INTERVAL_US);
      ~ObCounter();


      void set_static_interval(const int64_t interval_us = DEFAULT_STATIC_INTERVAL_US);
      int64_t get_static_interval()const;
      /// increate counter by one and return the value after inc
      void inc(const int64_t timeused_us = 0);

      /// get avg counter of  the every last interval_count intervals
      int64_t get_avg_count(const int64_t interval_count);
      /// get avg process time in us of  every count happened in the last interval_count intervals
      int64_t get_avg_time_used(const int64_t interval_count);
      void get_avg(const int64_t interval_count, int64_t &avg_count, int64_t &avg_time_used);



      /// static most MAX_COUNTER_STATIC_INTERVAL intervals
      static const int64_t MAX_COUNTER_STATIC_INTERVAL = 4096;
    private:
      int32_t get_cur_idx(int64_t &now);
      void clear();
      struct interval_count_info
      {
        int64_t count_;
        int64_t time_used_us_;
        int64_t interval_beg_;
      };
      interval_count_info intervals_[MAX_COUNTER_STATIC_INTERVAL];
      int64_t cur_idx_;
      int64_t static_interval_us_;
      int64_t passed_interval_count_;
      pthread_spinlock_t spin_lock_;
    };

    class ObCounterInfos
    {
    public:
      virtual ~ObCounterInfos()
      {
      }
      virtual const char * get_counter_name(const int64_t counter_id) const = 0;
      virtual int64_t get_counter_static_interval(const int64_t counter_id) const = 0;
      virtual int64_t get_max_counter_size()const = 0;
    };

    class ObCounterSet
    {
    public:
      ObCounterSet()
      {
      }
      ~ObCounterSet()
      {
      }

      int init(const ObCounterInfos &counter_infos);

      void inc(const int64_t counter_id, const int64_t time_used_us = 0);

      /// get avg counter of  the every last interval_count intervals
      int64_t get_avg_count(const int64_t counter_id, const int64_t interval_count);
      /// get avg process time in us of  every count happened in the last interval_count intervals
      int64_t get_avg_time_used(const int64_t counter_id, const int64_t interval_count);

      int64_t get_static_interval(const int64_t counter_id);

      /// print static info of every counter [static_interval,avg_counter_of_last_one_interval, avg_process_time_of_last_one_interval,
      /// avg_counter_of_last_three_interval, avg_process_time_of_last_three_interval,
      /// avg_counter_of_last_five_interval, avg_process_time_of_last_five_interval,
      /// avg_counter_of_last_ten_interval, avg_process_time_of_last_ten_interval]
      void print_static_info(const int64_t log_level);

      /// at most can static 1024 counters
      static const int64_t MAX_COUNTER_SIZE = 1024;
    private:
      const ObCounterInfos *infos_;
      ObCounter counters_[MAX_COUNTER_SIZE];
    };
  }
}  
#endif /* COMMON_OB_COUNTER_H_ */
