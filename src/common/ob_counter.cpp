/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_counter.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_counter.h"
#include "tblog.h"
#include "tbsys.h"
#include "threadmutex.h"
#include "ob_define.h"
#include "common/ob_atomic.h"

using namespace oceanbase;
using namespace oceanbase::common;

oceanbase::common::ObCounter::ObCounter(const int64_t interval_us)
{
  clear();
  static_interval_us_ = interval_us;
  if (static_interval_us_ <= 0)
  {
    static_interval_us_ = DEFAULT_STATIC_INTERVAL_US;
  }
  intervals_[cur_idx_].interval_beg_ =   tbsys::CTimeUtil::getTime();
  pthread_spin_init(&spin_lock_, PTHREAD_PROCESS_PRIVATE);
}

oceanbase::common::ObCounter::~ObCounter()
{
  pthread_spin_destroy(&spin_lock_);
}


void oceanbase::common::ObCounter::clear()
{
  cur_idx_ = 0;
  static_interval_us_ = 0;
  passed_interval_count_ = 0;
  memset(intervals_,0x00, sizeof(intervals_));
}

void oceanbase::common::ObCounter::set_static_interval(const int64_t interval_us)
{
  clear();
  static_interval_us_ = interval_us;
  if (static_interval_us_ <= 0)
  {
    static_interval_us_ = DEFAULT_STATIC_INTERVAL_US;
  }
  intervals_[cur_idx_].interval_beg_ =   tbsys::CTimeUtil::getTime();
}

int64_t oceanbase::common::ObCounter::get_static_interval()const
{
  return static_interval_us_;
}


int32_t oceanbase::common::ObCounter::get_cur_idx(int64_t &now)
{
  now = tbsys::CTimeUtil::getTime();
  int64_t current = atomic_add((volatile uint64_t*)&cur_idx_,0);
  if (now - intervals_[current].interval_beg_ > static_interval_us_)
  {
    pthread_spin_lock(&spin_lock_);
    current = atomic_add((volatile uint64_t*)&cur_idx_,0);
    if (now - intervals_[current].interval_beg_ > static_interval_us_)
    {
      cur_idx_ = (cur_idx_ + 1)%MAX_COUNTER_STATIC_INTERVAL;
      __asm__ __volatile__("" : : : "memory");
      current = cur_idx_;
      intervals_[current].count_ = 0;
      intervals_[current].interval_beg_ = now;
      intervals_[current].time_used_us_ = 0;
      passed_interval_count_ ++;
    }
    pthread_spin_unlock(&spin_lock_);
  }
  return static_cast<int32_t>(current);
}

void oceanbase::common::ObCounter::inc(const int64_t timeused_us)
{
  int64_t now = 0;
  int64_t current = get_cur_idx(now);
  atomic_inc((uint64_t*)&(intervals_[current].count_));
  if (timeused_us > 0)
  {
    atomic_add((uint64_t*)&(intervals_[current].time_used_us_), timeused_us);
  }
}


void oceanbase::common::ObCounter::get_avg(const int64_t interval_count, int64_t &avg_count, int64_t &avg_time_used)
{
  avg_count = 0;
  avg_time_used = 0;
  if (interval_count > 0)
  {
    int64_t now = 0;
    /// @note do not change the order of the next tow lines
    int64_t max_interval_count =passed_interval_count_ + 1; 
    int64_t current = get_cur_idx(now);
    int64_t total_count = 0;
    int64_t total_time_used = 0;
    int64_t got_interval_count = 0;
    int64_t total_static_interval_us = interval_count*static_interval_us_;
    int64_t earliest_interval_beg_time = 0;
    for (; 
      (got_interval_count < interval_count) 
      && (got_interval_count < max_interval_count)
      && (now - intervals_[current].interval_beg_ < total_static_interval_us); 
      got_interval_count++)
    {
      total_count += intervals_[current].count_;
      total_time_used += intervals_[current].time_used_us_;
      earliest_interval_beg_time = intervals_[current].interval_beg_;
      current = current - 1;
      if (current < 0)
      {
        current = MAX_COUNTER_STATIC_INTERVAL - 1;
      }
    }
    if ((got_interval_count > 0) && (total_count > 0))
    {
      double passed_interval_count = 1; 
      if (got_interval_count > 1)
      {
        passed_interval_count = (static_cast<double>(now - earliest_interval_beg_time))/static_cast<double>(static_interval_us_);
      }
      avg_count = static_cast<int64_t>(static_cast<double>(total_count)/passed_interval_count);
      avg_time_used = total_time_used/total_count;
    }
  }
}

int64_t oceanbase::common::ObCounter::get_avg_count(const int64_t interval_count)
{
  int64_t avg_count = 0;
  int64_t avg_time_used = 0;
  get_avg(interval_count,avg_count,avg_time_used);
  return avg_count;
}

int64_t oceanbase::common::ObCounter::get_avg_time_used(const int64_t interval_count)
{
  int64_t avg_count = 0;
  int64_t avg_time_used = 0;
  get_avg(interval_count,avg_count,avg_time_used);
  return avg_time_used;
}

int oceanbase::common::ObCounterSet::init(const ObCounterInfos &counter_infos)
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (counter_infos.get_max_counter_size() > MAX_COUNTER_SIZE))
  {
    TBSYS_LOG(WARN,"there are two many counters in counter info [counter_infos.get_max_counter_size():%ld,"
      "max_allowable_counter_size:%ld]", counter_infos.get_max_counter_size(), MAX_COUNTER_SIZE);
  }
  if (OB_SUCCESS == err)
  {
    infos_ = &counter_infos;
    new(&counters_) ObCounter[MAX_COUNTER_SIZE];
    for (int64_t c_idx = 0; c_idx < infos_->get_max_counter_size(); c_idx++)
    {
      counters_[c_idx].set_static_interval(infos_->get_counter_static_interval(c_idx));
    }
  }
  return err;
}

void oceanbase::common::ObCounterSet::inc(const int64_t counter_id, const int64_t time_used_us)
{
  int err = OB_SUCCESS;
  if (NULL == infos_)
  {
    TBSYS_LOG(WARN,"counter set has not been initialized yet");
    err = OB_INVALID_ARGUMENT;
  }
  else if ((counter_id < 0) || (counter_id >= MAX_COUNTER_SIZE) || (counter_id >= infos_->get_max_counter_size()))
  {
    TBSYS_LOG(WARN,"counter_id out of range [counter_id:%ld,infos_->get_max_counter_size():%ld,max_counter_size:%ld]", 
      counter_id, infos_->get_max_counter_size(), MAX_COUNTER_SIZE);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    counters_[counter_id].inc(time_used_us);
  }
}

int64_t oceanbase::common::ObCounterSet::get_avg_count(const int64_t counter_id, const int64_t interval_count)
{
  int err = OB_SUCCESS;
  int res = 0;
  if (NULL == infos_)
  {
    TBSYS_LOG(WARN,"counter set has not been initialized yet");
    err = OB_INVALID_ARGUMENT;
  }
  else if ((counter_id < 0) || (counter_id >= MAX_COUNTER_SIZE) || (counter_id >= infos_->get_max_counter_size()))
  {
    TBSYS_LOG(WARN,"counter_id out of range [counter_id:%ld,infos_->get_max_counter_size():%ld,max_counter_size:%ld]", 
      counter_id, infos_->get_max_counter_size(), MAX_COUNTER_SIZE);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    res = static_cast<int32_t>(counters_[counter_id].get_avg_count(interval_count));
  }
  return res;
}

int64_t oceanbase::common::ObCounterSet::get_avg_time_used(const int64_t counter_id, const int64_t interval_count)
{
  int err = OB_SUCCESS;
  int res = 0;
  if (NULL == infos_)
  {
    TBSYS_LOG(WARN,"counter set has not been initialized yet");
    err = OB_INVALID_ARGUMENT;
  }
  else if ((counter_id < 0) || (counter_id >= MAX_COUNTER_SIZE) || (counter_id >= infos_->get_max_counter_size()))
  {
    TBSYS_LOG(WARN,"counter_id out of range [counter_id:%ld,infos_->get_max_counter_size():%ld,max_counter_size:%ld]", 
      counter_id, infos_->get_max_counter_size(), MAX_COUNTER_SIZE);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    res = static_cast<int32_t>(counters_[counter_id].get_avg_time_used(interval_count));
  }
  return res;
}


int64_t oceanbase::common::ObCounterSet::get_static_interval(const int64_t counter_id)
{
  int err = OB_SUCCESS;
  int res = 0;
  if (NULL == infos_)
  {
    TBSYS_LOG(WARN,"counter set has not been initialized yet");
    err = OB_INVALID_ARGUMENT;
  }
  else if ((counter_id < 0) || (counter_id >= MAX_COUNTER_SIZE) || (counter_id >= infos_->get_max_counter_size()))
  {
    TBSYS_LOG(WARN,"counter_id out of range [counter_id:%ld,infos_->get_max_counter_size():%ld,max_counter_size:%ld]", 
      counter_id, infos_->get_max_counter_size(), MAX_COUNTER_SIZE);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    res = static_cast<int32_t>(counters_[counter_id].get_static_interval());
  }
  return res;
}

void oceanbase::common::ObCounterSet::print_static_info(const int64_t log_level )
{
  static int64_t interval_array[] = {1, 3, 5, 10};
  int64_t level = log_level;
  if (level < std::min<int>((TBSYS_LOG_LEVEL_ERROR), (TBSYS_LOG_LEVEL_DEBUG)))
  {
    level = std::min<int>((TBSYS_LOG_LEVEL_ERROR), (TBSYS_LOG_LEVEL_DEBUG));
  }
  if (level > std::max<int>((TBSYS_LOG_LEVEL_ERROR), (TBSYS_LOG_LEVEL_DEBUG)))
  {
    level = std::max<int>((TBSYS_LOG_LEVEL_ERROR), (TBSYS_LOG_LEVEL_DEBUG));
  }

  for (int64_t c_idx = 0; c_idx < infos_->get_max_counter_size(); c_idx++)
  {
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(static_cast<int32_t>(level)), 
      "[%ld][counter_name:%s][static_interval_us:%ld][#interval:%ld,avg_count:%ld,avg_time_used:%ld]"
      "[#interval:%ld,avg_count:%ld,avg_time_used:%ld][#interval:%ld,avg_count:%ld,avg_time_used:%ld]",
      pthread_self(), infos_->get_counter_name(c_idx), infos_->get_counter_static_interval(c_idx), 
      interval_array[0], 
      counters_[c_idx].get_avg_count(interval_array[0]), 
      counters_[c_idx].get_avg_time_used(interval_array[0]),
      interval_array[1], 
      counters_[c_idx].get_avg_count(interval_array[1]), 
      counters_[c_idx].get_avg_time_used(interval_array[1]),
      interval_array[2], 
      counters_[c_idx].get_avg_count(interval_array[2]), 
      counters_[c_idx].get_avg_time_used(interval_array[2])
      );
  }
}
