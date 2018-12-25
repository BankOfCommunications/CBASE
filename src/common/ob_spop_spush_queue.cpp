/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_spop_spush_queue.cpp for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */

#include "ob_spop_spush_queue.h"
#include "ob_define.h"
#include "ob_malloc.h"
using namespace oceanbase;
using namespace oceanbase::common;

oceanbase::common::ObSPopSPushQueue::ObSPopSPushQueue()
{
  push_count_ = 0;
  pop_count_ = 0;
  push_idx_ = 0;
  pop_idx_ = 0;
  queue_size_ = 0;
  queue_arr_ = NULL;
  inited_ = false;
  waiter_exist_ = false;
  pthread_mutex_init(&mutex_,NULL);
  pthread_cond_init(&cond_, NULL) ;
  pthread_mutex_init(&push_lock_,NULL);
}

void oceanbase::common::ObSPopSPushQueue::reset()
{
  waiter_exist_ = false;
  push_count_ = 0;
  pop_count_ = 0;
  push_idx_ = 0;
  pop_idx_ = 0;
}
oceanbase::common::ObSPopSPushQueue::~ObSPopSPushQueue()
{
  inited_ = false;
  waiter_exist_ = false;
  push_count_ = 0;
  pop_count_ = 0;
  push_idx_ = 0;
  pop_idx_ = 0;
  queue_size_ = 0;
  if (NULL != queue_arr_)
  {
    ob_free(queue_arr_);
    queue_arr_ = NULL;
  }
  pthread_mutex_destroy(&mutex_);
  pthread_cond_destroy(&cond_);
  pthread_mutex_destroy(&push_lock_);
}

int oceanbase::common::ObSPopSPushQueue::init(const int64_t queue_size, const int32_t mod_id)
{
  int err = OB_SUCCESS;
  if (queue_size <= 0)
  {
    TBSYS_LOG(WARN,"arg error [queue_size:%ld]", queue_size);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    if (!inited_ || queue_size != queue_size_)
    {
      if (NULL != queue_arr_)
      {
        ob_free(queue_arr_);
        queue_arr_ = NULL;
      }
      if ((NULL == (queue_arr_ = reinterpret_cast<void**>(ob_malloc(queue_size*sizeof(void*),mod_id)))))
      {
        TBSYS_LOG(WARN,"fail to allocate memory for queue array [errno:%d]", errno);
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"[err:%d,queue_size:%ld]", err, queue_size);
      }
      else
      {
        queue_size_ = queue_size;
        inited_ = true;
      }
    }
  }
  return err;
}


int oceanbase::common::ObSPopSPushQueue::push(void *msg)
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (NULL == queue_arr_))
  {
    TBSYS_LOG(WARN,"queue not initialized yet [queue_arr_:%p]",  queue_arr_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (push_count_ - pop_count_ >= static_cast<uint64_t>(queue_size_)))
  {
    TBSYS_LOG(WARN,"queue is full [push_count_:%lu,pop_count_:%lu,queue_size_:%ld]", push_count_, pop_count_, queue_size_);
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  if (OB_SUCCESS == err)
  {
    pthread_mutex_lock(&push_lock_);
    queue_arr_[push_idx_] = msg;
    push_idx_ ++;
    if (push_idx_ == queue_size_)
    {
      push_idx_ = 0;
    }
    push_count_++;
    pthread_mutex_unlock(&push_lock_);

    /// queue is empty
    pthread_mutex_lock(&mutex_);
    if ((pop_count_ + 1 == push_count_) && (waiter_exist_))
    {
      waiter_exist_ = false;
      pthread_cond_signal(&cond_);
    }
    pthread_mutex_unlock(&mutex_);
  }
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN,"[err:%d]", err);
  }
  return err;
}

int oceanbase::common::ObSPopSPushQueue::pop(const int64_t timeout_us, void *&msg)
{
  msg = NULL;
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (NULL == queue_arr_))
  {
    TBSYS_LOG(WARN,"queue not initialized yet [queue_arr_:%p]",  queue_arr_);
    err = OB_INVALID_ARGUMENT;
  }
  struct timespec expired_time;
  if ((OB_SUCCESS == err) && (0 != (errno = clock_gettime(CLOCK_REALTIME, &expired_time))))
  {
    TBSYS_LOG(WARN,"fail to get clock time [errno:%d]", errno);
    err = OB_ERR_SYS;
  }
  if ((OB_SUCCESS == err) && (pop_count_ == push_count_) && (timeout_us > 0))
  {
    expired_time.tv_nsec += 1000 * (timeout_us % 1000000L);
    expired_time.tv_sec += (timeout_us/1000000L + expired_time.tv_nsec/1000000000L);
    expired_time.tv_nsec %= 1000000000L;

    pthread_mutex_lock(&mutex_);
    waiter_exist_ = true;
    while ((pop_count_ == push_count_) && (ETIMEDOUT != (err = pthread_cond_timedwait(&cond_,&mutex_,&expired_time))))
    {
      if ((err != 0) && (err != ETIMEDOUT))
      {
        TBSYS_LOG(ERROR, "check cond timedwait failed:ret[%d], msg[%m]", err);
      }
      waiter_exist_ = true;
    }
    pthread_mutex_unlock(&mutex_);
  }
  uint64_t org_pop_count = pop_count_;
  if ((OB_SUCCESS == err) && (pop_count_ != push_count_))
  {
    msg = queue_arr_[pop_idx_];
    pop_idx_ ++;
    if (queue_size_ == pop_idx_)
    {
      pop_idx_ = 0;
    }
    pop_count_ ++;
  }
  if ((OB_SUCCESS == err) && (org_pop_count == pop_count_))
  {
    msg = NULL;
    TBSYS_LOG(WARN,"queue is empty");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(TRACE,"[pop err:%d]", err);
  }
  return err;
}
