////===================================================================
 //
 // ob_queue_thread.cpp common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-09-01 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include <errno.h>
#include "common/ob_trace_log.h"
#include "priority_packet_queue_thread.h"
#include "ob_queue_thread.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    ObCond::ObCond(const int64_t spin_wait_num) : spin_wait_num_(spin_wait_num),
                                                  bcond_(false),
                                                  last_waked_time_(0)
    {
      pthread_mutex_init(&mutex_, NULL);
      pthread_cond_init(&cond_, NULL);
    }

    ObCond::~ObCond()
    {
      pthread_mutex_destroy(&mutex_);
      pthread_cond_destroy(&cond_);
    }

    void ObCond::signal()
    {
      if (false == ATOMIC_CAS(&bcond_, false, true))
      {
        __sync_synchronize();
        pthread_mutex_lock(&mutex_);
        pthread_cond_signal(&cond_);
        pthread_mutex_unlock(&mutex_);
      }
    }

    int ObCond::timedwait(const int64_t time_us)
    {
      int ret = OB_SUCCESS;
      bool need_wait = true;
      if ((last_waked_time_ + BUSY_INTERVAL) > tbsys::CTimeUtil::getTime())
      {
        //return OB_SUCCESS;
        for (register int64_t i = 0; i < spin_wait_num_; i++)
        {
          if (true == ATOMIC_CAS(&bcond_, true, false))
          {
            need_wait = false;
            break;
          }
          asm("pause");
        }
      }
      if (need_wait)
      {
        int64_t abs_time = time_us + tbsys::CTimeUtil::getTime();
        struct timespec ts;
        ts.tv_sec = abs_time / 1000000;
        ts.tv_nsec = (abs_time % 1000000) * 1000;
        pthread_mutex_lock(&mutex_);
        while (false == ATOMIC_CAS(&bcond_, true, false))
        {
          int tmp_ret = pthread_cond_timedwait(&cond_, &mutex_, &ts);
          if (ETIMEDOUT == tmp_ret)
          {
            ret = OB_PROCESS_TIMEOUT;
            break;
          }
        }
        pthread_mutex_unlock(&mutex_);
      }
      if (OB_SUCCESS == ret)
      {
        last_waked_time_ = tbsys::CTimeUtil::getTime();
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    S2MQueueThread::S2MQueueThread() : thread_num_(0),
                                       thread_conf_iter_(0),
                                       thread_conf_lock_(),
                                       queued_num_(0),
                                       high_prio_queued_num_(0),
                                       normal_prio_queued_num_(0),
                                       hotspot_queued_num_(0),
                                       low_prio_queued_num_(0),
                                       queue_rebalance_(false)
    {
    }

    S2MQueueThread::~S2MQueueThread()
    {
      destroy();
    }

    int S2MQueueThread::init(const int64_t thread_num, const int64_t task_num_limit, const bool queue_rebalance, const bool dynamic_rebalance)
    {
      int ret = OB_SUCCESS;
      queue_rebalance_ = queue_rebalance;
      if (0 != thread_num_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= thread_num
              || MAX_THREAD_NUM < thread_num
              || 0 >= task_num_limit)
      {
        TBSYS_LOG(WARN, "invalid param, thread_num=%ld task_num_limit=%ld",
                  thread_num, task_num_limit);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = launch_thread_(thread_num, task_num_limit)))
      {
        TBSYS_LOG(WARN, "launch thread fail, ret=%d thread_num=%ld task_num_limit=%ld", ret, thread_num, task_num_limit);
      }
      else if (OB_SUCCESS != (ret = balance_filter_.init(thread_num_, dynamic_rebalance)))
      {
        TBSYS_LOG(WARN, "init balance_filter fail, ret=%d thread_num=%ld", ret, thread_num_);
      }
      else
      {
        // do nothing
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      else
      {
        thread_conf_iter_ = 0;
      }
      return ret;
    }

    void S2MQueueThread::destroy()
    {
      balance_filter_.destroy();
      for (int64_t i = 0; i < thread_num_; i++)
      {
        ThreadConf &tc = thread_conf_array_[i];
        tc.run_flag = false;
        tc.stop_flag = true;
        tc.queue_cond.signal();
        pthread_join(tc.pd, NULL);
        tc.high_prio_task_queue.destroy();
        tc.spec_task_queue.destroy();
        tc.comm_task_queue.destroy();
        tc.low_prio_task_queue.destroy();
      }
      thread_num_ = 0;
    }

    int64_t S2MQueueThread::get_queued_num() const
    {
      return queued_num_;
    }

    int S2MQueueThread::push(void *task, const int64_t prio)
    {
      int ret = OB_SUCCESS;
      if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        SpinRLockGuard guard(thread_conf_lock_);
        uint64_t i = 0;
        if (QUEUE_SIZE_TO_SWITCH < thread_conf_array_[thread_conf_iter_ % thread_num_].comm_task_queue.get_total())
        {
          i = ATOMIC_INC(&thread_conf_iter_);
        }
        else
        {
          i = thread_conf_iter_;
        }
        ThreadConf &tc = thread_conf_array_[i % thread_num_];
        switch(prio)
        {
          case PriorityPacketQueueThread::HIGH_PRIV:
            ret = tc.high_prio_task_queue.push(task);
            break;
          case PriorityPacketQueueThread::NORMAL_PRIV:
            ret = tc.comm_task_queue.push(task);
            break;
          case PriorityPacketQueueThread::LOW_PRIV:
            ret = tc.low_prio_task_queue.push(task);
            break;
          default:
            ret = OB_NOT_SUPPORTED;
        }
        if (OB_SUCCESS == ret)
        {
          tc.queue_cond.signal();
        }
        if (OB_SIZE_OVERFLOW == ret)
        {
          ret = OB_EAGAIN;
        }
      }
      if (OB_SUCCESS == ret)
      {
        ATOMIC_ADD(&queued_num_, 1);
        switch(prio)
        {
          case PriorityPacketQueueThread::HIGH_PRIV:
            ATOMIC_ADD(&high_prio_queued_num_, 1);
            break;
          case PriorityPacketQueueThread::NORMAL_PRIV:
            ATOMIC_ADD(&normal_prio_queued_num_, 1);
            break;
          case PriorityPacketQueueThread::LOW_PRIV:
            ATOMIC_ADD(&low_prio_queued_num_, 1);
            break;
          default:
            break;
        }
      }
      return ret;
    }

    int S2MQueueThread::push(void *task, const uint64_t task_sign, const int64_t prio)
    {
      int ret = OB_SUCCESS;
      if (0 == task_sign
          || OB_INVALID_ID == task_sign)
      {
        ret = push(task, prio);
      }
      else if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        uint64_t filted_task_sign = balance_filter_.filt(task_sign);
        SpinRLockGuard guard(thread_conf_lock_);
        ThreadConf &tc = thread_conf_array_[filted_task_sign % thread_num_];
        TBSYS_TRACE_LOG("req_sign=%lu,%lu hash=%lu", filted_task_sign % thread_num_, filted_task_sign, task_sign);
        if (OB_SUCCESS != (ret = tc.spec_task_queue.push(task)))
        {
          //TBSYS_LOG(WARN, "push task to queue fail, ret=%d", ret);
          if (OB_SIZE_OVERFLOW == ret)
          {
            ret = OB_EAGAIN;
          }
        }
        else
        {
          tc.queue_cond.signal();
          ATOMIC_ADD(&queued_num_, 1);
          ATOMIC_ADD(&hotspot_queued_num_, 1);
        }
      }
      return ret;
    }

    int S2MQueueThread::set_prio_quota(v4si& quota)
    {
      int ret = OB_SUCCESS;
      if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SpinWLockGuard guard(thread_conf_lock_);
        for (int64_t i = 0; i < thread_num_; i++)
        {
          ThreadConf &tc = thread_conf_array_[i];
          tc.scheduler_.set_quota(quota);
        }
      }
      return ret;
    }

    int S2MQueueThread::add_thread(const int64_t thread_num, const int64_t task_num_limit)
    {
      int ret = OB_SUCCESS;
      if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else if (0 >= thread_num)
      {
        TBSYS_LOG(WARN, "invalid param, thread_num=%ld", thread_num);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        SpinWLockGuard guard(thread_conf_lock_);
        int64_t prev_thread_num = thread_num_;
        ret = launch_thread_(thread_num, task_num_limit);
        balance_filter_.add_thread(thread_num_ - prev_thread_num);
      }
      return ret;
    }

    int S2MQueueThread::sub_thread(const int64_t thread_num)
    {
      int ret = OB_SUCCESS;
      int64_t prev_thread_num = 0;
      int64_t cur_thread_num = 0;
      if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SpinWLockGuard guard(thread_conf_lock_);
        if (0 >= thread_num
            || thread_num >= thread_num_)
        {
          TBSYS_LOG(WARN, "invalid param, thread_num=%ld thread_num_=%ld",
                    thread_num, thread_num_);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          prev_thread_num = thread_num_;
          thread_num_ -= thread_num;
          cur_thread_num = thread_num_;
        }
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = cur_thread_num; i < prev_thread_num; i++)
        {
          balance_filter_.sub_thread(1);
          ThreadConf &tc = thread_conf_array_[i];
          tc.run_flag = false;
          tc.stop_flag = true;
          tc.queue_cond.signal();
          pthread_join(tc.pd, NULL);
          tc.high_prio_task_queue.destroy();
          tc.spec_task_queue.destroy();
          tc.comm_task_queue.destroy();
          tc.low_prio_task_queue.destroy();
          TBSYS_LOG(INFO, "join thread succ, index=%ld", i);
        }
      }
      return ret;
    }

    void *S2MQueueThread::rebalance_(const ThreadConf &cur_thread)
    {
      void *ret = NULL;
      static __thread int64_t rebalance_counter = 0;
      for (uint64_t i = 1; (int64_t)i <= thread_num_; i++)
      {
        SpinRLockGuard guard(thread_conf_lock_);
        uint64_t balance_idx = (cur_thread.index + i) % thread_num_;
        ThreadConf &tc = thread_conf_array_[balance_idx];
        if (!tc.using_flag
           )//&& (tc.last_active_time + THREAD_BUSY_TIME_LIMIT) < tbsys::CTimeUtil::getTime())
        {
          tc.comm_task_queue.pop(ret);
        }
        if (NULL != ret)
        {
          ATOMIC_ADD(&normal_prio_queued_num_, -1);
          if (0 == (rebalance_counter++ % 10000))
          {
            TBSYS_LOG(INFO, "task has been rebalance between threads rebalance_counter=%ld cur_idx=%ld balance_idx=%ld", rebalance_counter, cur_thread.index, balance_idx);
          }
          break;
        }
      }
      return ret;
    }

    int S2MQueueThread::launch_thread_(const int64_t thread_num, const int64_t task_num_limit)
    {
      int ret = OB_SUCCESS;
      int64_t thread_num2launch = std::min(MAX_THREAD_NUM - thread_num_, thread_num);
      for (int64_t i = 0; i < thread_num2launch; i++)
      {
        ThreadConf &tc = thread_conf_array_[thread_num_];
        tc.index = thread_num_;
        tc.run_flag = true;
        tc.stop_flag = false;
        tc.using_flag = false;
        tc.last_active_time = 0;
        tc.host = this;
        if (OB_SUCCESS != (ret = tc.high_prio_task_queue.init(task_num_limit)))
        {
          TBSYS_LOG(WARN, "high prio task queue init fail, task_num_limit=%ld", task_num_limit);
          break;
        }
        if (OB_SUCCESS != (ret = tc.spec_task_queue.init(task_num_limit)))
        {
          TBSYS_LOG(WARN, "spec task queue init fail, task_num_limit=%ld", task_num_limit);
          break;
        }
        if (OB_SUCCESS != (ret = tc.comm_task_queue.init(task_num_limit)))
        {
          TBSYS_LOG(WARN, "comm task queue init fail, task_num_limit=%ld", task_num_limit);
          break;
        }
        if (OB_SUCCESS != (ret = tc.low_prio_task_queue.init(task_num_limit)))
        {
          TBSYS_LOG(WARN, "low prio task queue init fail, task_num_limit=%ld", task_num_limit);
          break;
        }
        int tmp_ret = 0;
        if (0 != (tmp_ret = pthread_create(&(tc.pd), NULL, thread_func_, &tc)))
        {
          TBSYS_LOG(WARN, "pthread_create fail, ret=%d", tmp_ret);
          ret = OB_ERR_UNEXPECTED;
          tc.high_prio_task_queue.destroy();
          tc.spec_task_queue.destroy();
          tc.comm_task_queue.destroy();
          tc.low_prio_task_queue.destroy();
          break;
        }
        //cpu_set_t cpu_set;
        //CPU_ZERO(&cpu_set);
        //CPU_SET(thread_num_ % get_cpu_num(), &cpu_set);
        //tmp_ret = pthread_setaffinity_np(tc.pd, sizeof(cpu_set), &cpu_set);
        TBSYS_LOG(INFO, "create thread succ, index=%ld tmp_ret=%d", thread_num_, tmp_ret);
        thread_num_ += 1;
      }
      return ret;
    }

    void *S2MQueueThread::thread_func_(void *data)
    {
      ThreadConf *const tc = (ThreadConf*)data;
      if (NULL == tc
          || NULL == tc->host)
      {
        TBSYS_LOG(WARN, "thread_func param null pointer");
      }
      else
      {
        tc->host->thread_index() = tc->index;
        void *pdata = tc->host->on_begin();
        bool last_rebalance_got = false;
        while (tc->run_flag
              || 0 != tc->high_prio_task_queue.get_total()
              || 0 != tc->spec_task_queue.get_total()
              || 0 != tc->comm_task_queue.get_total()
              || 0 != tc->low_prio_task_queue.get_total())
        {
          void *task = NULL;
          int64_t start_time = tbsys::CTimeUtil::getTime();
          int64_t idx = -1;
          tc->using_flag = true;
          switch(tc->scheduler_.get())
          {
            case 0:
              tc->high_prio_task_queue.pop(task);
              if (NULL != task)
              {
                idx = 0;
              }
              break;
            case 1:
              tc->spec_task_queue.pop(task);
              if (NULL != task)
              {
                idx = 1;
              }
              break;
            case 2:
              tc->comm_task_queue.pop(task);
              if (NULL != task)
              {
                idx = 2;
              }
              break;
            case 3:
              tc->low_prio_task_queue.pop(task);
              if (NULL != task)
              {
                idx = 3;
              }
              break;
            default:;
          };
          if (NULL == task)
          {
            tc->high_prio_task_queue.pop(task);
            if (NULL != task)
            {
              idx = 0;
            }
          }
          if (NULL == task)
          {
            tc->spec_task_queue.pop(task);
            if (NULL != task)
            {
              idx = 1;
            }
          }
          if (NULL == task)
          {
            tc->comm_task_queue.pop(task);
            if (NULL != task)
            {
              idx = 2;
            }
          }
          if (NULL == task)
          {
            tc->low_prio_task_queue.pop(task);
            if (NULL != task)
            {
              idx = 3;
            }
          }
          tc->using_flag = false; // not need strict consist, so do not use barrier
          if (NULL != task)
          {
            switch(idx)
            {
              case 0:
                ATOMIC_ADD(&tc->host->high_prio_queued_num_, -1);
                break;
              case 1:
                ATOMIC_ADD(&tc->host->hotspot_queued_num_, -1);
                break;
              case 2:
                ATOMIC_ADD(&tc->host->normal_prio_queued_num_, -1);
                break;
              case 3:
                ATOMIC_ADD(&tc->host->low_prio_queued_num_, -1);
                break;
              default:
                break;
            }
          }
          if (NULL != task
             || (tc->host->queue_rebalance_
                && (last_rebalance_got || TC_REACH_TIME_INTERVAL(QUEUE_WAIT_TIME))
                && (last_rebalance_got = (NULL != (task = tc->host->rebalance_(*tc))))))
          {
            ATOMIC_ADD(&tc->host->queued_num_, -1);
            tc->host->handle_with_stopflag(task, pdata, tc->stop_flag);
            tc->last_active_time = tbsys::CTimeUtil::getTime();
          }
          else
          {
            tc->queue_cond.timedwait(QUEUE_WAIT_TIME);
          }
          v4si queue_len = {
            tc->high_prio_task_queue.get_total(),
            tc->spec_task_queue.get_total(),
            tc->comm_task_queue.get_total(),
            tc->low_prio_task_queue.get_total(),
          };
          tc->scheduler_.update(idx, tbsys::CTimeUtil::getTime() - start_time, queue_len);
        }
        tc->host->on_end(pdata);
      }
      return NULL;
    }

    int64_t &S2MQueueThread::thread_index()
    {
      static __thread int64_t index = 0;
      return index;
    }

    int64_t S2MQueueThread::get_thread_index() const
    {
      int64_t ret = const_cast<S2MQueueThread&>(*this).thread_index();
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    const int64_t M2SQueueThread::QUEUE_WAIT_TIME = 100 * 1000;

    M2SQueueThread::M2SQueueThread() : inited_(false),
                                       pd_(0),
                                       run_flag_(true),
                                       queue_cond_(),
                                       task_queue_(),
                                       idle_interval_(INT64_MAX),
                                       last_idle_time_(0)
    {
    }

    M2SQueueThread::~M2SQueueThread()
    {
      destroy();
    }

    int M2SQueueThread::init(const int64_t task_num_limit,
                            const int64_t idle_interval)
    {
      int ret = OB_SUCCESS;
      int tmp_ret = 0;
      run_flag_ = true;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (ret = task_queue_.init(task_num_limit)))
      {
        TBSYS_LOG(WARN, "task_queue init fail, ret=%d task_num_limit=%ld", ret, task_num_limit);
      }
      else if (0 != (tmp_ret = pthread_create(&pd_, NULL, thread_func_, this)))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "pthread_create fail, ret=%d", tmp_ret);
      }
      else
      {
        inited_ = true;
        idle_interval_ = idle_interval;
        last_idle_time_ = 0;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void M2SQueueThread::destroy()
    {
      if (0 != pd_)
      {
        run_flag_ = false;
        queue_cond_.signal();
        pthread_join(pd_, NULL);
        pd_ = 0;
      }

      task_queue_.destroy();

      inited_ = false;
    }

    int M2SQueueThread::push(void *task)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = task_queue_.push(task)))
        {
          //TBSYS_LOG(WARN, "push task to queue fail, ret=%d", ret);
          if (OB_SIZE_OVERFLOW == ret)
          {
            ret = OB_EAGAIN;
          }
        }
        else
        {
          queue_cond_.signal();
        }
      }
      return ret;
    }

    int64_t M2SQueueThread::get_queued_num() const
    {
      return task_queue_.get_total();
    }

    void *M2SQueueThread::thread_func_(void *data)
    {
      M2SQueueThread *const host = (M2SQueueThread*)data;
      if (NULL == host)
      {
        TBSYS_LOG(WARN, "thread_func param null pointer");
      }
      else
      {
        void *pdata = host->on_begin();
        while (host->run_flag_
              || 0 != host->task_queue_.get_total())
        {
          void *task = NULL;
          host->task_queue_.pop(task);
          if (NULL != task)
          {
            host->handle(task, pdata);
          }
          else if ((host->last_idle_time_ + host->idle_interval_) <= tbsys::CTimeUtil::getTime())
          {
            host->on_idle();
            host->last_idle_time_ = tbsys::CTimeUtil::getTime();
          }
          else
          {
            host->queue_cond_.timedwait(std::min(QUEUE_WAIT_TIME, host->idle_interval_));
          }
        }
        host->on_end(pdata);
      }
      return NULL;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    SeqQueueThread::SeqQueueThread() : inited_(false),
                                       pd_(0),
                                       run_flag_(true),
                                       idle_interval_(INT64_MAX),
                                       last_idle_time_(0)
    {
    }

    SeqQueueThread::~SeqQueueThread()
    {
      destroy();
    }

    int SeqQueueThread::init(const int64_t task_num_limit,
                            const int64_t idle_interval)
    {
      int ret = OB_SUCCESS;
      int tmp_ret = 0;
      run_flag_ = true;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (ret = task_queue_.init(task_num_limit)))
      {
        TBSYS_LOG(WARN, "task_queue init fail, ret=%d task_num_limit=%ld", ret, task_num_limit);
      }
      else if (0 != (ret = task_queue_.start(1)))
      {
        TBSYS_LOG(ERROR, "task_queue_.start(1)=>%d", ret);
      }
      else if (0 != (tmp_ret = pthread_create(&pd_, NULL, thread_func_, this)))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "pthread_create fail, ret=%d", tmp_ret);
      }
      else
      {
        inited_ = true;
        idle_interval_ = idle_interval;
        last_idle_time_ = 0;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void SeqQueueThread::destroy()
    {
      if (0 != pd_)
      {
        run_flag_ = false;
        pthread_join(pd_, NULL);
        pd_ = 0;
      }

      inited_ = false;
    }

    int SeqQueueThread::push(void *task)
    {
      int ret = OB_SUCCESS;
      int64_t seq = 0;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= (seq = get_seq(task)))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get_seq(task[%p])>%ld", task, seq);
      }
      else
      {
        ret = task_queue_.add(seq, task);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "task_queue_.add(seq=%ld, task=%p)=>%d", seq, task, ret);
        }
      }
      if (OB_SUCCESS != ret)
      {
        on_push_fail(task);
      }
      return ret;
    }

    int64_t SeqQueueThread::get_queued_num() const
    {
      return task_queue_.next_is_ready()? 1: 0;
    }

    void *SeqQueueThread::thread_func_(void *data)
    {
      int err = OB_SUCCESS;
      SeqQueueThread *const host = (SeqQueueThread*)data;
      if (NULL == host)
      {
        TBSYS_LOG(WARN, "thread_func param null pointer");
      }
      else
      {
        void *pdata = host->on_begin();
        int64_t seq = 0;
        while (host->run_flag_)
        {
          void *task = NULL;
          if (OB_SUCCESS != (err = host->task_queue_.get(seq, task, QUEUE_WAIT_TIME))
            && OB_EAGAIN != err)
          {
            TBSYS_LOG(ERROR, "get(seq=%ld)=>%d", seq, err);
            break;
          }
          else if (OB_SUCCESS == err)
          {
            host->handle(task, pdata);
          }
          else if ((host->last_idle_time_ + host->idle_interval_) <= tbsys::CTimeUtil::getTime())
          {
            host->on_idle();
            host->last_idle_time_ = tbsys::CTimeUtil::getTime();
          }
        }
        host->on_end(pdata);
      }
      return NULL;
    }
  }
}

