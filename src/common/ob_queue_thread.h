////===================================================================
 //
 // ob_queue_thread.h common / Oceanbase
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

#ifndef  OCEANBASE_COMMON_QUEUE_THREAD_H_
#define  OCEANBASE_COMMON_QUEUE_THREAD_H_
#include <sys/epoll.h>
#include "common/ob_define.h"
#include "common/ob_fixed_queue.h"
#include "common/page_arena.h"
#include "common/ob_spin_rwlock.h"
#include "common/ob_seq_queue.h"
#include "common/ob_balance_filter.h"
#include "priority_packet_queue_thread.h"
#include "ob_priority_scheduler.h"

#define CPU_CACHE_LINE 64

namespace oceanbase
{
  namespace common 
  {
    class ObCond
    {
      static const int64_t SPIN_WAIT_NUM = 0;
      static const int64_t BUSY_INTERVAL = 1000;
      public:
        ObCond(const int64_t spin_wait_num = SPIN_WAIT_NUM);
        ~ObCond();
      public:
        void signal();
        int timedwait(const int64_t time_us);
      private:
        const int64_t spin_wait_num_;
        volatile bool bcond_;
        int64_t last_waked_time_;
        pthread_cond_t cond_;
        pthread_mutex_t mutex_;
    } __attribute__ ((aligned (64)));

    typedef ObCond S2MCond;
    class S2MQueueThread
    {
      struct ThreadConf
      {
        pthread_t pd;
        uint64_t index;
        volatile bool run_flag;
        volatile bool stop_flag;
        S2MCond queue_cond;
        volatile bool using_flag;
        volatile int64_t last_active_time;
        ObFixedQueue<void> high_prio_task_queue;
        ObFixedQueue<void> spec_task_queue;
        ObFixedQueue<void> comm_task_queue;
        ObFixedQueue<void> low_prio_task_queue;
        ObPriorityScheduler scheduler_;
        S2MQueueThread *host;
        ThreadConf() : pd(0),
                       index(0),
                       run_flag(true),
                       stop_flag(false),
                       queue_cond(),
                       using_flag(false),
                       last_active_time(0),
                       spec_task_queue(),
                       comm_task_queue(),
                       host(NULL)
        {
        };
      } __attribute__ ((aligned (CPU_CACHE_LINE)));
      static const int64_t THREAD_BUSY_TIME_LIMIT = 10 * 1000;
      static const int64_t QUEUE_WAIT_TIME = 100 * 1000;
      static const int64_t MAX_THREAD_NUM = 256;
      static const int64_t QUEUE_SIZE_TO_SWITCH = 4;
      public:
        S2MQueueThread();
        virtual ~S2MQueueThread();
      public:
        int init(const int64_t thread_num, const int64_t task_num_limit, const bool queue_rebalance, const bool dynamic_rebalance);
        int set_prio_quota(v4si& quota);
        void destroy();
        int64_t get_queued_num() const;
        int64_t get_high_prio_queued_num() { return high_prio_queued_num_; }
        int64_t get_normal_prio_queued_num() { return normal_prio_queued_num_; }
        int64_t get_hotspot_queued_num() { return hotspot_queued_num_; }
        int64_t get_low_prio_queued_num() { return low_prio_queued_num_; }
        int add_thread(const int64_t thread_num, const int64_t task_num_limit);
        int sub_thread(const int64_t thread_num);
        int64_t get_thread_num() const {return thread_num_;};
      public:
        int push(void *task, const int64_t prio = PriorityPacketQueueThread::NORMAL_PRIV);
        int push(void *task, const uint64_t task_sign, const int64_t prio);
        int push_low_prio(void *task);
        int64_t &thread_index();
        int64_t get_thread_index() const;
        virtual void handle(void *task, void *pdata) = 0;
        virtual void handle_with_stopflag(void *task, void *pdata, volatile bool &stop_flag)
        {handle(task, pdata); if (stop_flag) {}};
        virtual void *on_begin() {return NULL;};
        virtual void on_end(void *ptr) {UNUSED(ptr);};
      private:
        void *rebalance_(const ThreadConf &cur_thread);
        int launch_thread_(const int64_t thread_num, const int64_t task_num_limit);
        static void *thread_func_(void *data);
      private:
        int64_t thread_num_;
        volatile uint64_t thread_conf_iter_;
        SpinRWLock thread_conf_lock_;
        ThreadConf thread_conf_array_[MAX_THREAD_NUM];
        volatile int64_t queued_num_ CACHE_ALIGNED;
        volatile int64_t high_prio_queued_num_ CACHE_ALIGNED;
        volatile int64_t normal_prio_queued_num_ CACHE_ALIGNED; 
        volatile int64_t hotspot_queued_num_ CACHE_ALIGNED;
        volatile int64_t low_prio_queued_num_ CACHE_ALIGNED;
        bool queue_rebalance_;
        ObBalanceFilter balance_filter_;
    };

    typedef S2MCond M2SCond;
    class M2SQueueThread
    {
      static const int64_t QUEUE_WAIT_TIME;
      public:
        M2SQueueThread();
        virtual ~M2SQueueThread();
      public:
        int init(const int64_t task_num_limit,
                const int64_t idle_interval);
        void destroy();
      public:
        int push(void *task);
        int64_t get_queued_num() const;
        virtual void handle(void *task, void *pdata) = 0;
        virtual void *on_begin() {return NULL;};
        virtual void on_end(void *ptr) {UNUSED(ptr);};
        virtual void on_idle() {};
      private:
        static void *thread_func_(void *data);
      private:
        bool inited_;
        pthread_t pd_;
        volatile bool run_flag_;
        M2SCond queue_cond_;
        ObFixedQueue<void> task_queue_;
        int64_t idle_interval_;
        int64_t last_idle_time_;
    } __attribute__ ((aligned (CPU_CACHE_LINE)));

    class SeqQueueThread
    {
      static const int64_t QUEUE_WAIT_TIME = 10 * 1000;
      public:
        SeqQueueThread();
        virtual ~SeqQueueThread();
      public:
        int init(const int64_t task_num_limit,
                const int64_t idle_interval);
        void destroy();
      public:
        virtual int push(void *task);
        int64_t get_queued_num() const;
        virtual void handle(void *task, void *pdata) = 0;
        virtual void on_push_fail(void *ptr) {UNUSED(ptr);};
        virtual void *on_begin() {return NULL;};
        virtual void on_end(void *ptr) {UNUSED(ptr);};
        virtual void on_idle() {};
        virtual int64_t get_seq(void* task) = 0;
      private:
        static void *thread_func_(void *data);
      protected:
        ObSeqQueue task_queue_;
      private:
        bool inited_;
        pthread_t pd_;
        volatile bool run_flag_;
        int64_t idle_interval_;
        int64_t last_idle_time_;
    } __attribute__ ((aligned (CPU_CACHE_LINE)));
  }
}

#endif //OCEANBASE_COMMON_QUEUE_THREAD_H_

