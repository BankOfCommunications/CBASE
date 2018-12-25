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
#ifndef __OB_UPDATESERVER_OB_LOG_REPLAY_WORKER_H__
#define __OB_UPDATESERVER_OB_LOG_REPLAY_WORKER_H__
#include "common/ob_log_entry.h"
#include "common/ob_seq_queue.h"
#include "common/ob_queue_thread.h"
#include "common/ob_fifo_allocator.h"
#include "ob_ups_table_mgr.h"
#include "ob_async_log_applier.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObLogReplayWorker: public tbsys::CDefaultRunnable
    {
      public:
        friend class ApplyWorker;
        class ApplyWorker: public common::S2MQueueThread
        {
          public:
            ApplyWorker(ObLogReplayWorker& replay_worker): replay_worker_(replay_worker),
                                                           affinity_start_cpu_(-1),
                                                           affinity_end_cpu_(-1)
            {}
            virtual ~ApplyWorker()
            {}
            virtual void handle(void *task, void *pdata)
            {
              static volatile uint64_t cpu = 0;
              rebind_cpu(affinity_start_cpu_, affinity_end_cpu_, cpu, __FILE__, __LINE__);
              int err = OB_SUCCESS;
              UNUSED(pdata);
              if (OB_SUCCESS != (err = replay_worker_.handle_apply(static_cast<ObLogTask*>(task))))
              {
                TBSYS_LOG(WARN, "replay_worker_->handle_apply()=>%d", err);
              }
            }
            void set_cpu_affinity(const int64_t start, const int64_t end)
            {
              affinity_start_cpu_ = start;
              affinity_end_cpu_ = end;
            };
          private:
            ObLogReplayWorker& replay_worker_;
            int64_t affinity_start_cpu_;
            int64_t affinity_end_cpu_;
        };
      public:
        const static int64_t MAX_N_WORKER = 256;
        const static int64_t CHECK_SUM_BUF_SIZE = 1<<16;
        const static int64_t MAX_LOG_BUF_SIZE = 1<<22;
      public:
        ObLogReplayWorker();
        virtual ~ObLogReplayWorker();
        int init(IAsyncLogApplier* log_applier,
                 const int32_t n_thread,
                 const int64_t log_buf_limit,
                 const int64_t queue_len,
                 const int64_t replay_thread_start_cpu,
                 const int64_t replay_thread_end_cpu);
        virtual void run(tbsys::CThread* thread, void* arg);
        int64_t to_string(char* buf, const int64_t len) const;
      protected:
        int handle_apply(ObLogTask* task);
        int64_t set_next_commit_log_id(const int64_t log_id);
        //int64_t set_next_flush_log_id(const int64_t log_id);
      public:
        void stop();
        void wait();
        //[UPS replication]
        int64_t set_next_flush_log_id(const int64_t log_id);
        //[]:e
        int64_t wait_next_commit_log_id(const int64_t log_id, const int64_t timeout_us);
        int64_t wait_next_flush_log_id(const int64_t log_id, const int64_t timeout_us);
        bool is_task_finished(const int64_t log_id) const;
        bool is_all_task_finished() const;
        int64_t get_replayed_log_id() const;
        int get_replay_cursor(ObLogCursor& cursor) const;
        //add wangdonghui [ups_replication] 20161221 :b
        int reset_replay_cursor() const;
        //add :e
        int update_replay_cursor(const ObLogCursor& cursor);
        int start_log(const ObLogCursor& log_cursor);
        int wait_task(int64_t id) const;
        int submit(int64_t& task_id, const char* buf, int64_t len, int64_t& pos, const ReplayType replay_type);
        int submit_batch(int64_t& task_id, const char* buf, int64_t len, const ReplayType replay_type);
        void set_cpu_affinity(const int64_t start, const int64_t end)
        {
          apply_worker_.set_cpu_affinity(start, end);
        };
        //add wangdonghui [ups_replication] 20161214 :b
        void set_master_max_commit_log_id(const int64_t commit_id);
        //add :e
      public:
        int64_t get_next_submit_log_id() const { return next_submit_log_id_; }
        int64_t get_next_commit_log_id() const { return next_commit_log_id_; }
        int64_t get_next_flush_log_id() const { return next_flush_log_id_; }
        int64_t get_last_barrier_log_id() const { return last_barrier_log_id_; }
      protected:
        bool is_inited() const;
        bool is_task_commited(const int64_t log_id) const;
        bool is_task_flushed(const int64_t log_id) const;
        int submit(ObLogTask& task, int64_t& log_id, const char* buf, int64_t len, int64_t& pos, const ReplayType replay_type);
        int do_work(int64_t thread_id);
        int do_commit(int64_t thread_id);
        int do_replay(int64_t thread_id);
        int replay(ObLogTask& task);
        void on_error(ObLogTask* task, int err);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObLogReplayWorker);
      private:
        int32_t n_worker_;
        int64_t queue_len_;
        int64_t flying_trans_no_limit_;
        common::FIFOAllocator allocator_;
        common::ObResourcePool<ObLogTask, 0, 1<<16> task_pool_;
        IAsyncLogApplier* log_applier_;
        ApplyWorker apply_worker_;
        ObSeqQueue commit_queue_;
        ObLogCursor replay_cursor_;
        tbsys::CThreadCond commit_log_id_cond_;
        tbsys::CThreadCond flush_log_id_cond_;

        volatile int err_;
        volatile int64_t next_submit_log_id_;
        volatile int64_t next_commit_log_id_;
        volatile int64_t next_flush_log_id_;
        volatile int64_t last_barrier_log_id_;
        //add wangdonghui [ups_replication] 20170111 :b
        volatile int64_t master_cmt_log_id_;
        //add :e
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_LOG_REPLAY_WORKER_H__ */
