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
#include "ob_log_replay_worker.h"
#include "ob_ups_log_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    ObLogReplayWorker::ObLogReplayWorker(): n_worker_(0), queue_len_(0),
                                            flying_trans_no_limit_(0),
                                            log_applier_(NULL), apply_worker_(*this),
                                            replay_cursor_(), err_(OB_SUCCESS),
                                            next_submit_log_id_(0), next_commit_log_id_(0),
                                            next_flush_log_id_(0), last_barrier_log_id_(0)
    {}

    ObLogReplayWorker::~ObLogReplayWorker()
    {}

    int ObLogReplayWorker::init(IAsyncLogApplier* log_applier,
                                const int32_t n_worker,
                                const int64_t log_buf_limit,
                                const int64_t queue_len,
                                const int64_t replay_thread_start_cpu,
                                const int64_t replay_thread_end_cpu)
    {
      int err = OB_SUCCESS;
      //mod zhaoqiong [Bugfix::Replay barrier_log]:20170714:b
      //bool queue_rebalance = true;
      bool queue_rebalance = false;
      //mod:e
      bool dynamic_rebalance = false;
      set_cpu_affinity(replay_thread_start_cpu, replay_thread_end_cpu);
      if (NULL == log_applier || n_worker <= 0 || n_worker > MAX_N_WORKER || log_buf_limit < 0 || queue_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != log_applier_ || n_worker_ > 0 || queue_len_ > 0)
      {
        err = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (err = allocator_.init(log_buf_limit + 1, log_buf_limit, OB_MAX_PACKET_LENGTH)))
      {
        TBSYS_LOG(ERROR, "allocator.init(limit=%ld, page_size=%ld)=>%d", log_buf_limit, OB_MAX_PACKET_LENGTH, err);
      }
      else if (OB_SUCCESS != (err = apply_worker_.init(n_worker, queue_len/n_worker, queue_rebalance, dynamic_rebalance)))
      {
        TBSYS_LOG(ERROR, "apply_worker_.init(n_thread=%d, queue_len=%ld)=>%d", n_worker, queue_len, err);
      }
      else if (OB_SUCCESS != (err = commit_queue_.init(queue_len)))
      {
        TBSYS_LOG(ERROR, "commit_queue_.init(%ld)=>%d", queue_len, err);
      }
      else
      {
        n_worker_ = n_worker;
        queue_len_ = queue_len;
        flying_trans_no_limit_ = queue_len;
        log_applier_ = log_applier;
        setThreadCount(1);
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "log_replay_worker.init(log_applier=%p, n_worker=%d): %d", log_applier, n_worker, err);
      }
      else
      {
        TBSYS_LOG(INFO, "log_replay_worker.init(log_applier=%p, n_worker=%d): success", log_applier, n_worker);
      }
      return err;
    }

    //add wangdonghui [ups_replication] 20161214 :b
    void ObLogReplayWorker::set_master_max_commit_log_id(const int64_t commit_id)
    {
    volatile int64_t x;
    while ((x = master_cmt_log_id_) < commit_id
    && !__sync_bool_compare_and_swap(&master_cmt_log_id_, x, commit_id))
    {
    TBSYS_LOG(WARN, "replay_worker set_master_max_commit_log_id waiting for __sync");
    }
    }
    //add:e

    bool ObLogReplayWorker::is_inited() const
    {
      return NULL != log_applier_ && n_worker_ >0 ;
    }

    int64_t ObLogReplayWorker::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      if (NULL != this)
      {
        databuff_printf(buf, len, pos, "ReplayWorker(rp=%s, next_/submit/commit/flush=%ld/%ld/%ld, barrier=%ld, err_=%d)",
                        to_cstring(replay_cursor_), next_submit_log_id_, next_commit_log_id_, next_flush_log_id_, last_barrier_log_id_, err_);
      }
      return pos;
    }

    void ObLogReplayWorker::on_error(ObLogTask* task, int err)
    {
      static int64_t killed = 0;
      TBSYS_LOG(ERROR, "replay error: task=%s, err=%d, will kill self", NULL == task? "NULL": to_cstring(*task), err);
      if (__sync_bool_compare_and_swap(&killed, 0, 1))
      {
        kill(getpid(), SIGTERM);
        TBSYS_LOG(ERROR, "kill self, pid=%d", getpid());
      }
    }

    int ObLogReplayWorker::get_replay_cursor(ObLogCursor& cursor) const
    {
      int err = OB_SUCCESS;
      cursor = replay_cursor_;
      return err;
    }
    //add wangdonghui [ups_replication] 20161222 :b
    int ObLogReplayWorker::reset_replay_cursor() const
    {
      int err = OB_SUCCESS;
      ((ObLogCursor&)replay_cursor_).reset();
      return err;
    }
    //add :e
    int ObLogReplayWorker::update_replay_cursor(const ObLogCursor& cursor)
    {
      int err = OB_SUCCESS;
      //TBSYS_LOG(INFO, "update_cursor(%s)", to_cstring(cursor));
      if (replay_cursor_.newer_than(cursor))
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(ERROR, "update_replay_cursor(replay_cursor[%ld:%ld+%ld] > curosr[%ld:%ld+%ld])",
                  replay_cursor_.log_id_, replay_cursor_.file_id_, replay_cursor_.offset_,
                  cursor.log_id_, cursor.file_id_, cursor.offset_);
      }
      else if (OB_SUCCESS != (err = commit_queue_.update(cursor.log_id_)))
      {
        TBSYS_LOG(ERROR, "commit_queue_.update(%ld)=>%d", cursor.log_id_, err);
      }
      else
      {
        last_barrier_log_id_ = cursor.log_id_ - 1;
        next_submit_log_id_ = cursor.log_id_;
        set_next_commit_log_id(cursor.log_id_);
        set_next_flush_log_id(cursor.log_id_);
        replay_cursor_ = cursor;
      }
      return err;
    }

    int ObLogReplayWorker::start_log(const ObLogCursor& log_cursor)
    {
      int err = OB_SUCCESS;
      if (!log_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (replay_cursor_.is_valid())
      {
        if (log_cursor.equal(replay_cursor_))
        {
          TBSYS_LOG(WARN, "start_log(log_cursor=%s): ALREADY STARTED.", log_cursor.to_str());
        }
        else
        {
          err = OB_INIT_TWICE;
        }
      }
      else if (OB_SUCCESS != (err = update_replay_cursor(log_cursor)))
      {
        TBSYS_LOG(ERROR, "update_replay_cursor(%s)=>%d", to_cstring(log_cursor), err);
      }
      return err;
    }

    void ObLogReplayWorker::stop()
    {
      tbsys::CDefaultRunnable::stop();
      apply_worker_.destroy();
    }

    void ObLogReplayWorker::wait()
    {
      CDefaultRunnable::wait();
    }

    void ObLogReplayWorker::run(tbsys::CThread* thread, void* arg)
    {
      int err = OB_SUCCESS;
      int64_t id = (int64_t)arg;
      UNUSED(thread);
      TBSYS_LOG(INFO, "log replay worker[%ld] start", id);
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = do_work(id)))
      {
        stop();
        if (OB_SUCCESS == err_ && OB_EAGAIN != err)
        {
          err_ = err;
        }
        TBSYS_LOG(ERROR, "do_work(id=%ld)=>%d", id, err);
      }
      TBSYS_LOG(INFO, "log replay worker[%ld] exit: err=%d", id, err);
    }

    int ObLogReplayWorker::do_work(int64_t thread_id)
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (thread_id < 0 || thread_id >= n_worker_ + 1)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "thread_id=%ld", thread_id);
      }
      else if (0 == thread_id)
      {
        //add wangdonghui 20161214 [ups_replication] :b
        //while((master_cmt_log_id_ < next_commit_log_id_)){}
        //add :e
        if (OB_SUCCESS != (err = do_commit(thread_id)))
        {
          TBSYS_LOG(ERROR, "do_commit(%ld)=>%d", thread_id, err);
        }
      }
      else
      {
        if (OB_SUCCESS != (err = do_replay(thread_id)))
        {
          TBSYS_LOG(ERROR, "do_replay(%ld)=>%d", thread_id, err);
        }
      }
      return err;
    }

    int64_t ObLogReplayWorker::set_next_commit_log_id(const int64_t log_id)
    {
      return set_counter(commit_log_id_cond_, next_commit_log_id_, log_id);
    }

    int64_t ObLogReplayWorker::set_next_flush_log_id(const int64_t log_id)
    {
      return set_counter(flush_log_id_cond_, next_flush_log_id_, log_id);
    }

    int64_t ObLogReplayWorker::wait_next_commit_log_id(const int64_t log_id, const int64_t timeout_us)
    {
      return wait_counter(commit_log_id_cond_, next_commit_log_id_, log_id, timeout_us);
    }

    int64_t ObLogReplayWorker::wait_next_flush_log_id(const int64_t log_id, const int64_t timeout_us)
    {
      return wait_counter(flush_log_id_cond_, next_flush_log_id_, log_id, timeout_us);
    }

    int ObLogReplayWorker::do_commit(int64_t thread_id)
    {
      int err = OB_SUCCESS;
      ObLogTask* task = NULL;
      int64_t seq = 0;
      int64_t wait_time_us = 10 * 1000;
      UNUSED(thread_id);
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      while (!_stop && OB_SUCCESS == err && OB_SUCCESS == err_)
      {
        if (OB_SUCCESS != (err = commit_queue_.get(seq, (void*&)task, wait_time_us))
            && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "commit_queue_.get()=>%d", err);
        }
        else if (OB_EAGAIN == err)
        {
          //del shili [LONG_TRANSACTION_LOG]  20160926:b
          //err = OB_SUCCESS;
          //del e
        }
        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        else if (OB_UPS_BIG_LOG_DATA == task->log_entry_.cmd_&& (!task->is_big_log_completed_))
        {
          //do nothing
        }
        // add e
        else if (OB_SUCCESS != (err = log_applier_->end_transaction(*task)))
        {
          TBSYS_LOG(ERROR, "log_applier->end_transaction(task=%p)=>%d", task, err);
        }

        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        if (OB_EAGAIN == err)
        {
          err = OB_SUCCESS;
        }
        else if (OB_SUCCESS != err)
        {
          //do nothing
          TBSYS_LOG(ERROR, "ERR = %d", err);
        }
       //add e
        else
        {
          set_next_commit_log_id(task->log_id_ + 1);
          if (task->is_last_log_of_batch())
          {
		    //mod wangdonghui 20170111 [ups_replication] :b
		    /*
            if (RT_APPLY == task->replay_type_ && OB_SUCCESS != (err = log_applier_->flush(*task)))
            {
              // 失败之后不释放
              TBSYS_LOG(ERROR, "flush(end_id=%ld, %p[%ld])=>%d", task->log_id_,
                        task->batch_buf_, task->batch_buf_len_, err);
            }
            else
            {*/
              //mod wangjiahao [Paxos ups_replication_tmplog] 20150712 :b
              //will setted when log writted in tmplog
              //set_next_flush_log_id(task->log_id_ + 1);
              //mod :e
              allocator_.free((void*)task->batch_buf_);
            //}
			//mod:e
          }
          if (OB_SUCCESS == err)
          {
            log_applier_->on_destroy(*task);
            task_pool_.free(task);
          }
        }
      }
      if (OB_SUCCESS != err)
      {
        on_error(task, err);
      }
      return err;
    }

    int ObLogReplayWorker::do_replay(int64_t thread_id)
    {
      int err = OB_NOT_SUPPORTED;
      UNUSED(thread_id);
      return err;
    }

    int ObLogReplayWorker::handle_apply(ObLogTask* task)
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        err = OB_INVALID_ARGUMENT;
        err_ = err;
        TBSYS_LOG(WARN, "task == NULL");
      }
      else
      {
        while(!_stop && OB_SUCCESS == err_ && OB_EAGAIN == (err = replay(*task)))
          ;
        if (OB_SUCCESS != err && OB_EAGAIN != err)
        {
          err_ = err;
          TBSYS_LOG(ERROR, "replay()=>%d", err);
        }
        while(!_stop && OB_SUCCESS == err_ && OB_EAGAIN == (err = commit_queue_.add(task->log_id_, (void*)task)))
          ;
        if (OB_SUCCESS != err && OB_EAGAIN != err)
        {
          err_ = err;
          TBSYS_LOG(ERROR, "commit_queue.add(%ld, %p)=>%d", task->log_id_, task, err);
        }
      }
      if (OB_SUCCESS != err)
      {
        on_error(task, err);
      }
      return err;
    }

    bool ObLogReplayWorker::is_task_commited(const int64_t log_id) const
    {
      return log_id < next_commit_log_id_;
    }

    bool ObLogReplayWorker::is_task_flushed(const int64_t log_id) const
    {
      return log_id < next_flush_log_id_;
    }

    bool ObLogReplayWorker::is_task_finished(const int64_t log_id) const
    {
      //mod wangjiahao [Paxos ups_replication_tmplog] 20150714 :b
      //When log commited, they must be already flushed in tmp_log before
      //return is_task_commited(log_id) && is_task_flushed(log_id);
      return is_task_commited(log_id);
      //mod :e
    }

    bool ObLogReplayWorker::is_all_task_finished() const
    {
      //mod wangjiahao [Paxos ups_replication_tmplog] 20150714 :b
      //When log commited, they must be already flushed in tmp_log before
      //return is_task_commited(next_submit_log_id_ - 1) && is_task_flushed(next_submit_log_id_ - 1);
      //TBSYS_LOG(INFO, "WJH_INFO check_all_task_finished next_submit_log_id=%ld next_commit_log_id=%ld", next_submit_log_id_, next_commit_log_id_);
      return is_task_commited(next_submit_log_id_ - 1);
    }

    int ObLogReplayWorker::wait_task(int64_t id) const
    {
      int err = OB_SUCCESS;
      while (!_stop && OB_SUCCESS == err_ && !is_task_finished(id))
        ;
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      else if (_stop)
      {
        err = OB_CANCELED;
      }
      return err;
    }

    int ObLogReplayWorker::replay(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      //TBSYS_LOG(INFO, "replay(task.log_id[%ld], next_submit_log_id[%ld], next_commit_log_id[%ld])", task.log_id_, next_submit_log_id_, next_commit_log_id_);
      if (task.log_id_ == 0)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "task.log_id[%ld] is invalid", task.log_id_);
      }
      else if (!is_task_commited(task.barrier_log_id_))
      {
        err = OB_EAGAIN;
      }
      else if (OB_SUCCESS != (err = log_applier_->start_transaction(task)))
      {
        TBSYS_LOG(ERROR, "log_applier->start_transaction()=>%d", err);
      }
      else if (OB_SUCCESS != (err = log_applier_->apply(task)))
      {
        if (OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "log_applier->apply()=>%d", err);
        }
      }
      return err;
    }

    static int is_barrier_log(bool& is_barrier, const LogCommand cmd, const char* buf, const int64_t len)
    {
      int err = OB_SUCCESS;
      is_barrier = true;
      if (NULL == buf || 0 >= len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_LOG_NOP == cmd || OB_LOG_SWITCH_LOG == cmd)
      {
        is_barrier = false;
      }
      else if (OB_LOG_UPS_MUTATOR == cmd)
      {
        ObUpsMutator mutator;
        int64_t pos = 0;
        if (OB_SUCCESS != (err = mutator.deserialize_header(buf, len, pos)))
        {
          TBSYS_LOG(ERROR, "mutator.deserialize(buf=%p[%ld])=>%d", buf, len, err);
        }
        else if (mutator.is_normal_mutator())
        {
          is_barrier = false;
        }
      }
      return err;
    }

    int64_t ObLogReplayWorker::get_replayed_log_id() const
    {
      return next_commit_log_id_;
    }

    int ObLogReplayWorker::submit(ObLogTask& task, int64_t& log_id, const char* buf, int64_t len, int64_t& pos, const ReplayType replay_type)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      bool check_integrity = true;
      bool is_barrier = true;
      //TBSYS_LOG(INFO, "submit(task.log_id[%ld], next_submit_log_id[%ld], next_commit_log_id[%ld])", task.log_id_, next_submit_log_id_, next_commit_log_id_);
      if (_stop)
      {
        err = OB_CANCELED;
      }
      else if (new_pos + task.log_entry_.get_serialize_size() > len)
      {
        err = OB_PARTIAL_LOG;
      }
      else if (OB_SUCCESS != (err = task.log_entry_.deserialize(buf, len, new_pos)))
      {
        TBSYS_LOG(ERROR, "task.log_entry.deserialize()=>%d", err);
      }
      else if (new_pos + task.log_entry_.get_log_data_len() > len)
      {
        err = OB_PARTIAL_LOG;
      }
      else if (check_integrity && OB_SUCCESS != (err = task.log_entry_.check_header_integrity()))
      {
        TBSYS_LOG(ERROR, "log_entry.check_header_integrity()=>%d", err);
      }
      else if (check_integrity && OB_SUCCESS != (err = task.log_entry_.check_data_integrity(buf + new_pos)))
      {
        TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
      }
      else if (next_submit_log_id_ != (int64_t)task.log_entry_.seq_)
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(ERROR, "next_submit_log_id[%ld] != task.log_id[%ld], replay_cursor=%s",
                  next_submit_log_id_, task.log_entry_.seq_, to_cstring(replay_cursor_));
      }
      else if (next_commit_log_id_  + flying_trans_no_limit_  <= (int64_t)task.log_entry_.seq_)
      {
        err = OB_EAGAIN;
        TBSYS_LOG(DEBUG, "next_commit_id[%ld] + flying_limit[%ld] <= cur_seq[%ld]",
                  next_commit_log_id_, flying_trans_no_limit_, task.log_entry_.seq_);

      }
      else if (OB_SUCCESS != (err = is_barrier_log(is_barrier, (LogCommand)task.log_entry_.cmd_,
                                                   buf + new_pos, task.log_entry_.get_log_data_len())))
      {
        TBSYS_LOG(ERROR, "is_barrier_log()=>%d", err);
      }
      else
      {
        ObLogEntry entry = task.log_entry_;
        if (is_barrier)
        {
          task.barrier_log_id_ = task.log_entry_.seq_ - 1;
        }
        else
        {
          task.barrier_log_id_ = last_barrier_log_id_;
        }
        task.trans_id_.reset();
        task.log_data_ = buf + new_pos;
        task.replay_type_ = replay_type;
        task.log_id_ = task.log_entry_.seq_;
        task.batch_buf_ = buf;
        task.batch_buf_len_ = len;
        log_id = task.log_id_;
        task.profile_.enable_ = (TraceLog::get_log_level() <= TBSYS_LOG_LEVEL_INFO);

        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        task.is_big_log_completed_= false;
        //add e

        log_applier_->on_submit(task);
        if (OB_SUCCESS != (err = apply_worker_.push(&task))
            && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "queue_.push(log_id=%ld)=>%d", log_id, err);
        }
        else if (OB_EAGAIN == err)
        {
          //TBSYS_LOG(WARN, "queue.push(log_id=%ld): EAGAIN", log_id);
        }
        else if (OB_SUCCESS != (err = replay_cursor_.advance(entry)))
        {
          TBSYS_LOG(ERROR, "replay_cursor.advance()=>%d", err);
        }
        else
        {
          if (is_barrier)
          {
            last_barrier_log_id_ = log_id;
          }
          next_submit_log_id_ = log_id + 1;
          pos = new_pos + entry.get_log_data_len();
        }
      }
      return err;
    }

    int ObLogReplayWorker::submit(int64_t& log_id, const char* buf, int64_t len, int64_t& pos, const ReplayType replay_type)
    {
      int err = OB_SUCCESS;
      ObLogTask* task = NULL;
      if (!is_inited() || !replay_cursor_.is_valid())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || len <= 0 || pos >= len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (_stop)
      {
        err = OB_CANCELED;
      }
      else if (OB_SUCCESS != err_)
      {
        err = err_;
        //TBSYS_LOG(ERROR, "replay_worker err_=%d", err);
      }
      else if (NULL == (task = task_pool_.alloc()))
      {
        err = OB_EAGAIN;
        TBSYS_LOG(ERROR, "task_pool.alloc(free=%ld)=>NULL", task_pool_.get_free_num());
      }
      else if (OB_SUCCESS != (err = submit(*task, log_id, buf, len, pos, replay_type))
               && OB_EAGAIN != err)
      {
        TBSYS_LOG(ERROR, "submit(next_submit_id=%ld)=>%d", next_submit_log_id_, err);
      }
      else if (OB_EAGAIN == err)
      {
        task_pool_.free(task);
      }
      return err;
    }

    int ObLogReplayWorker::submit_batch(int64_t& log_id, const char* buf, int64_t len, const ReplayType replay_type)
    {
      int err = OB_SUCCESS;
      char* new_buf = NULL;
      int64_t pos = 0;
      if (NULL == (new_buf = (char*)allocator_.alloc(len)))
      {
        err = OB_EAGAIN;
        TBSYS_LOG(ERROR, "alloc(%ld) failed", len);
      }
      else
      {
        memcpy(new_buf, buf, len);
      }
      // 提交一定要成功, 如果失败也不释放内存
      while(OB_SUCCESS == err && pos < len)
      {
        if (OB_SUCCESS != (err = submit(log_id, new_buf, len, pos, replay_type))
            && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "submit(log_id=%ld, buf=%p[%ld], pos=%ld,rt=%d)=>%d",
                    log_id, new_buf, len, pos, replay_type, err);
        }
        else if (OB_EAGAIN == err)
        {
          err = OB_SUCCESS;
        }
      }
      if (OB_SUCCESS != err && NULL != new_buf)
      {
        //allocator_.free(new_buf);
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
