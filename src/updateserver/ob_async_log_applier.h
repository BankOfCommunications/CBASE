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
#ifndef __OB_UPDATESERVER_OB_ASYNC_LOG_APPLIER_H__
#define __OB_UPDATESERVER_OB_ASYNC_LOG_APPLIER_H__
#include "common/ob_define.h"
#include "ob_ups_table_mgr.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObLogTask;
    struct ReplayTaskProfile
    {
      static const int64_t MAX_PROFILE_INFO_BUF_LEN = 1024;
      private:
        static int64_t get_cur_time_us() { return tbsys::CTimeUtil::getTime(); }
      public:
      ReplayTaskProfile(ObLogTask& host): host_(host)
      {
        reset();
      }
      ~ReplayTaskProfile()
      {}
      void reset()
      {
        enable_ = false;
        submit_time_us_ = 0;
        start_apply_time_us_ = 0;
        end_apply_time_us_ = 0;
        start_commit_time_us_ = 0;
        end_commit_time_us_ = 0;
        start_flush_time_us_ = 0;
        end_flush_time_us_ = 0;
      }

      void on_submit() { if (enable_) submit_time_us_ = get_cur_time_us(); }
      void start_apply() { if (enable_) start_apply_time_us_ = get_cur_time_us(); }
      void end_apply() { if (enable_) end_apply_time_us_ = get_cur_time_us(); }
      void start_commit() { if (enable_) start_commit_time_us_ = get_cur_time_us(); }
      void end_commit() { if (enable_) end_commit_time_us_ = get_cur_time_us(); }
      void start_flush() { if (enable_) start_flush_time_us_ = get_cur_time_us(); }
      void end_flush() { if (enable_) end_flush_time_us_ = get_cur_time_us(); }
      int64_t to_string(char* buf, int64_t len) const;

      bool enable_;
      ObLogTask& host_;
      int64_t submit_time_us_;
      int64_t start_apply_time_us_;
      int64_t end_apply_time_us_;
      int64_t start_commit_time_us_;
      int64_t end_commit_time_us_;
      int64_t start_flush_time_us_;
      int64_t end_flush_time_us_;
    };

    struct ObLogTask
    {
      const static int64_t CHECK_SUM_BUF_SIZE = 1<<21;
      ObLogTask();
      ~ObLogTask();
      void reset();
      bool is_last_log_of_batch() { return log_data_ + log_entry_.get_log_data_len() == batch_buf_ + batch_buf_len_; }

      int64_t to_string(char* buf, const int64_t buf_len) const
      {
        int64_t pos = 0;
        databuff_printf(buf, buf_len, pos, "[LogTask] log_id=%ld barrier_log_id=%ld trans_id=%s", log_id_, barrier_log_id_, to_cstring(trans_id_));
        pos += log_entry_.to_string(buf + pos, buf_len - pos);

        databuff_printf(buf, buf_len, pos, " log_data=%p batch_buf=%p batch_buf_len_=%ld ", log_data_, batch_buf_, batch_buf_len_);
        pos += trans_id_.to_string(buf + pos, buf_len - pos);

        databuff_printf(buf, buf_len, pos, " mutation_ts=%ld checksum_before_mutate=%lu checksum_after_mutate=%lu replay_type=%d ",
                        mutation_ts_, checksum_before_mutate_, checksum_after_mutate_, replay_type_);
        pos += profile_.to_string(buf + pos, buf_len - pos);
        return pos;
      }

      volatile int64_t log_id_;
      volatile int64_t barrier_log_id_;
      ObLogEntry log_entry_;
      const char* log_data_;
      const char* batch_buf_;
      int64_t batch_buf_len_;
      ObTransID trans_id_;
      int64_t mutation_ts_;
      uint64_t checksum_before_mutate_;
      uint64_t checksum_after_mutate_;
      ReplayType replay_type_;
      ReplayTaskProfile profile_;
      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      bool is_big_log_completed_;
      //add e
    };

    class IAsyncLogApplier
    {
      public:
        IAsyncLogApplier() {}
        virtual ~IAsyncLogApplier() {}
      public:
        virtual int on_submit(ObLogTask& log_task) = 0;
        virtual int start_transaction(ObLogTask& log_task) = 0;
        virtual int apply(ObLogTask& log_task) = 0;
        virtual int end_transaction(ObLogTask& log_task) = 0;
        virtual int flush(ObLogTask& log_task) = 0;
        virtual int on_destroy(ObLogTask& log_task) = 0;
    };

    class TransExecutor;
    class ObUpsTableMgr;
    class ObUpsLogMgr;
    class ObTriggerHandler;
    class ObAsyncLogApplier: public IAsyncLogApplier
    {
      public:
        const static int64_t REPLAY_RETRY_WAIT_TIME_US = 100 * 1000;
        const static int64_t MEM_OVERFLOW_REPORT_INTERVAL = 5 * 1000 * 1000;
      public:
        ObAsyncLogApplier();
        virtual ~ObAsyncLogApplier();
      public:
        int init(TransExecutor* trans_executor, SessionMgr* session_mgr, LockMgr* lock_mgr,
                 ObUpsTableMgr* table_mgr, ObUpsLogMgr* log_mgr);
        int on_submit(ObLogTask& log_task);
        int start_transaction(ObLogTask& log_task);
        int apply(ObLogTask& log_entry);
        int end_transaction(ObLogTask& log_task);
        int flush(ObLogTask& log_task);
        int on_destroy(ObLogTask& log_task);
      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      int handle_normal_mutator(const char *buf, const int32_t data_size, ObLogTask &task);
      //add e
      private:
        bool is_memory_warning();
        int add_memtable_uncommited_checksum_(const uint32_t session_descriptor, uint64_t *ret_checksum);
        int handle_normal_mutator(ObLogTask& task);
        bool is_inited() const;
      private:
        bool inited_;
        int64_t retry_wait_time_us_;
        TransExecutor* trans_executor_;
        SessionMgr* session_mgr_;
        LockMgr* lock_mgr_;
        ObUpsTableMgr* table_mgr_;
        ObUpsLogMgr* log_mgr_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_ASYNC_LOG_APPLIER_H__ */
