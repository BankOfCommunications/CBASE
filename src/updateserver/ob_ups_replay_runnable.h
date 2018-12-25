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
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_

#include "common/ob_obi_role.h"
#include "common/ob_log_replay_runnable.h"
#include "common/ob_log_entry.h"
#include "common/ob_log_reader.h"
#include "common/ob_repeated_log_reader.h"
#include "ob_log_sync_delay_stat.h"
#include "ob_ups_role_mgr.h"
#include "common/ob_obi_role.h"
#include "common/ob_switch.h"
//add wangdonghui [ups replication] 20170526:b
#include "common/ob_fifo_allocator.h"
#include "ob_trans_executor.h"
#include "common/ob_single_pop_queue.h"
//add :e
namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
    {
      //forward decleration
      class TestObUpsReplayRunnable_test_init_Test;
    }
  }
  namespace updateserver
  {
    class ObUpsLogMgr;
    class ObUpsReplayRunnable : public tbsys::CDefaultRunnable 
    {
      const static int64_t DEFAULT_REPLAY_WAIT_TIME_US = 50 * 1000;
      const static int64_t DEFAULT_FETCH_LOG_WAIT_TIME_US = 500 * 1000;
      friend class tests::updateserver::TestObUpsReplayRunnable_test_init_Test;
      public:
        ObUpsReplayRunnable();
        virtual ~ObUpsReplayRunnable();
        virtual int init(ObUpsLogMgr* log_mgr, common::ObiRole *obi_role, ObUpsRoleMgr *role_mgr);
        virtual void run(tbsys::CThread* thread, void* arg);
        // 不停线程等待开始和结束
        bool wait_stop();
        bool wait_start();
        // 等待线程结束
        void stop();
        virtual void clear();
        void set_replay_wait_time_us(const int64_t wait_time){ replay_wait_time_us_ = wait_time; }
        void set_fetch_log_wait_time_us(const int64_t wait_time){ fetch_log_wait_time_us_ = wait_time; }
        ObSwitch& get_switch() { return switch_; }
      private:
        ObSwitch switch_;
        ObUpsLogMgr* log_mgr_;
        common::ObiRole* obi_role_;
        ObUpsRoleMgr* role_mgr_;
        bool is_initialized_;
        int64_t replay_wait_time_us_;
        int64_t fetch_log_wait_time_us_;
    };
    //add wangdonghui [Ups_replication] 20161009 :b
    class ObUpdateServer;
    class ObUpsWaitFlushRunnable : public tbsys::CDefaultRunnable
    {
      static const int64_t ALLOCATOR_TOTAL_LIMIT = 10L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_HOLD_LIMIT = ALLOCATOR_TOTAL_LIMIT / 2;
      static const int64_t ALLOCATOR_PAGE_SIZE = 4L * 1024L * 1024L;
      public:
        ObUpsWaitFlushRunnable();
        virtual ~ObUpsWaitFlushRunnable();
        virtual int init(ObUpsLogMgr* log_mgr, common::ObiRole *obi_role, ObUpsRoleMgr *role_mgr,
                         ObUpdateServer* updateserver, ObLogReplayWorker* replay_worker_);
        virtual void run(tbsys::CThread* thread, void* arg);
        // 不停线程等待开始和结束
        bool wait_stop();
        bool wait_start();
        // 等待线程结束
        void stop();
        virtual void clear();
        int process_head_task();
        int push(TransExecutor::Task* packet);
      private:
        ObSwitch switch_;
        common::ObSinglePopQueue<TransExecutor::Task *> task_queue_;
        tbsys::CThreadCond _cond;
        bool is_initialized_;
        ObUpsLogMgr* log_mgr_;
        common::ObiRole* obi_role_;
        ObUpsRoleMgr* role_mgr_;
        ObUpdateServer* updateserver_;
        ObLogReplayWorker* replay_worker_;
        common::FIFOAllocator allocator_;
      private:
        static const int MY_VERSION = 1;
    };
    //add 20161009 :e

  } // end namespace updateserver
} // end namespace oceanbase


#endif // OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_

