/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#ifndef _OB_DAILY_MERGE_CHECKER_H_
#define _OB_DAILY_MERGE_CHECKER_H_

#include "tbsys.h"
#include "common/ob_se_array.h"
#include "common/ob_timer.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace rootserver
  {
    struct ObRowChecksumInfo
    {
      uint64_t table_id_;
      uint64_t row_checksum_;
      void reset(){table_id_ = common::OB_INVALID_ID; row_checksum_ = 0;}
      bool operator == (const ObRowChecksumInfo &other) const;
      int64_t to_string(char* buffer, const int64_t length) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObRowChecksum
    {
      int64_t data_version_;
      common::ObSEArray<ObRowChecksumInfo, common::OB_MAX_TABLE_NUMBER> rc_array_;
      void reset(){data_version_ = 0; rc_array_.reset();}
      bool is_equal(const ObRowChecksum &slave) const;
      int64_t to_string(char* buffer, const int64_t length) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    // this runnable checker is a runnable thread for check root table's healthy
    // 1. at first check the daily merge whether all tablet merge finished
    // 2. if daily merge not finished, check the root table contains dropped tables
    // if exist, clear the tables and in the next round recheck it
    class ObRootServer2;
    class ObDailyMergeChecker : public tbsys::CDefaultRunnable
    {
      public:
        ObDailyMergeChecker(ObRootServer2 * root_server);
        virtual ~ObDailyMergeChecker();
        virtual void runTimerTask(void){}
      public:
        void run(tbsys::CThread * thread, void * arg);
      private:
        friend class MonitorRowChecksumTask;
        class MonitorRowChecksumTask : public common::ObTimerTask
      {
        public:
          MonitorRowChecksumTask (ObDailyMergeChecker* merge_checker, ObRootServer2 *root_server) : merge_checker_(merge_checker), root_server_(root_server), task_scheduled_(false) {}
        public:
          virtual void runTimerTask();
          inline bool is_scheduled() const { return task_scheduled_;}
          inline void set_scheduled() { task_scheduled_ = true; }
        private:
          ObDailyMergeChecker* merge_checker_;
          ObRootServer2* root_server_;
          ObRowChecksum master_;
          ObRowChecksum slave_;
          bool task_scheduled_;
      };

      private:
        const static int64_t CHECK_INTERVAL_SECOND = 3;
        const static int64_t MAX_RESERVED_VERSION_COUNT = 2;
      private:
        ObRootServer2 * root_server_;
        MonitorRowChecksumTask check_task_;
        common::ObTimer timer_;
    };
  }
}

#endif // _OB_DAILY_MERGE_CHECKER_H_
