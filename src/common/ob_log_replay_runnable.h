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

#ifndef OCEANBASE_COMMON_OB_LOG_REPLAY_RUNNABLE_H_
#define OCEANBASE_COMMON_OB_LOG_REPLAY_RUNNABLE_H_

#include "tbsys.h"
#include "ob_log_reader.h"
#include "ob_repeated_log_reader.h"
#include "ob_role_mgr.h"
#include "ob_log_entry.h"
#include "ob_obi_role.h"

namespace oceanbase
{
  namespace common
  {
    /// 回放日志的线程代码
    /// ObLogReplayRunnable中读取日志数据, 并且调用replay虚函数进行回放
    /// Rootserver和updateserver可以实现不同的replay代码
    class ObLogReplayRunnable : public tbsys::CDefaultRunnable
    {
    public:
        const static int64_t DEFAULT_REPLAY_WAIT_TIME_US = 100 * 10000;
      ObLogReplayRunnable();
      virtual ~ObLogReplayRunnable();
        virtual int init(const char* log_dir, const uint64_t log_file_id_start, const uint64_t log_seq_start, ObRoleMgr *role_mgr, ObiRole *obi_role, int64_t replay_wait_time);
      virtual void run(tbsys::CThread* thread, void* arg);
      virtual void clear();

      virtual void set_max_log_file_id(uint64_t max_log_file_id)
      {
        log_reader_.set_max_log_file_id(max_log_file_id);
      }

      virtual void set_has_no_max()
      {
        log_reader_.set_has_no_max();
      }
      int64_t get_max_log_seq()
      {
        return max_log_seq_;
      }
      int64_t get_cur_log_id()
      {
        return log_reader_.get_cur_log_file_id();
      }
      virtual int reset_file_id(const uint64_t log_id_start, const uint64_t log_seq_start);
      void get_cur_replay_point(int64_t& log_file_id, int64_t& log_seq_id);

      void get_cur_replay_point(int64_t& log_file_id, int64_t& log_seq_id, int64_t& log_offset);
        int get_replayed_cursor(ObLogCursor& replayed_cursor);
    protected:
      virtual int replay(LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len) = 0;

    protected:
      int64_t replay_wait_time_;
      ObRoleMgr *role_mgr_;
      ObiRole *obi_role_;
      ObLogReader log_reader_;
      ObRepeatedLogReader single_log_reader_;
      bool is_initialized_;
      int64_t max_log_seq_; 
    };
  } // end namespace common
} // end namespace oceanbase


#endif // OCEANBASE_COMMON_OB_LOG_REPLAY_RUNNABLE_H_

