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

#include "ob_log_replay_runnable.h"

#include "ob_define.h"
#include "ob_log_entry.h"

#include "utility.h"

using namespace oceanbase::common;

ObLogReplayRunnable::ObLogReplayRunnable()
{
  replay_wait_time_ = DEFAULT_REPLAY_WAIT_TIME_US;
  role_mgr_ = NULL;
  obi_role_ = NULL;
  is_initialized_ = false;
}

ObLogReplayRunnable::~ObLogReplayRunnable()
{
}

int ObLogReplayRunnable::init(const char* log_dir, const uint64_t log_file_id_start, const uint64_t log_seq_start, ObRoleMgr *role_mgr, ObiRole *obi_role, int64_t replay_wait_time)
{
  int ret = OB_SUCCESS;

  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogReplayRunnable has been initialized");
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == role_mgr)
    {
      TBSYS_LOG(ERROR, "Parameter is invalid[role_mgr=%p]", role_mgr);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = log_reader_.init(&single_log_reader_, log_dir, log_file_id_start,
        log_seq_start, true);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObLogReader init error[ret=%d], ObLogReplayRunnable init failed", ret);
    }
    else
    {
      role_mgr_ = role_mgr;
      obi_role_ = obi_role;
      replay_wait_time_ = replay_wait_time;
      max_log_seq_ = log_seq_start; 
      is_initialized_ = true;
    }
  }

  return ret;
}

int ObLogReplayRunnable::reset_file_id(const uint64_t log_id_start, const uint64_t log_seq_start)
{
  if (0 != log_seq_start)
  {
    max_log_seq_ = log_seq_start;
  }
  return log_reader_.reset_file_id(log_id_start, log_seq_start);
}

void ObLogReplayRunnable::clear()
{
  if (NULL != _thread)
  {
    delete[] _thread;
    _thread = NULL;
  }
  _stop = false;
}


void ObLogReplayRunnable::run(tbsys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;

  UNUSED(thread);
  UNUSED(arg);

  char* log_data = NULL;
  int64_t data_len = 0;
  LogCommand cmd = OB_LOG_UNKNOWN;
  uint64_t seq;

  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogReplayRunnable has not been initialized");
    ret = OB_NOT_INIT;
  }
  else
  {
    while (!_stop)
    {
      ret = log_reader_.read_log(cmd, seq, log_data, data_len);
      if (OB_READ_NOTHING == ret)
      {
        if (ObRoleMgr::MASTER == role_mgr_->get_role()
            && (NULL == obi_role_ || ObiRole::MASTER == obi_role_->get_role()))
        {
          stop();
        }
        else
        {
          usleep(static_cast<useconds_t>(replay_wait_time_));
        }
        continue;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObLogReader read_data error[ret=%d]", ret);
        break;
      }
      else
      {
        if (OB_LOG_NOP != cmd)
        {
          ret = replay(cmd, seq, log_data, data_len);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "replay log error[ret=%d]", ret);
            hex_dump(log_data, static_cast<int32_t>(data_len), false, TBSYS_LOG_LEVEL_ERROR);
            break;
          }
        }
      }
    }
  }

  // stop server
  if (NULL != role_mgr_) // double check
  {
    if (OB_SUCCESS != ret && OB_READ_NOTHING != ret)
    {
      TBSYS_LOG(WARN, "role_mgr_=%p", role_mgr_);
      role_mgr_->set_state(ObRoleMgr::ERROR);
    }
  }
  TBSYS_LOG(INFO, "ObLogReplayRunnable finished[stop=%d ret=%d]", _stop, ret);
}

void ObLogReplayRunnable::get_cur_replay_point(int64_t& log_file_id, int64_t& log_seq_id)
{
  log_file_id = log_reader_.get_cur_log_file_id();
  log_seq_id = log_reader_.get_last_log_seq_id() + 1;
}
void ObLogReplayRunnable::get_cur_replay_point(int64_t& log_file_id, int64_t& log_seq_id, int64_t& log_offset)
{
  log_file_id = log_reader_.get_cur_log_file_id();
  log_seq_id = log_reader_.get_last_log_seq_id() + 1;
  log_offset = log_reader_.get_last_log_offset();
}

int ObLogReplayRunnable::get_replayed_cursor(ObLogCursor& replay_cursor)
{
  return log_reader_.get_next_cursor(replay_cursor);
}
