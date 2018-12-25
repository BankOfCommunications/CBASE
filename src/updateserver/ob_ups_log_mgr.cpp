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

#include "ob_ups_log_mgr.h"

#include "common/file_utils.h"
#include "common/file_directory_utils.h"
#include "common/ob_log_dir_scanner.h"
#include "common/ob_log_reader.h"
#include "common/ob_direct_log_reader.h"
#include "common/utility.h"
#include "common/ob_delay_guard.h"
#include "ob_update_server_main.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
#define UPS ObUpdateServerMain::get_instance()->get_update_server()

namespace oceanbase
{
  namespace updateserver
  {
    // 获得sstable指示的最大日志文件ID
    uint64_t get_max_file_id_by_sst()
    {
      uint64_t file_id = OB_INVALID_ID;
      ObUpdateServerMain *ups = ObUpdateServerMain::get_instance();
      if (NULL == ups)
      {
        file_id = OB_INVALID_ID;
      }
      else
      {
        file_id = ups->get_update_server().get_sstable_mgr().get_max_clog_id();
      }
      return file_id;
    }

    // 备UPS向主机询问日志的起始点时，主机应该返回上一次major frozen的点
    int64_t get_last_major_frozen_log_file_id(const char* log_dir)
    {
      int err = OB_SUCCESS;
      uint64_t file_id = 0;
      ObUpdateServerMain *ups = ObUpdateServerMain::get_instance();
      ObLogDirScanner scanner;
      if (NULL != ups)
      {
        file_id = ups->get_update_server().get_sstable_mgr().get_last_major_frozen_clog_file_id();
      }
      if (0 < file_id)
      {}
      else if (OB_SUCCESS != (err = scanner.init(log_dir))
               && OB_DISCONTINUOUS_LOG != err)
      {
        TBSYS_LOG(ERROR, "scanner.init(log_dir=%s)=>%d", log_dir, err);
      }
      else if (OB_SUCCESS != (err = scanner.get_min_log_id((uint64_t&)file_id)) && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "scanner.get_min_log_file_id()=>%d", err);
      }
      return file_id;
    }

    //add wangjiahao [Paxos ups_replication_tmplog] 20150805 :b
    int compare_log_buffer(const ObLogCursor& start_cursor, int64_t end_id, const char* buf1, const char* buf2, int64_t data_len, ObLogCursor& pass_cursor)
    {
      int err = OB_SUCCESS;
      int64_t pos1 = 0;
      int64_t pos2 = 0;
      ObLogEntry log_entry1, log_entry2;

      pass_cursor = start_cursor;
      if (NULL == buf1 || NULL == buf2 || data_len <= 0 || !start_cursor.is_valid() || start_cursor.log_id_ >= end_id)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument, buf1=%p, buf2=%p, end_id=%ld, start_cursor=%s data_len=%ld", buf1, buf2, end_id, start_cursor.to_str(), data_len);
      }

      while(OB_SUCCESS == err && pos1 < data_len)
      {
        if (OB_SUCCESS != (err = log_entry1.deserialize(buf1, data_len, pos1)))
        {
          TBSYS_LOG(ERROR, "log_entry1.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", buf1, data_len, pos1, err);
        }
        else if (OB_SUCCESS != (err = log_entry2.deserialize(buf2, data_len, pos2)))
        {
          TBSYS_LOG(ERROR, "log_entry2.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", buf2, data_len, pos2, err);
        }
        //add chujiajia [Paxos ups_replication] 20160106:b
        else if(log_entry1.header_.header_checksum_ != log_entry2.header_.header_checksum_)
        {
          TBSYS_LOG(WARN, "Checking TMP_LOG LOG not match @(%s)", pass_cursor.to_str());
          err = OB_INVALID_LOG;
        }
        //add:e
        else if (log_entry1.header_.data_checksum_ != log_entry2.header_.data_checksum_)
        {
          TBSYS_LOG(WARN, "Checking TMP_LOG LOG not match @(%s)", pass_cursor.to_str());
          err = OB_INVALID_LOG;
        }
        else
        {
          pos1 += log_entry1.get_log_data_len();
          pos2 += log_entry2.get_log_data_len();
          if (OB_SUCCESS != (err = pass_cursor.advance(log_entry1)))
          {
            TBSYS_LOG(ERROR, "pass_cursor[%ld].advance(%ld)=>%d", pass_cursor.log_id_, log_entry1.seq_, err);
          }
        }
      }
      //TBSYS_LOG(INFO, "pass_cursor.log_id_[%ld], log_entry1.seq_[%ld], err=[%d]", pass_cursor.log_id_, static_cast<int64_t>(log_entry1.seq_), err);
      //TBSYS_LOG(INFO, "current_log_term[%ld]", UPS.get_log_mgr().get_current_log_term());
      return err;
    }
    //add :e

    int parse_log_buffer(const char* log_data, int64_t data_len, const ObLogCursor& start_cursor, ObLogCursor& end_cursor)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      int64_t tmp_pos = 0;
      int64_t file_id = 0;
      ObLogEntry log_entry;
      end_cursor = start_cursor;
      if (NULL == log_data || data_len <= 0 || !start_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument, log_data=%p, data_len=%ld, start_cursor=%s",
                  log_data, data_len, start_cursor.to_str());
      }

      while (OB_SUCCESS == err && pos < data_len)
      {
        if (OB_SUCCESS != (err = log_entry.deserialize(log_data, data_len, pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, pos, err);
        }
        else if (OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
        }
        else
        {
          tmp_pos = pos;
        }

        if (OB_SUCCESS != err)
        {}
        else if (OB_LOG_SWITCH_LOG == log_entry.cmd_
                 && !(OB_SUCCESS == (err = serialization::decode_i64(log_data, data_len, tmp_pos, (int64_t*)&file_id)
                                     && start_cursor.file_id_ + 1 == file_id)))
        {
          TBSYS_LOG(ERROR, "decode switch_log failed(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, tmp_pos, err);
        }
        else
        {
          pos += log_entry.get_log_data_len();
          if (OB_SUCCESS != (err = end_cursor.advance(log_entry)))
          {
            TBSYS_LOG(ERROR, "end_cursor[%ld].advance(%ld)=>%d", end_cursor.log_id_, log_entry.seq_, err);
          }
        }
      }
      if (OB_SUCCESS == err && pos != data_len)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "pos[%ld] != data_len[%ld]", pos, data_len);
      }

      if (OB_SUCCESS != err)
      {
        //hex_dump(log_data, static_cast<int32_t>(data_len), TBSYS_LOG_LEVEL_WARN);
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase


int64_t ObUpsLogMgr::FileIdBeforeLastMajorFrozen::get()
{
  uint64_t file_id = 0;
  ObUpdateServerMain *ups = ObUpdateServerMain::get_instance();
  if (NULL != ups)
  {
    file_id = ups->get_update_server().get_sstable_mgr().get_last_major_frozen_clog_file_id();
  }
  return (0 < file_id) ? (file_id - 1) : file_id;
}
//mod by wangdonghui 20161017 [ups_replication]:b:e
ObUpsLogMgr::ObUpsLogMgr(): log_buffer_for_get_(LOG_BUFFER_SIZE),log_buffer_for_fetch_(LOG_BUFFER_SIZE), log_buffer_for_replay_(LOG_BUFFER_SIZE)
{
  table_mgr_ = NULL;
  role_mgr_ = NULL;
  stop_ = false;
  last_receive_log_time_ = 0;
  last_receive_log_id_ = 0; //add lbzhong[Clog Monitor] 20151218:b:e
  //add by wangjiahao [Paxos temp_log] 20150629 :b
  last_cpoint_flush_time_ = 0;
  last_wrote_commit_point_ = 0; //add lbzhong[Clog Monitor] 20151218:b:e
  pos = 0;
  tmp_stop_ = false;
  last_flush_time_ = 0; //add by wangdonghui [ups_replication_optimize] 20161009 :b:e
  //add :e
  master_getter_ = NULL;
  master_log_id_ = 0;
  max_log_id_ = 0;
  local_max_log_id_when_start_ = -1;
  is_initialized_ = false;
  log_dir_[0] = '\0';
  is_started_ = false;
}

ObUpsLogMgr::~ObUpsLogMgr()
{
}

bool ObUpsLogMgr::is_inited() const
{
  return is_initialized_;
}

//mod shili [LONG_TRANSACTION_LOG]  20160926:b
//int ObUpsLogMgr::init(const char* log_dir, const int64_t log_file_max_size,
//                      ObLogReplayWorker* replay_worker, ObReplayLogSrc* replay_log_src, ObUpsTableMgr* table_mgr,
//                      ObUpsSlaveMgr *slave_mgr, ObiRole* obi_role, ObUpsRoleMgr *role_mgr, int64_t log_sync_type)
int ObUpsLogMgr::init(const char* log_dir, const int64_t log_file_max_size,
                      ObLogReplayWorker* replay_worker, ObReplayLogSrc* replay_log_src, ObUpsTableMgr* table_mgr,
                      ObUpsSlaveMgr *slave_mgr, ObiRole* obi_role, ObUpsRoleMgr *role_mgr,ObBigLogWriter *big_log_writer, int64_t log_sync_type)
//mod e
{

  int ret = OB_SUCCESS;
  int64_t len = 0;

  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsLogMgr has been initialized");
    ret = OB_INIT_TWICE;
  }
  else if (NULL == log_dir || NULL == replay_log_src || NULL == table_mgr
           || NULL == slave_mgr || NULL == obi_role || NULL == role_mgr
            ||NULL== big_log_writer)//add shili [LONG_TRANSACTION_LOG]  20160926
  {
    TBSYS_LOG(ERROR, "Invalid argument[log_dir=%p replay_log_src=%p, table_mgr=%p]"
              "[slave_mgr=%p obi_role=%p, role_mgr=%p]",
              log_dir, replay_log_src, table_mgr, slave_mgr, obi_role, role_mgr);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 >= (len = snprintf(log_dir_, sizeof(log_dir_), "%s", log_dir)) || len >= (int64_t)sizeof(log_dir_))
  {
    TBSYS_LOG(ERROR, "Argument is invalid[log_dir_len=%ld log_dir=%s]", len, log_dir);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = recent_log_cache_.init()))
  {
    TBSYS_LOG(ERROR, "recent_log_cache.init()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = pos_log_reader_.init(log_dir)))
  {
    TBSYS_LOG(ERROR, "pos_log_reader.init(log_dir=%s)=>%d", log_dir, ret);
  }
  else if (OB_SUCCESS != (ret = cached_pos_log_reader_.init(&pos_log_reader_, &recent_log_cache_)))
  {
    TBSYS_LOG(ERROR, "cached_pos_log_reader_.init(pos_log_reader=%p, log_buf=%p)=>%d",
              &pos_log_reader_, &recent_log_cache_, ret);
  }
  else if (OB_SUCCESS != (ret = replay_local_log_task_.init(this)))
  {
    TBSYS_LOG(ERROR, "reaplay_local_log_task.init()=>%d", ret);
  }
  //add wangjiahao [Paxos] 20150719 :b
  else if (OB_SUCCESS != (ret = replay_tmp_log_task_.init(this)))
  {
    TBSYS_LOG(ERROR, "reaplay_tmp_log_task.init()=>%d", ret);
  }
  //add :e
  else if (OB_SUCCESS != (ret = replay_point_.init(log_dir_)))
  {
    TBSYS_LOG(ERROR, "replay_point_file.init(log_dir=%s)=>%d", log_dir_, ret);
  }
  //add by wangjiahao [Paxos temp_log] 20150629 :b
  //***TODO_wangjiahao modify ups_commitpoint dir to a variable***
  else if (OB_SUCCESS != (ret = commit_point_.init("data/ups_commitlog", "commit_point")))
  {
    TBSYS_LOG(ERROR, "commit_point_file.init(log_dir=%s)=>%d", log_dir_, ret);
  }
  else if (OB_SUCCESS != (ret = disk_region_reader_.init(log_dir)))
  {
    TBSYS_LOG(ERROR, "pos_log_reader.init(log_dir=%s)=>%d", log_dir, ret);
  }
  else if (OB_SUCCESS != (ret = local_reader_.init(&disk_region_reader_, &recent_log_cache_)))
  {
    TBSYS_LOG(ERROR, "cached_pos_log_reader_.init(pos_log_reader=%p, log_buf=%p)=>%d",
              &pos_log_reader_, &recent_log_cache_, ret);
  }
  //add :e

  if (OB_SUCCESS == ret)
  {
    ObSlaveMgr *slave = reinterpret_cast<ObSlaveMgr*>(slave_mgr);
    ret = ObLogWriter::init(log_dir_, log_file_max_size, slave, log_sync_type, &last_fid_before_frozen_, &UPS.get_self());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObLogWriter init failed[ret=%d]", ret);
    }
    else
    {
      table_mgr_ = table_mgr;
      replay_worker_ = replay_worker;
      replay_log_src_ = replay_log_src;
      obi_role_ = obi_role;
      role_mgr_ = role_mgr;
      is_initialized_ = true;
      replay_worker_->start();
      big_log_writer_=big_log_writer;//add shili [LONG_TRANSACTION_LOG]  20160926
      TBSYS_LOG(INFO, "ObUpsLogMgr[this=%p] init succ", this);
    }
  }

  return ret;
}

int64_t ObUpsLogMgr::to_string(char* buf, const int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buf, len, pos, "LogMgr(master_id=%ld, worker=%s, rlog_cache=%s, flushed_cursor_=%ld, tmp_log_cursor_=%ld)", master_log_id_, to_cstring(*replay_worker_), to_cstring(recent_log_cache_)
                  //add wangjiahao [Paxos ups_replication] 20150817 :b
                  , flushed_cursor_.log_id_, tmp_log_cursor_.log_id_
                  //add:e
                  );
  return pos;
}

int ObUpsLogMgr::write_replay_point(uint64_t replay_point)
{
  int ret = 0;

  if (OB_SUCCESS != (ret = check_inner_stat()))
  {
    TBSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = replay_point_.write(replay_point)))
  {
    TBSYS_LOG(ERROR, "replay_point_file.write(%lu)=>%d", replay_point, ret);
  }
  return ret;
}

int ObUpsLogMgr::add_slave(const ObServer& server, uint64_t &new_log_file_id, const bool switch_log
                           //add wangjiahao [Paxos ups_replication] 20150817:b
                           , const int64_t log_id
                           //add:e
                           )
{
  int ret = OB_SUCCESS;

  ObSlaveMgr* slave_mgr = get_slave_mgr();
  if (NULL == slave_mgr)
  {
    TBSYS_LOG(ERROR, "slave_mgr is NULL");
    ret = OB_ERROR;
  }
  else
  {
    //mod wangjiahao [Paoxs ups_replication] 20150817 :b
    //ret = slave_mgr->add_server(server);
    ret = slave_mgr->add_server_with_log_id(server, log_id);
    //mod :e
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObSlaveMgreadd_server error[ret=%d]", ret);
    }
    else
    {
      if (true) // 现在的日志同步方案不用切日志了
      {}
      else
      if (switch_log)
      {
        ret = switch_log_file(new_log_file_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "switch_log_file error[ret=%d]", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "log mgr switch log file succ");
        }
      }
      else
      {
        //take new_log_file_id as slave_slave_ups's send_log_point
        ret = slave_mgr->set_send_log_point(server, new_log_file_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "fail to set_send_log_point to slave. slave=%s", server.to_cstring());
        }
        else
        {
          TBSYS_LOG(INFO, "slave [%s] send log point [%ld]", server.to_cstring(), new_log_file_id);
        }
      }
    }
  }

  return ret;
}

int ObUpsLogMgr::add_log_replay_event(const int64_t seq, const int64_t mutation_time, ReplayType replay_type)
{
  return delay_stat_.add_log_replay_event(seq, mutation_time, master_log_id_, replay_type);
}

// 一定从一个文件的开始重放，只能调用一次，
// 可能没有日志或日志不连续，这时重放本地日志实际上什么都不用做，
// 重放完之后replayed_cursor_可能依然无效
int ObUpsLogMgr::replay_local_log()
{
  int err = OB_SUCCESS;
  ObLogCursor end_cursor;
  int64_t clog_point = 0;
  uint64_t log_file_id_by_sst = get_max_file_id_by_sst();
  if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  else if (replay_worker_->get_replayed_log_id() > 0)
  {
    TBSYS_LOG(WARN, "local log is already replayed: cur_cursor=%ld", replay_worker_->get_replayed_log_id());
    err = OB_ALREADY_DONE; // 已经重放过了
  }
  //add wangjiahao [Paxos ups_replication_tmplog] 20150713 :b
  else if (OB_SUCCESS != (err = commit_point_.get(clog_point)) && err != OB_FILE_NOT_EXIST)
  {
    TBSYS_LOG(ERROR, "get_commit_point_func(log_dir=%s)=>%d", log_dir_, err);
  }
  else if (OB_SUCCESS != (err = slave_mgr_->set_acked_clog_id(clog_point)))
  {
    TBSYS_LOG(ERROR, "set_acked_clog_id(clog_point=%ld)=>%d", clog_point, err);
  }
  //add :e
  else if (OB_INVALID_ID != log_file_id_by_sst)
  {
    start_cursor_.file_id_ = (int64_t)log_file_id_by_sst;
  }
  else if (OB_SUCCESS != (err = replay_point_.get(start_cursor_.file_id_)))
  {
    TBSYS_LOG(ERROR, "get_replay_point_func(log_dir=%s)=>%d", log_dir_, err);
  }
  TBSYS_LOG(INFO, "get_commit_point(log_id=%ld)", clog_point);
  TBSYS_LOG(INFO, "get_replay_point(file_id=%ld)", start_cursor_.file_id_);

  //mod wangjiahao [Paxos ups_replication_tmplog] 20150713 :b
  //add parameter clog_point
  // 可能会有单个空文件存在
  //mod wangdonghui [ups_replication] 20170612 :b
  //if (OB_SUCCESS != err || start_cursor_.file_id_ <= 0 || clog_point <=0)
  if (OB_SUCCESS != err || start_cursor_.file_id_ <= 0)
  //mod :e
  {}
  else if (OB_SUCCESS != (err = replay_local_log_func(stop_, log_dir_, start_cursor_, end_cursor, *replay_worker_, clog_point, tmp_log_cursor_))
           && OB_ENTRY_NOT_EXIST != err)
  //mod :e
  {
    if (OB_CANCELED == err)
    {
      TBSYS_LOG(WARN, "replay_log_func(log_dir=%s, start_cursor=%s): CANCELD", log_dir_, start_cursor_.to_str());
    }
    else
    {
      TBSYS_LOG(ERROR, "replay_log_func(log_dir=%s, start_cursor=%s)=>%d", log_dir_, start_cursor_.to_str(), err);
    }
  }
  else if (end_cursor.log_id_ <= 0
           && OB_SUCCESS != (err = get_local_max_log_cursor_func(log_dir_, get_max_file_id_by_sst(), end_cursor)))
  {
    TBSYS_LOG(ERROR, "get_local_max_log_cursor(log_dir=%s)=>%d", log_dir_, err);
  }
  else if (end_cursor.log_id_ <= 0)
  {
    TBSYS_LOG(WARN, "replayed_cursor.log_id[%ld] <= 0 after replay local log", end_cursor.log_id_);
  }
  //add wangjiahao [Paxos ups_replication_tmplog] 20150715
  // try to write end_of_log failed
  /*
  else
  {
      tmp_cursor = end_cursor;
      tmp_cursor.log_id_ ++;
      tmp_cursor.offset_ += LOG_FILE_ALIGN_SIZE;
  }
  if (err != OB_SUCCESS)
  {}
  else if (OB_SUCCESS != (err =  log_writer_.write(end_cursor, tmp_cursor, ObLogGenerator::eof_flag_buf_, LOG_FILE_ALIGN_SIZE)))
  {
    TBSYS_LOG(ERROR, "WJH_ERROR try to write log end failed err=%d", err);
  }
  */
  //add :e
  else if (OB_SUCCESS != (err = start_log(end_cursor)))
  {
    TBSYS_LOG(ERROR, "start_log(cursor=%s)=>%d", end_cursor.to_str(), err);
  }
  else
  {
    TBSYS_LOG(INFO, "start_log_after_replay_local_log(replay_cursor=%s): OK.", end_cursor.to_str());
  }

  // 在UPS主循环中调用start_log_for_master_write()并设置状态为ACTIVE
  // if (OB_SUCCESS != err || !is_master_master())
  // {}
  // else if (OB_SUCCESS != (err = start_log_for_master_write()))
  // {
  //   TBSYS_LOG(ERROR, "start_log_for_master_write()=>%d", err);
  // }
  // else
  // {
  //   set_state_as_active();
  // }
  if (OB_ALREADY_DONE == err || err == OB_FILE_NOT_EXIST)
  {
    err = OB_SUCCESS;
  }
  if (OB_SUCCESS != err)
  {
    role_mgr_->set_state(ObUpsRoleMgr::FATAL);
  }
  return err;
}

//add wangjiahao [Paxos ups_replication_tmplog] 20150719 :b
int ObUpsLogMgr::replay_tmp_log()
{
    int err = OB_SUCCESS;
    ObLogCursor end_cursor;
    if (OB_SUCCESS != (err = replay_tmp_log_func(tmp_stop_, log_dir_, end_cursor, *replay_worker_))
            && OB_ENTRY_NOT_EXIST != err)
    {
     if (OB_CANCELED == err)
     {
       TBSYS_LOG(WARN, "replay_log_func(log_dir=%s): CANCELD", log_dir_);
     }
     else
     {
       TBSYS_LOG(ERROR, "replay_log_func(log_dir=%s)=>%d", log_dir_, err);
     }
    }
    else if (end_cursor.log_id_ <= 0)
    {
      TBSYS_LOG(WARN, "replay_tmp_log.log_id[%ld] <= 0 after replay local log", end_cursor.log_id_);
    }
    else if (OB_SUCCESS != (err = start_log(end_cursor)))
    {
      TBSYS_LOG(ERROR, "start_log(cursor=%s)=>%d", end_cursor.to_str(), err);
    }
    //add wangdonghui [ups_replication] 20170713:b
    else if (OB_SUCCESS != (err = write_cpoint(end_cursor.log_id_, true)))
    {
    TBSYS_LOG(WARN, "master write commit point fail");
    }
    //add:e
    return err;
}
//add :e

// 备切换为主时需要调用一次
int ObUpsLogMgr::start_log_for_master_write()
{
  int err = OB_SUCCESS;
  const bool allow_missing_log_file = true;
  ObLogCursor start_cursor;
  set_cursor(start_cursor, (start_cursor_.file_id_ > 0)? start_cursor_.file_id_: 1, 1, 0);
  if (start_cursor_.log_id_ > 0)
  {
    TBSYS_LOG(INFO, "start_log_for_master(replay_cursor=%s): ALREADY STARTED", start_cursor_.to_str());
  }
  else if (!allow_missing_log_file && start_cursor_.file_id_ > 0)
  {
    err = OB_LOG_MISSING;
    TBSYS_LOG(ERROR, "missing log_file[file_id=%ld]", start_cursor_.file_id_);
  }
  else if (OB_SUCCESS != (err = start_log(start_cursor)))
  {
    TBSYS_LOG(ERROR, "start_log()=>%d", err);
  }
  else
  {
    TBSYS_LOG(INFO, "start_log_for_master(replay_cursor=%s): STARTE OK.", start_cursor_.to_str());
  }
  return err;
}

bool ObUpsLogMgr::is_slave_master() const
{
  return NULL != obi_role_ && NULL != role_mgr_
    && ObiRole::SLAVE == obi_role_->get_role() &&  ObUpsRoleMgr::MASTER == role_mgr_->get_role();
}

bool ObUpsLogMgr::is_master_master() const
{
  return NULL != obi_role_ && NULL != role_mgr_
    && ObiRole::MASTER == obi_role_->get_role() &&  ObUpsRoleMgr::MASTER == role_mgr_->get_role();
}

int ObUpsLogMgr::get_replayed_cursor(ObLogCursor& cursor) const
{
  return replay_worker_->get_replay_cursor(cursor);
}
//add wangdonghui [ups_replication] 20161222 :b
int ObUpsLogMgr::reset_replayed_cursor() const
{
  return replay_worker_->reset_replay_cursor();
}
//add :e
//add wangjiahao [Paxos ups_replication_tmplog] 20150718 :b
int ObUpsLogMgr::get_cmt_log_cursor(ObLogCursor& cursor) const
{
  int err = OB_SUCCESS;
  if (NULL == role_mgr_)
  {
    err = OB_NOT_INIT;
  }
  else
  {
    cursor = flushed_cursor_;
  }
  return err;
}
//add :e

ObLogCursor& ObUpsLogMgr::get_replayed_cursor_(ObLogCursor& cursor) const
{
  replay_worker_->get_replay_cursor(cursor);
  return cursor;
}

int ObUpsLogMgr::set_state_as_active()
{
  int err = OB_SUCCESS;
  ObLogCursor replay_cursor;
  if (NULL == role_mgr_)
  {
    err = OB_NOT_INIT;
  }
  else if (ObUpsRoleMgr::ACTIVE != role_mgr_->get_state())
  {
    TBSYS_LOG(INFO, "set ups state to be ACTIVE. master_log_id=%ld, log_cursor=%s",
              master_log_id_, to_cstring(get_replayed_cursor_(replay_cursor)));
    role_mgr_->set_state(ObUpsRoleMgr::ACTIVE);
  }
  return err;
}

int ObUpsLogMgr::write_log_as_slave(const char* buf, const int64_t len)
{
  int err = OB_SUCCESS;
  bool need_send_log = is_slave_master();
  if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (err = store_log(buf, len, need_send_log)))
  {
    TBSYS_LOG(ERROR, "store_log(%p[%ld], sync_to_slave=%s)=>%d", buf, len, STR_BOOL(need_send_log), err);
  }
  return err;
}
//mod wangdonghui [ups_replication] 20161213 :b
int ObUpsLogMgr::replay_and_write_log(const int64_t start_id, const int64_t end_id, const char* buf, int64_t len)
{
  int err = OB_SUCCESS;
  ObLogCursor start_cursor;
  ObLogCursor end_cursor;
  int64_t real_end_id = 0;
  UNUSED(start_id);
  UNUSED(end_id);
  if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  else if (NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "replay_and_write_log(buf=%p[%ld])", buf, len);
  }
  else if (0 == len)
  {}
  else if (len & (OB_DIRECT_IO_ALIGN-1))
  {
    err = OB_LOG_NOT_ALIGN;
    TBSYS_LOG(ERROR, "len[%ld] is not aligned, start_id=%ld", len, start_id);
  }
  else if (OB_SUCCESS != (err = replay_worker_->get_replay_cursor(start_cursor)))
  {
    TBSYS_LOG(ERROR, "get_replay_cursor()=>%d", err);
  }
  else if (OB_SUCCESS != (err = parse_log_buffer(buf, len, start_cursor, end_cursor)))
  {
    TBSYS_LOG(ERROR, "parse_log_buffer(log_data=%p, data_len=%ld, log_cursor=%s)=>%d",
              buf, len, start_cursor.to_str(), err);
  }
  else if (OB_SUCCESS != (err = replay_worker_->submit_batch(real_end_id, buf, len, RT_APPLY)))
  {
    TBSYS_LOG(ERROR, "replay_worker.submit_batch(buf=%p[%ld])=>%d", buf, len, err);
  }
  return err;
}
//int ObUpsLogMgr::replay_and_write_log(const int64_t start_id, const int64_t end_id, const char* buf, int64_t len)
//{
//  int err = OB_SUCCESS;
//  ObLogCursor start_cursor = flushed_cursor_;
//  ObLogCursor end_cursor;
//  int64_t real_end_id = 0;
//  if (!is_inited())
//  {
//    err = OB_NOT_INIT;
//  }
//  else if (NULL == buf || 0 > len)
//  {
//    err = OB_INVALID_ARGUMENT;
//    TBSYS_LOG(ERROR, "replay_and_write_log(buf=%p[%ld])", buf, len);
//  }
//  else if (0 == len)
//  {}
//  else if (len & (OB_DIRECT_IO_ALIGN-1))
//  {
//    err = OB_LOG_NOT_ALIGN;
//    TBSYS_LOG(ERROR, "len[%ld] is not aligned, start_id=%ld", len, start_id);
//  }
//  else if (OB_SUCCESS != (err = parse_log_buffer(buf, len, start_cursor, end_cursor)))
//  {
//    TBSYS_LOG(ERROR, "parse_log_buffer(log_data=%p, data_len=%ld, log_cursor=%s)=>%d",
//              buf, len, start_cursor.to_str(), err);
//  }
//  else if(OB_SUCCESS != (err = write_log_as_slave(buf, len)))
//  {
//    TBSYS_LOG(ERROR, "write_log_as_slave(buf=%p[%ld])=>%d", buf, len, err);
//  }
//  else
//  {
//    flushed_cursor_ = end_cursor;
//    tmp_log_cursor_ = end_cursor;
//    replay_worker_->set_next_flush_log_id(end_id);
//  }
//  if( OB_SUCCESS != err){}
//  else if (OB_SUCCESS != write_cpoint(master_max_commit_log_id_))
//  {
//    TBSYS_LOG(WARN, "write_cpoint(master_max_commit_log_id_=%ld)=>", master_max_commit_log_id_);
//  }
//  else if (OB_SUCCESS != (err = replay_worker_->submit_batch(real_end_id, buf, len, RT_APPLY)))
//  {
//    TBSYS_LOG(ERROR, "replay_worker.submit_batch(buf=%p[%ld])=>%d", buf, len, err);
//  }
//  return err;
//}
// mod:e

int ObUpsLogMgr::set_master_log_id(const int64_t master_log_id)
{
  int err = OB_SUCCESS;
  if (master_log_id < master_log_id_)
  {
    TBSYS_LOG(WARN, "master_log_id[%ld] < master_log_id_[%ld]", master_log_id, master_log_id_);
  }
  set_counter(master_log_id_cond_, master_log_id_, master_log_id);
  return err;
}

int ObUpsLogMgr::slave_receive_log(const char* buf, int64_t len, const int64_t wait_sync_time_us,
                                   //add chujiajia [Paxos ups_replication]
                                   const int64_t cmt_log_id,
                                   //add:e
                                   const WAIT_SYNC_TYPE wait_event_type)
{
  UNUSED(wait_sync_time_us);
  UNUSED(wait_event_type);
  int err = OB_SUCCESS;
  int64_t start_id = 0;
  int64_t end_id = 0;
  int64_t last_log_term = OB_INVALID_LOG;
  if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  else if (NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "slave_receive_log(buf=%p[%ld]): invalid argument.", buf, len);
  }
  else if (OB_SUCCESS != (err = parse_log_buffer(buf, len, start_id, end_id, last_log_term))) //mod1 wangjiahao [Paxos ups_replication_tmplog] 20150804 add last_log_term
  {
    TBSYS_LOG(ERROR, "parse_log_buffer(log_data=%p[%ld])=>%d", buf, len, err);
  }
  //add chujiajia [Paxos ups_replication] 20160106:b
  else if (end_id < master_max_commit_log_id_)
  {
    err = OB_INVALID_LOG;
    TBSYS_LOG(WARN, "last_log_term < current_log_term, Invalid log, end_id=%ld, err=%d", end_id, err);
  }
  else if (cmt_log_id >= 0 && OB_SUCCESS != (err = set_master_max_commit_log_id(cmt_log_id)))
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "set_master_max_commit_log_id error! end_id=%ld, err=%d", end_id, err);
  }
  //add:e
  //add wangjiahao [Paxos ups_replication_tmplog] 20150804 :b
  else if (OB_SUCCESS != (err = set_current_log_term(last_log_term)))
  {
    TBSYS_LOG(WARN, "fail to set_current_log_term(%ld)=>%d", last_log_term, err);
  }
  //add :e
  else if (OB_SUCCESS != (err = append_to_log_buffer(&recent_log_cache_, start_id, end_id, buf, len)))
  {
    TBSYS_LOG(ERROR, "append_to_log_buffer(log_id=[%ld,%ld))=>%d", start_id, end_id, err);
  }
  else
  {
  //del wangdonghui [ups_log_replication_optimize] 20161009 :b
    /*int64_t next_flush_log_id = 0;
    int64_t next_commit_log_id = 0;*/
	//del:e
    set_master_log_id(end_id);
    last_receive_log_time_ = tbsys::CTimeUtil::getTime();
    last_receive_log_id_ = end_id; //add lbzhong[Clog Monitor] 20151218:b:e
    //del wangdonghui [ups_log_replication_optimize] 20161009 :b
    /*
    if (wait_sync_time_us <= 0 || WAIT_NONE == wait_event_type)
    {}
    else if (ObUpsRoleMgr::ACTIVE != role_mgr_->get_state())
    {
      //mod wangjiahao [Paxos ups_replication_bugfix] 20150730 :b
      TBSYS_LOG(WARN, "recv log(log=[%ld,%ld)). But slave state not ACTIVE, do not wait flush until log catch up master", start_id, end_id);
      //add wangjiahao [Paxos ups_replication_bugfix] 20150730 :b
      if (ObUpsRoleMgr::STOP == role_mgr_->get_state() || ObUpsRoleMgr::FATAL == role_mgr_->get_state())
      {
          err = OB_ERR_UNEXPECTED;
      }
      err = OB_LOG_NOT_SYNC;
      //add :e
    }
    else if (WAIT_COMMIT == wait_event_type)
    {
      if (end_id > (next_commit_log_id = replay_worker_->wait_next_commit_log_id(end_id, wait_sync_time_us)))
      {
        TBSYS_LOG(WARN, "wait_flush_log_id(end_id=%ld, next_flush_log_id=%ld, timeout=%ld) Fail.",
                  end_id, next_commit_log_id, wait_sync_time_us);
      }
    }
    else if (WAIT_FLUSH == wait_event_type)
    {
      if (end_id > (next_flush_log_id = replay_worker_->wait_next_flush_log_id(end_id, wait_sync_time_us)))
      {
        TBSYS_LOG(WARN, "wait_flush_log_id(end_id=%ld, next_flush_log_id=%ld, timeout=%ld) Fail.",
                  end_id, next_flush_log_id, wait_sync_time_us);
        //add wangjiahao [Paxos ups_replication_tmplog] 20150716 :b
        err = OB_LOG_NOT_SYNC;
        //add :e
      }
      else
      {
        int64_t end_time = tbsys::CTimeUtil::getTime();
        TBSYS_LOG(INFO, "wangdonghui: log_range [%ld, %ld] slave whole time[%ld], begin:[%ld], end:[%ld]",
                  start_id, end_id, end_time - receive_time, receive_time, end_time);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "unknown wait_event_type=%d", wait_event_type);
    }
    */
    //del :e
  }

  return err;
}

int ObUpsLogMgr::get_log_for_slave_fetch(ObFetchLogReq& req, ObFetchedLog& result)
{
  int err = OB_SUCCESS;
  ThreadSpecificBuffer::Buffer* my_buffer = log_buffer_for_fetch_.get_buffer();
  if (NULL == my_buffer)
  {
    err = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "get_thread_buffer fail.");
  }
  else
  {
    my_buffer->reset();
  }
  if (OB_SUCCESS != err)
  {}
  else if (OB_SUCCESS != (err = result.set_buf(my_buffer->current(), my_buffer->remain())))
  {
    TBSYS_LOG(ERROR, "result.set_buf(buf=%p[%d]): INVALID_ARGUMENT", my_buffer->current(), my_buffer->remain());
  }
  else if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  //mod wangjiahao [Paxos ups_replication] 20150817 :b
  //else if (0 >= req.start_id_ || req.start_id_ >= replay_worker_->get_replayed_log_id())
  else if (0 >= req.start_id_ || req.start_id_ >= tmp_log_cursor_.log_id_)
  //mod :e
  {
    TBSYS_LOG(DEBUG, "get_log_for_slave_fetch data not server. req.start_id_=%ld, replayed_cursor_=%ld",
              req.start_id_, replay_worker_->get_replayed_log_id());
    err = OB_DATA_NOT_SERVE;
  }
  if (OB_SUCCESS == err && OB_SUCCESS != (err = cached_pos_log_reader_.get_log(req, result))
      && OB_DATA_NOT_SERVE != err)
  {
    TBSYS_LOG(ERROR, "cached_pos_log_reader.get_log(log_id=%ld)=>%d", req.start_id_, err);
  }
  if (OB_SUCCESS == err && result.data_len_ & (OB_DIRECT_IO_ALIGN-1))
  {
    // FIXME: XiuMing 2014-07-11
    // Disable "LOG NOT ALIGN" checking and set LOG level to WARN for LIBOBLOG
    // err = OB_LOG_NOT_ALIGN;
    TBSYS_LOG(WARN, "result.data_len[%ld] is not aligned. req='%s', result='%s'",
        result.data_len_, to_cstring(req), to_cstring(result));
  }
  TBSYS_LOG(DEBUG, "get_log_for_slave_fetch(start_id=%ld)=>%d", req.start_id_, err);
  return err;
}

int ObUpsLogMgr::fill_log_cursor(ObLogCursor& log_cursor)
{
  int err = OB_SUCCESS;
  if (log_cursor.log_id_ == 0 && log_cursor.file_id_ == 0)
  {
    log_cursor.file_id_ =  max(get_last_major_frozen_log_file_id(log_dir_), 1);
  }
  if (log_cursor.log_id_ == 0 && log_cursor.file_id_ > 0
      && OB_SUCCESS != (err = get_first_log_id_func(log_dir_, log_cursor.file_id_, log_cursor.log_id_))
      && OB_ENTRY_NOT_EXIST != err)
  {
    TBSYS_LOG(ERROR, "get_first_log_id_func()=>%d", err);
  }
  else if (OB_ENTRY_NOT_EXIST == err)
  {
    err = OB_SUCCESS;
  }
  TBSYS_LOG(TRACE, "fill_log_cursor[for slave](log_cursor=%s)=>%d", log_cursor.to_str(), err);
  return err;
}
//TODO_wangjiahao check if ObLogWriter::start_log modifyed correct
int ObUpsLogMgr::start_log(const ObLogCursor& start_cursor)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = ObLogWriter::start_log(start_cursor)))
  {
    TBSYS_LOG(ERROR, "start_log(start_cursor=%s)=>%d", start_cursor.to_str(), err);
  }
  else if (OB_SUCCESS != (err = replay_worker_->start_log(start_cursor)))
  {
    TBSYS_LOG(ERROR, "replay_worker_.start_log(start_cursor=%s)=>%d", start_cursor.to_str(), err);
  }
  else
  {
    start_cursor_ = start_cursor;
    //add wangjiahao [Paxos ups_replication_tmplog] 20150711 :b
    flushed_cursor_ = start_cursor;
    //add :e
  }
  return err;
}

int ObUpsLogMgr::start_log_for_replay()
{
  int err = OB_SUCCESS;
  if (start_cursor_.log_id_ > 0)
  {
    //TBSYS_LOG(INFO, "start_log_for_replay(replayed_cursor=%s): ALREADY STARTED.", replayed_cursor_.to_str() );
  }
  else if (OB_SUCCESS != (err = replay_log_src_->get_remote_log_src().fill_start_cursor(start_cursor_))
      && OB_NEED_RETRY != err)
  {
    TBSYS_LOG(ERROR, "fill_start_cursor(replayed_cursor=%s)=>%d", start_cursor_.to_str(), err);
  }
  else if (OB_SUCCESS == err && 0 >= start_cursor_.log_id_)
  {
    err = OB_NEED_RETRY;
  }
  else if (OB_NEED_RETRY == err)
  {}
  else if (OB_SUCCESS != (err = start_log(start_cursor_)))
  {
    TBSYS_LOG(ERROR, "start_log(start_cursor=%s)=>%d", start_cursor_.to_str(), err);
  }
  else
  {
    TBSYS_LOG(INFO, "start_log_for_replay(replayed_cursor=%s): STARTED OK.", start_cursor_.to_str());
  }
  return err;
}

bool ObUpsLogMgr::is_sync_with_master() const
{
  return 0 >= master_log_id_ || replay_worker_->get_replayed_log_id() >= master_log_id_;
}

static bool in_range(const int64_t x, const int64_t lower, const int64_t upper)
{
  return x >= lower && x < upper;
}

bool ObUpsLogMgr::has_nothing_in_buf_to_replay() const
{
  return !in_range(replay_worker_->get_replayed_log_id(), recent_log_cache_.get_start_id(), recent_log_cache_.get_end_id())
    && replay_worker_->is_all_task_finished();
}
//add wangjiahao [Paxos ups_replication_tmplog] 20150716 :b
bool ObUpsLogMgr::has_nothing_to_replay() const
{
  return replay_worker_->is_all_task_finished();
}
//add :e
bool ObUpsLogMgr::has_log_to_replay() const
{
  return replay_log_src_->get_remote_log_src().is_using_lsync() || master_log_id_ > replay_worker_->get_replayed_log_id();
}

int64_t ObUpsLogMgr::wait_new_log_to_replay(const int64_t timeout_us)
{
  return replay_log_src_->get_remote_log_src().is_using_lsync()?
    -1: wait_counter(master_log_id_cond_, master_log_id_, replay_worker_->get_next_flush_log_id() + 1, timeout_us);
}
//mod wangdonghui [ups_replication] 20161213 :b
// 可能返回OB_NEED_RETRY;
//mod wangjiahao [Paxos ups_replication_tmplog] 20150711 :b
int ObUpsLogMgr::replay_log()
{
  int err = OB_SUCCESS;
  int64_t end_id = 0;
  ObServer server;
  ObLogCursor replay_cursor, en_cursor;
  ThreadSpecificBuffer::Buffer* my_buffer = log_buffer_for_get_.get_buffer();  //for get log
  ThreadSpecificBuffer::Buffer* my_buffer2 = log_buffer_for_replay_.get_buffer();  // for reply log
  char* buf_for_reply = NULL;
  int64_t len_for_reply = 0;
  char* buf = NULL;
  int64_t len = 0;
  int64_t read_count = 0;
  //int64_t cur_time = tbsys::CTimeUtil::getTime();
  if (NULL == my_buffer)
  {
    TBSYS_LOG(ERROR, "get thread specific buffer fail");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    my_buffer->reset();
    buf = my_buffer->current();
    len = my_buffer->remain();
  }

  if (NULL == my_buffer2)
  {
    TBSYS_LOG(ERROR, "get thread specific buffer fail");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    my_buffer2->reset();
    buf_for_reply = my_buffer2->current();
    len_for_reply = my_buffer2->remain();
  }

  if (OB_SUCCESS != err)
  {
    // do nothing
  }
  else if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  else if (ObUpsRoleMgr::STOP == role_mgr_->get_state())
  {
    err = OB_CANCELED;
  }
  else if (ObUpsRoleMgr::FATAL == role_mgr_->get_state())
  {
    err = OB_NEED_RETRY;
  }
  else if (
  !is_log_replay_finished())
  {
    err = OB_NEED_RETRY;
  }
  else if (
  is_master_master() &&  //mod lbzhong [Paxos ups_replication] 20160104
  is_tmp_log_replay_started())
  {
    err = OB_NEED_RETRY;
  }
  else if (replay_worker_->get_replayed_log_id() < 0)
  {
    err = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (err = start_log_for_replay())
           && OB_NEED_RETRY != err)
  {
    TBSYS_LOG(ERROR, "start_log_for_replay()=>%d", err);
  }
  else if (OB_NEED_RETRY == err)
  {
    err = OB_NEED_WAIT;
  }
  else if (!replay_log_src_->get_remote_log_src().is_using_lsync() &&  master_log_id_ <= flushed_cursor_.log_id_)
  {
    err = OB_NEED_RETRY;
  }
  //mod chujiajia [Paxos ups_replication] 20160112:b
  //else if (OB_SUCCESS != write_cpoint(max(flushed_cursor_.log_id_, master_max_commit_log_id_)))  //write once in a while which setted in TODO_wangjiahao
  else if (OB_SUCCESS != write_cpoint(min(flushed_cursor_.log_id_, master_max_commit_log_id_)))
  //mod:e
  {
    TBSYS_LOG(WARN, "write_cpoint(master_max_commit_log_id_=%ld)=>", master_max_commit_log_id_);
    //TODO_wangjiahao if write error many times
  }
  else if (OB_SUCCESS != (err = replay_log_src_->get_log(flushed_cursor_, end_id, buf, len, read_count))//只要没有日志就会去拉
           && OB_DATA_NOT_SERVE != err)
  {
    TBSYS_LOG(ERROR, "get_log(replayed_cursor=%s)=>%d, flushed_cursor_=%s", replay_cursor.to_str(), err, flushed_cursor_.to_str());
  }
  else if (OB_DATA_NOT_SERVE == err)
  {
    err = OB_NEED_WAIT;
  }
  else if (0 == read_count)
  {
    err = OB_NEED_RETRY;
  }
  else if (OB_SUCCESS != (err = parse_log_buffer(buf, read_count, flushed_cursor_, en_cursor)))  //check log
  {
    TBSYS_LOG(ERROR, "parse_log_buffer(log_data=%p, data_len=%ld, log_cursor=%s)=>%d",
              buf, len, flushed_cursor_.to_str(), err);
  }
  //TODO_wangjiahao need to change condition to activate ups
  else if (master_log_id_ > 0 && end_id == master_log_id_ && OB_SUCCESS != (err = set_state_as_active()))
  {
    TBSYS_LOG(ERROR, "set_state_as_active()=>%d", err);
  }
  /*
  else if (OB_SUCCESS != (err = write_log_as_slave(buf, read_count)))    //write to disk as tmp_log
  {
    TBSYS_LOG(ERROR, "slave write log failed. err = %d buf_len=%ld", err, len);
  }
  else
  {
    flushed_cursor_ = en_cursor;
    tmp_log_cursor_ = en_cursor;
    replay_worker_->set_next_flush_log_id(flushed_cursor_.log_id_);
  }
  */
  else if (tmp_log_cursor_.newer_than(flushed_cursor_)) // on disk
  {
      ObLogCursor t_cursor, pass_cursor;
      int64_t data_ondisk_len = 0;
      if (en_cursor.newer_than(tmp_log_cursor_))
      {
        t_cursor = tmp_log_cursor_;
      }
      else
      {
        t_cursor = en_cursor;
      }
      if (OB_SUCCESS != (err = local_reader_.get_log(flushed_cursor_.log_id_, t_cursor.log_id_, buf_for_reply, len_for_reply, OB_MAX_LOG_BUFFER_SIZE, pos, true)))
      {
        TBSYS_LOG(ERROR, "local_reader_.get_log from flushed_id(%ld) to t_cursor.log_id_(%ld) <= tmp_log_id(%ld)", flushed_cursor_.log_id_, t_cursor.log_id_, tmp_log_cursor_.log_id_);
      }
      else if (OB_SUCCESS != (err = compare_log_buffer(flushed_cursor_, end_id, buf_for_reply, buf, t_cursor.offset_ - flushed_cursor_.offset_, pass_cursor))
               && err != OB_INVALID_LOG)
      {
        TBSYS_LOG(ERROR, "compare_log_buffer ERR(%d)", err);
      }
      else if (err == OB_INVALID_LOG)
      {
        TBSYS_LOG(INFO, "compare_log_buffer=>OB_INVALID_LOG pass_cursor %s", pass_cursor.to_str());
        data_ondisk_len = pass_cursor.offset_ - flushed_cursor_.offset_;
        flushed_cursor_ = pass_cursor;
        tmp_log_cursor_ = pass_cursor;
        //TODO_wangjiahao write end_of_log
        err = OB_SUCCESS;
      }
      else
      {
        data_ondisk_len = t_cursor.offset_ - flushed_cursor_.offset_;
        flushed_cursor_ = t_cursor;
      }

      //write the rest
      if (OB_SUCCESS != err)
      {}
      if (OB_SUCCESS != (err = ObLogWriter::start_log(flushed_cursor_)))
      {
        TBSYS_LOG(ERROR, "fail to start_log(flushed_cursor_=%s)=>%d", flushed_cursor_.to_str(), err);
      }
      else if (!en_cursor.newer_than(flushed_cursor_))
      {
        TBSYS_LOG(INFO, "all log is in disk.");
      }
      //del wangdonghui [ups_replication] log in disk may elapse two files　:b
      /*else if (read_count - data_ondisk_len != en_cursor.offset_ - flushed_cursor_.offset_)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "replay log error: read_count - data_ondisk_len != en_cursor.offset_ - flushed_cursor_.offset_");
      }*/
      //del :e
      else if (OB_SUCCESS != (err = write_log_as_slave(buf + data_ondisk_len, read_count - data_ondisk_len)))
      {
        TBSYS_LOG(ERROR, "slave write log failed. err = %d buf_len=%ld", err, read_count - data_ondisk_len);
      }
      else
      {
        flushed_cursor_ = en_cursor;
        tmp_log_cursor_ = en_cursor;
        replay_worker_->set_next_flush_log_id(flushed_cursor_.log_id_);
      }
  }
  else  // not on disk
  {
      if (OB_SUCCESS != (err = write_log_as_slave(buf, read_count)))    //write to disk as tmp_log
      {
        TBSYS_LOG(ERROR, "slave write log failed. err = %d buf_len=%ld", err, len);
      }
      else
      {
        flushed_cursor_ = en_cursor;
        tmp_log_cursor_ = en_cursor;
        replay_worker_->set_next_flush_log_id(flushed_cursor_.log_id_);
      }
  }

//get log to replay  period 2
  my_buffer2->reset();
  int err1 = err;
  err = OB_SUCCESS;
  if (OB_SUCCESS != (err = replay_worker_->get_replay_cursor(replay_cursor)))
  {
    TBSYS_LOG(ERROR, "get_replay_cursor()=>%d", err);
  }
  else if (master_max_commit_log_id_ <= replay_worker_->get_next_submit_log_id() || flushed_cursor_.log_id_ <= replay_worker_->get_next_submit_log_id())
  {
    err = OB_NEED_RETRY;
    //TBSYS_LOG(INFO, "master_max_commit_log_id_ =%ld, replay_worker_->get_next_submit_log_id()=%ld, flushed_cursor_:%s replay_worker_->get_next_submit_log_id():%ld", master_max_commit_log_id_, replay_worker_->get_next_submit_log_id(), flushed_cursor_.to_str(), replay_worker_->get_next_submit_log_id());
  }
  else if (OB_SUCCESS != (err = local_reader_.get_log(replay_cursor.log_id_, end_id = min(master_max_commit_log_id_, flushed_cursor_.log_id_), buf_for_reply, len_for_reply, OB_MAX_LOG_BUFFER_SIZE, pos)))
  {
    TBSYS_LOG(ERROR, "local_reader get log failed. err = %d pos=%ld", err, pos);
  }
  else if (OB_SUCCESS != (err = replay_and_write_log(replay_worker_->get_replayed_log_id(), end_id, buf_for_reply, len_for_reply)))
  {
    TBSYS_LOG(ERROR, "replay_and_write_log(buf=%p[%ld])=>%d", buf, read_count, err);
  }
  if (OB_SUCCESS != err && OB_NEED_RETRY != err && OB_NEED_WAIT != err && OB_CANCELED != err)
  {
    role_mgr_->set_state(ObUpsRoleMgr::FATAL);
    TBSYS_LOG(ERROR, "replay failed, ret=%d, will kill self", err);
    kill(getpid(), SIGTERM);
  }
  else if (err1 != OB_SUCCESS)
  {
      err = err1;
  }
  //TBSYS_LOG(INFO, "master_max_commit_log_id_ =%ld, replay_worker_->get_next_submit_log_id()=%ld, flushed_cursor_:%s replay_worker_->get_next_submit_log_id():%ld", master_max_commit_log_id_, replay_worker_->get_next_submit_log_id(), flushed_cursor_.to_str(), replay_worker_->get_next_submit_log_id());
  //TBSYS_LOG(INFO, "err=%d", err);
  return err;
}

//int ObUpsLogMgr::replay_log()
//{
//  int err = OB_SUCCESS;
//  int64_t end_id = 0;
//  //ObLogCursor en_cursor;
//  ThreadSpecificBuffer::Buffer* my_buffer = log_buffer_for_get_.get_buffer();  //for get log
//  char* buf = NULL;
//  int64_t len = 0;
//  int64_t read_count = 0;

//  if (NULL == my_buffer)
//  {
//    TBSYS_LOG(ERROR, "get thread specific buffer fail");
//    err = OB_ALLOCATE_MEMORY_FAILED;
//  }
//  else
//  {
//    my_buffer->reset();
//    buf = my_buffer->current();
//    len = my_buffer->remain();
//  }

//  if (OB_SUCCESS != err)
//  {
//    // do nothing
//  }
//  else if (!is_inited())
//  {
//    err = OB_NOT_INIT;
//  }
//  else if (ObUpsRoleMgr::STOP == role_mgr_->get_state())
//  {
//    err = OB_CANCELED;
//  }
//  else if (ObUpsRoleMgr::FATAL == role_mgr_->get_state())
//  {
//    err = OB_NEED_RETRY;
//  }
//  else if (!is_log_replay_finished())
//  {
//    err = OB_NEED_RETRY;
//  }
//  else if (
//  is_master_master() &&  //mod lbzhong [Paxos ups_replication] 20160104
//  is_tmp_log_replay_started())
//  {
//    err = OB_NEED_RETRY;
//  }
//  else if (replay_worker_->get_replayed_log_id() < 0)
//  {
//    err = OB_ERR_UNEXPECTED;
//  }
//  else if (OB_SUCCESS != (err = start_log_for_replay())
//           && OB_NEED_RETRY != err)
//  {
//    TBSYS_LOG(ERROR, "start_log_for_replay()=>%d", err);
//  }
//  else if (OB_NEED_RETRY == err)
//  {
//    err = OB_NEED_WAIT;
//  }
//  else if (!replay_log_src_->get_remote_log_src().is_using_lsync() &&  master_log_id_ <= flushed_cursor_.log_id_)
//  {
//    err = OB_NEED_RETRY;
//  }
//  //mod chujiajia [Paxos ups_replication] 20160219:b
//  //else if (OB_SUCCESS != (err = replay_log_src_->get_log(flushed_cursor_, end_id, buf, len, read_count))
//  else if (OB_SUCCESS != (err = replay_log_src_->get_log(flushed_cursor_, end_id, buf, OB_MAX_LOG_BUFFER_SIZE, read_count))
//  //mod:e
//           && OB_DATA_NOT_SERVE != err)
//  {
//    TBSYS_LOG(ERROR, "get_log(flushed_cursor_=%s)=>%d", flushed_cursor_.to_str(), err);
//  }
//  else if (OB_DATA_NOT_SERVE == err)
//  {
//    err = OB_NEED_WAIT;
//  }
//  //TODO_wangjiahao need to change condition to activate ups
//  else if (master_log_id_ > 0 && end_id == master_log_id_ && OB_SUCCESS != (err = set_state_as_active()))
//  {
//    TBSYS_LOG(ERROR, "set_state_as_active()=>%d", err);
//  }
//  else if (0 == read_count)
//  {
//    err = OB_NEED_RETRY;
//  }
//  //else if (OB_SUCCESS != (err = replay_and_write_log(replay_worker_->get_replayed_log_id(), end_id, buf, read_count)))
//  else if (OB_SUCCESS != (err = replay_and_write_log(flushed_cursor_.log_id_, end_id, buf, read_count)))
//  {
//    TBSYS_LOG(ERROR, "replay_and_write_log(buf=%p[%ld])=>%d", buf, read_count, err);
//  }
//  if (OB_SUCCESS != err && OB_NEED_RETRY != err && OB_NEED_WAIT != err && OB_CANCELED != err)
//  {
//    role_mgr_->set_state(ObUpsRoleMgr::FATAL);
//    TBSYS_LOG(ERROR, "replay failed, ret=%d, will kill self", err);
//    kill(getpid(), SIGTERM);
//  }
//  return err;
//}
//mod :e


// sstable_mgr必须初始化完成
int  ObUpsLogMgr::get_max_log_seq_in_file(int64_t& log_seq) const
{
  int err = OB_SUCCESS;
  ObLogCursor log_cursor;
  log_seq = 0;
  if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  else if (is_log_replay_finished())
  {
    //mod wangjiahao [Paxos ups_replication_tmplog] 20150810 :b
    //log_seq = replay_worker_->get_replayed_log_id();
    log_seq = tmp_log_cursor_.log_id_;
    //mod :e
  }
  else if (local_max_log_id_when_start_ >= 0)
  {
    log_seq = local_max_log_id_when_start_;
  }
  else if (OB_SUCCESS != (err = get_local_max_log_cursor_func(log_dir_, get_max_file_id_by_sst(), log_cursor)))
  {
    TBSYS_LOG(ERROR, "get_local_max_log_cursor(log_dir=%s)=>%d", log_dir_, err);
  }
  else
  {
    log_seq = log_cursor.log_id_;
    const_cast<int64_t&>(local_max_log_id_when_start_) = log_seq;
    if (log_cursor.log_id_ <= 0)
    {
      TBSYS_LOG(INFO, "local log_dir has no log or log is not continuous.");
    }
  }
  return err;
}

int ObUpsLogMgr::get_max_log_seq_in_buffer(int64_t& log_seq) const
{
  int err = OB_SUCCESS;
  log_seq = replay_worker_->get_replayed_log_id();
  if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  else if (!is_log_replay_finished())
  {}
  else if (log_seq > recent_log_cache_.get_start_id())
  {
    log_seq = max(log_seq, recent_log_cache_.get_end_id());
  }
  return err;
}

int ObUpsLogMgr::get_max_log_seq_replayable(int64_t& max_log_seq, int32_t sync_mode) const  //= log_mgr_.tmp_cursor_.log_id
{
  int err = OB_SUCCESS;
  int64_t max_log_seq_in_file = 0;
  int64_t max_log_seq_in_buffer = 0;
  if (!is_inited())
  {
    err = OB_NOT_INIT;
  }
  //add pangtianze [Paxos ups_replication] 20161220:b
  else if (ObUpsLogMgr::WAIT_FLUSH == sync_mode)
  {
    if (OB_SUCCESS != (err = get_max_log_seq_in_file(max_log_seq_in_file)))
    {
        TBSYS_LOG(ERROR, "get_max_log_seq_in_file()=>%d", err);
    }
    else
    {
      max_log_seq = max(max_log_seq_in_file, max_log_seq_in_buffer);
    }
  }
  else if (ObUpsLogMgr::WAIT_NONE == sync_mode)
  {
  //add:e
  /*else*/if (OB_SUCCESS != (err = get_max_log_seq_in_file(max_log_seq_in_file)))
    {
      TBSYS_LOG(ERROR, "get_max_log_seq_in_file()=>%d", err);
    }
    else if (OB_SUCCESS != (err = get_max_log_seq_in_buffer(max_log_seq_in_buffer)))
    {
      TBSYS_LOG(ERROR, "get_max_log_seq_in_buffer()=>%d", err);
    }
    else
    {
      max_log_seq = max(max_log_seq_in_file, max_log_seq_in_buffer);
    }
  }
  return err;
}
//update some information in master ups. i.e. flush_cursor
int ObUpsLogMgr::write_log_hook(const bool is_master,
                                const ObLogCursor start_cursor, const ObLogCursor end_cursor,
                                const char* log_data, const int64_t data_len)
{
  int err = OB_SUCCESS;
  int64_t start_id = start_cursor.log_id_;
  int64_t end_id = end_cursor.log_id_;
  int64_t log_size = (end_cursor.file_id_ - 1) * get_file_size() + end_cursor.offset_;
  clog_stat_.add_disk_us(start_id, end_id, get_last_disk_elapse());
  OB_STAT_SET(UPDATESERVER, UPS_STAT_COMMIT_LOG_SIZE, log_size);
  OB_STAT_SET(UPDATESERVER, UPS_STAT_COMMIT_LOG_ID, end_cursor.log_id_);
  if (is_master)
  {
    last_receive_log_time_ = tbsys::CTimeUtil::getTime();
    if (OB_SUCCESS != (err = append_to_log_buffer(&recent_log_cache_, start_id, end_id, log_data, data_len)))
    {
      TBSYS_LOG(ERROR, "append_to_log_buffer([%s,%s], data=%p[%ld])=>%d",
                to_cstring(start_cursor), to_cstring(end_cursor), log_data, data_len, err);
    }
    //del wangdonghui [ups_replication] 20170207 :b
    /*else if (OB_SUCCESS != (err = replay_worker_->update_replay_cursor(end_cursor)))
    {
      TBSYS_LOG(ERROR, "update_replay_cursor(log_cursor=%s)=>%d", end_cursor.to_str(), err);
    }*/
    //del :e
    else
    {
      //add wangjiahao [Paxos ups_replication] 20150805 :b
      flushed_cursor_ = end_cursor;
      tmp_log_cursor_ = end_cursor;
      //add :e
      set_counter(master_log_id_cond_, master_log_id_, end_cursor.log_id_);
    }
  }
  return err;
}

ObLogBuffer& ObUpsLogMgr::get_log_buffer()
{
  return recent_log_cache_;
}


//add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
int ObUpsLogMgr::set_master_max_commit_log_id(const int64_t commit_id)
{
    int err = OB_SUCCESS;
    volatile int64_t x;
    while ((x = master_max_commit_log_id_) < commit_id
           && !__sync_bool_compare_and_swap(&master_max_commit_log_id_, x, commit_id))
    {
        TBSYS_LOG(WARN, "slave set_master_max_commit_log_id waiting for __sync");
    }
    //add wangdonghui [ups_replication] 20161214 :b
    replay_worker_->set_master_max_commit_log_id(commit_id);
    //add :e
    return err;
}
//add :e

//add by wangjiahao [Paxos temp_log] 20150629 :b
//if need flush cpoint to disk return TRUE
//TODO_wangjiahao change flush_interval to a const
int ObUpsLogMgr::write_cpoint(const int64_t cmt_point, bool is_master)
{
    int ret = OB_SUCCESS;
    int flush_interval = 1;
    if (cmt_point > 0)
    {
        if (!is_master)
        {
            flush_interval *= 2;
        }
        int64_t now = tbsys::CTimeUtil::getTime();

        if ((now - last_cpoint_flush_time_) / 1000L / 1000L >= flush_interval)
        {
            if (OB_SUCCESS == (ret = commit_point_.write(cmt_point)))
            {
                last_cpoint_flush_time_ = now;
                last_wrote_commit_point_ = cmt_point; //add lbzhong [Clog Monitor] 20151218:b:e
            }
        }
    }
    return ret;
}

//add :e

//add wangjiahao [Paxos temp_log] 20150717 :b
int64_t ObUpsLogMgr::get_current_log_term()
{
    int64_t* p = log_generator_.get_log_term_ptr();
    return *p;
}

//return OB_INVALID_ARGUMENT if term < current log_term
int ObUpsLogMgr::set_current_log_term(const int64_t term)
{
    int err = OB_SUCCESS;
    int64_t x;
    int64_t* p = get_log_term_ptr();
    while ((x = *p) < term && !__sync_bool_compare_and_swap(p, x, term))
    {
        TBSYS_LOG(WARN, "set_current_log_term(%ld) waiting for __sync", term);
    }
    if (x > term)
    {
        TBSYS_LOG(WARN, "set_current_log_term(%ld) < current log_term_(%ld)", term, *p);
        err = OB_ERR_TERM;
    }
    else if (x < term)
    {
      TBSYS_LOG(INFO, "new log_term setted. term=%ld", term);
    }
    return err;
}

int ObUpsLogMgr::restore_log_term_from_log(const int64_t log_id)
{
  int err = OB_SUCCESS;
  int64_t read_count = 0;
  int64_t pos = 0;
  int64_t pos1 = 0;
  int64_t len = 0;
  ObLogEntry entry;
  char* buf = NULL;
  ThreadSpecificBuffer::Buffer* my_buffer = log_buffer_for_term_.get_buffer();
  if (NULL == my_buffer)
  {
    TBSYS_LOG(ERROR, "get thread specific buffer fail");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    my_buffer->reset();
    buf = my_buffer->current();
    len = my_buffer->remain();
  }
  if (!local_reader_.is_inited())
  {
    err = OB_NOT_INIT;
  }
  if (log_id <= 0)
  {
    set_current_log_term(OB_DEFAULT_TERM);
    TBSYS_LOG(WARN, "local_max_log_id_when_start_ is not valid. set log_term_ as default term = 0");
  }
  else if (OB_SUCCESS != (err = local_reader_.get_log(log_id - 1, log_id, buf, read_count, len, pos)))
  {
    TBSYS_LOG(ERROR, "local_reader get log failed. err = %d pos=%ld", err, pos);
  }
  else if (OB_SUCCESS != (err = entry.deserialize(buf, read_count, pos1)))
  {
    TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", buf, read_count, pos1, err);
  }
  else
  {
    //mod chujiajia [Paxos ups_replication] 20160106:b
    //set_current_log_term(entry.header_.get_reserved());
    //TBSYS_LOG(INFO, "log_term get from max_log_id(%ld) succ. term = %ld", log_id, entry.header_.get_reserved());
    if (entry.header_.get_reserved() < get_current_log_term())
    {
      err = OB_INVALID_TERM;
      //TBSYS_LOG(WARN, "log_term from max_log_id is invalid, err=%d.", err);
    }
    else
    {
      set_current_log_term(entry.header_.get_reserved());
      //TBSYS_LOG(INFO, "log_term get from max_log_id(%ld) succ. term = %ld", log_id, entry.header_.get_reserved());
    }
    //mod:e
  }
  return err;
}
//add :e
//add wangdonghui [ups_replication] 20161221 :b
int ObUpsLogMgr::get_replay_cursor_for_disk(ObLogCursor& cursor)
{
    int ret = OB_SUCCESS;
    ObLogCursor start_cursor;
    int64_t clog_point;
    ObLogDirScanner scanner;
    int64_t max_log_file_id = 0;
    uint64_t max_log_file_id_by_sst = get_max_file_id_by_sst();
    if (NULL == log_dir_)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "log_dir == NULL");
    }
    else if (OB_SUCCESS != (ret = scanner.init(log_dir_)) && OB_DISCONTINUOUS_LOG != ret)
    {
      TBSYS_LOG(ERROR, "scanner.init(log_dir=%s)=>%d", log_dir_, ret);
    }
    else if (OB_SUCCESS != (ret = scanner.get_max_log_id((uint64_t&)max_log_file_id)) && OB_ENTRY_NOT_EXIST != ret)
    {
      TBSYS_LOG(ERROR, "get_max_log_file_id error[ret=%d]", ret);
    }
    else
    {
      ret = OB_SUCCESS;
      max_log_file_id = max(max_log_file_id, max_log_file_id_by_sst);
      start_cursor.file_id_ = max(max_log_file_id, 0);
    }
    if (OB_SUCCESS != (ret = commit_point_.get(clog_point)) && ret != OB_FILE_NOT_EXIST)
    {
      TBSYS_LOG(ERROR, "get_commit_point_func(log_dir=%s)=>%d", log_dir_, ret);
    }
    //add wangdonghui [ups replication] 20170210 judge whether the replay cursor still exists
    while(OB_SUCCESS == ret)
    {
        if(OB_SUCCESS != (ret = get_replay_cursor_from_file(log_dir_, start_cursor, clog_point, cursor))
                && OB_EAGAIN != ret)
        {
            TBSYS_LOG(WARN, "get_replay_cursor_from_file=>%d", ret);
        }
        else if(OB_EAGAIN == ret)
        {
            TBSYS_LOG(WARN, "replay cursor doesn't exist in file[%ld], read prior", start_cursor.file_id_);
            start_cursor.file_id_--;
            cursor.reset();
            ret = OB_SUCCESS;
        }
        else break;//get_replay_log succ
        if(start_cursor.file_id_ <= 0)
        {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(ERROR, "get_replay_cursor_for_disk fail, may be deleted by accident!");
        }
    }
    //add :e
    return ret;
}
//add :e
