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

#include "ob_ups_log_utils.h"
#include "common/ob_log_reader.h"
#include "common/ob_direct_log_reader.h"
#include "common/ob_log_dir_scanner.h"
#include "ob_ups_table_mgr.h"
#include "common/file_utils.h"
#include "common/file_directory_utils.h"
#include "ob_ups_log_mgr.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    const char* ObLogReplayPoint::REPLAY_POINT_FILE = "log_replay_point";

    int load_replay_point_func(const char* log_dir, const char* replay_point_file, int64_t& replay_point)
    {
      int err = 0;
      int64_t len = 0;
      int open_ret = 0;
      const int UINT64_MAX_LEN = 16;
      char rplpt_fn[OB_MAX_FILE_NAME_LENGTH];
      char rplpt_str[UINT64_MAX_LEN];
      int rplpt_str_len = 0;
      FileUtils rplpt_file;

      if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "Arguments are invalid[log_dir=%p]", log_dir);
      }
      else if ((len = snprintf(rplpt_fn, sizeof(rplpt_fn), "%s/%s", log_dir, replay_point_file) < 0)
               && len >= (int64_t)sizeof(rplpt_fn))
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "generate_replay_point_fn()=>%d", err);
      }
      else if (!FileDirectoryUtils::exists(rplpt_fn))
      {
        err = OB_FILE_NOT_EXIST;
        TBSYS_LOG(WARN, "replay point file[\"%s\"] does not exist", rplpt_fn);
      }
      else if (0 > (open_ret = rplpt_file.open(rplpt_fn, O_RDONLY)))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(WARN, "open file[\"%s\"] error[%s]", rplpt_fn, strerror(errno));
      }
      else if (0 > (rplpt_str_len = static_cast<int32_t>(rplpt_file.read(rplpt_str, sizeof(rplpt_str))))
               || rplpt_str_len >= (int)sizeof(rplpt_str))
      {
        err = rplpt_str_len < 0 ? OB_IO_ERROR: OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "read error[%s] or file contain invalid data[len=%d]", strerror(errno), rplpt_str_len);
      }
      else
      {
        rplpt_str[rplpt_str_len] = '\0';
        const int STRTOUL_BASE = 10;
        char* endptr;
        replay_point = strtoul(rplpt_str, &endptr, STRTOUL_BASE);
        if ('\0' != *endptr)
        {
          err = OB_INVALID_DATA;
          TBSYS_LOG(ERROR, "non-digit exist in replay point file[rplpt_str=%.*s]", rplpt_str_len, rplpt_str);
        }
        else if (ERANGE == errno)
        {
          err = OB_INVALID_DATA;
          TBSYS_LOG(ERROR, "replay point contained in replay point file is out of range");
        }
      }
      if (0 > open_ret)
      {
        rplpt_file.close();
      }
      return err;
    }

    int write_replay_point_func(const char* log_dir, const char* replay_point_file, const int64_t replay_point)
    {
      int err = OB_SUCCESS;
      const int UINT64_MAX_LEN = 16;
      int len = 0;
      int open_ret = 0;
      FileUtils rplpt_file;
      char rplpt_fn[OB_MAX_FILE_NAME_LENGTH];
      char rplpt_str[UINT64_MAX_LEN];
      int64_t rplpt_str_len = 0;

      if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "Arguments are invalid[log_dir=%p]", log_dir);
      }
      else if ((len = snprintf(rplpt_fn, sizeof(rplpt_fn), "%s/%s", log_dir, replay_point_file) < 0)
               && len >= (int64_t)sizeof(rplpt_fn))
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "generate_replay_point_fn()=>%d", err);
      }
      else if (0 > (open_ret = rplpt_file.open(rplpt_fn, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)))
      {
        err = OB_FILE_NOT_EXIST;
        TBSYS_LOG(ERROR, "open file[\"%s\"] error[%s]", rplpt_fn, strerror(errno));
      }
      else if ((rplpt_str_len = snprintf(rplpt_str, sizeof(rplpt_str), "%lu", replay_point)) < 0
               || rplpt_str_len >= (int64_t)sizeof(rplpt_str))
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "snprintf rplpt_str error[%s][replay_point=%lu]", strerror(errno), replay_point);
      }
      else if (0 > (rplpt_str_len = rplpt_file.write(rplpt_str, rplpt_str_len)))
      {
        err = OB_ERR_SYS;
        TBSYS_LOG(ERROR, "write error[%s][rplpt_str=%p rplpt_str_len=%ld]", strerror(errno), rplpt_str, rplpt_str_len);
      }
      if (0 > open_ret)
      {
        rplpt_file.close();
      }
      return err;
    }

    int scan_log_dir_func(const char* log_dir, const char* replay_point_file,
                          int64_t& replay_point, int64_t& min_log_file_id, int64_t& max_log_file_id)
    {
      int err = OB_SUCCESS;
      ObLogDirScanner scanner;
      min_log_file_id = 0;
      max_log_file_id = 0;
      replay_point = 0;
      if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "log_dir == NULL");
      }
      else if (OB_SUCCESS != (err = scanner.init(log_dir))
               && OB_DISCONTINUOUS_LOG != err)
      {
        TBSYS_LOG(ERROR, "scanner.init(log_dir=%s)=>%d", log_dir, err);
      }
      else if (OB_SUCCESS != (err = scanner.get_min_log_id((uint64_t&)min_log_file_id)) && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "scanner.get_min_log_file_id()=>%d", err);
      }
      else if (OB_SUCCESS != (err = scanner.get_max_log_id((uint64_t&)max_log_file_id)) && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "get_max_log_file_id error[ret=%d]", err);
      }
      else if (OB_SUCCESS != (err = load_replay_point_func(log_dir, replay_point_file, replay_point))
               && OB_FILE_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "load_replay_point(log_dir=%s)=>%d", log_dir, err);
      }
      else
      {
        if (0 >= min_log_file_id)
        {
          min_log_file_id = 0;
        }
        if (0 >= replay_point)
        {
          replay_point = min_log_file_id;
        }
        if (min_log_file_id > replay_point || (max_log_file_id >0 && replay_point > max_log_file_id))
        {
          err = OB_DISCONTINUOUS_LOG;
          TBSYS_LOG(WARN, "min_log_file_id=%ld, max_log_file_id=%ld, replay_point=%ld", min_log_file_id, max_log_file_id, replay_point);
        }
      }
      if (OB_FILE_NOT_EXIST == err)
      {
        err = OB_SUCCESS;
      }
      return err;
    }

    int ObLogReplayPoint::init(const char* log_dir)
    {
      int err = OB_SUCCESS;
      log_dir_ = log_dir;
      return err;
    }

    int ObLogReplayPoint::write(const int64_t replay_point)
    {
      return write_replay_point_func(log_dir_, REPLAY_POINT_FILE, replay_point);
    }

    int ObLogReplayPoint::get(int64_t& replay_point)
    {
      int64_t min_log_id = -1;
      int64_t max_log_id = -1;
      return scan_log_dir_func(log_dir_, REPLAY_POINT_FILE, replay_point, min_log_id, max_log_id);
    }

    int64_t set_counter(tbsys::CThreadCond& cond, volatile int64_t& counter, const int64_t new_counter)
    {
      int64_t old_counter = 0;
      cond.lock();
      old_counter = counter;
      counter = new_counter;
      cond.broadcast();
      cond.unlock();
      return old_counter;
    }

    int64_t wait_counter(tbsys::CThreadCond& cond, volatile int64_t& counter, const int64_t limit, const int64_t timeout_us)
    {
      int64_t end_time_us = 0;
      int64_t wait_time_us = 0;
      int wait_time_ms = (int)(wait_time_us/1000LL);
      if (counter >= limit)
      {}
      else
      {
        end_time_us = tbsys::CTimeUtil::getTime() + timeout_us;
        wait_time_us = timeout_us;
        cond.lock();
        while((wait_time_ms = (int)(wait_time_us/1000LL)) > 0)
        {
          if (counter >= limit)
          {
            break;
          }
          else
          {
            cond.wait(wait_time_ms);
            wait_time_us = end_time_us - tbsys::CTimeUtil::getTime();
          }
        }
        cond.unlock();
      }
      return counter;
    }

    static bool is_align(int64_t x, int64_t n_bits)
    {
      return 0 == (x & ((1<<n_bits) - 1));
    }

    int get_local_max_log_cursor_func(const char* log_dir, const ObLogCursor& start_cursor, ObLogCursor& end_cursor)
    {
      int err = OB_SUCCESS;
      ObLogReader log_reader;
      ObDirectLogReader direct_reader;
      char* log_data = NULL;
      int64_t data_len = 0;
      LogCommand cmd = OB_LOG_UNKNOWN;
      uint64_t seq;
      end_cursor = start_cursor;

      if (NULL == log_dir || start_cursor.file_id_ <= 0 || start_cursor.log_id_ != 0 || start_cursor.offset_ != 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: log_dir=%s, log_cursor=%s", log_dir, start_cursor.to_str());
      }
      else if (OB_SUCCESS != (err = log_reader.init(&direct_reader, log_dir, start_cursor.file_id_, 0, false)))
      {
        TBSYS_LOG(ERROR, "ObLogReader init error[err=%d]", err);
      }
      while (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = log_reader.read_log(cmd, seq, log_data, data_len)) &&
            OB_FILE_NOT_EXIST != err && OB_READ_NOTHING != err && OB_LAST_LOG_RUINNED != err)
        {
          TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", err);
        }
        else if (OB_LAST_LOG_RUINNED == err)
        {
          TBSYS_LOG(WARN, "last_log[%s] broken!", end_cursor.to_str());
          err = OB_ITER_END;
        }
        else if (OB_SUCCESS != err)
        {
          err = OB_ITER_END; // replay all
        }
        else if (OB_SUCCESS != (err = log_reader.get_next_cursor(end_cursor)))
        {
          TBSYS_LOG(ERROR, "log_reader.get_cursor()=>%d",  err);
        }
      }
      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      return err;
    }

    int get_local_max_log_cursor_func(const char* log_dir, const uint64_t log_file_id_by_sst, ObLogCursor& log_cursor)
    {
      int err = OB_SUCCESS;
      ObLogCursor new_cursor;
      int64_t max_log_file_id = 0;
      ObLogDirScanner scanner;
      set_cursor(log_cursor, 0, 0, 0);
      if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "log_dir == NULL");
      }
      else if (OB_SUCCESS != (err = scanner.init(log_dir)) && OB_DISCONTINUOUS_LOG != err)
      {
        TBSYS_LOG(ERROR, "scanner.init(log_dir=%s)=>%d", log_dir, err);
      }
      else if (OB_SUCCESS != (err = scanner.get_max_log_id((uint64_t&)max_log_file_id)) && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "get_max_log_file_id error[ret=%d]", err);
      }
      else
      {
        err = OB_SUCCESS;
        if (OB_INVALID_ID != log_file_id_by_sst)
        {
          max_log_file_id = max(max_log_file_id, log_file_id_by_sst);
        }
        TBSYS_LOG(INFO, "max_log_file_id=%ld, log_file_id_by_sst=%ld", max_log_file_id, log_file_id_by_sst);
        log_cursor.file_id_ = max(max_log_file_id, 0);
        if (log_cursor.file_id_ >0 && OB_SUCCESS != (err = get_local_max_log_cursor_func(log_dir, log_cursor, new_cursor)))
        {
          TBSYS_LOG(ERROR, "get_max_local_cursor(file_id=%ld)=>%d", log_cursor.file_id_, err);
        }
        else
        {
          log_cursor = new_cursor;
        }
        // 可能最后一个文件存在，但是为空，这时如果前一个文件存在的话，从前一个文件开始找本地最后的日志
        if (OB_SUCCESS == err && log_cursor.log_id_ <= 0 && log_cursor.file_id_ - 1 > 0)
        {
          ObLogCursor tmp_cursor = log_cursor;
          tmp_cursor.file_id_--;
          if (OB_SUCCESS != (err = get_local_max_log_cursor_func(log_dir, tmp_cursor, new_cursor)))
          {
            TBSYS_LOG(ERROR, "get_max_local_cursor(): try previous file[id=%ld]=>%d", log_cursor.file_id_, err);
          }
          else if (new_cursor.log_id_ > 0)
          {
            log_cursor = new_cursor;
          }
        }
      }
      TBSYS_LOG(INFO, "get_local_max_log_cursor(return_cursor=%s)=>%d", log_cursor.to_str(), err);
      return err;
    }
    //add wangjiahao [Paxus ups_replication_tmplog] 20150713 :b
    //mod wangdonghui [Paxus ups_replication_tmplog] 20170714 :b
    //int find_log_from_buf(const char* log_data, int64_t data_len, ObLogCursor& cursor, const int64_t log_id)
    int find_log_from_buf(const char* log_data, int64_t data_len, ObLogCursor& cursor, const int64_t log_id, int64_t &read_count)
    //mod :e
    {
      ObLogEntry log_entry;
      int64_t pos = 0;
      int err = OB_SUCCESS;
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
          else if (OB_SUCCESS != cursor.advance(log_entry))
          {
            TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
          }
          else
          {
            pos += log_entry.get_log_data_len();
            if (cursor.log_id_ == log_id)
            {
                break;
            }
          }
      }
      //add wangdonghui [Paxus ups_replication_tmplog] 20170714 :b
      read_count = pos;
      //add :e
      return err;
    }
    //add :e

    int replay_local_log_func(const volatile bool& stop, const char* log_dir,
                              const ObLogCursor& start_cursor, ObLogCursor& end_cursor,
                              ObLogReplayWorker& replay_worker
                              //add wangjiahao [Paxus ups_replication_tmplog] 20150713 :b
                              , int64_t clog_point, ObLogCursor& end_file_cursor
                              //add:e
                              )
    {
      int err = OB_SUCCESS;
      char* buf = NULL;
      int64_t len = ObLogWriter::LOG_BUFFER_SIZE;
      int64_t read_count = 0;
      int64_t end_id = 0;
      bool is_last_clog_batch_ = false;
      bool need_replay = true;
      ObPosLogReader reader;
      ObLogLocation start_location;
      ObLogLocation end_location;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      end_cursor = start_cursor;
      end_file_cursor = start_cursor;

      if (NULL == log_dir || start_cursor.file_id_ <= 0 || start_cursor.log_id_ != 0 || start_cursor.offset_ != 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: log_dir=%s, log_cursor=%s", log_dir, start_cursor.to_str());
      }
      else if (NULL == (buf = (char*)ob_malloc(len, ObModIds::OB_UPS_COMMON)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "ob_malloc(len=%ld) fail", len);
      }
      else if (OB_SUCCESS != (err = get_first_log_id_func(log_dir, end_cursor.file_id_, end_cursor.log_id_, NULL))
               && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "get_first_log_id_func()=>%d", err);
      }
      else if (OB_ENTRY_NOT_EXIST == err)
      {
        TBSYS_LOG(WARN, "replay_local_log(start_file=%ld): NOT EXIST", start_location.file_id_);
      }
      else if (OB_SUCCESS != (err = reader.init(log_dir, true)))
      {
        TBSYS_LOG(ERROR, "located_log_reader init error[err=%d]", err);
      }
      else if (OB_SUCCESS != (err = replay_worker.start_log(end_cursor)))
      {
        TBSYS_LOG(ERROR, "replay_worker.start_log(%s)=>%d", end_cursor.to_str(), err);
      }
      else
      {
        start_location.file_id_ = end_cursor.file_id_;
        start_location.log_id_ = end_cursor.log_id_;
        end_location = start_location;
      }
      while (!stop && OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = reader.get_log(start_location.log_id_, start_location, end_location,
                                                     buf, len, read_count)))
        {
          TBSYS_LOG(ERROR, "reader.get_log(log_id=%ld)=>%d", start_location.log_id_, err);
        }
        else if (read_count <= 0)
        {
          break;
        }
        //add wangdonghui [ups_replication] 20170612 :b  when log_replay_point exist and commit point was removed.
        else if(clog_point == 0)
        {
          ObLogCursor tmp_cursor;
          tmp_cursor.file_id_ = start_location.file_id_;
          tmp_cursor.log_id_ = start_location.log_id_;
          tmp_cursor.offset_ = start_location.offset_;
          replay_worker.update_replay_cursor(tmp_cursor);
          TBSYS_LOG(INFO, "commit point file was removed, replay_cursor(file_id:%ld,log_id:%ld,offset:%ld",
                    tmp_cursor.file_id_, tmp_cursor.log_id_, tmp_cursor.offset_);
          break;
        }
        //add :e
        //add wangjiahao [Paxos ups_replication] 20150713 :b
        if (end_location.log_id_ >= clog_point && start_location.log_id_ < clog_point)
        {
          ObLogCursor tmp_cursor;
          tmp_cursor.file_id_ = start_location.file_id_;
          tmp_cursor.log_id_ = start_location.log_id_;
          tmp_cursor.offset_ = start_location.offset_;
          int err1 = OB_SUCCESS;
          int64_t count = 0;
          if (OB_SUCCESS != (err1 = find_log_from_buf(buf, read_count, tmp_cursor, clog_point, count)))
          {
            TBSYS_LOG(ERROR, "fail to find log from buf(log_id=%ld)=>%d", clog_point, err);
          }
          else if (tmp_cursor.log_id_ == clog_point) //find that log
          {
            //mod wangdonghui [ups_replication] 20170714 :b
            //read_count = tmp_cursor.offset_ - start_location.offset_;
            read_count = count;
            //mod :e
            end_location.file_id_ = tmp_cursor.file_id_;
            end_location.log_id_ = tmp_cursor.log_id_;
            end_location.offset_ = tmp_cursor.offset_;
            end_cursor = tmp_cursor;
            is_last_clog_batch_ = true;
          }
          else
          {
            TBSYS_LOG(ERROR, "WTF(log_id=%ld)=>%d", clog_point, err);
          }
        }
        if (OB_SUCCESS != err)
        {}
        else if (!need_replay)
        {
          start_location = end_location;
        }
        //add :e
        else if (OB_SUCCESS != (err = replay_worker.submit_batch(end_id, buf, read_count, RT_LOCAL)))
        {
          TBSYS_LOG(ERROR, "replay_worker.submit_batch(end_id=%ld, buf=%p[%ld])=>%d", end_id, buf, len, err);
        }
        else
        {
          start_location = end_location;
          //add wangjiahao [Paxos ups_replication] 20150713 :b
          if (is_last_clog_batch_)
          {
            //TBSYS_LOG(INFO, "WJH_INFO replay_CMT_LOG_() submit_end_id=%ld", end_id);
            need_replay = false;
          }
          //add :e
        }
      }
      //del wangdonghui [ups_replication] 20170612 :b commit point file may be removed
      /*
      if (!is_last_clog_batch_)
      {
        TBSYS_LOG(ERROR, "Replaying Local log, log with Commit_Point no find. end_id=%ld", end_id);
        err = OB_CLPT_NOFIND;
      }*/
      //del :e
      if (stop)
      {
        err = OB_CANCELED;
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = replay_worker.wait_task(end_id)))
      {
        TBSYS_LOG(ERROR, "replay_worker.wait_task(end_id=%ld)=>%d", end_id, err);
      }
      else
      {
        //mod wangjiahao [Paxos ups_replication_tmplog] 20150805 :b
        //end_cursor.log_id_ = end_location.log_id_;
        //end_cursor.file_id_ = end_location.file_id_;
        //end_cursor.offset_ = end_location.offset_;
        end_file_cursor.log_id_ = start_location.log_id_;
        end_file_cursor.file_id_ = start_location.file_id_;
        end_file_cursor.offset_ = start_location.offset_;
        TBSYS_LOG(INFO, "replay_CMT_LOG_ tmp_log_cursor=%s", end_file_cursor.to_str());
        //mod :e
      }
      if (NULL != buf)
      {
        ob_free(buf);
      }
      TBSYS_LOG(INFO, "replay_local_log_profile: duration=%ld", tbsys::CTimeUtil::getTime() - start_time);
      return err;
    }
    int replay_tmp_log_func(const volatile bool& stop, const char* log_dir, ObLogCursor& end_cursor,
                            ObLogReplayWorker& replay_worker)
    {
        int err = OB_SUCCESS;
        char* buf = NULL;
        int64_t len = ObLogWriter::LOG_BUFFER_SIZE;
        int64_t read_count = 0;
        int64_t end_id = 0;
        bool is_last_clog_batch_ = false;
        ObPosLogReader reader;
        ObLogLocation start_location;
        ObLogLocation end_location;
        ObLogCursor start_cursor;
        int64_t start_time = tbsys::CTimeUtil::getTime();
        if (NULL == (buf = (char*)ob_malloc(len, ObModIds::OB_UPS_COMMON)))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "ob_malloc(len=%ld) fail", len);
        }
        else if (OB_SUCCESS != (err = replay_worker.get_replay_cursor(start_cursor)))
        {
            TBSYS_LOG(ERROR,"replay_worker_replay_worker_->get_replay_cursor=>%d", err);
        }
        else if (OB_SUCCESS != (err = reader.init(log_dir, true)))
        {
          TBSYS_LOG(ERROR, "located_log_reader init error[err=%d]", err);
        }
        else
        {
          //add wangdonghui [ups_replication] 20170314 :b
          //replay from the head commit point was removed
          if(start_cursor.file_id_ <= 0)
          {
              start_cursor.file_id_ = 1;
              start_cursor.log_id_ = 1;
              replay_worker.update_replay_cursor(start_cursor);
          }
          //add :e
          start_location.file_id_ = start_cursor.file_id_;
          start_location.log_id_ = start_cursor.log_id_;
          start_location.offset_ = start_cursor.offset_;
          end_location = start_location;
        }
        TBSYS_LOG(DEBUG, "replay_tmp_log_func start_cursor[%s] start_location.log_id_ = %ld", start_cursor.to_str(), start_location.log_id_);
        if (OB_SUCCESS != err || start_cursor.file_id_ <= 0)
        {}
        else{
            //TBSYS_LOG(INFO, "WJH_INFO replay_tmp_log_func start_cursor[%s] start_location.log_id_ = %ld", start_cursor.to_str(), start_location.log_id_);
            while (!stop && OB_SUCCESS == err)
            {
              if (OB_SUCCESS != (err = reader.get_log(start_location.log_id_, start_location, end_location,
                                                           buf, len, read_count)))
              {
                TBSYS_LOG(ERROR, "reader.get_log(log_id=%ld)=>%d", start_location.log_id_, err);
              }
              else if (read_count <= 0)
              {
                break;
              }
              //TBSYS_LOG(INFO, "WJH_INFO reader.get_log(log_id=%ld) readcount=%ld", start_location.log_id_, read_count);
              if (OB_SUCCESS != err)
              {}
              else if (OB_SUCCESS != (err = replay_worker.submit_batch(end_id, buf, read_count, RT_LOCAL)))
              {
                TBSYS_LOG(ERROR, "replay_worker.submit_batch(end_id=%ld, buf=%p[%ld])=>%d", end_id, buf, len, err);
              }
              else
              {
                start_location = end_location;
                if (is_last_clog_batch_)
                {
                  //TBSYS_LOG(INFO, "WJH_INFO replay_TMP_LOG_func last_end_id=%ld", end_id);
                  break;
                }
              }
            }
            if (stop)
            {
              err = OB_CANCELED;
            }
            if (OB_SUCCESS != err)
            {}
            else if (OB_SUCCESS != (err = replay_worker.wait_task(end_id)))
            {
              TBSYS_LOG(ERROR, "replay_worker.wait_task(end_id=%ld)=>%d", end_id, err);
            }
            else
            {
              end_cursor.log_id_ = end_location.log_id_;
              end_cursor.file_id_ = end_location.file_id_;
              end_cursor.offset_ = end_location.offset_;
              TBSYS_LOG(INFO, "last_clog_batch_submitted end_cursor=%s", end_cursor.to_str());
            }
            if (NULL != buf)
            {
              ob_free(buf);
            }
            TBSYS_LOG(INFO, "replay_tmp_log_profile: duration=%ld", tbsys::CTimeUtil::getTime() - start_time);
       }
       return err;
    }

    int replay_single_log_func(ObUpsMutator& mutator, CommonSchemaManagerWrapper& schema,
                               ObUpsTableMgr* table_mgr, LogCommand cmd, const char* log_data, int64_t data_len,
                               const int64_t commit_id, const ReplayType replay_type)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      int64_t file_id = 0;
      if (NULL == table_mgr || NULL == log_data || 0 >= data_len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "replay_single_log_func(table_mgr=%p, cmd=%d, log_data=%p, data_len=%ld)=>%d", table_mgr, cmd, log_data, data_len, err);
      }
      else
      {
        switch(cmd)
        {
          case OB_LOG_UPS_MUTATOR:
            if (OB_SUCCESS != (err = mutator.deserialize(log_data, data_len, pos)))
            {
              TBSYS_LOG(ERROR, "UpsMutator deserialize error[ret=%d log_data=%p data_len=%ld]", err, log_data, data_len);
            }
            else if (OB_SUCCESS != (err = table_mgr->replay(mutator, replay_type))
                     && OB_MEM_OVERFLOW != err)
            {
              TBSYS_LOG(ERROR, "UpsTableMgr replay error[ret=%d]", err);
            }
            else if (OB_MEM_OVERFLOW == err)
            {
              TBSYS_LOG(WARN, "table_mgr->replay(log_cmd=%d, data=%p[%ld]):MEM_OVERFLOW", cmd, log_data, data_len);
            }
            break;
          case OB_UPS_SWITCH_SCHEMA:
            if (OB_SUCCESS != (err = schema.deserialize(log_data, data_len, pos)))
            {
              TBSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error[ret=%d log_data=%p data_len=%ld]",
                        err, log_data, data_len);
            }
            else if (OB_SUCCESS != (err = table_mgr->set_schemas(schema)))
            {
              TBSYS_LOG(ERROR, "UpsTableMgr set_schemas error, ret=%d schema_version=%ld", err, schema.get_version());
            }
            else
            {
              TBSYS_LOG(INFO, "switch schema succ");
            }
            break;
          case OB_LOG_SWITCH_LOG:
            if (OB_SUCCESS != (err = serialization::decode_i64(log_data, data_len, pos, (int64_t*)&file_id)))
            {
              TBSYS_LOG(ERROR, "decode_i64 log_id error, err=%d", err);
            }
            else
            {
              pos = data_len;
              TBSYS_LOG(INFO, "replay log: SWITCH_LOG, file_id=%ld", file_id);
            }
            break;
          case OB_LOG_NOP:
            pos = data_len;
            break;
          default:
            err = OB_ERROR;
            break;
        }
      }
      if (pos != data_len)
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "pos[%ld] != data_len[%ld]", pos, data_len);
        //common::hex_dump(log_data, static_cast<int32_t>(data_len), false, TBSYS_LOG_LEVEL_WARN);
      }
      if (OB_SUCCESS != err && OB_MEM_OVERFLOW != err)
      {
        TBSYS_LOG(ERROR, "replay_log(cmd=%d, log_data=%p, data_len=%ld, commit_id=%ld)=>%d",
                  cmd, log_data, data_len, commit_id, err);
      }
      return err;
    }

    int replay_local_log_func(const volatile bool& stop, const char* log_dir,
                              const ObLogCursor& start_cursor, ObLogCursor& end_cursor, IObLogApplier* log_applier)
    {
      int err = OB_SUCCESS;
      ObLogReader log_reader;
      ObDirectLogReader direct_reader;
      char* log_data = NULL;
      int64_t data_len = 0;
      LogCommand cmd = OB_LOG_UNKNOWN;
      uint64_t seq;
      int64_t retry_wait_time_us = 100 * 1000;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      end_cursor = start_cursor;

      if (NULL == log_dir || start_cursor.file_id_ <= 0 || start_cursor.log_id_ != 0 || start_cursor.offset_ != 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: log_dir=%s, log_cursor=%s", log_dir, start_cursor.to_str());
      }
      else if (OB_SUCCESS != (err = log_reader.init(&direct_reader, log_dir, start_cursor.file_id_, 0, false)))
      {
        TBSYS_LOG(ERROR, "ObLogReader init error[err=%d]", err);
      }
      while (!stop && OB_SUCCESS == err)
      {
        //TBSYS_LOG(INFO, "log_cursor=%s", end_cursor.to_str());
        if (OB_SUCCESS != (err = log_reader.read_log(cmd, seq, log_data, data_len)) &&
            OB_FILE_NOT_EXIST != err && OB_READ_NOTHING != err && OB_LAST_LOG_RUINNED != err)
        {
          TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", err);
        }
        else if (OB_LAST_LOG_RUINNED == err)
        {
          TBSYS_LOG(WARN, "last_log[%s] broken!", end_cursor.to_str());
          err = OB_ITER_END;
        }
        else if (OB_SUCCESS != err)
        {
          err = OB_ITER_END; // replay all
        }
        else if (OB_SUCCESS != (err = log_reader.get_next_cursor(end_cursor)))
        {
          TBSYS_LOG(ERROR, "log_reader.get_cursor()=>%d",  err);
        }
        else if (NULL != log_applier)
        {
          err = OB_NEED_RETRY;
          while(!stop && OB_NEED_RETRY == err)
          {
            if (OB_SUCCESS != (err = log_applier->apply_log(cmd, seq, log_data, data_len, RT_LOCAL))
                && OB_NEED_RETRY != err && OB_CANCELED != err)
            {
              TBSYS_LOG(ERROR, "replay_single_log()=>%d",  err);
            }
            else if (OB_NEED_RETRY == err)
            {
              usleep((useconds_t)retry_wait_time_us);
            }
          }
        }
      }
      if (stop)
      {
        err = OB_CANCELED;
      }
      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      if (NULL == log_applier)
      {
        TBSYS_LOG(DEBUG, "get_local_log_cursor(log_dir=%s, end_cursor=%s)=>%d", log_dir, end_cursor.to_str(), err);
      }
      else
      {
        TBSYS_LOG(DEBUG, "replay_local_log(log_dir=%s, end_cursor=%s)=>%d", log_dir, end_cursor.to_str(), err);
      }
      TBSYS_LOG(INFO, "replay_local_log_profile: duration=%ld", tbsys::CTimeUtil::getTime() - start_time);
      return err;
    }

    int replay_log_in_buf_func(const char* log_data, int64_t data_len, IObLogApplier* log_applier)
    {
      int err = OB_SUCCESS;
      ObLogEntry log_entry;
      int64_t pos = 0;
      int64_t retry_wait_time_us = 100 * 1000;
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
          err = OB_NEED_RETRY;
          while(OB_NEED_RETRY == err)
          {
            if (OB_SUCCESS != (err = log_applier->apply_log((LogCommand)log_entry.cmd_,
                                                            log_entry.seq_, log_data + pos, log_entry.get_log_data_len(), RT_APPLY))
                && OB_NEED_RETRY != err && OB_CANCELED != err)
            {
              TBSYS_LOG(ERROR, "replay_log(cmd=%d, log_data=%p, data_len=%d)=>%d", log_entry.cmd_, log_data + pos, log_entry.get_log_data_len(), err);
            }
            else if (OB_NEED_RETRY == err)
            {
              usleep((useconds_t)retry_wait_time_us);
            }
          }
        }
        if (OB_SUCCESS != err)
        {}
        else
        {
          pos += log_entry.get_log_data_len();
        }
      }
      return err;
    }

    int serialize_log_entry(char* buf, const int64_t len, int64_t& pos, ObLogEntry& entry,
                            const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || 0 >= len || pos > len || NULL == log_data || 0 >= data_len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "serialize_log_entry(buf=%p, len=%ld, pos=%ld, log_data=%p, data_len=%ld)=>%d",
                  buf, len, pos, log_data, data_len, err);
      }
      else if (pos + entry.get_serialize_size() + data_len > len)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(DEBUG, "pos[%ld] + entry.serialize_size[%ld] + data_len[%ld] > len[%ld]",
                  pos, entry.get_serialize_size(), data_len, len);
      }
      else if (OB_SUCCESS != (err = entry.serialize(buf, len, pos)))
      {
        TBSYS_LOG(ERROR, "entry.serialize(buf=%p, pos=%ld, capacity=%ld)=>%d",
                  buf, len, pos, err);
      }
      else
      {
        memcpy(buf + pos, log_data, data_len);
        pos += data_len;
      }
      return err;
    }

    int generate_log(char* buf, const int64_t len, int64_t& pos, ObLogCursor& cursor, const LogCommand cmd,
                 const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      ObLogEntry entry;
      if (NULL == buf || 0 >= len || pos > len || NULL == log_data || 0 >= data_len || !cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "generate_log(buf=%p, len=%ld, pos=%ld, log_data=%p, data_len=%ld, cursor=%s)=>%d",
                  buf, len, pos, log_data, data_len, cursor.to_str(), err);
      }
      else if (OB_SUCCESS != (err = cursor.next_entry(entry, cmd, log_data, data_len)))
      {
        TBSYS_LOG(ERROR, "cursor[%s].next_entry()=>%d", cursor.to_str(), err);
      }
      else if (OB_SUCCESS != (err = serialize_log_entry(buf, len, pos, entry, log_data, data_len)))
      {
        TBSYS_LOG(DEBUG, "serialize_log_entry(buf=%p, len=%ld, entry[id=%ld], data_len=%ld)=>%d",
                  buf, len, entry.seq_, data_len, err);
      }
      else if (OB_SUCCESS != (err = cursor.advance(entry)))
      {
        TBSYS_LOG(ERROR, "cursor[id=%ld].advance(entry.id=%ld)=>%d", cursor.log_id_, entry.seq_, err);
      }
      return err;
    }

    int set_entry(ObLogEntry& entry, const int64_t seq, const LogCommand cmd, const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      entry.set_log_seq(seq);
      entry.set_log_command(cmd);
      if (OB_SUCCESS != (err = entry.fill_header(log_data, data_len)))
      {
        TBSYS_LOG(ERROR, "entry.fill_header(log_data=%p, data_len=%ld)=>%d", log_data, data_len, err);
      }
      return err;
    }

    int serialize_log_entry(char* buf, const int64_t len, int64_t& pos, const LogCommand cmd, const int64_t seq, const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      ObLogEntry entry;
      if (OB_SUCCESS != (err = set_entry(entry, seq, cmd, log_data, data_len)))
      {
        TBSYS_LOG(ERROR, "set_entry(seq=%ld, cmd=%d, log_data=%p, data_len=%ld)=>%d", seq, cmd, log_data, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialize_log_entry(buf, len, pos, entry, log_data, data_len))
               && OB_BUF_NOT_ENOUGH != err)
      {
        TBSYS_LOG(ERROR, "serialize_log_entry(buf=%p, len=%ld, pos=%ld, log_data=%p, data_len=%ld)=>%d",
                  buf, len, pos, log_data, data_len, err);
      }
      return err;
    }

    int fill_log(char* buf, const int64_t len, int64_t& pos, ObLogCursor& cursor, const LogCommand cmd, const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      ObLogEntry entry;
      if (NULL == buf || len < 0 || pos > len || NULL == log_data || data_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = cursor.this_entry(entry, cmd, log_data, data_len)))
      {
        TBSYS_LOG(ERROR, "cursor[%s].next_entry()=>%d", cursor.to_str(), err);
      }
      else if (OB_SUCCESS != (err = serialize_log_entry(buf, len, pos, entry, log_data, data_len)))
      {
        TBSYS_LOG(DEBUG, "serialize_log_entry(buf[remain=%ld], entry[id=%ld], data_len=%ld)=>%d",
                  len-pos, entry.seq_, data_len, err);
      }
      else if (OB_SUCCESS != (err = cursor.advance(entry)))
      {
        TBSYS_LOG(ERROR, "cursor[id=%ld].advance(entry.id=%ld)=>%d", cursor.log_id_, entry.seq_, err);
      }
      return err;
    }

    //mod interface wangjiahao [Paxos ups_replicationtmplog] 20150803 add term
    int parse_log_buffer(const char* log_data, const int64_t len, int64_t& start_id, int64_t& end_id, int64_t& term)
    {
      int err = OB_SUCCESS;
      int64_t end_pos = 0;
      if (OB_SUCCESS != (err = trim_log_buffer(log_data, len, end_pos, start_id, end_id, term)))//mod wangjiahao [Paxos ups_replicationtmplog] 20150803 add term
      {
        TBSYS_LOG(ERROR, "trim_log_buffer(log_data=%p[%ld])=>%d", log_data, len, err);
      }
      else if (end_pos != len)
      {
        err = OB_INVALID_LOG;
      }
      return err;
    }

    //mod interface wangjiahao [Paxos ups_replicationtmplog] 20150803 add term
    int trim_log_buffer(const char* log_data, const int64_t len, int64_t& end_pos,
                        int64_t& start_id, int64_t& end_id, int64_t& term)
    {
      bool is_file_end = false;
      return trim_log_buffer(log_data, len, end_pos, start_id, end_id, is_file_end, term);//mod wangjiahao [Paxos ups_replicationtmplog] 20150803 add term
    }
    int trim_log_buffer(const char* log_data, const int64_t len, int64_t& end_pos,
                        int64_t& start_id, int64_t& end_id, bool& is_file_end
                        //add wangjiahao [Paxos ups_replicationtmplog] 20150803:b
                        , int64_t& term
                        //add:e
                        )
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      int64_t old_pos = 0;
      ObLogEntry log_entry;
      int64_t real_start_id = 0;
      int64_t real_end_id = 0;
      //add wangjiahao [Paxos ups_replication_tmplog] 20150803 :b
      int64_t last_term = OB_INVALID_TERM;
      //add :e
      end_pos = 0;
      if (NULL == log_data || 0 > len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "trim_log_buffer(buf=%p[%ld]): invalid argument", log_data, len);
      }
      while (OB_SUCCESS == err && pos + log_entry.get_serialize_size() < len)
      {
        old_pos = pos;
        if (OB_SUCCESS != (err = log_entry.deserialize(log_data, len, pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, len=%ld, pos=%ld)=>%d", log_data, len, pos, err);
        }
        else if (old_pos + log_entry.get_serialize_size() + log_entry.get_log_data_len() > len)
        {
          pos = old_pos;
          break;
        }
        else if (OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
        }

        if (OB_SUCCESS != err)
        {}
        else if (real_end_id > 0 && real_end_id != (int64_t)log_entry.seq_)
        {
          err = OB_DISCONTINUOUS_LOG;
          TBSYS_LOG(WARN, "expected_id[%ld] != log_entry.seq[%ld]", real_end_id, log_entry.seq_);
        }
        //add wangjiahao [Paxos ups_replication_tmplog] 20150803 :b
        else if (last_term > 0 && last_term > log_entry.header_.get_reserved())
        {
          err = OB_DISCONTINUOUS_LOG;
          TBSYS_LOG(WARN, "last_term[%ld] != log_entry.header_.reserved[%ld]", last_term, log_entry.header_.get_reserved());
        }
        //add :e
        else
        {
          pos = old_pos + log_entry.get_serialize_size() + log_entry.get_log_data_len();
          real_end_id = log_entry.seq_ + 1;
          //add wangjiahao [Paxos ups_replication_tmplog] 20150803 :b
          last_term = log_entry.header_.get_reserved();
          //add :e
          if (0 >= real_start_id)
          {
            real_start_id = log_entry.seq_;
          }
          if (OB_LOG_SWITCH_LOG == log_entry.cmd_)
          {
            is_file_end = true;
            break;
          }
        }
      }

      if (OB_SUCCESS != err && OB_INVALID_ARGUMENT != err)
      {
        TBSYS_LOG(ERROR, "parse log buf error:");
        //hex_dump(log_data, static_cast<int32_t>(len), true, TBSYS_LOG_LEVEL_WARN);
      }
      else if (real_start_id > 0)
      {
        end_pos = pos;
        start_id = real_start_id;
        end_id = real_end_id;
        //add wangjiahao [Paxos ups_replication_tmplog] 20150803 :b
        term = last_term;
        //add :e
      }
      return err;
    }

    int trim_log_buffer(const int64_t offset, const int64_t align_bits,
                        const char* log_data, const int64_t len, int64_t& end_pos,
                        int64_t& start_id, int64_t& end_id, bool& is_file_end)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      int64_t old_pos = 0;
      ObLogEntry log_entry;
      int64_t real_start_id = 0;
      int64_t real_end_id = 0;
      int64_t last_align_end_pos = 0;
      int64_t last_align_end_id = 0;
      end_pos = 0;
      if (NULL == log_data || 0 > len || align_bits > 12)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "trim_log_buffer(buf=%p[%ld], align_bits=%ld): invalid argument", log_data, len, align_bits);
      }
      if (!is_align(offset, align_bits))
      {
        TBSYS_LOG(WARN, "offset[%ld] is not aligned[bits=%ld]", offset, align_bits);
      }
      while (OB_SUCCESS == err && pos <= len)
      {
        if (is_align(offset + pos, align_bits))
        {
          last_align_end_id = real_end_id;
          last_align_end_pos = offset + pos;
        }
        old_pos = pos;
        if (ObLogGenerator::is_eof(log_data + pos, len - pos))
        {
          TBSYS_LOG(WARN, "read_eof: offset=%ld, pos=%ld, log_id=%ld", offset, pos, real_end_id);
          break;
        }
        else if (is_file_end)
        {
          break;
        }
        else if (pos + log_entry.get_serialize_size() > len || is_file_end)
        {
          break;
        }
        else if (OB_SUCCESS != (err = log_entry.deserialize(log_data, len, pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, len=%ld, pos=%ld)=>%d", log_data, len, pos, err);
        }
        else if (old_pos + log_entry.get_serialize_size() + log_entry.get_log_data_len() > len)
        {
          pos = old_pos;
          break;
        }
        else if (OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
        }
        else if (real_end_id > 0 && real_end_id != (int64_t)log_entry.seq_)
        {
          err = OB_DISCONTINUOUS_LOG;
          TBSYS_LOG(WARN, "expected_id[%ld] != log_entry.seq[%ld]", real_end_id, log_entry.seq_);
        }
        else
        {
          pos = old_pos + log_entry.get_serialize_size() + log_entry.get_log_data_len();
          real_end_id = log_entry.seq_ + 1;
          if (0 >= real_start_id)
          {
            real_start_id = log_entry.seq_;
          }
          if (OB_LOG_SWITCH_LOG == log_entry.cmd_)
          {
            is_file_end = true;
          }
        }
      }

      if (OB_SUCCESS != err && OB_INVALID_ARGUMENT != err)
      {
        TBSYS_LOG(ERROR, "parse log buf error:");
        //hex_dump(log_data, static_cast<int32_t>(len), true, TBSYS_LOG_LEVEL_WARN);
      }
      else if (last_align_end_id > 0)
      {
        // 只有当最后一条日志的位置恰好对齐时，才可能把SWITCH_LOG包含进来
        is_file_end = (last_align_end_pos == offset + pos)? is_file_end: false;
        end_pos = last_align_end_pos - offset;
        start_id = real_start_id;
        end_id = last_align_end_id;
      }
      else
      {
        is_file_end = false;
        end_pos = 0;
        end_id = start_id;
      }
      if (OB_SUCCESS == err && len > 0 && end_pos <= 0)
      {
        TBSYS_LOG(WARN, "trim_log_buffer(offset=%ld, align=%ld, len=%ld, end_pos=%ld): not found aligned pos",
                  offset, align_bits, len, end_pos);
      }
      return err;
    }

    //add wangjiahao [paxos ups_replication_tmplog] 20150629 :b
    int load_file_for_log_func(const char* file_dir, const char* file_name, int64_t& data)
    {
      int err = 0;
      int64_t len = 0;
      int open_ret = 0;
      const int UINT64_MAX_LEN = 20;
      char file_fn[OB_MAX_FILE_NAME_LENGTH];
      char file_str[UINT64_MAX_LEN];
      int file_str_len = 0;
      FileUtils file_file;

      if (NULL == file_dir)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "Arguments are invalid[file_dir=%p]", file_dir);
      }
      else if ((len = snprintf(file_fn, sizeof(file_fn), "%s/%s", file_dir, file_name) < 0)
             && len >= (int64_t)sizeof(file_fn))
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "generate_data_fn()=>%d", err);
      }
      else if (!FileDirectoryUtils::exists(file_fn))
      {
        err = OB_FILE_NOT_EXIST;
        data = 0;
        TBSYS_LOG(WARN, "file_for_log file[\"%s\"] does not exist.", file_fn);
      }
      else if (0 > (open_ret = file_file.open(file_fn, O_RDONLY)))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(WARN, "open file[\"%s\"] error[%s]", file_fn, strerror(errno));
      }
      else if (0 > (file_str_len = static_cast<int32_t>(file_file.read(file_str, sizeof(file_str))))
             || file_str_len >= (int)sizeof(file_str))
      {
        err = file_str_len < 0 ? OB_IO_ERROR: OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "read error[%s] or file contain invalid data[len=%d]", strerror(errno), file_str_len);
      }
      else
      {
        file_str[file_str_len] = '\0';
        const int STRTOUL_BASE = 10;
        char* endptr;
        data = static_cast<int64_t>(strtoull(file_str, &endptr, STRTOUL_BASE));
        if ('\0' != *endptr)
        {
          err = OB_INVALID_DATA;
          TBSYS_LOG(ERROR, "non-digit exist in file_for_log file[file_str=%.*s]", file_str_len, file_str);
        }
        else if (ERANGE == errno)
        {
          err = OB_INVALID_DATA;
          TBSYS_LOG(ERROR, "data contained in file_for_log file is out of range");
        }
      }
      if (0 > open_ret)
      {
        file_file.close();
      }
      return err;
    }


    int write_file_for_log_func(const char* file_dir, const char* file_name, const int64_t data)
    {
      int err = OB_SUCCESS;
      const int UINT64_MAX_LEN = 20;
      int len = 0;
      int open_ret = 0;
      FileUtils file_file;
      char file_fn[OB_MAX_FILE_NAME_LENGTH];
      char file_str[UINT64_MAX_LEN];
      int64_t file_str_len = 0;

      if (NULL == file_dir)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "Arguments are invalid[file_dir=%p]", file_dir);
      }
      else if ((len = snprintf(file_fn, sizeof(file_fn), "%s/%s", file_dir, file_name) < 0)
             && len >= (int64_t)sizeof(file_fn))
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "generate_data_fn()=>%d", err);
      }
      else if (0 > (open_ret = file_file.open(file_fn, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)))
      {
        err = OB_FILE_NOT_EXIST;
        TBSYS_LOG(ERROR, "open file[\"%s\"] error[%s]", file_fn, strerror(errno));
      }
      else if ((file_str_len = snprintf(file_str, sizeof(file_str), "%ld", data)) < 0
             || file_str_len >= (int64_t)sizeof(file_str))
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "snprintf file_str error[%s][data=%ld]", strerror(errno), data);
      }
      else if (0 > (file_str_len = file_file.write(file_str, file_str_len)))
      {
        err = OB_ERR_SYS;
        TBSYS_LOG(ERROR, "write error[%s][file_str=%p file_str_len=%ld]", strerror(errno), file_str, file_str_len);
      }
      if (0 > open_ret)
      {
        file_file.close();
      }
      return err;
    }

    int ObLogCommitPoint::init(const char* file_dir, const char* file_name)
    {
      int err = OB_SUCCESS;
      file_dir_ = file_dir;
      file_name_ = file_name;
      return err;
    }

    int ObLogCommitPoint::write(const int64_t data)
    {
      return write_file_for_log_func(file_dir_, file_name_, data);
    }


    int ObLogCommitPoint::get(int64_t& data)
    {
      return load_file_for_log_func(file_dir_, file_name_, data);
    }

    int ObLogTerm::init(const char* file_dir, const char* file_name)
    {
      int err = OB_SUCCESS;
      int64_t value;
      if (file_dir == NULL || file_name == NULL)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (is_init_)
      {
        err = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (err = load_file_for_log_func(file_dir, file_name, value)) && err != OB_FILE_NOT_EXIST)
      {
        TBSYS_LOG(ERROR, "load_file_for_log_func(file_dir=%s, file_name=%s)=>%d", file_dir, file_name, err);
      }
      else if (err != OB_FILE_NOT_EXIST)
      {
          file_dir_ = file_dir;
          file_name_ = file_name;
          term_value_ = value;
      }
      else
      {
          write_file_for_log_func(file_dir, file_name, 0);
          file_dir_ = file_dir;
          file_name_ = file_name;
          term_value_ = 0;
      }
      return err;
    }

    int ObLogTerm::write(const int64_t data)
    {
      int err = OB_SUCCESS;
      if (!is_init_)
      {
          err = OB_NOT_INIT;
      }
      else
      {
          tbsys::CThreadGuard guard(&lock_);
          if (OB_SUCCESS != (err = write_file_for_log_func(file_dir_, file_name_, data)))
          {
            TBSYS_LOG(ERROR, "write_file_for_log_func(file_dir=%s, file_name=%s)=>%d", file_dir_, file_name_, err);
          }
          else
          {
            term_value_ = data;
          }

      }
      return err;
    }


    int ObLogTerm::get(int64_t& data)
    {
        int err = OB_SUCCESS;
        if (!is_init_)
        {
            err = OB_NOT_INIT;
        }
        else
        {
            data = term_value_;
        }
      return err;
    }


//add :e
//add wangdonghui [ups_replication] 20161221 :b
int get_replay_cursor_from_file(const char* log_dir, ObLogCursor end_cursor, int64_t clog_point, ObLogCursor& replay_cursor)
    {
      int err = OB_SUCCESS;
      char* buf = NULL;
      int64_t len = ObLogWriter::LOG_BUFFER_SIZE;
      int64_t read_count = 0;
      ObPosLogReader reader;
      ObLogLocation start_location;
      ObLogLocation end_location;

      if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: log_dir=%s", log_dir);
      }
      else if (NULL == (buf = (char*)ob_malloc(len, ObModIds::OB_UPS_COMMON)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "ob_malloc(len=%ld) fail", len);
      }
      else if (OB_SUCCESS != (err = get_first_log_id_func(log_dir, end_cursor.file_id_, end_cursor.log_id_, NULL))
               && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "get_first_log_id_func()=>%d", err);
      }
      // add 20170210 :b
      else if(end_cursor.log_id_ > clog_point)
      {
          err = OB_EAGAIN;//replay_cursor doesn't exist in this log file
      }
      //add :e
      else if (OB_SUCCESS != (err = reader.init(log_dir, true)))
      {
        TBSYS_LOG(ERROR, "located_log_reader init error[err=%d]", err);
      }
      else
      {
        start_location.file_id_ = end_cursor.file_id_;
        start_location.log_id_ = end_cursor.log_id_;
        end_location = start_location;
      }
      while (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = reader.get_log(start_location.log_id_, start_location, end_location,
                                                     buf, len, read_count)))
        {
          TBSYS_LOG(ERROR, "reader.get_log(log_id=%ld)=>%d", start_location.log_id_, err);
        }
        if (end_location.log_id_ >= clog_point && start_location.log_id_ < clog_point)
        {
          ObLogCursor tmp_cursor;
          tmp_cursor.file_id_ = start_location.file_id_;
          tmp_cursor.log_id_ = start_location.log_id_;
          tmp_cursor.offset_ = start_location.offset_;
          int64_t count;
          if (OB_SUCCESS != (err = find_log_from_buf(buf, read_count, tmp_cursor, clog_point, count)))
          {
            TBSYS_LOG(ERROR, "fail to find log from buf(log_id=%ld)=>%d", clog_point, err);
          }
          else if (tmp_cursor.log_id_ == clog_point) //find that log
          {
            replay_cursor.file_id_ = tmp_cursor.file_id_;
            replay_cursor.log_id_ = tmp_cursor.log_id_;
            replay_cursor.offset_ = tmp_cursor.offset_;
            break;
          }
          else
          {
            TBSYS_LOG(ERROR, "WTF(log_id=%ld)=>%d", clog_point, err);
          }
        }
        else
        {
            start_location = end_location;
        }
    }
    return err;
   }
//add :e
  } // end namespace updateserver
} // end namespace oceanbase
