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

#include "common/ob_define.h"
#include "common/ob_repeated_log_reader.h"
#include "common/ob_log_dir_scanner.h"
#include "ob_on_disk_log_locator.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    int get_max_log_file_id_in_dir(const char* log_dir, int64_t& max_log_file_id)
    {
      int err = OB_SUCCESS;
      ObLogDirScanner log_dir_scanner;
      if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = log_dir_scanner.init(log_dir)))
      {
        if (OB_DISCONTINUOUS_LOG == err)
        {
          TBSYS_LOG(WARN, "log_dir_scanner.init(%s): log file not continuous", log_dir);
        }
        else
        {
          TBSYS_LOG(ERROR, "log_dir_scanner.init(%s)=>%d", log_dir, err);
        }
      }
      else if (OB_SUCCESS != (err = log_dir_scanner.get_max_log_id((uint64_t&)max_log_file_id)))
      {
        if (OB_ENTRY_NOT_EXIST == err)
        {
          TBSYS_LOG(WARN, "log_dir_scanner.get_max_log_id(log_dir=%s): no log file in dir.", log_dir);
        }
        else
        {
          TBSYS_LOG(ERROR, "log_dir_scanner.get_max_log_id(log_dir=%s)=>%d.", log_dir, err);
        }
      }
      return err;
    }

    int get_log_file_id_range_in_dir(const char* log_dir, int64_t& min_log_file_id, int64_t& max_log_file_id)
    {
      int err = OB_SUCCESS;
      ObLogDirScanner log_dir_scanner;
      if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = log_dir_scanner.init(log_dir)))
      {
        if (OB_DISCONTINUOUS_LOG == err)
        {
          TBSYS_LOG(WARN, "log_dir_scanner.init(%s): log file not continuous", log_dir);
        }
        else
        {
          TBSYS_LOG(ERROR, "log_dir_scanner.init(%s)=>%d", log_dir, err);
        }
      }
      else if (OB_SUCCESS != (err = log_dir_scanner.get_min_log_id((uint64_t&)min_log_file_id)))
      {
        if (OB_ENTRY_NOT_EXIST == err)
        {
          TBSYS_LOG(WARN, "log_dir_scanner.get_min_log_id(log_dir=%s): no log file in dir.", log_dir);
        }
        else
        {
          TBSYS_LOG(ERROR, "log_dir_scanner.get_min_log_id(log_dir=%s)=>%d.", log_dir, err);
        }
      }
      else if (OB_SUCCESS != (err = log_dir_scanner.get_max_log_id((uint64_t&)max_log_file_id)))
      {
        if (OB_ENTRY_NOT_EXIST == err)
        {
          TBSYS_LOG(WARN, "log_dir_scanner.get_max_log_id(log_dir=%s): no log file in dir.", log_dir);
        }
        else
        {
          TBSYS_LOG(ERROR, "log_dir_scanner.get_max_log_id(log_dir=%s)=>%d.", log_dir, err);
        }
      }
      return err;
    }

    int get_first_log_id_func(const char* log_dir, const int64_t file_id, int64_t& log_id, ObFirstLogIdCache* log_id_cache)
    {
      int err = OB_SUCCESS;
      ObRepeatedLogReader log_reader;
      LogCommand cmd = OB_LOG_NOP;
      uint64_t log_seq = 0;
      char* log_data = NULL;
      int64_t data_len = 0;

      if (NULL == log_dir || 0 >= file_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != log_id_cache && OB_SUCCESS != (err = log_id_cache->get(file_id, log_id))
               && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "log_id_cache->get(file_id=%ld)=>%d", file_id, err);
      }
      else if (NULL != log_id_cache && OB_SUCCESS == err)
      {}
      else if (OB_SUCCESS != (err = log_reader.init(log_dir)))
      {
        TBSYS_LOG(ERROR, "log_reader.init(log_dir=%s)=>%d", log_dir, err);
      }
      else if (OB_SUCCESS != (err = log_reader.open(file_id)))
      {
        if (OB_FILE_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "log_reader.open(file_id=%ld)=>%d", file_id, err);
        }
        else
        {
          err = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(DEBUG, "log_reader.open(file_id=%ld)=>FILE_NOT_EXIST", file_id);
        }
      }
      else if (OB_SUCCESS != (err = log_reader.read_log(cmd, log_seq, log_data, data_len)))
      {
        if (OB_READ_NOTHING != err)
        {
          TBSYS_LOG(ERROR, "log_reader.read_log()=>%d", err);
        }
        else
        {
          err = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(DEBUG, "log_reader.read_log(file_id=%ld)=>READ_NOTHING", file_id);
        }
      }
      else if (NULL != log_id_cache && OB_SUCCESS != (err = log_id_cache->add(file_id, log_seq)))
      {
          TBSYS_LOG(ERROR, "log_id_cache->add(file_id=%ld, log_id=%ld)=>%d", file_id, log_seq, err);
      }
      else
      {
        log_id = (int64_t)log_seq;
      }

      if (log_reader.is_opened())
      {
        log_reader.close();
      }
      return err;
    }

    int get_file_id_func(ObFirstLogIdCache* log_id_cache, const char* log_dir, const int64_t max_log_file_id, int64_t log_id, int64_t& file_id)
    {
      int err = OB_SUCCESS;
      int64_t first_log_id = 0;
      if (NULL == log_dir || 0 >= max_log_file_id || 0 >= log_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for(file_id = max_log_file_id; OB_SUCCESS == err && file_id > 0; file_id--)
      {
        if (OB_SUCCESS != (err = get_first_log_id_func(log_dir, file_id, first_log_id, log_id_cache))
            && OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_first_log_id_func(log_dir=%s, file_id=%ld)=>%d", log_dir, file_id, err);
        }
        else if (OB_ENTRY_NOT_EXIST == err)
        {
          if (file_id == max_log_file_id)
          {
            err = OB_SUCCESS;
          }
        }
        else if (log_id >= first_log_id)
        {
          break;
        }
      }
      if (OB_SUCCESS == err && file_id <= 0) // 成功扫描完了所有文件，但是依然未找到比log_id编号小的日志
      {
        err = OB_ENTRY_NOT_EXIST;
      }
      return err;
    }

    int get_file_id_func2(ObFirstLogIdCache* log_id_cache, const char* log_dir,
                          const int64_t min_log_file_id, const int64_t max_log_file_id, int64_t log_id, int64_t& file_id)
    {
      int err = OB_SUCCESS;
      int64_t first_log_id = 0;
      int64_t start = min_log_file_id;
      int64_t end = max_log_file_id;
      if (NULL == log_dir || 0 >= min_log_file_id || 0 >= max_log_file_id || min_log_file_id > max_log_file_id || 0 >= log_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for(; OB_SUCCESS == err && start < end; )
      {
        file_id = (start + end + 1)/2;
        if (OB_SUCCESS != (err = get_first_log_id_func(log_dir, file_id, first_log_id, log_id_cache))
            && OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_first_log_id_func(log_dir=%s, file_id=%ld)=>%d", log_dir, file_id, err);
        }
        else if (OB_ENTRY_NOT_EXIST == err)
        {
          if (max_log_file_id == file_id)
          {
            err = OB_SUCCESS;
            end = file_id -1;
            TBSYS_LOG(WARN, "last_log_file[%ld] is empty", max_log_file_id);
          }
          else
          {
            TBSYS_LOG(ERROR, "get_first_log_id(file_id=%ld): ENTRY_NOT_EXIST", file_id);
          }
        }
        else if (log_id < first_log_id)
        {
          end = file_id - 1;
        }
        else
        {
          start = file_id;
        }
      }
      file_id = start;
      TBSYS_LOG(DEBUG, "get_file_id_by_log_id(last_candinate=%ld)", file_id);
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = get_first_log_id_func(log_dir, file_id, first_log_id, log_id_cache))
          && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "get_first_log_id_func(log_dir=%s, file_id=%ld)=>%d", log_dir, file_id, err);
      }
      else if (OB_ENTRY_NOT_EXIST == err)
      {}
      else if (log_id < first_log_id)
      {
        err = OB_ENTRY_NOT_EXIST;
      }
      return err;
    }

    int get_log_file_offset_func(const char* log_dir, const int64_t file_id, const int64_t log_id, int64_t& offset)
    {
      int err = OB_SUCCESS;
      ObRepeatedLogReader log_reader;
      LogCommand cmd = OB_LOG_NOP;
      uint64_t log_seq = 0;
      char* log_data = NULL;
      int64_t data_len = 0;
      if (NULL == log_dir || 0 >= file_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = log_reader.init(log_dir)))
      {
        TBSYS_LOG(ERROR, "log_reader.init(log_dir=%s)=>%d", log_dir, err);
      }
      else if (OB_SUCCESS != (err = log_reader.open(file_id)))
      {
        if (OB_FILE_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "log_reader.open(file_id=%ld)=>%d", file_id, err);
        }
        else
        {
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      while (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = log_reader.read_log(cmd, log_seq, log_data, data_len)))
        {
          if (OB_READ_NOTHING != err)
          {
            TBSYS_LOG(ERROR, "log_reader.read_log()=>%d", err);
          }
          else
          {
            err = OB_ENTRY_NOT_EXIST;
          }
        }
        else if ((int64_t)log_seq > log_id)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
        else if ((int64_t)log_seq + 1 == log_id)
        {
          offset = log_reader.get_last_log_offset();
          if (cmd == OB_LOG_SWITCH_LOG)
          {
            err = OB_ENTRY_NOT_EXIST;
            TBSYS_LOG(WARN, "find log[%ld] in file %ld, but is SWITCH_LOG", log_seq, file_id);
          }
          break;
        }
        else if ((int64_t)log_seq == log_id) // 第一条日志
        {
          offset = 0;
          break;
        }
      }

      if (log_reader.is_opened())
      {
        log_reader.close();
      }
      return err;
    }

    ObOnDiskLogLocator::ObOnDiskLogLocator(): enable_log_id_cache_(true), log_dir_(NULL)
    {}

    ObOnDiskLogLocator::~ObOnDiskLogLocator()
    {}

    bool ObOnDiskLogLocator::is_inited() const
    {
      return NULL != log_dir_;
    }

    int ObOnDiskLogLocator::init(const char* log_dir)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = first_log_id_cache_.init(DEFAULT_FIRST_LOG_ID_CACHE_CAPACITY)))
      {
        TBSYS_LOG(ERROR, "first_log_id_cache.init()=>%d", err);
      }
      else
      {
        log_dir_ = log_dir;
      }
      return err;
    }

    int ObOnDiskLogLocator::get_location(const int64_t log_id, ObLogLocation& location)
    {
      int err = OB_SUCCESS;
      int64_t min_log_file_id = 0;
      int64_t max_log_file_id = 0;
      int64_t file_id = 0;
      int64_t offset = 0;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = get_log_file_id_range_in_dir(log_dir_, min_log_file_id, max_log_file_id)))
      {
        if (OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_max_log_file_id(log_dir=%s)=>%d", log_dir_, err);
        }
      }
      else if (OB_SUCCESS != (err = get_file_id_func2(enable_log_id_cache_? &first_log_id_cache_: NULL, log_dir_, min_log_file_id, max_log_file_id, log_id, file_id)))
      {
        if (OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_file_id_helper(log_dir=%s, max_log_file_id=%ld, log_id=%ld)=>%d",
                    log_dir_, max_log_file_id, log_id, err);
        }
      }
      else if (OB_SUCCESS != (err = get_log_file_offset_func(log_dir_, file_id, log_id, offset)))
      {
        if (OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_log_file_offset(log_dir=%s, file_id=%ld, log_id=%ld)=>%d", log_dir_, file_id, log_id, err);
        }
      }
      else
      {
        location.log_id_ = log_id;
        location.file_id_ = file_id;
        location.offset_ = offset;
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
