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

#include "tbsys.h"
#include "common/ob_define.h"
#include "ob_pos_log_reader.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    ObPosLogReader::ObPosLogReader()
    {
      memset(log_dir_, 0, sizeof(log_dir_));
    }

    ObPosLogReader::~ObPosLogReader()
    {}

    bool ObPosLogReader::is_inited() const
    {
      return log_dir_[0] != 0;
    }

    int ObPosLogReader::init(const char* log_dir, const bool dio)
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
      else if (0 >= snprintf(log_dir_, sizeof(log_dir_), "%s", log_dir))
      {
        err = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (err = on_disk_log_locator_.init(log_dir_)))
      {
        TBSYS_LOG(ERROR, "on_disk_log_locator_.init(log_dir=%s)=>%d", log_dir_, err);
      }
      else if (OB_SUCCESS != (err = located_log_reader_.init(log_dir_, dio)))
      {
        TBSYS_LOG(ERROR, "located_log_reader_.init(log_dir)=>%d", err);
      }
      return err;
    }

    int ObPosLogReader::get_log(const int64_t start_id, ObLogLocation& start_location, ObLogLocation& end_location,
                                char* buf, const int64_t len, int64_t& read_count)
    {
      int err = OB_SUCCESS;
      bool is_file_end = false;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (0 >= start_id || NULL == buf || 0 > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if ((!start_location.is_valid() || start_location.log_id_ != start_id)
               && OB_SUCCESS != (err = on_disk_log_locator_.get_location(start_id, start_location)))
      {
        if (OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_location(start_id=%ld)=>%d", start_location.log_id_, err);
        }
        else
        {
          err = OB_DATA_NOT_SERVE;
        }
      }
      else if (OB_SUCCESS != (err = located_log_reader_.read_log(start_location.file_id_, start_location.offset_,
                                                                 start_location.log_id_, end_location.log_id_,
                                                                 buf, len, read_count, is_file_end)))
      {
        TBSYS_LOG(ERROR, "located_log_reader.read_log(file=%ld:+%ld, start_id=%ld, buf=%p[%ld])=>%d",
                  start_location.file_id_, start_location.offset_, start_location.log_id_, buf, len, err);
      }
      else if (is_file_end)
      {
        end_location.file_id_ = start_location.file_id_ + 1;
        end_location.offset_ = 0;
      }
      else
      {
        end_location.file_id_ = start_location.file_id_;
        end_location.offset_ = start_location.offset_ + read_count;
      }
      return err;
    }

    //add wangjiahao [Paxos ups_replication_tmplog] 20150630 :b
    ObRegionLogReader::ObRegionLogReader()
    {
      memset(log_dir_, 0, sizeof(log_dir_));
    }

    ObRegionLogReader::~ObRegionLogReader()
    {}
    int ObRegionLogReader::get_log(const int64_t start_id, const int64_t end_id, char* buf, int64_t& read_count, int64_t max_data_len)
    {
      int err = OB_SUCCESS;
      int err1 = OB_SUCCESS;
      bool is_file_end = false;
      int64_t len = 0;
      ObLogLocation start_location, end_location;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (0 >= start_id || NULL == buf || end_id < start_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = on_disk_log_locator_.get_location(start_id, start_location)))
      {
        if (OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_location(start_id=%ld)=>%d", start_id, err);
        }
        else
        {
          err = OB_DATA_NOT_SERVE;
        }
      }
      else
      {
        if (OB_SUCCESS != (err1 = on_disk_log_locator_.get_location(end_id, end_location)))
        {
            len = max_data_len; //bug_fix becauese of the last log with end_id cannot be found by get_location() method, so read all of them
        }
        else
        {
            len = end_location.offset_ - start_location.offset_;
            if (len > max_data_len || start_location.file_id_ != end_location.file_id_ || len <= 0)  //if sizeof data is larger than max_data_len or data locate in diffrent files
            {
                len = max_data_len;
            }
        }
      }

      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = located_log_reader_.read_log(start_location.file_id_, start_location.offset_,
                                                                 start_location.log_id_, end_location.log_id_,
                                                                 buf, len,
                                                                 read_count, is_file_end)))
      {
        TBSYS_LOG(ERROR, "located_log_reader.read_log(file=%ld:+%ld, start_id=%ld, buf=%p[%ld])=>%d",
                  start_location.file_id_, start_location.offset_, start_location.log_id_, buf, end_location.offset_ - start_location.offset_, err);
      }
      else if (is_file_end)
      {
        end_location.file_id_ = start_location.file_id_ + 1;
        end_location.offset_ = 0;
      }
      else
      {
        end_location.file_id_ = start_location.file_id_;
        end_location.offset_ = start_location.offset_ + read_count;
      }
      return err;
    }
    //add :e
  }; // end namespace updateserver
}; // end namespace oceanbase
