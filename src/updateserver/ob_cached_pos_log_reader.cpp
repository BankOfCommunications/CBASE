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
#include "ob_cached_pos_log_reader.h"
#include "ob_fetched_log.h"
#include "ob_pos_log_reader.h"
#include "ob_log_buffer.h"
#include "common/utility.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    // 产生的id只是用于检验session是否匹配，可以不唯一，
    // 因为如果备机传来错误的位置，在读取日志时可以检查到
    static int64_t gen_session_id()
    {
      int64_t now_us = tbsys::CTimeUtil::getTime();
      return now_us;
    }

    struct ObLogLocationInfo
    {
      ObLogLocationInfo(): version_(cur_version_), location_(), buf_pos_(0) {}
      ~ObLogLocationInfo() {}
      static int64_t cur_version_;
      int64_t version_;
      ObLogLocation location_;
      int64_t buf_pos_;

      int save_to_session(ObSessionBuffer& buf) const
      {
        int err = OB_SUCCESS;
        if (sizeof(*this) > sizeof(buf.data_))
        {
          err = OB_BUF_NOT_ENOUGH;
        }
        else
        {
          buf.data_len_ = sizeof(*this);
          *(ObLogLocationInfo*)buf.data_ = *this;
        }
        return err;
      }

      int restore_from_session(ObSessionBuffer& buf)
      {
        int err = OB_SUCCESS;
        if (buf.data_len_ == 0)
        {} // 备机第一次连接时使用空session
        else if (buf.data_len_ != sizeof(*this) || *(int64_t*)buf.data_ != cur_version_)
        {
          TBSYS_LOG(WARN, "invalid session: maybe master changed!");
        }
        else
        {
          *this = *(ObLogLocationInfo*)buf.data_;
        }
        return err;
      }

      char* to_str() const
      {
        static __thread char buf[OB_MAX_DEBUG_MSG_LEN];
        snprintf(buf, sizeof(buf), "location{buf_pos=%ld, log_id=%ld, file=%ld:+%ld}",
                buf_pos_, location_.log_id_, location_.file_id_, location_.offset_);
        buf[sizeof(buf)-1] = 0;
        return buf;
      }
    };
    int64_t ObLogLocationInfo::cur_version_ = gen_session_id();

    int get_local_log(ObPosLogReader* log_reader, ObLogBuffer* log_buffer,
                      ObFetchLogReq& req, ObFetchedLog& fetched_log)
    {
      int err = OB_SUCCESS;
      ObLogLocationInfo location;
      ObLogLocationInfo end_location;
      ObFetchLogReq next_req;
      char* buf = fetched_log.log_data_;
      int64_t len = min(req.max_data_len_, fetched_log.max_data_len_);
      int64_t read_count = 0;

      if (NULL == log_reader || NULL == log_buffer)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = location.restore_from_session(req.session_)))
      {
        TBSYS_LOG(ERROR, "location.deserialize()=>%d", err);
      }
      else if (OB_SUCCESS != (err = get_from_log_buffer(log_buffer, OB_DIRECT_IO_ALIGN_BITS, location.buf_pos_, end_location.buf_pos_, req.start_id_, next_req.start_id_, buf, len, read_count))
               && OB_DATA_NOT_SERVE != err)
      {
        TBSYS_LOG(ERROR, "get_from_log_buffer(start_id=%ld)=>%d", req.start_id_, err);
      }
      else if (OB_SUCCESS == err)
      {} // read from buf
      else if (OB_SUCCESS != (err = log_reader->get_log(req.start_id_, location.location_, end_location.location_, buf, len, read_count))
               && OB_DATA_NOT_SERVE != err)
      {
        TBSYS_LOG(ERROR, "pos_log_reader.get_log(start_id=%ld)=>%d", req.start_id_, err);
      }
      else if (OB_SUCCESS == err)
      {
        next_req.start_id_ = end_location.location_.log_id_;
      }
      else
      {
        TBSYS_LOG(WARN, "pos_log_reader.get_log(): DATA_NOT_SERVE, maybe slave has more recent log than master.");
      }

      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = end_location.save_to_session(next_req.session_)))
      {
        TBSYS_LOG(ERROR, "location.deserialize()=>%d", err);
      }
      else
      {
        fetched_log.start_id_ = req.start_id_;
        fetched_log.end_id_ = next_req.start_id_;
        fetched_log.data_len_ = read_count;
        fetched_log.next_req_ = next_req;
      }
      TBSYS_LOG(DEBUG, "get_local_log(log_id=%ld, read_count=%ld)=>%d", req.start_id_, read_count, err);
      return err;
    }

    ObCachedPosLogReader::ObCachedPosLogReader(): log_reader_(NULL), log_buffer_(NULL)
    {}

    ObCachedPosLogReader::~ObCachedPosLogReader()
    {}

    bool ObCachedPosLogReader::is_inited() const
    {
      return NULL != log_reader_ && NULL != log_buffer_;
    }

    int ObCachedPosLogReader::init(ObPosLogReader* log_reader, ObLogBuffer* log_buffer)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == log_reader || NULL == log_buffer)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        log_reader_ = log_reader;
        log_buffer_ = log_buffer;
      }
      return err;
    }

    int ObCachedPosLogReader:: get_log(ObFetchLogReq& req, ObFetchedLog& result)
    {
      int err = OB_SUCCESS;
      req.max_data_len_ = min(req.max_data_len_, OB_MAX_LOG_BUFFER_SIZE);
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = get_local_log(log_reader_, log_buffer_, req, result))
               && OB_DATA_NOT_SERVE != err)
      {
        TBSYS_LOG(ERROR, "get_local_log(req.start_id=%ld)=>%d", req.start_id_, err);
      }
      return err;
    }

    //add wangjiahao [Paxos ups_replication] 20150709
    ObCachedRegionLogReader::ObCachedRegionLogReader(): log_reader_(NULL), log_buffer_(NULL)
    {}

    ObCachedRegionLogReader::~ObCachedRegionLogReader()
    {}

    bool ObCachedRegionLogReader::is_inited() const
    {
      return NULL != log_reader_ && NULL != log_buffer_;
    }

    int ObCachedRegionLogReader::init(ObRegionLogReader* log_reader, ObLogBuffer* log_buffer)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == log_reader || NULL == log_buffer)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        log_reader_ = log_reader;
        log_buffer_ = log_buffer;
      }
      return err;
    }

    int ObCachedRegionLogReader::get_log(int64_t start_id, int64_t end_id, char *data, int64_t& data_len, int64_t max_data_len, int64_t& old_pos, bool disk_only/*=false*/)
    {
      int err = OB_SUCCESS;
      char* buf = data;
      int64_t new_pos = 0;
      int64_t read_count = 0;

      if (!is_inited())
      {
          err = OB_NOT_INIT;
      }
      else if (!disk_only)
      {
          if (OB_SUCCESS != (err = get_from_log_buffer_2(log_buffer_, OB_DIRECT_IO_ALIGN_BITS, old_pos, new_pos, start_id, end_id, buf, read_count, max_data_len))
               && OB_DATA_NOT_SERVE != err)
          {
            TBSYS_LOG(ERROR, "get_from_log_buffer(start_id=%ld)=>%d", start_id, err);
          }
          else if (OB_SUCCESS == err)
          { // read from buf
              old_pos = new_pos;
              data_len = read_count;
              //TBSYS_LOG(INFO, "wangjiahao get_from_log_buffer2 succ start_id=%ld  end_id=%ld", start_id, end_id);
          }
      }
      if (!disk_only && OB_SUCCESS == err)
      {}
      else if (OB_SUCCESS != (err = log_reader_->get_log(start_id, end_id, buf, read_count, max_data_len))
               && OB_DATA_NOT_SERVE != err)
      {
        TBSYS_LOG(ERROR, "pos_log_reader.get_log(start_id=%ld)=>%d", start_id, err);
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "wangjiahao_region_log Region_log_reader.get_log(start_id=%ld end_id=%ld): err=%d", start_id, end_id, err);
      }
      else
      { //read from disk
          data_len = read_count;
          //TBSYS_LOG(INFO, "wangjiahao get_log_from_disk succ start_id=%ld  end_id=%ld [%ld]", start_id, end_id, read_count);
      }
      return err;
    }
    //add :e

  }; // end namespace updateserver
}; // end namespace oceanbase
