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

#include "ob_log_writer2.h"

namespace oceanbase
{
  namespace common
  {
    ObLogWriterV2::ObLogWriterV2(): log_dir_(NULL), file_(), dio_(true), align_mask_(0), log_sync_type_(OB_LOG_SYNC)
    {
    }

    ObLogWriterV2::~ObLogWriterV2()
    {
      file_.close();
      if (NULL != log_dir_)
      {
        free(log_dir_);
        log_dir_ = NULL;
      }
    }

    int ObLogWriterV2::check_inited() const
    {
      int err = OB_SUCCESS;
      if (NULL == log_dir_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "log_dir==NULL");
      }
      return err;
    }

    int ObLogWriterV2::check_state() const
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_inited()))
      {
        TBSYS_LOG(ERROR, "check_inited()=>%d", err);
      }
      else if (!log_cursor_.is_valid())
      {
        TBSYS_LOG(ERROR, "log_cursor[%s].is_valid()=>false", log_cursor_.to_str());
      }
      return err;
    }

    int ObLogWriterV2::init(const char* log_dir, const int64_t align_mask, int64_t log_sync_type)
    {
      int err = OB_SUCCESS;

      if (NULL != log_dir_)
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "init twice");
      }
      else if (NULL == log_dir || (align_mask & (align_mask + 1)))
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "Parameter are invalid[log_dir=%p, align_mask=%ld]", log_dir, align_mask);
      }
      else if (NULL == (log_dir_ = strndup(log_dir, OB_MAX_FILE_NAME_LENGTH)))
      {
        TBSYS_LOG(ERROR, "strdup()=>NULL, log_dir=%s", log_dir);
      }
      else
      {
        align_mask_ = align_mask;
        log_sync_type_ = log_sync_type;
      }

      return err;
    }

    int ObLogWriterV2::reset()
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_inner_stat()=>%d", err);
      }
      else
      {
        if (file_.is_opened())
        {
          file_.close();
        }
        log_cursor_.reset();
      }
      return err;
    }

    static int open_log_file_func(ObFileAppender& file, const char* log_dir, const uint64_t log_file_id,
                                  const bool dio, const bool is_trunc)
    {
      int err = OB_SUCCESS;
      struct stat file_info;
      int64_t len = 0;
      char file_name[OB_MAX_FILE_NAME_LENGTH];
      if (OB_SUCCESS != (err = stat(log_dir, &file_info))
          && OB_SUCCESS != (err = mkdir(log_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)))
      {
        TBSYS_LOG(ERROR, "create \"%s\" directory error[%s]", log_dir, strerror(errno));
      }
      else if ((len = snprintf(file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", log_dir, log_file_id)) <= 0
               && len >= OB_MAX_FILE_NAME_LENGTH)
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "generate_file_name(len=%ld, log_dir=%s, log_file_id=%ld)=>%d", len, log_dir, log_file_id, err);
      }
      else if (OB_SUCCESS != (err = file.open(ObString(static_cast<int32_t>(len),
                                                       static_cast<int32_t>(len), file_name),
                                              dio, true, is_trunc, OB_DIRECT_IO_ALIGN)))
      {
        TBSYS_LOG(ERROR, "open commit log file[file_name=%s] ret=%d", file_name, err);
      }
      return err;
    }

    int ObLogWriterV2::start_log(const ObLogCursor& log_cursor)
    {
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "start_log(log_cursor=%s)", log_cursor.to_str());
      if (OB_SUCCESS != (err = check_inited()))
      {
        TBSYS_LOG(ERROR, "check_inner_stat()=>%d", err);
      }
      else if (!log_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "log_cursor[%s].is_valid()=>false", log_cursor.to_str());
      }
      else if (log_cursor_.is_valid())
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "ObLogWriter is init already init, log_cursor_=%s", log_cursor_.to_str());
      }
      else if (OB_SUCCESS != (err = open_log_file_func(file_, log_dir_, log_cursor.file_id_, dio_, false)))
      {
        TBSYS_LOG(ERROR, "open_log_file_ error[ret=%d]", err);
      }
      else if (-1 == file_.get_file_pos())
      {
        err = OB_ERR_SYS;
        TBSYS_LOG(ERROR, "file_.get_file_pos(): %s", strerror(errno));
      }
      else if (log_cursor.log_id_ != 0 && file_.get_file_pos() != log_cursor.offset_)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "log file is broken");
      }
      else if (0 != (log_cursor.offset_ & align_mask_))
      {
        err = OB_LOG_NOT_ALIGN;
        TBSYS_LOG(WARN, "start_log(%s): LOG_NOT_ALIGNED", to_cstring(log_cursor));
      }
      else
      {
        log_cursor_ = log_cursor;
      }
      return err;
    }

    static int open_log_file_maybe_(ObFileAppender& file, const char* log_dir,
                                    const int64_t old_file_id, const int64_t new_file_id, const bool dio, const bool is_trunc)
    {
      int err = OB_SUCCESS;

      if (old_file_id  != new_file_id)
      {
        if (old_file_id > 0)
        {
          file.close();
        }
        if (OB_SUCCESS != (err = open_log_file_func(file, log_dir, new_file_id, dio, is_trunc)))
        {
          TBSYS_LOG(ERROR, "open_log_file(new_file_id=%ld)=>%d", new_file_id, err);
        }
        else
        {
          TBSYS_LOG(INFO, "open_log_file(new_file_id=%ld)", new_file_id);
        }
      }
      else
      {} // do nothing
      return err;
    }

    static int parse_log_buffer(const char* log_data, int64_t data_len, const ObLogCursor& start_cursor, ObLogCursor& end_cursor, bool check_data_integrity = false)
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
        else if (check_data_integrity && OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
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
                                     && start_cursor.log_id_ == file_id)))
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
        hex_dump(log_data, static_cast<int32_t>(data_len), TBSYS_LOG_LEVEL_WARN);
      }
      return err;
    }

    int ObLogWriterV2::write_log(const char* log_data, int64_t data_len)
    {
      int err = OB_SUCCESS;
      ObLogCursor new_cursor;
      if (0 != (data_len % OB_DIRECT_IO_ALIGN))
      {
        err = OB_LOG_NOT_ALIGN;
        TBSYS_LOG(ERROR, "write_log(data_len=%ld): NOT_ALIGN", data_len);
      }
      else if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_inner_stat()=>%d", err);
      }
      else if (NULL == log_data || 0 > data_len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 == data_len)
      {}
      else if (0 != (data_len & align_mask_))
      {
        TBSYS_LOG(WARN, "buf_len[%ld] not aligned align_mask=%lx", data_len, align_mask_);
      }
      else if (OB_SUCCESS != (err = parse_log_buffer(log_data, data_len, log_cursor_, new_cursor)))
      {
        TBSYS_LOG(ERROR, "parse_log_buffer(log_data=%p, data_len=%ld, log_cursor=%s)=>%d",
                  log_data, data_len, log_cursor_.to_str(), err);
      }
      else if (OB_SUCCESS != (err = file_.append(log_data, data_len, OB_LOG_NOSYNC == log_sync_type_ ? false : true)))
      {
        TBSYS_LOG(ERROR, "file_.append(log_data=%p, data_len=%ld, sync_type=%ld)=>%d", log_data, data_len, log_sync_type_, err);
      }
      else if (OB_SUCCESS != (err = open_log_file_maybe_(file_, log_dir_, log_cursor_.file_id_, new_cursor.file_id_, dio_, true)))
      {
        TBSYS_LOG(ERROR, "open_log_file_maybe(old_id=%ld, new_id=%ld)=>%d", log_cursor_.file_id_, new_cursor.file_id_, err);
      }
      else
      {
        log_cursor_ = new_cursor;
      }
      return err;
    }

    int ObLogWriterV2::get_cursor(ObLogCursor& log_cursor) const
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_inner_stat()=>%d", err);
      }
      else
      {
        log_cursor = log_cursor_;
      }
      return err;
    }

    int ObLogWriterV2::flush_log()
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_inner_stat()=>%d", err);
      }
      return err;
    }
  }; // end namespace common
}; // end namespace oceanbase
