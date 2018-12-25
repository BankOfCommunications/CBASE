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

#include "ob_single_log_reader.h"
#include "ob_log_dir_scanner.h"
#include "ob_log_generator.h"
using namespace oceanbase::common;

const int64_t ObSingleLogReader::LOG_BUFFER_MAX_LENGTH = 1 << 21;

ObSingleLogReader::ObSingleLogReader()
{
  is_initialized_ = false;
  dio_ = true;
  file_name_[0] = '\0';
  file_id_ = 0;
  last_log_seq_ = 0;
  log_buffer_.reset();
}

ObSingleLogReader::~ObSingleLogReader()
{
  if (NULL != log_buffer_.get_data())
  {
    ob_free(log_buffer_.get_data());
    log_buffer_.reset();
  }
}

int ObSingleLogReader::init(const char* log_dir)
{
  int ret = OB_SUCCESS;

  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObSingleLogReader has been initialized");
    ret = OB_INIT_TWICE;
  }
  else
  {
    if (NULL == log_dir)
    {
      TBSYS_LOG(ERROR, "Parameter is invalid[log_dir=%p]", log_dir);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      int log_dir_len = static_cast<int32_t>(strlen(log_dir));
      if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "log_dir is too long[len=%d log_dir=%s]", log_dir_len, log_dir);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        strncpy(log_dir_, log_dir, log_dir_len + 1);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == log_buffer_.get_data())
    {
      char* buf = static_cast<char*>(ob_malloc(LOG_BUFFER_MAX_LENGTH, ObModIds::OB_SINGLE_LOG_READER));
      if (NULL == buf)
      {
        TBSYS_LOG(ERROR, "ob_malloc for log_buffer_ failed");
        ret = OB_ERROR;
      }
      else
      {
        log_buffer_.set_data(buf, LOG_BUFFER_MAX_LENGTH);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "ObSingleLogReader initialize successfully");
    last_log_seq_ = 0;

    is_initialized_ = true;
  }

  return ret;
}

int ObSingleLogReader::get_max_log_file_id(uint64_t &max_log_file_id)
{
  int err = OB_SUCCESS;
  ObLogDirScanner scanner;
  err = scanner.init(log_dir_);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "fail to init scanner.");
  }
  else
  {
    err = scanner.get_max_log_id(max_log_file_id);
  }
  return err;
}

int ObSingleLogReader::open(const uint64_t file_id, const uint64_t last_log_seq/* = 0*/)
{
  int ret = check_inner_stat_();

  if (OB_SUCCESS == ret)
  {
    int err = snprintf(file_name_, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", log_dir_, file_id);
    if (err < 0)
    {
      TBSYS_LOG(ERROR, "snprintf file_name[log_dir_=%s file_id=[%lu] error[%s]", log_dir_, file_id, strerror(errno));
      ret = OB_ERROR;
    }
    else if (err >= OB_MAX_FILE_NAME_LENGTH)
    {
      TBSYS_LOG(ERROR, "snprintf file_name[file_id=%lu] error[%s]", file_id, strerror(errno));
      ret = OB_ERROR;
    }
    else
    {
      int fn_len = static_cast<int32_t>(strlen(file_name_));
      if (OB_SUCCESS == ret)
      {
        file_id_ = file_id;
        last_log_seq_ = last_log_seq;
        pos = 0;
        pread_pos_ = 0;
        log_buffer_.get_position() = 0;
        log_buffer_.get_limit() = 0;
      }
      ret = file_.open(ObString(fn_len, fn_len, file_name_), dio_);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "open log file(name=%s id=%lu)", file_name_, file_id);
      }
      else if (OB_FILE_NOT_EXIST == ret)
      {
        TBSYS_LOG(DEBUG, "log file(name=%s id=%lu) not found", file_name_, file_id);
      }
      else
      {
        TBSYS_LOG(WARN, "open file[name=%s] error[%s]", file_name_, strerror(errno));
      }
    }
  }

  return ret;
}

int ObSingleLogReader::open_with_lucky(const uint64_t file_id, const uint64_t last_log_seq)
{
  int err = OB_SUCCESS;
  if (file_id == file_id_ )
  {
    if(last_log_seq != 0 && last_log_seq_ != 0 && last_log_seq != last_log_seq_)
    {
      err = OB_DISCONTINUOUS_LOG;
      TBSYS_LOG(ERROR, "open(file_id=%ld, req_log_seq=%ld, last_log_seq=%ld)=>%d",
                file_id, last_log_seq, last_log_seq_, err);
    }
  }
  else if (OB_SUCCESS != (err = close()))
  {
    TBSYS_LOG(ERROR, "close()=>%d", err);
  }
  else if (OB_SUCCESS != (err = open(file_id, last_log_seq)))
  {
    if (OB_FILE_NOT_EXIST == err)
    {
      TBSYS_LOG(WARN, "open(file_id=%ld, last_log_seq=%ld)=>%d", file_id, last_log_seq, err);
    }
    else
    {
      TBSYS_LOG(ERROR, "open(file_id=%ld, last_log_seq=%ld)=>%d", file_id, last_log_seq, err);
    }
  }
  return  err;
}

int ObSingleLogReader::close()
{
  int ret = check_inner_stat_();

  if (OB_SUCCESS == ret)
  {
    file_.close();
    if (last_log_seq_ == 0)
    {
      TBSYS_LOG(INFO, "close file[name=%s], read no data from this log", file_name_);
    }
    else
    {
      TBSYS_LOG(INFO, "close file[name=%s] successfully, the last log sequence is %lu",
          file_name_, last_log_seq_);
    }
  }

  return ret;
}

int ObSingleLogReader::reset()
{
  int ret = check_inner_stat_();

  if (OB_SUCCESS == ret)
  {
    ret = close();
    if (OB_SUCCESS == ret)
    {
      ob_free(log_buffer_.get_data());
      log_buffer_.reset();
      is_initialized_ = false;
    }
  }

  return ret;
}

int ObSingleLogReader::read_header(ObLogEntry& entry)
{
  int err = OB_SUCCESS;
  if (log_buffer_.get_remain_data_len() < entry.get_serialize_size())
  {
    if (OB_SUCCESS != (err = read_log_()) && OB_READ_NOTHING != err)
    {
      TBSYS_LOG(ERROR, "read_log_()=>%d", err);
    }
  }
  if (OB_SUCCESS != err)
  {}
  else if (log_buffer_.get_remain_data_len() < entry.get_serialize_size())
  {
    if (log_buffer_.get_remain_data_len() == 0)
    {
      TBSYS_LOG(INFO, "Reach the end of log");
      err = OB_READ_NOTHING;
    }
    else
    {
      TBSYS_LOG(ERROR, "Get an illegal log entry, log file maybe corrupted");
      err = OB_LAST_LOG_RUINNED;
    }
  }
  else if (ObLogGenerator::is_eof(log_buffer_.get_data() + log_buffer_.get_position(),
                                  log_buffer_.get_limit() - log_buffer_.get_position()))
  {
    err = OB_READ_NOTHING;
    pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
    log_buffer_.get_limit() = log_buffer_.get_position();
    //TBSYS_LOG(WARN, "read eof, reset pread_pos=%ld, log_buf_pos=%ld", pread_pos_, log_buffer_.get_limit());
  }
  else if (OB_SUCCESS != (err = entry.deserialize(log_buffer_.get_data(), log_buffer_.get_limit(),
                                                  log_buffer_.get_position())))
  {
    err = OB_LAST_LOG_RUINNED;
    TBSYS_LOG(ERROR, "Log entry deserialize error");
  }
  return err;
}

int ObSingleLogReader::read_log_()
{
  int ret = OB_SUCCESS;

  if (log_buffer_.get_remain_data_len() > 0)
  {
    memmove(log_buffer_.get_data(), log_buffer_.get_data() + log_buffer_.get_position(), log_buffer_.get_remain_data_len());
    log_buffer_.get_limit() = log_buffer_.get_remain_data_len();
    log_buffer_.get_position() = 0;
  }
  else
  {
    log_buffer_.get_limit() = log_buffer_.get_position() = 0;
  }

  int64_t read_size = 0;
  ret = file_.pread(log_buffer_.get_data() + log_buffer_.get_limit(),
                  log_buffer_.get_capacity() - log_buffer_.get_limit(),
                  pread_pos_, read_size);
  TBSYS_LOG(DEBUG, "pread:: pread_pos=%ld read_size=%ld buf_pos=%ld buf_limit=%ld", pread_pos_, read_size, log_buffer_.get_position(), log_buffer_.get_limit());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "read log file[file_id=%lu] ret=%d", file_id_, ret);
  }
  else
  {
    // comment this log due to too frequent invoke by replay thread
    //TBSYS_LOG(DEBUG, "read %dB amount data from log file[file_id=%lu fd=%d]", err, file_id_, log_fd_);
    log_buffer_.get_limit() += read_size;
    pread_pos_ += read_size;

    if (0 == read_size)
    {
      // comment this log due to too frequent invoke by replay thread
      //TBSYS_LOG(DEBUG, "reach end of log file[file_id=%d]", file_id_);
      ret = OB_READ_NOTHING;
    }
  }

  return ret;
}

