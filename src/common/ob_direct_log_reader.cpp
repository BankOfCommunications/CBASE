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

#include "ob_direct_log_reader.h"

using namespace oceanbase::common;

ObDirectLogReader::ObDirectLogReader()
{
}

ObDirectLogReader::~ObDirectLogReader()
{
}

int ObDirectLogReader::read_log(LogCommand &cmd, uint64_t &log_seq,
    char *&log_data, int64_t &data_len)
{
  int ret = OB_SUCCESS;

  ObLogEntry entry;
  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObSingleLogReader has not been initialized, "
        "please initialize first");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = read_header(entry)) && OB_READ_NOTHING != ret)
    {
      TBSYS_LOG(ERROR, "read_header()=>%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (log_buffer_.get_remain_data_len() < entry.get_log_data_len())
    {
      read_log_();
    }
    if (log_buffer_.get_remain_data_len() < entry.get_log_data_len())
    {
      TBSYS_LOG(ERROR, "Get an illegal log entry, log file maybe corrupted, "
              "file_id=%ld, get_remain_data_len()=%ld get_log_data_len()=%d",
                file_id_, log_buffer_.get_remain_data_len(), entry.get_log_data_len());
      TBSYS_LOG(WARN, "log_buffer_ get_data()=%p get_limit()=%ld "
              "get_position()=%ld get_capacity()=%ld",
              log_buffer_.get_data(), log_buffer_.get_limit(),
              log_buffer_.get_position(), log_buffer_.get_capacity());
      hex_dump(log_buffer_.get_data(), static_cast<int32_t>(log_buffer_.get_limit()), true,
              TBSYS_LOG_LEVEL_WARN);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (last_log_seq_ != 0 &&
        (OB_LOG_SWITCH_LOG == entry.cmd_?
         (last_log_seq_ != entry.seq_ && last_log_seq_ + 1 != entry.seq_):
         (last_log_seq_ + 1) != entry.seq_))
    {
      TBSYS_LOG(ERROR, "the log sequence is not continuous[%lu => %lu[cmd=%d]",
                last_log_seq_, entry.seq_, entry.cmd_);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    last_log_seq_ = entry.seq_;
    cmd = static_cast<LogCommand>(entry.cmd_);
    log_seq = entry.seq_;
    log_data = log_buffer_.get_data() + log_buffer_.get_position();
    data_len = entry.get_log_data_len();
    log_buffer_.get_position() += data_len;
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "LOG ENTRY: SEQ[%lu] CMD[%d] DATA_LEN[%ld] POS[%ld]",
        entry.seq_, cmd, data_len, pos);
    pos += entry.header_.header_length_ + entry.header_.data_length_;
  }

  return ret;
}

