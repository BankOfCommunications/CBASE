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

#include "ob_repeated_log_reader.h"

using namespace oceanbase::common;

ObRepeatedLogReader::ObRepeatedLogReader()
{
}

ObRepeatedLogReader::~ObRepeatedLogReader()
{
}

int ObRepeatedLogReader::read_log(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t & data_len)
{
  int ret = OB_SUCCESS;

  ObLogEntry entry;
  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObSingleLogReader has not been initialized, please initialize first");
    ret = OB_NOT_INIT;
  }
  else
  {
    if (OB_SUCCESS == ret
        && OB_SUCCESS != (ret = read_header(entry)) && OB_READ_NOTHING != ret && OB_LAST_LOG_RUINNED != ret)
    {
      TBSYS_LOG(ERROR, "read_header(entry, file_id=%ld, pos=%ld, seq=%ld)=>%d", file_id_, pos, last_log_seq_, ret);
    }
    else if (OB_LAST_LOG_RUINNED == ret)
    {
      ret = OB_READ_NOTHING;
      TBSYS_LOG(DEBUG, "read_log(): read end of file.");
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != entry.check_header_integrity())
      {
        log_buffer_.get_position() -= entry.get_serialize_size();
        pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
        if (all_zero(log_buffer_.get_data() + log_buffer_.get_position(), log_buffer_.get_remain_data_len()))
        {
          ret = OB_READ_ZERO_LOG;
          TBSYS_LOG(WARN, "read an invalid log_entry, data are allzero");
        }
        else
        {
          ret = OB_INVALID_LOG;
          //hex_dump(log_buffer_.get_data() + log_buffer_.get_position(), log_buffer_.get_remain_data_len(), false, TBSYS_LOG_LEVEL_WARN);
          TBSYS_LOG(WARN, "Log entry header is corrupted, file_id_=%lu pread_pos_=%ld pos=%ld last_log_seq_=%lu",
                    file_id_, pread_pos_, pos, last_log_seq_);
          TBSYS_LOG(WARN, "log_buffer_ position_=%ld limit_=%ld capacity_=%ld",
                    log_buffer_.get_position(), log_buffer_.get_limit(), log_buffer_.get_capacity());
        }
        log_buffer_.get_limit() = log_buffer_.get_position();
      }
      else if (entry.get_log_data_len() > log_buffer_.get_remain_data_len())
      {
        //TBSYS_LOG(DEBUG, "do not get a full entry, when checking log data");
        log_buffer_.get_position() -= entry.get_serialize_size();

        ret = read_log_();
        if (OB_READ_NOTHING == ret)
        {
          TBSYS_LOG(DEBUG, "do not get a full entry, when reading ObLogEntry");
        }
        else
        {
          ret = entry.deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "ObLogEntry deserialize error[ret=%d]", ret);
          }
          else
          {
            if (OB_SUCCESS != entry.check_header_integrity()
                || entry.get_log_data_len() > log_buffer_.get_remain_data_len())
            {
              TBSYS_LOG(DEBUG, "do not get a full entry, when checking log data");
              TBSYS_LOG(DEBUG, "log_data_len=%d remaining=%ld", entry.get_log_data_len(), log_buffer_.get_remain_data_len());
              TBSYS_LOG(DEBUG, "limit=%ld pos=%ld", log_buffer_.get_limit(), log_buffer_.get_position());
              hex_dump(log_buffer_.get_data(), static_cast<int32_t>(log_buffer_.get_limit()));
              log_buffer_.get_position() -= entry.get_serialize_size();
              ret = OB_READ_NOTHING;
            }
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS == ret)
    {
      ret = entry.check_data_integrity(log_buffer_.get_data() + log_buffer_.get_position());
      if (OB_SUCCESS != ret)
      {
        if (all_zero(log_buffer_.get_data() + log_buffer_.get_position(), log_buffer_.get_remain_data_len()))
        {
          ret = OB_READ_ZERO_LOG;
          TBSYS_LOG(WARN, "read an invalid log_entry, data are allzero");
        }
        else
        {
          ret = OB_INVALID_LOG;
          TBSYS_LOG(WARN, "data corrupt, when check_data_integrity, file_id_=%lu pread_pos_=%ld pos=%ld last_log_seq_=%lu",
              file_id_, pread_pos_, pos, last_log_seq_);
          TBSYS_LOG(WARN, "log_buffer_ position_=%ld limit_=%ld capacity_=%ld",
              log_buffer_.get_position(), log_buffer_.get_limit(), log_buffer_.get_capacity());
          hex_dump(log_buffer_.get_data(), static_cast<int32_t>(log_buffer_.get_limit()), true, TBSYS_LOG_LEVEL_ERROR);
        }
        log_buffer_.get_position() -= entry.get_serialize_size();
        pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
        log_buffer_.get_limit() = log_buffer_.get_position();
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (last_log_seq_ != 0 &&
          (OB_LOG_SWITCH_LOG == entry.cmd_?
           (last_log_seq_ != entry.seq_ && last_log_seq_ + 1 != entry.seq_):
           (last_log_seq_ + 1) != entry.seq_))
      {
        TBSYS_LOG(ERROR, "the log sequence is not continuous[%lu => %lu[cmd=%d]]",
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
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "LOG ENTRY: SEQ[%lu] CMD[%d] DATA_LEN[%ld] POS[%ld]", entry.seq_, cmd, data_len, pos);
    pos += entry.header_.header_length_ + entry.header_.data_length_;
  }
  if (OB_READ_ZERO_LOG == ret || OB_INVALID_LOG == ret)
  {
    ret = OB_READ_NOTHING;
    TBSYS_LOG(WARN, "read invalid log[%ld], reset err=OB_READ_NOTHING", last_log_seq_);
  }

  return ret;
}

