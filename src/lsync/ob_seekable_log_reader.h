/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_seekable_log_reader.h
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#ifndef _OCEANBASE_LSYNC_OB_SEEKABLE_LOG_READER_H_
#define _OCEANBASE_LSYNC_OB_SEEKABLE_LOG_READER_H_

#include "common/ob_define.h"
#include "common/ob_repeated_log_reader.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace lsync
  {
    struct ObLsyncLogEntry
    {
      uint64_t last_file_id;
      uint64_t last_seq_id;
      uint64_t seq;
      LogCommand cmd;
      char* buf;
      int64_t data_len;
      int get(uint64_t& log_file_id, uint64_t& log_seq_id,
              LogCommand& cmd, uint64_t& seq, char*& buf, int64_t& data_len)
      {
        int ret = OB_SUCCESS;
        if (last_file_id == log_file_id && last_seq_id == log_seq_id)
        {
          cmd = this->cmd;
          seq = this->seq;
          buf = this->buf;
          data_len = this->data_len;
          log_seq_id = seq;
          if (cmd == OB_LOG_SWITCH_LOG)
          {
            log_file_id++;
          }
          ret = OB_SUCCESS;
        }
        else
        {
          ret = OB_ERROR;
        }
        return ret;
      }
      int set(uint64_t log_file_id, uint64_t log_seq_id,
              LogCommand cmd, uint64_t seq, char* buf, int64_t data_len)
      {
        last_file_id = log_file_id;
        last_seq_id = log_seq_id;
        this->cmd = cmd;
        this->seq = seq;
        this->buf = buf;
        this->data_len = data_len;
        return OB_SUCCESS;
      }
    };

    class ObSeekableLogReader
    {
      public:
        ObSeekableLogReader(): stop_(false), log_dir_(NULL), last_log_file_id_(0), last_log_seq_id_(0)
      {}
        ObSeekableLogReader(const char* log_dir): log_dir_(log_dir), last_log_file_id_(0), last_log_seq_id_(0)
      {}
        ~ObSeekableLogReader()
      {
      }
        int initialize(const char* log_dir)
      {
        int ret = OB_SUCCESS;
        log_dir_ = log_dir;
        if(OB_SUCCESS != (ret = (reader_.init(log_dir))))
        {
          TBSYS_LOG(ERROR, "init single log reader failed.");
        }
        return ret;
      }
        int get_with_timeout(uint64_t& log_file_id, uint64_t& log_seq_id,
                             LogCommand& cmd, uint64_t& seq, char*& buf, int64_t& data_len, int64_t timeout);
        int get(uint64_t& log_file_id, uint64_t& log_seq_id,
                LogCommand& cmd, uint64_t& seq, char*& buf, int64_t& data_len);
        int get_log_entry(uint64_t& log_file_id, uint64_t& log_seq_id,
                          ObLogEntry& entry, char*& log_data, int64_t timeout);
        int get_log_entry_buf(uint64_t& log_file_id, uint64_t& log_seq_id,
                              char* buf, int64_t limit, int64_t& pos, int64_t timeout);
        int get_log_entries_buf(uint64_t& log_file_id, uint64_t& log_seq_id,
                                char* buf, int64_t limit, int64_t& pos, int64_t timeout);
        void stop() { stop_ = true; }
      private:
        int seek(uint64_t log_file_id, uint64_t log_seq_id);
        int get_next(LogCommand& cmd, uint64_t& seq, char*& buf, int64_t& data_len);
        int get_(uint64_t& log_file_id, uint64_t& log_seq_id,
                 LogCommand& cmd, uint64_t& seq, char*& buf, int64_t& data_len);
        volatile bool stop_;
        const char* log_dir_;
        ObRepeatedLogReader reader_;
        int64_t last_log_file_id_;
        int64_t last_log_seq_id_;
        ObLsyncLogEntry last_log_;
        ObDataBuffer log_buffer_;
    };

  } // end namespace lsync
} // end namespace oceanbase
#endif // _OCEANBASE_LSYNC_OB_SEEKABLE_LOG_READER_H_

