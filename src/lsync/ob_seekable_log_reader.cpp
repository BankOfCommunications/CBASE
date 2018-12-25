#include "ob_seekable_log_reader.h"
#include "Time.h"

namespace oceanbase
{
  namespace lsync
  {
    // @pre: reader_ != NULL && reader_.is_opened()
    static int seek_within_one_file_(ObSingleLogReader& reader, uint64_t last_log_seq_id)
    {
      int ret = OB_SUCCESS;
      LogCommand cmd;
      uint64_t seq = 0;
      char* log_data;
      int64_t data_len;
      while(seq < last_log_seq_id)
      {
        if (OB_SUCCESS != (ret = reader.read_log(cmd, seq, log_data, data_len)))
        {
          TBSYS_LOG(DEBUG, "seek_within_on_file:read_log()=>%d", ret);
          break;
        }
      }
      if (OB_SUCCESS == ret && last_log_seq_id > 0)
      {
        if (last_log_seq_id+1 == seq || (OB_LOG_SWITCH_LOG == cmd && last_log_seq_id == seq))
        {
          ret = OB_NEED_RETRY;
        }
        else if (last_log_seq_id != seq)
        {
          ret = OB_ERROR_OUT_OF_RANGE;
        }
      }
      return ret;
    }

    static int seek_may_need_retry(ObSingleLogReader& reader, uint64_t last_log_file_id, uint64_t last_log_seq_id)
    {
      int ret = OB_SUCCESS;
      reader.unset_dio();
      if (OB_SUCCESS != (ret = reader.is_opened()? reader.close(): OB_SUCCESS))
      {
        TBSYS_LOG(DEBUG, "close() failed");
      }
      else if (OB_SUCCESS != (ret = reader.open(last_log_file_id, 0)))
      {
        TBSYS_LOG(DEBUG, "open log failed: log_file_id = %ld, err=%d", last_log_file_id, ret);
      }
      else if (OB_SUCCESS != (ret = seek_within_one_file_(reader, last_log_seq_id)))
      {
        TBSYS_LOG(DEBUG, "seek within one file failed: log_seq_id = %ld", last_log_seq_id);
      }

      return ret;
    }

    static int seek_(ObSingleLogReader& reader, uint64_t last_log_file_id, uint64_t last_log_seq_id)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(DEBUG, "seek_(log_file_id=%lu, log_seq_id=%lu)", last_log_file_id, last_log_seq_id);
      ret = seek_may_need_retry(reader, last_log_file_id, last_log_seq_id);
      if (OB_NEED_RETRY == ret)
      {
        ret = seek_may_need_retry(reader, last_log_file_id, 0);
      }
      return ret;
    }

    static bool need_seek(uint64_t file_id, uint64_t seq_id, uint64_t target_file_id, uint64_t target_seq_id)
    {
      return file_id != target_file_id || seq_id != target_seq_id;
    }

    int ObSeekableLogReader:: seek(uint64_t last_log_file_id, uint64_t last_log_seq_id)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = last_log_file_id == 0? OB_INVALID_ARGUMENT: OB_SUCCESS))
      {
        TBSYS_LOG(ERROR, "seek() invalid argument: last_log_file_id=%ld", last_log_file_id);
      }
      else if (OB_SUCCESS != (ret = (need_seek(last_log_file_id_, last_log_seq_id_, last_log_file_id, last_log_seq_id)?
                                     seek_(reader_, last_log_file_id, last_log_seq_id):OB_SUCCESS)))
      {
        TBSYS_LOG(DEBUG, "seek() failed: file_id=%ld, log_id=%ld, ret=%d", last_log_file_id, last_log_seq_id, ret);
      }
      if (OB_SUCCESS == ret)
      {
        last_log_file_id_ = last_log_file_id;
        last_log_seq_id_ = last_log_seq_id;
      }

      return ret;
    }

    int ObSeekableLogReader:: get_next(LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len)
    {
      int ret = OB_SUCCESS;
      tbutil::Time now = tbutil::Time::now();

      if (OB_SUCCESS != (ret = reader_.is_opened()? OB_SUCCESS: OB_NOT_INIT))
      {
        TBSYS_LOG(DEBUG, "get_next:reader_.is_opened() == false");
      }
      else
      {
        ret = reader_.read_log(cmd, seq, log_data, data_len);
        if (OB_SUCCESS == ret)
        {
          if (cmd == OB_LOG_SWITCH_LOG)
          {
            TBSYS_LOG(INFO, "switch log: last_log_file_id = %ld", last_log_file_id_);
            last_log_file_id_ = 0;
          }
          else
          {
            last_log_seq_id_ = seq;
          }
        }
      }
    return ret;
  }

/*
 * log_file_id, log_seq_id will updated to indicate location of current LogEntry incase of SUCCESS.
 *
 */
    int ObSeekableLogReader::get_(uint64_t& log_file_id, uint64_t& log_seq_id,
                                  LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = seek(log_file_id, log_seq_id)))
      {
        TBSYS_LOG(DEBUG, "seek(log_file_id=%ld log_seq_id=%ld)=>%d", log_file_id, log_seq_id, ret);
      }
      else if (OB_SUCCESS != (ret = get_next(cmd, seq, log_data, data_len)))
      {
        TBSYS_LOG(DEBUG, "get_log_entries_buf(log_file_id=%ld, log_seq_id=%ld)=>%d",
                  log_file_id, log_seq_id, ret);
      }
      else
      {
        log_seq_id = seq;
        if (cmd == OB_LOG_SWITCH_LOG)
        {
          log_file_id++;
        }
      }

      if (OB_FILE_NOT_EXIST == ret)
      {
        ret = OB_READ_NOTHING;
      }
      return ret;
    }

    int ObSeekableLogReader::get(uint64_t& log_file_id, uint64_t& log_seq_id,
                                 LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len)
    {
      int ret = OB_SUCCESS;
      uint64_t old_log_file_id = log_file_id;
      uint64_t old_log_seq_id = log_seq_id;

      if (OB_SUCCESS == (ret = last_log_.get(log_file_id, log_seq_id, cmd, seq, log_data, data_len)))
      {
        TBSYS_LOG(DEBUG, "last_log.get(log_file_id=%ld log_seq_id=%ld)=>%d", log_file_id, log_seq_id, ret);
      }
      else if (OB_SUCCESS != (ret = get_(log_file_id, log_seq_id, cmd, seq, log_data, data_len)))
      {
        TBSYS_LOG(DEBUG, "get_(log_file_id=%ld log_seq_id=%ld)=>%d", log_file_id, log_seq_id, ret);
      }
      else
      {
        last_log_.set(old_log_file_id, old_log_seq_id, cmd, seq, log_data, data_len);
      }
      return ret;
    }

    int ObSeekableLogReader::get_with_timeout(uint64_t& log_file_id, uint64_t& log_seq_id,
                                 LogCommand& cmd, uint64_t& seq, char*& log_data, int64_t& data_len, int64_t timeout)
    {
      int ret = OB_SUCCESS;
      tbutil::Time now = tbutil::Time::now();

      while(!stop_)
      {
        ret = get(log_file_id, log_seq_id, cmd, seq, log_data, data_len);
        if (OB_READ_NOTHING == ret && (tbutil::Time::now() - now).toMicroSeconds() < timeout)
        {
          usleep(10000);
          continue;
        }
        else
        {
          break;
        }
      }
      if(stop_)
      {
        ret = OB_READ_NOTHING;
      }

      return ret;
    }

    int ObSeekableLogReader::get_log_entries_buf(uint64_t& log_file_id, uint64_t& log_seq_id,
                                                 char* buf, int64_t limit, int64_t& pos, int64_t timeout)
    {
      int ret = OB_SUCCESS;
      int64_t old_pos = pos;

      while(1)
      {
        ret = get_log_entry_buf(log_file_id, log_seq_id, buf, limit, pos, timeout);
        if (OB_SUCCESS != ret)
          break;
        timeout = 0 ; // once We get some data, We want sending it to ups in time.
      }

      if (old_pos < pos)
      {
        ret = OB_SUCCESS;
      }
      return ret;
    }

    static int mem_chunk_serialize(char* buf, int64_t len, int64_t& pos, char* pmem, int64_t mem_len)
    {
      int rc = OB_SUCCESS;
      if (len - pos >= mem_len)
      {
        memcpy(buf + pos, pmem, mem_len);
        pos += mem_len;
        rc = OB_SUCCESS;
      }
      else
      {
        rc = OB_BUF_NOT_ENOUGH;
      }
      return rc;
    }

    int ObSeekableLogReader:: get_log_entry_buf(uint64_t& log_file_id, uint64_t& log_seq_id,
                                                char* buf, int64_t limit, int64_t& pos, int64_t timeout)
    {
      int ret = OB_SUCCESS;
      ObLogEntry log_entry;
      char* log_data;

      int64_t old_pos = pos;
      uint64_t old_log_file_id = log_file_id;
      uint64_t old_log_seq_id = log_seq_id;

      if (OB_SUCCESS != (ret = get_log_entry(log_file_id, log_seq_id, log_entry, log_data, timeout)))
      {
        TBSYS_LOG(DEBUG, "get_log_entry(log_file_id=%ld, log_seq_id=%ld)=>%d",
                  log_file_id, log_seq_id, ret);
      }
      else if (OB_SUCCESS != (ret = log_entry.serialize(buf, limit, pos)))
      {
        TBSYS_LOG(DEBUG, "log_entry.serialize(buf=%p, limit=%ld, pos=%ld)=>%d",
                  buf, limit, pos, ret);
      }
      else if (OB_SUCCESS != (ret = mem_chunk_serialize(buf, limit, pos, log_data, log_entry.get_log_data_len())))
      {
        TBSYS_LOG(DEBUG, "copy_log_data(buf=%p,limit=%ld,pos=%ld,data_len=%d)=>%d",
                  buf, limit, pos, log_entry.get_log_data_len(), ret);
      }

      if (OB_SUCCESS != ret)
      {
        pos = old_pos;
        log_file_id = old_log_file_id;
        log_seq_id = old_log_seq_id;
      }
      if (OB_SUCCESS == ret && OB_LOG_SWITCH_LOG == log_entry.cmd_)
      {
        ret = OB_ITER_END;
      }
      return ret;
    }

    int ObSeekableLogReader:: get_log_entry(uint64_t& log_file_id, uint64_t& log_seq_id, ObLogEntry& log_entry, char*& log_data, int64_t timeout)
    {
      int ret = OB_SUCCESS;
      LogCommand cmd = OB_LOG_UNKNOWN;
      uint64_t seq = 0;
      int64_t data_len = 0;
      if (OB_SUCCESS != (ret = get_with_timeout(log_file_id, log_seq_id, cmd, seq, log_data, data_len, timeout)))
      {
        TBSYS_LOG(DEBUG, "get_log_entry(log_file_id=%lu, log_seq_id=%lu)=>%d",
                  log_file_id, log_seq_id, ret);
      }
      else
      {
        log_entry.set_log_command(cmd);
        log_entry.set_log_seq(seq);
        log_entry.fill_header(log_data, data_len);
      }
      return ret;
    }
  } // end namespace lsync
} // end namespace oceanbase

