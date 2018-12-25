#include "ob_seekable_log_reader2.h"
#include "updateserver/ob_on_disk_log_locator.h"
#include "common/ob_repeated_log_reader.h"
#include "Time.h"

namespace oceanbase
{
  namespace updateserver
  {
    int get_log_file_offset_func(const char* log_dir, const int64_t file_id, const int64_t log_id, int64_t& offset);
    int read_log_file_by_location(const char* log_dir, const int64_t file_id, const int64_t offset,
                                  char* buf, const int64_t len, int64_t& read_count, const bool dio = true);
  };

  namespace lsync
  {
    bool ObSeekableLogReader::is_inited() const
    {
      return NULL != log_dir_;
    }

    int ObSeekableLogReader::init(const char* log_dir, const bool is_read_old_log, const int64_t lsync_retry_wait_time_us)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == log_dir || 0 >= lsync_retry_wait_time_us)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = located_log_reader_.init(log_dir)))
      {
        TBSYS_LOG(ERROR, "located_log_reader.init(log_dir=%s)=>%d", log_dir, err);
      }
      else
      {
        log_dir_ = log_dir;
        is_read_old_log_ = is_read_old_log;
        lsync_retry_wait_time_us_ = lsync_retry_wait_time_us;
      }
      return err;
    }

    int get_log_file_offset_func(const char* log_dir, const int64_t file_id, const int64_t log_id, int64_t& offset, bool is_read_old_log)
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
        else if (is_read_old_log)
        {
          log_seq += (OB_LOG_SWITCH_LOG == cmd) ?file_id : file_id - 1;
        }
        if (OB_SUCCESS != err)
        {}
        else if ((int64_t)log_seq > log_id)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
        else if ((int64_t)log_seq + 1 == log_id)
        {
          offset = log_reader.get_last_log_offset();
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

    static bool is_align(int64_t x, int64_t n_bits)
    {
      return 0 == (x & ((1<<n_bits) - 1));
    }

    int trim_log_buffer(const int64_t file_id, const int64_t offset, const int64_t align_bits,
                        const char* log_data, const int64_t len, int64_t& end_pos,
                        int64_t& start_id, int64_t& end_id, bool& is_file_end, bool is_old_log)
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
        if (pos + log_entry.get_serialize_size() > len || is_file_end) // 循环唯一的出口
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
          TBSYS_LOG(WARN, "log_entry.check_data_integrity()=>%d", err);
        }
        else if (is_old_log)
        {
          log_entry.seq_ += (OB_LOG_SWITCH_LOG == log_entry.cmd_) ?file_id : file_id - 1;
        }
        if (OB_SUCCESS != err)
        {}
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
        TBSYS_LOG(WARN, "parse log buf error:");
        hex_dump(log_data, static_cast<int32_t>(len), true, TBSYS_LOG_LEVEL_DEBUG);
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
      TBSYS_LOG(DEBUG, "trim_log_buffer(offset=%ld, align_bits=%ld, pos=%ld, aligned_pos=%ld, log=[%ld,%ld])=>%d",
                offset, align_bits, pos, end_pos, start_id, end_id, err);
      return err;
    }

    int ObSeekableLogReader::get_location(const int64_t file_id, const int64_t start_id, ObLogLocation& start_location)
    {
      int err = OB_SUCCESS;
      int64_t offset = 0;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (0 > start_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 == start_id)
      {
        offset = 0;
      }
      else if (start_location.is_valid() && start_location.file_id_ == file_id && start_location.log_id_ == start_id)
      {
        offset = start_location.offset_;
        //TBSYS_LOG(DEBUG, "start_location hit: file_id=%ld, log_id=%ld, offset=%ld", start_location.file_id_, start_location.log_id_, start_location.offset_);
      }
      // 向lsync取日志时，传进的start_id表示已经备机已收到的日志ID
      // 从UPS fetch日志时，传进的start_id表示备机要取的下一条日志ID
      // get_log_file_offset_func()定位时传入的参数表示要取的下一条日志，所以这里用"start_id+1"作为入参。
      else if (OB_SUCCESS != (err = get_log_file_offset_func(log_dir_, file_id, start_id, offset, is_read_old_log_))
               && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "get_log_file_offset_func(log_dir=%s, file_id=%ld, start_id=%ld)=>%d", log_dir_, file_id, start_id, err);
      }
      else if (OB_ENTRY_NOT_EXIST == err)
      {
        TBSYS_LOG(DEBUG, "get_location(file_id=%ld, log_id=%ld): OB_ENTRY_NOT_EXIST", file_id, start_id);
      }
      if (OB_SUCCESS == err)
      {
        start_location.file_id_ = file_id;
        start_location.log_id_ = start_id;
        start_location.offset_ = offset;
      }
      TBSYS_LOG(DEBUG, "get_location(file_id=%ld, start_id=%ld, start_location=[%ld+%ld:%ld])=>%d",
                file_id, start_id, start_location.file_id_, start_location.offset_, start_location.log_id_, err);
      return err;
    }

    // 输入输出都按0.3版的commitlog来
    int ObSeekableLogReader::get_log(const int64_t file_id, const int64_t start_id, ObLogLocation& start_location, ObLogLocation& end_location, char* buf, const int64_t len, int64_t& read_count)
    {
      int err = OB_SUCCESS;
      int64_t real_start_id = 0;
      int64_t real_end_id = 0;
      bool is_file_end = false;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (0 > start_id || NULL == buf || 0 > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = get_location(file_id, start_id, start_location)))
      {
        if (OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "get_location(start_id=%ld)=>%d", start_location.log_id_, err);
        }
        else
        {
          err = OB_READ_NOTHING;
        }
      }
      else if (OB_SUCCESS != (err = read_log_file_by_location(log_dir_, file_id, start_location.offset_, buf, len, read_count, dio_)))
      {
        TBSYS_LOG(ERROR, "read_log_file_by_location(log_dir=%s, file=%ld:+%ld, start_id=%ld, buf=%p[%ld])=>%d",
                  log_dir_, file_id, start_location.offset_, start_id, buf, len, err);
      }
      else if (OB_SUCCESS != (err = trim_log_buffer(file_id, start_location.offset_, OB_DIRECT_IO_ALIGN_BITS,
                                                    buf, read_count, read_count, real_start_id, real_end_id, is_file_end, is_read_old_log_)))
      {
        TBSYS_LOG(WARN, "trim_log_buffer()=>%d", err);
      }
      else if (0 >= read_count)
      {
        err = OB_READ_NOTHING;
        TBSYS_LOG(DEBUG, "located_log_reader.get_log(file_id=%ld, log_id=%ld, offset=%ld): READ_NOTHING",
                  start_location.file_id_, start_location.log_id_, start_location.offset_);
      }
      else if (start_id != 0 && real_start_id != start_id)
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(ERROR, "log_not_continuous: real_start_id[%ld] != start_id[%ld]", real_start_id, start_id);
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
      if (OB_SUCCESS == err)
      {
        end_location.log_id_ = real_end_id;
      }
      if (OB_SUCCESS != err && OB_READ_NOTHING != err)
      {
        TBSYS_LOG(WARN, "get_log(start_id=%ld): err=%d, reset to OB_READ_NOTHING", start_id, err);
        err = OB_READ_NOTHING;
      }
      TBSYS_LOG(DEBUG, "get_log(file_id=%ld, start_id=%ld, offset=%ld, read_count=%ld)=>%d",
                file_id, start_id, start_location.offset_, read_count, err);
      return err;
    }
    // 每条log的seq = seq - file_id
    int do_convert_log_seq(const int64_t file_id, char* buf, const int64_t len)
    {
      int err = OB_SUCCESS;
      int64_t old_pos = 0;
      int64_t pos = 0;
      ObLogEntry log_entry;
      if (0 >= file_id || NULL == buf || 0 >= len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      while(OB_SUCCESS == err && pos < len)
      {
        old_pos = pos;
        if (OB_SUCCESS != (err = log_entry.deserialize(buf, len, pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
        }
        else
        {
          //TBSYS_LOG(DEBUG, "log_entry[seq=%ld, file_id=%ld]", log_entry.seq_, file_id);
          log_entry.set_log_seq(log_entry.seq_ - ((OB_LOG_SWITCH_LOG == log_entry.cmd_) ?file_id : file_id - 1));
        }

        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = log_entry.fill_header(buf + pos, log_entry.get_log_data_len())))
        {
          TBSYS_LOG(ERROR, "log_entry.fill_header()=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_entry.serialize(buf, len, old_pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.serialize()=>%d", err);
        }
        if (OB_SUCCESS == err)
        {
          pos += log_entry.get_log_data_len();
        }
      }
      if (OB_SUCCESS == err && pos != len)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", pos, len);
      }
      return err;
    }

    // 输入输出都按0.2版commitlog来
    int ObSeekableLogReader::get_log(const int64_t file_id, const int64_t start_id, char* buf, const int64_t len, int64_t& read_count, const int64_t timeout_us)
    {
      int err = OB_READ_NOTHING;
      ObLogLocation end_location;
      int64_t start_us = tbsys::CTimeUtil::getTime();

      TBSYS_LOG(DEBUG, "get_log(timeout_us=%ld)", timeout_us);

      while(!stop_ && OB_READ_NOTHING == err)
      {
        err = get_log(file_id, start_id == 0? 0: start_id + file_id, start_location_, end_location, buf, len, read_count);
        if (OB_SUCCESS != err && OB_READ_NOTHING != err)
        {
          TBSYS_LOG(ERROR, "get_log(file_id=%ld, log_id=%ld)=>%d", file_id, start_id + file_id -1, err);
        }
        else if (OB_READ_NOTHING == err && tbsys::CTimeUtil::getTime() + reserved_time_to_send_packet_ + lsync_retry_wait_time_us_ < start_us + timeout_us)
        {
          usleep(static_cast<useconds_t>(lsync_retry_wait_time_us_));
        }
        else
        {
          break;
        }
      }
      if(stop_ && OB_SUCCESS == err)
      {
        err = OB_READ_NOTHING;
      }
      if (OB_SUCCESS != err)
      {}
      // convert to v0.1&v0.2 recognized form
      else if (!is_read_old_log_ && OB_SUCCESS != (err = do_convert_log_seq(file_id, buf, read_count)))
      {
        TBSYS_LOG(ERROR, "conver_log_seq(file_id=%ld, buf=%p[%ld])=>%d", file_id, buf, read_count, err);
      }
      else
      {
        start_location_ = end_location;
      }

      static int64_t last_end_id = 0;
      if (last_end_id != start_id + file_id)
      {
        TBSYS_LOG(WARN, "last_end_id[%ld] != start_id[%ld] + file_id[%ld]", last_end_id, start_id, file_id);
      }
      last_end_id = start_location_.log_id_;
      return err;
    }
  } // end namespace lsync
} // end namespace oceanbase

