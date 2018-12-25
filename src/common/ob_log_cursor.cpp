#include "ob_define.h"
#include "tbsys.h"
#include "serialization.h"
#include "ob_log_entry.h"
#include "ob_log_cursor.h"

namespace oceanbase
{
  namespace common
  {
    ObLogCursor::ObLogCursor(): file_id_(0), log_id_(0), offset_(0)
    {}

    ObLogCursor::~ObLogCursor()
    {}

    bool ObLogCursor::is_valid() const
    {
      return file_id_ > 0 && log_id_ >= 0 && offset_ >= 0;
    }

    int ObLogCursor::to_start()
    {
      int err = OB_SUCCESS;
      if (file_id_ == 0 && log_id_ == 0 && offset_ == 0)
      {
        file_id_ = 1;
      }
      return err;
    }

    void ObLogCursor::reset()
    {
      file_id_ = 0;
      log_id_ = 0;
      offset_ = 0;
    }

    int ObLogCursor::serialize(char* buf, int64_t len, int64_t& pos) const
    {
      int rc = OB_SUCCESS;
      if(!(OB_SUCCESS == serialization::encode_i64(buf, len, pos, file_id_)
           && OB_SUCCESS == serialization::encode_i64(buf, len, pos, log_id_)
           && OB_SUCCESS == serialization::encode_i64(buf, len, pos, offset_)))
      {
        rc = OB_SERIALIZE_ERROR;
        TBSYS_LOG(WARN, "ObLogCursor.serialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
      }
      return rc;
    }

    int ObLogCursor::deserialize(const char* buf, int64_t len, int64_t& pos) const
    {
      int rc = OB_SUCCESS;
      if(! (OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&file_id_)
            && OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&log_id_)
            && OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&offset_)))
      {
        rc = OB_DESERIALIZE_ERROR;
        TBSYS_LOG(WARN, "ObLogCursor.deserialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
      }
      return rc;
    }

    char* ObLogCursor::to_str() const
    {
      static char buf[512];
      snprintf(buf, sizeof(buf), "ObLogCursor{file_id=%ld, log_id=%ld, offset=%ld}", file_id_, log_id_, offset_);
      buf[sizeof(buf)-1] = 0;
      return buf;
    }

    int64_t ObLogCursor::to_string(char* buf, const int64_t limit) const
    {
      int64_t len = -1;
      if (NULL == buf || 0 >= limit)
      {
        TBSYS_LOG(ERROR, "Null buf");
      }
      else if (0 >= (len = snprintf(buf, limit, "ObLogCursor{file_id=%ld, log_id=%ld, offset=%ld}",
                                    file_id_, log_id_, offset_)) || len >= limit)
      {
        TBSYS_LOG(ERROR, "Buf not enough, buf=%p[%ld]", buf, limit);
      }
      return len;
    }

    int ObLogCursor::next_entry(ObLogEntry& entry, const LogCommand cmd, const char* log_data, const int64_t data_len) const
    {
      int err = OB_SUCCESS;
      entry.set_log_seq(log_id_);
      entry.set_log_command(cmd);
      err = entry.fill_header(log_data, data_len);
      return err;
    }
    //add wangjiahao [Paxos ups_replication_tmplog] 20160715 :b
    //@overload
    int ObLogCursor::next_entry(ObLogEntry& entry, const LogCommand cmd, const char* log_data, const int64_t data_len, const int64_t term) const
    {
      int err = OB_SUCCESS;
      entry.set_log_seq(log_id_);
      entry.set_log_command(cmd);
      err = entry.fill_header(log_data, data_len, term);
      return err;
    }
    //add :e

    int ObLogCursor::this_entry(ObLogEntry& entry, const LogCommand cmd, const char* log_data, const int64_t data_len) const
    {
      int err = OB_SUCCESS;
      entry.set_log_seq(log_id_);
      entry.set_log_command(cmd);
      err = entry.fill_header(log_data, data_len);
      return err;
    }

    int ObLogCursor:: advance(LogCommand cmd, int64_t seq, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      ObLogEntry entry;
      if(log_id_ > 0 && seq != log_id_)
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(ERROR, "entry.advance(old_id=%ld, log_id=%ld)=>%d", log_id_, entry.seq_, err);
      }
      else
      {
        log_id_ = seq + 1;
        offset_ += entry.get_header_size() + data_len;
        if (OB_LOG_SWITCH_LOG == cmd)
        {
          file_id_++;
          offset_ = 0;
        }
      }
      return err;
    }

    int ObLogCursor::advance(const ObLogEntry& entry)
    {
      return advance((LogCommand)entry.cmd_, entry.seq_, entry.get_log_data_len());
    }

    bool ObLogCursor::newer_than(const ObLogCursor& that) const
    {
      return file_id_ > that.file_id_  || (file_id_ == that.file_id_ && log_id_ > that.log_id_);
    }
    
    bool ObLogCursor::equal(const ObLogCursor& that) const
    {
      return file_id_ == that.file_id_  && log_id_ == that.log_id_ && offset_ == that.offset_;
    }

    ObLogCursor& set_cursor(ObLogCursor& cursor, const int64_t file_id, const int64_t log_id, const int64_t offset)
    {
      cursor.file_id_ = file_id;
      cursor.log_id_ = log_id;
      cursor.offset_ = offset;
      return cursor;
    }

    int ObAtomicLogCursor::get_cursor(ObLogCursor& cursor) const
    {
      int err = OB_SUCCESS;
      cursor_lock_.rdlock();
      cursor = log_cursor_;
      cursor_lock_.unlock();
      return err;
    }

    int ObAtomicLogCursor::set_cursor(ObLogCursor& cursor)
    {
      int err = OB_SUCCESS;
      cursor_lock_.wrlock();
      log_cursor_ = cursor;
      cursor_lock_.unlock();
      return err;
    }
  }; // end namespace common
}; // end namespace oceanbase
