/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_log_generator.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */
#include "ob_malloc.h"
#include "ob_log_generator.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    int DebugLog::advance()
    {
      int err = OB_SUCCESS;
      last_ctime_ = ctime_;
      ctime_ = tbsys::CTimeUtil::getTime();
      return err;
    }

    int64_t DebugLog::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "DebugLog(%s, ctime=%s[%ld], last_ctime=%s[%ld])", to_cstring(server_),
                      time2str(ctime_), ctime_, time2str(last_ctime_), last_ctime_);
      return pos;
    }

    int DebugLog::serialize(char* buf, int64_t limit, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || limit < 0 || pos > limit)
      {
        err = OB_INVALID_ARGUMENT;
        //TBSYS_LOG(ERROR, "INVALID_ARGUMENT: buf=%p, limit=%ld, pos=%ld", buf, limit, pos);
      }
      else if (OB_SUCCESS != serialization::encode_i64(buf, limit, new_pos, MAGIC)
               || OB_SUCCESS != server_.serialize(buf, limit, new_pos)
               || OB_SUCCESS != serialization::encode_i64(buf, limit, new_pos, ctime_)
               || OB_SUCCESS != serialization::encode_i64(buf, limit, new_pos, last_ctime_))
      {
        err = OB_SERIALIZE_ERROR;
        //TBSYS_LOG(ERROR, "serialize(buf=%p, limit=%ld, pos=%ld): fail", buf, limit, pos);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int DebugLog::deserialize(const char* buf, int64_t limit, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      int64_t magic = 0;
      if (NULL == buf || limit < 0 || pos > limit)
      {
        err = OB_INVALID_ARGUMENT;
        //TBSYS_LOG(ERROR, "INVALID_ARGUMENT: buf=%p, limit=%ld, pos=%ld", buf, limit, pos);
      }
      else if (OB_SUCCESS != serialization::decode_i64(buf, limit, new_pos, (int64_t*)&magic))
      {
        err = OB_DESERIALIZE_ERROR;
      }
      else if (magic != MAGIC)
      {
        err = OB_DESERIALIZE_ERROR;
      }
      else if (OB_SUCCESS != server_.deserialize(buf, limit, new_pos)
               || OB_SUCCESS != serialization::decode_i64(buf, limit, new_pos, (int64_t*)&ctime_)
               || OB_SUCCESS != serialization::decode_i64(buf, limit, new_pos, (int64_t*)&last_ctime_))
      {
        err = OB_DESERIALIZE_ERROR;
        //TBSYS_LOG(ERROR, "deserialize(buf=%p, limit=%ld, pos=%ld): fail", buf, limit, pos);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    inline int64_t get_align_padding_size(const int64_t x, const int64_t mask)
    {
      return -x & mask;
    }

    static bool is_aligned(int64_t x, int64_t mask)
    {
      return !(x & mask);
    }

    static int64_t calc_nop_log_len(int64_t pos, int64_t min_log_size)
    {
      ObLogEntry entry;
      int64_t header_size = entry.get_serialize_size();
      return get_align_padding_size(pos + header_size + min_log_size, ObLogGenerator::LOG_FILE_ALIGN_MASK) + min_log_size;
    }

    char ObLogGenerator::eof_flag_buf_[LOG_FILE_ALIGN_SIZE] __attribute__ ((aligned(DIO_ALIGN_SIZE)));
    static class EOF_FLAG_BUF_CONSTRUCTOR
    {
      public:
        EOF_FLAG_BUF_CONSTRUCTOR() {
          const char* mark_str = "end_of_log_file";
          memset(ObLogGenerator::eof_flag_buf_, 0, sizeof(ObLogGenerator::eof_flag_buf_));
          for(int64_t i = 0; i + strlen(mark_str) < sizeof(ObLogGenerator::eof_flag_buf_); i += strlen(mark_str))
          {
            strcpy(ObLogGenerator::eof_flag_buf_ + i, mark_str);
          }
        }
        ~EOF_FLAG_BUF_CONSTRUCTOR() {}
    } eof_flag_buf_constructor_;

    ObLogGenerator::ObLogGenerator(): is_frozen_(false), log_file_max_size_(1<<24), start_cursor_(), end_cursor_(),
                                      log_buf_(NULL), log_buf_len_(0), pos_(0)
    {
      memset(empty_log_, 0, sizeof(empty_log_));
      memset(nop_log_, 0, sizeof(nop_log_));
    }

    ObLogGenerator:: ~ObLogGenerator()
    {
      if(NULL != log_buf_)
      {
        free(log_buf_);
        log_buf_ = NULL;
      }
    }

    bool ObLogGenerator::is_inited() const
    {
      return NULL != log_buf_ && log_buf_len_ > 0;
    }

    int ObLogGenerator::dump_for_debug() const
    {
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "[start_cursor[%ld], end_cursor[%ld]]", start_cursor_.log_id_, end_cursor_.log_id_);
      return err;
    }

    int ObLogGenerator::check_state() const
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      return err;
    }

    int ObLogGenerator::init(int64_t log_buf_size, int64_t log_file_max_size, const ObServer* server)
    {
      int err = OB_SUCCESS;
      int sys_err = 0;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if(log_buf_size <= 0 || log_file_max_size <= 0 || log_file_max_size < 2 * LOG_FILE_ALIGN_SIZE)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 != (sys_err = posix_memalign((void**)&log_buf_, LOG_FILE_ALIGN_SIZE, log_buf_size + LOG_FILE_ALIGN_SIZE)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "posix_memalign(%ld):%s", log_buf_size, strerror(sys_err));
      }
      else
      {
        if (NULL != server)
        {
          debug_log_.server_ = *server;
        }
        log_file_max_size_ = log_file_max_size;
        log_buf_len_ = log_buf_size + LOG_FILE_ALIGN_SIZE;
        TBSYS_LOG(INFO, "log_generator.init(log_buf_size=%ld, log_file_max_size=%ld)", log_buf_size, log_file_max_size);
      }
      return err;
    }

    bool ObLogGenerator::is_log_start() const
    {
      return start_cursor_.is_valid();
    }

    bool ObLogGenerator::is_clear() const
    {
      return 0 == pos_ && false == is_frozen_ && start_cursor_.equal(end_cursor_);
    }

    int64_t ObLogGenerator::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "LogGenerator([%s,%s], len=%ld, frozen=%s)",
                      to_cstring(start_cursor_), to_cstring(end_cursor_), pos_, STR_BOOL(is_frozen_));
      return pos;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    bool ObLogGenerator::is_normal_mutator_size(const int64_t size) const
    {
      ObLogEntry entry;
      bool ret = (size > 0 && size + LOG_BUF_RESERVED_SIZE + entry.get_serialize_size() <= log_buf_len_);
      return ret;
    }

    bool ObLogGenerator::check_log_size(const int64_t size,const int64_t max_mutator_size) const
    {
      ObLogEntry entry;
      bool ret = (size > 0 && size + LOG_BUF_RESERVED_SIZE + entry.get_serialize_size() <= max_mutator_size);
      if (!ret)
      {
        TBSYS_LOG(ERROR, "log_size[%ld] + reserved[%ld] + header[%ld] >= max_mutator_size[%ld]",
                  size, LOG_BUF_RESERVED_SIZE, entry.get_serialize_size(), max_mutator_size);
      }
      return ret;
    }
    //add e

    bool ObLogGenerator::check_log_size(const int64_t size) const
    {
      ObLogEntry entry;
      bool ret = (size > 0 && size + LOG_BUF_RESERVED_SIZE + entry.get_serialize_size() <= log_buf_len_);
      if (!ret)
      {
        TBSYS_LOG(ERROR, "log_size[%ld] + reserved[%ld] + header[%ld] <= log_buf_len[%ld]",
                  size, LOG_BUF_RESERVED_SIZE, entry.get_serialize_size(), log_buf_len_);
      }
      return ret;
    }

    int ObLogGenerator::reset()
    {
      int err = OB_SUCCESS;
      if (!is_clear())
      {
        err = OB_LOG_NOT_CLEAR;
        TBSYS_LOG(ERROR, "log_not_clear, [%s,%s], len=%ld", to_cstring(start_cursor_), to_cstring(end_cursor_), pos_);
      }
      else
      {
        start_cursor_.reset();
        end_cursor_.reset();
        is_frozen_ = false;
        pos_ = 0;
      }
      return err;
    }

    //add wangdonghui [ups_replication] 20170220 :b
    int ObLogGenerator::clear()
    {
      int err = OB_SUCCESS;
      start_cursor_.reset();
      end_cursor_.reset();
      is_frozen_ = false;
      pos_ = 0;
      return err;
    }
    //add :e
    int ObLogGenerator::start_log(const ObLogCursor& log_cursor)
    {
      int err = OB_SUCCESS;
      if (!log_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "log_cursor.is_valid()=>false");
      }
      //del wangjiahao [UPS replication]
      /*
      else if (start_cursor_.is_valid())
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "cursor=[%ld,%ld] already inited.", start_cursor_.log_id_, end_cursor_.log_id_);
      }*/
      //del
      else
      {
        start_cursor_ = log_cursor;
        end_cursor_ = log_cursor;
        TBSYS_LOG(INFO, "ObLogGenerator::start_log(log_cursor=%s)", to_cstring(log_cursor));
      }
      return err;
    }

    int ObLogGenerator:: update_cursor(const ObLogCursor& log_cursor)
    {
      int err = OB_SUCCESS;
      if (!log_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (!is_clear())
      {
        err = OB_LOG_NOT_CLEAR;
        TBSYS_LOG(ERROR, "log_not_clear, [%s,%s], len=%ld", to_cstring(start_cursor_), to_cstring(end_cursor_), pos_);
      }
      else if (end_cursor_.newer_than(log_cursor))
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(ERROR, "end_cursor[%s].newer_than(log_cursor[%s])", to_cstring(end_cursor_), to_cstring(log_cursor));
      }
      else
      {
        start_cursor_ = log_cursor;
        end_cursor_ = log_cursor;
      }
      return err;
    }

    int ObLogGenerator:: get_start_cursor(ObLogCursor& log_cursor) const
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else
      {
        log_cursor = start_cursor_;
      }
      return err;
    }

    int ObLogGenerator:: get_end_cursor(ObLogCursor& log_cursor) const
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else
      {
        log_cursor = end_cursor_;
      }
      return err;
    }

    static int serialize_log_entry(char* buf, const int64_t len, int64_t& pos, ObLogEntry& entry,
                            const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || 0 >= len || pos > len || NULL == log_data || 0 >= data_len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "serialize_log_entry(buf=%p, len=%ld, pos=%ld, log_data=%p, data_len=%ld)=>%d",
                  buf, len, pos, log_data, data_len, err);
      }
      else if (pos + entry.get_serialize_size() + data_len > len)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(DEBUG, "pos[%ld] + entry.serialize_size[%ld] + data_len[%ld] > len[%ld]",
                  pos, entry.get_serialize_size(), data_len, len);
      }
      else if (OB_SUCCESS != (err = entry.serialize(buf, len, pos)))
      {
        TBSYS_LOG(ERROR, "entry.serialize(buf=%p, pos=%ld, capacity=%ld)=>%d",
                  buf, len, pos, err);
      }
      else
      {
        memcpy(buf + pos, log_data, data_len);
        pos += data_len;
      }
      return err;
    }

    //generate log with term in header
    static int generate_log(char* buf, const int64_t len, int64_t& pos, ObLogCursor& cursor, const LogCommand cmd,
                 const char* log_data, const int64_t data_len
                            //add wangjiahao [Paxos ups_replication_tmplog] 20150724 :b
                            , const int64_t term
                            //add:e
                            )
    {
      int err = OB_SUCCESS;
      ObLogEntry entry;
      if (NULL == buf || 0 >= len || pos > len || NULL == log_data || 0 >= data_len || !cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "generate_log(buf=%p, len=%ld, pos=%ld, log_data=%p, data_len=%ld, cursor=%s)=>%d",
                  buf, len, pos, log_data, data_len, to_cstring(cursor), err);
      }
      else if (entry.get_serialize_size() + data_len > len)
      {
        err = OB_LOG_TOO_LARGE;
        TBSYS_LOG(WARN, "header[%ld] + data_len[%ld] > len[%ld]", entry.get_serialize_size(), data_len, len);
      }
      else if (OB_SUCCESS != (err = cursor.next_entry(entry, cmd, log_data, data_len
                                                      //add wangjiahao [Paxos ups_replication_tmplog] 20150724 :b
                                                      , term
                                                      //add:e
                                                      )))
      {
        TBSYS_LOG(ERROR, "cursor[%s].next_entry()=>%d", to_cstring(cursor), err);
      }
      else if (OB_SUCCESS != (err = serialize_log_entry(buf, len, pos, entry, log_data, data_len)))
      {
        TBSYS_LOG(DEBUG, "serialize_log_entry(buf=%p, len=%ld, entry[id=%ld], data_len=%ld)=>%d",
                  buf, len, entry.seq_, data_len, err);
      }
      else if (OB_SUCCESS != (err = cursor.advance(entry)))
      {
        TBSYS_LOG(ERROR, "cursor[id=%ld].advance(entry.id=%ld)=>%d", cursor.log_id_, entry.seq_, err);
      }
      return err;
    }

    int ObLogGenerator:: do_write_log(const LogCommand cmd, const char* log_data, const int64_t data_len,
                                      const int64_t reserved_len)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (NULL == log_data || data_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (is_frozen_)
      {
        err = OB_STATE_NOT_MATCH;
        TBSYS_LOG(ERROR, "log_generator is frozen, cursor=[%s,%s]", to_cstring(start_cursor_), to_cstring(end_cursor_));
      }
      else if (OB_SUCCESS != (err = generate_log(log_buf_, log_buf_len_ - reserved_len, pos_,
                                                 end_cursor_, cmd, log_data, data_len
                                                 //add wangjiahao [Paxos ups_replication_tmplog] 20150724 :b
                                                 , log_term_
                                                 //add:e
                                                 )) && OB_BUF_NOT_ENOUGH != err)
      {
        TBSYS_LOG(WARN, "generate_log(pos=%ld)=>%d", pos_, err);
      }
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
                  log_data, data_len, to_cstring(start_cursor));
      }

      while (OB_SUCCESS == err && pos < data_len)
      {
        if (OB_SUCCESS != (err = log_entry.deserialize(log_data, data_len, pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, pos, err);
        }
        else if (pos + log_entry.get_log_data_len() > data_len)
        {
          err = OB_LAST_LOG_RUINNED;
          TBSYS_LOG(ERROR, "last_log broken, cursor=[%s,%s]", to_cstring(start_cursor), to_cstring(end_cursor));
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

    int ObLogGenerator:: fill_batch(const char* buf, int64_t len)
    {
      int err = OB_SUCCESS;
      ObLogCursor start_cursor, end_cursor;
      int64_t reserved_len = LOG_FILE_ALIGN_SIZE;
      start_cursor = end_cursor_;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (NULL == buf || len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 != (len & LOG_FILE_ALIGN_MASK))
      {
        err = OB_LOG_NOT_ALIGN;
        TBSYS_LOG(ERROR, "len[%ld] is not align[mask=%lx]", len, LOG_FILE_ALIGN_SIZE);
      }
      else if (is_frozen_)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "log_buf is frozen, end_cursor=%s", to_cstring(end_cursor_));
      }
      else if (pos_ != 0)
      {
        err = OB_LOG_NOT_CLEAR;
        TBSYS_LOG(ERROR, "fill_batch(pos[%ld] != 0, end_cursor=%s, buf=%p[%ld])",
                  pos_, to_cstring(end_cursor_), buf, len);
      }
      else if (len + reserved_len > log_buf_len_)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "len[%ld] + reserved_len[%ld] > log_buf_len[%ld]",
                  len, reserved_len, log_buf_len_);
      }
      else if (OB_SUCCESS != (err = parse_log_buffer(buf, len, start_cursor, end_cursor)))
      {
        TBSYS_LOG(ERROR, "parse_log_buffer(buf=%p[%ld], cursor=%s)=>%d", buf, len, to_cstring(end_cursor_), err);
      }
      else
      {
        memcpy(log_buf_, buf, len);
        pos_ = len;
        end_cursor_ = end_cursor;
        is_frozen_ = true;
      }
      return err;
    }

    int ObLogGenerator:: write_log(const LogCommand cmd, const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (NULL == log_data || data_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (is_frozen_)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "log_buf is frozen, end_cursor=%s", to_cstring(end_cursor_));
      }
      else if (OB_SUCCESS != (err = do_write_log(cmd, log_data, data_len, LOG_BUF_RESERVED_SIZE))
               && OB_BUF_NOT_ENOUGH != err)
      {
        TBSYS_LOG(WARN, "do_write_log(cmd=%d, pos=%ld, len=%ld)=>%d", cmd, pos_, data_len, err);
      }

      return err;
    }

    int ObLogGenerator::switch_log()
    {
      int err = OB_SUCCESS;
      ObLogEntry entry;
      int64_t header_size = entry.get_serialize_size();
      const int64_t buf_len = LOG_FILE_ALIGN_SIZE - header_size;
      char* buf = empty_log_;
      int64_t buf_pos = 0;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, buf_pos, end_cursor_.file_id_ + 1)))
      {
        TBSYS_LOG(ERROR, "encode_i64(file_id_=%ld)=>%d", end_cursor_.file_id_, err);
      }
      else if (OB_SUCCESS != (err = do_write_log(OB_LOG_SWITCH_LOG, buf, buf_len, 0)))
      {
        TBSYS_LOG(ERROR, "write(OB_LOG_SWITCH_LOG, len=%ld)=>%d", end_cursor_.file_id_, err);
      }
      else
      {
        TBSYS_LOG(INFO, "switch_log(file_id=%ld, log_id=%ld)", end_cursor_.file_id_, end_cursor_.log_id_);
      }
      return err;
    }

    int ObLogGenerator::check_point(int64_t& cur_log_file_id)
    {
      int err = OB_SUCCESS;
      ObLogEntry entry;
      int64_t header_size = entry.get_serialize_size();
      const int64_t buf_len = LOG_FILE_ALIGN_SIZE - header_size;
      char* buf = empty_log_;
      int64_t buf_pos = 0;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, buf_pos, end_cursor_.file_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(file_id_=%ld)=>%d", end_cursor_.file_id_, err);
      }
      else if (OB_SUCCESS != (err = do_write_log(OB_LOG_CHECKPOINT, buf, buf_len, 0)))
      {
        TBSYS_LOG(ERROR, "write(OB_LOG_SWITCH_LOG, len=%ld)=>%d", end_cursor_.file_id_, err);
      }
      else
      {
        cur_log_file_id = end_cursor_.file_id_;
        TBSYS_LOG(INFO, "checkpoint(file_id=%ld, log_id=%ld)", end_cursor_.file_id_, end_cursor_.log_id_);
      }
      return err;
    }

    int ObLogGenerator::gen_keep_alive()
    {
      return write_nop(/*force_write*/true);
    }

    int ObLogGenerator::append_eof()
    {
      int err = OB_SUCCESS;
      if (pos_ + (int64_t)sizeof(eof_flag_buf_) > log_buf_len_)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "no buf to append eof flag, pos=%ld, log_buf_len=%ld", pos_, log_buf_len_);
      }
      else
      {
        memcpy(log_buf_ + pos_, eof_flag_buf_, sizeof(eof_flag_buf_));
      }
      return err;
    }

    bool ObLogGenerator::is_eof(const char* buf, int64_t len)
    {
      return NULL != buf && len >= LOG_FILE_ALIGN_SIZE && 0 == memcmp(buf, eof_flag_buf_, LOG_FILE_ALIGN_SIZE);
    }

    int ObLogGenerator:: check_log_file_size()
    {
      int err = OB_SUCCESS;
      if (end_cursor_.offset_ + log_buf_len_ <= log_file_max_size_)
      {}
      else if (OB_SUCCESS != (err = switch_log()))
      {
        TBSYS_LOG(ERROR, "switch_log()=>%d", err);
      }
      return err;
    }

    int ObLogGenerator:: write_nop(const bool force_write)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      TBSYS_LOG(TRACE, "try write nop: pos=%ld, force_write=%s", pos_, STR_BOOL(force_write));
      if (is_aligned(pos_, LOG_FILE_ALIGN_MASK) && !force_write)
      {
        TBSYS_LOG(TRACE, "The log is aligned");
      }
      else if (OB_SUCCESS != (err = debug_log_.advance()))
      {
        TBSYS_LOG(ERROR, "debug_log.advance()=>%d", err);
      }
      else if (OB_SUCCESS != (err = debug_log_.serialize(nop_log_, sizeof(nop_log_), pos)))
      {
        TBSYS_LOG(ERROR, "serialize_nop_log(%s)=>%d", to_cstring(end_cursor_), err);
      }
      else if (OB_SUCCESS != (err = do_write_log(OB_LOG_NOP, nop_log_, calc_nop_log_len(pos_, pos), 0)))
      {
        TBSYS_LOG(ERROR, "write_log(OB_LOG_NOP, len=%ld)=>%d", calc_nop_log_len(pos_, pos), err);
      }
      return err;
    }

    int ObLogGenerator:: switch_log(int64_t& new_file_id)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (OB_SUCCESS != (err = write_nop()))
      {
        TBSYS_LOG(ERROR, "write_nop()=>%d", err);
      }
      else if (OB_SUCCESS != (err = switch_log()))
      {
        TBSYS_LOG(ERROR, "switch_log()=>%d", err);
      }
      else
      {
        is_frozen_ = true;
        new_file_id = end_cursor_.file_id_;
      }
      return err;
    }

    bool ObLogGenerator::has_log() const
    {
      return pos_ != 0;
    }

    int ObLogGenerator:: get_log(ObLogCursor& start_cursor, ObLogCursor& end_cursor, char*& buf, int64_t& len)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (!is_frozen_ && OB_SUCCESS != (err = write_nop(has_log())))
      {
        TBSYS_LOG(ERROR, "write_nop()=>%d", err);
      }
      else if (!is_frozen_ && OB_SUCCESS != (err = check_log_file_size()))
      {
        TBSYS_LOG(ERROR, "check_log_file_size()=>%d", err);
      }
      else if (OB_SUCCESS != (err = append_eof()))
      {
        TBSYS_LOG(ERROR, "write_eof()=>%d", err);
      }
      else
      {
        is_frozen_ = true;
        buf = log_buf_;
        len = pos_;
        end_cursor = end_cursor_;
        start_cursor = start_cursor_;
      }
      return err;
    }

    int ObLogGenerator:: commit(const ObLogCursor& end_cursor)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (!end_cursor.equal(end_cursor_))
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "end_cursor[%ld] != end_cursor_[%ld]", end_cursor.log_id_, end_cursor_.log_id_);
      }
      else
      {
        start_cursor_ = end_cursor_;
        pos_ = 0;
        is_frozen_ = false;
      }
      return err;
    }
  } // end namespace common
} // end namespace oceanbase
