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
#include "ob_replay_log_src.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    ObReplayLogSrc::ObReplayLogSrc(): read_pos_(0), log_buffer_(NULL), prefetch_log_task_submitter_(NULL)
    {}

    ObReplayLogSrc::~ObReplayLogSrc()
    {}

    bool ObReplayLogSrc:: is_inited() const
    {
      return NULL != log_buffer_;
    }

    int ObReplayLogSrc:: init(ObLogBuffer* log_buffer, IObAsyncTaskSubmitter* prefetch_log_task_submitter,
                              common::IObServerGetter* server_getter, ObUpsRpcStub* rpc_stub, const int64_t fetch_timeout,
                              const int64_t n_blocks, const int64_t block_size_shift)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == log_buffer || NULL == prefetch_log_task_submitter || NULL == server_getter || NULL == rpc_stub)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = remote_log_src_.init(server_getter, rpc_stub, fetch_timeout)))
      {
        TBSYS_LOG(ERROR, "remote_log_src.init()=>%d", err);
      }
      else if (OB_SUCCESS != (err = prefetch_log_buffer_.init(n_blocks, block_size_shift)))
      {
        TBSYS_LOG(ERROR, "prefetch_log_buffer_.init()=>%d", err);
      }
      else
      {
        log_buffer_ = log_buffer;
        prefetch_log_task_submitter_ = prefetch_log_task_submitter;
      }
      return err;
    }

    int64_t ObReplayLogSrc::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      ObLogCursor cursor;
      {
        SpinWLockGuard guard(pc_lock_);
        cursor = (ObLogCursor&)prefetch_log_cursor_;
      }
      databuff_printf(buf, len, pos, "ReplayLogSrc(rlog_src=%s, read_pos=%ld, prefetch_cursor=%s)", to_cstring(remote_log_src_), read_pos_, to_cstring(cursor));
      return pos;
    }

    int ObReplayLogSrc:: reset_prefetch_log_buffer()
    {
      int err = OB_SUCCESS;
      ((ObLogCursor&)prefetch_log_cursor_).reset();
      if (OB_SUCCESS != (err = prefetch_log_buffer_.reset()))
      {
        TBSYS_LOG(ERROR, "prefetch_log_buffer.reset()=>%d", err);
      }
      return err;
    }

    int ObReplayLogSrc:: submit_prefetch_log_task(const ObLogCursor& start_cursor)
    {
      int err = OB_SUCCESS;
      //TBSYS_LOG(DEBUG, "submit_prefetch_log_task(start_cursor=%s)", start_cursor.to_str());
      if (prefetch_log_buffer_.get_remain() <= prefetch_log_buffer_.get_block_size())
      {
        err = OB_SUCCESS;
        TBSYS_LOG(DEBUG, "no enough buf, need not submit prefetch task.");
      }
      else
      {
        if (prefetch_log_cursor_.log_id_ < start_cursor.log_id_)
        {
          ((ObLogCursor&)prefetch_log_cursor_) = start_cursor;
        }
        if (OB_SUCCESS != (err = prefetch_log_task_submitter_->submit_task(NULL)))
        {
          TBSYS_LOG(ERROR, "submit_prefetch_task()=>%d", err);
        }
      }
      return err;
    }

    int ObReplayLogSrc::get_log(const ObLogCursor& start_cursor, int64_t& end_id, char* buf, const int64_t len, int64_t& read_count)
    {
      int err = OB_SUCCESS;
      const int64_t align_bits = OB_DIRECT_IO_ALIGN_BITS;
      int prefetch_err = OB_SUCCESS;
      int64_t start_id = start_cursor.log_id_;
      int64_t real_pos = 0;
      bool get_from_recent_log_cache = false;

      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = get_from_log_buffer(log_buffer_, align_bits, read_pos_, real_pos,
                                                        start_id, end_id, buf, len, read_count))
               && OB_DATA_NOT_SERVE != err)
      {
        TBSYS_LOG(ERROR, "get_log_from_buffer()=>%d", err);
      }
      else if (OB_SUCCESS == err)  // 可能read_count == 0 这时表示读到了缓冲区末尾，read_pos依然有效
      {
        read_pos_ = real_pos + read_count;
        if (read_count == 0)
        {
          err = OB_DATA_NOT_SERVE;
        }
        else
        {
          get_from_recent_log_cache = true;
        }
      }
      else
      {
        read_pos_ = 0;
      }

      if (OB_DATA_NOT_SERVE != err)
      {}
      else
      {
        SpinWLockGuard guard(pc_lock_);
        if (OB_SUCCESS != (err = prefetch_log_buffer_.get_log(align_bits, start_id, end_id, buf, len, read_count)) && OB_DATA_NOT_SERVE != err && OB_DISCONTINUOUS_LOG != err)
        {
          TBSYS_LOG(ERROR, "prefetch_log_buffer.get_log()=>%d", err);
        }
        else if (OB_DISCONTINUOUS_LOG == err && OB_SUCCESS != (prefetch_err = reset_prefetch_log_buffer()))
        {
          err = prefetch_err;
          TBSYS_LOG(ERROR, "prefetch_log_buffer.reset()=>%d", prefetch_err);
        }
        else if (OB_SUCCESS != (prefetch_err = submit_prefetch_log_task(start_cursor)))
        {
          err = prefetch_err;
          TBSYS_LOG(ERROR, "submit_prefetch_log_task()=>%d", prefetch_err);
        }
        else if (OB_SUCCESS != err)
        {
          err = OB_DATA_NOT_SERVE;
        }
      }
      TBSYS_LOG(DEBUG, "get_log_for_replay(start_cursor=%s, end_id=%ld, read_count=%ld, get_from_cache=%s)=>%d",
                start_cursor.to_str(), end_id, read_count, get_from_recent_log_cache?"true":"false", err);
      return err;
    }

    int ObReplayLogSrc::get_prefetch_cursor(ObLogCursor& cursor)
    {
      int err = OB_SUCCESS;
      SpinWLockGuard guard(pc_lock_);
      cursor = (ObLogCursor&)prefetch_log_cursor_;
      return err;
    }

    int ObReplayLogSrc::prefetch_log()
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = log_buffer_for_prefetch_.get_buffer();
      char* buf = NULL;
      int64_t len = 0;
      int64_t read_count = 0;
      ObLogCursor start_cursor;
      ObLogCursor end_cursor;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = get_prefetch_cursor(start_cursor)))
      {
        TBSYS_LOG(ERROR, "get_prefetch_cursor()=>%d", err);
      }
      else if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "get thread specific buffer fail");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        my_buffer->reset();
        buf = my_buffer->current();
        len = my_buffer->remain();
        end_cursor = start_cursor;
      }
      if (OB_SUCCESS != err)
      {}
      else if (start_cursor.log_id_ <= 0)
      {
        err = OB_NEED_RETRY;
      }
      else if (OB_SUCCESS != (err = remote_log_src_.get_log(start_cursor, end_cursor, buf, len, read_count))
               && OB_NEED_RETRY != err && OB_LOG_SRC_CHANGED != err)
      {
        TBSYS_LOG(ERROR, "remote_log_src.get_log()=>%d", err);
      }
      else if (OB_LOG_SRC_CHANGED == err)
      {
        SpinWLockGuard guard(pc_lock_);
        err = OB_NEED_RETRY;
        ((ObLogCursor&)prefetch_log_cursor_).reset();
      }
      else if (OB_NEED_RETRY == err)
      {
        //TBSYS_LOG(DEBUG, "prefetch_log(start_cursor=%s): NEED_RETRY", start_cursor.to_str());
      }
      else if (OB_SUCCESS == err && 0 >= read_count)
      {
        err = OB_NEED_RETRY;
      }
      else
      {
          SpinWLockGuard guard(pc_lock_);
          if (OB_SUCCESS != (err = prefetch_log_buffer_.append_log(start_cursor.log_id_, end_cursor.log_id_, buf, read_count))
              && OB_DISCONTINUOUS_LOG != err)
          {
            TBSYS_LOG(ERROR, "prefetch_log_buffer_.append_log(log=[%ld,%ld], read_count=%ld)=>%d",
                      start_cursor.log_id_, end_cursor.log_id_, read_count, err);
          }
          else if (OB_DISCONTINUOUS_LOG == err)
          {
            if (start_cursor.log_id_ > prefetch_log_buffer_.get_end_id()
                && OB_SUCCESS != (err = prefetch_log_buffer_.reset()))
            {
              TBSYS_LOG(ERROR, "prefetch_log_buffer_.reset()=>%d", err);
            }
          }
          else if (OB_SUCCESS == err)
          {
            ((ObLogCursor&)prefetch_log_cursor_) = end_cursor;
          }
      }
      TBSYS_LOG(DEBUG, "prefetch_log(start_cursor=%s)=>%d", start_cursor.to_str(), err);
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase

