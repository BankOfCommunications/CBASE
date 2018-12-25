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
#include "tbsys.h"
#include "ob_prefetch_log_buffer.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    ObPrefetchLogBuffer::ObPrefetchLogBuffer(): read_pos_(0)
    {}

    ObPrefetchLogBuffer::~ObPrefetchLogBuffer()
    {}

    int64_t ObPrefetchLogBuffer::get_remain() const
    {
      return read_pos_ + (n_blocks_) * (1<<block_size_shift_) - end_pos_;
    }

    int ObPrefetchLogBuffer::reset()
    {
      int err = OB_SUCCESS;
      SpinWLockGuard lock_guard(write_lock_);
      if (OB_SUCCESS != (err = ObLogBuffer::reset()))
      {
        TBSYS_LOG(ERROR, "ObLogBuffer::reset()=>%d", err);
      }
      else
      {
        read_pos_ = end_pos_;
      }
      return err;
    }

    int ObPrefetchLogBuffer::get_log(const int64_t align_bits, const int64_t start_id, int64_t& end_id, char* buf, const int64_t len, int64_t& read_count)
    {
      int err = OB_SUCCESS;
      int64_t real_pos = 0;
      int64_t real_start_id = start_id;

      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 >= len || 0 > start_id)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = ObLogBuffer::get_log(align_bits, read_pos_, real_pos, real_start_id, end_id, buf, len, read_count)))
      {
        if (OB_DATA_NOT_SERVE != err && OB_DISCONTINUOUS_LOG != err)
        {
          TBSYS_LOG(ERROR, "log_buf_.get_log(pos, buf, len)=>%d", err);
        }
      }
      else if (start_id != real_start_id)
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(WARN, "start_id[%ld] != real_start_id[%ld]: need reset prefetch_buffer", start_id, real_start_id);
      }
      else
      {
        read_pos_ = real_pos + read_count;
      }
      TBSYS_LOG(DEBUG, "get_log(read_pos_=%ld, start_id=%ld, end_id=%ld)=>%d", read_pos_, start_id, end_id, err);
      return err;
    }

    int ObPrefetchLogBuffer::append_log(const int64_t start_id, const int64_t end_id,
                                        const char* buf, const int64_t len)
    {
      int err = OB_SUCCESS;
      SpinWLockGuard lock_guard(write_lock_);
      TBSYS_LOG(DEBUG, "append_log(log=[%ld,%ld], end_id=%ld)", start_id, end_id, get_end_id());
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 > len || 0 >= start_id
               || end_id < start_id || len + ObDataBlock::BLOCK_RESERVED_BYTES > get_block_size())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "append(buf=%p[%ld], log=[%ld,%ld]): invalid argument", buf, len, start_id, end_id);
      }
      // 检查是否会覆盖未读的日志
      else if (get_remain() < (1<<block_size_shift_))
      {
        err = OB_EAGAIN;
        TBSYS_LOG(DEBUG, "append_log(log=[%ld,%ld], end_id=%ld):WAIT_FOR_READER", start_id, end_id, get_end_id());
      }
      else if (OB_SUCCESS != (err = ObLogBuffer::append_log(start_id, end_id, buf, len))
               && OB_DISCONTINUOUS_LOG != err)
      {
        TBSYS_LOG(ERROR, "append_log(id=[%ld, %ld], buf=%p[%ld])=>%d", start_id, end_id, buf, len, err);
      }
      else if (OB_DISCONTINUOUS_LOG == err)
      {
        TBSYS_LOG(WARN, "append_log(log=[%ld,%ld], end_id=%ld):LOG_DISCONTINUOUS", start_id, end_id, get_end_id());
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase

