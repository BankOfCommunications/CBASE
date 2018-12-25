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
#ifndef __OB_UPDATESERVER_OB_PREFETCH_LOG_BUFFER_H__
#define __OB_UPDATESERVER_OB_PREFETCH_LOG_BUFFER_H__

#include "common/ob_spin_rwlock.h"
#include "ob_log_buffer.h"

namespace oceanbase
{
  namespace updateserver
  {
    // ObPrefetchLogBuffer在备机重放日志时实现取日志和重放并行化，
    // 当replay线程每次调用get_log()获取日志检测到prefetch_log_buffer有剩余空间时，便提交一个异步取日志任务。
    // 异步取日志任务执行时从主机取回日志，调用push_log()将日志追加到prefetch_log_buffer中
    // 所以prefetch_log_buffer是一个单读者，多写者的环形缓冲区
    // 读者读取日志时检测到日志不连续，需要调用reset()重置prefetch_log_buffer.
    // 这意味着get_log()和reset()的调用一定是串行的，但是它们和push_log()的调用是并发的。

    // 读者读取位置由read_pos_记录，写者最后追加的位置由end_pos_记录,
    // append_log()时保证不会覆盖read_pos_,
    // get_log()保证读到的是完整的日志，但是这些日志不一定连续，
    // 所以可能返回OB_DISCONTINUOUS_LOG
    class ObPrefetchLogBuffer : public ObLogBuffer
    {
      public:
        ObPrefetchLogBuffer();
        virtual ~ObPrefetchLogBuffer();
        // get_log()检测到要读的日志不连续时，返回OB_DISCONTINUOUS_LOG,
        int get_log(const int64_t align_bits, const int64_t start_id, int64_t& end_id, char* buf, const int64_t len, int64_t& read_count);
        int64_t get_remain() const;
        // reset()和get_log()串行地被调用
        // append_log()时如果检查出会覆盖未读日志，返回OB_EAGAIN
        int append_log(const int64_t start_id, const int64_t end_id, const char* buf, const int64_t len);
        int reset();
      private:
        DISALLOW_COPY_AND_ASSIGN(ObPrefetchLogBuffer);
        volatile int64_t read_pos_;
        mutable common::SpinRWLock write_lock_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase
#endif /* __OB_UPDATESERVER_OB_PREFETCH_LOG_BUFFER_H__ */
