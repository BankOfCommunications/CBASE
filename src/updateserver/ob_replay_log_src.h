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
#ifndef __OB_UPDATESERVER_OB_REPLAY_LOG_SRC_H__
#define __OB_UPDATESERVER_OB_REPLAY_LOG_SRC_H__
#include "common/ob_task.h"
#include "common/thread_buffer.h"
#include "ob_log_src.h"
#include "ob_log_buffer.h"
#include "ob_prefetch_log_buffer.h"
#include "ob_remote_log_src.h"

namespace oceanbase
{
  namespace common
  {
    class IObServerGetter;
  };
  namespace updateserver
  {
    class ObUpsRpcStub;
    class ObReplayLogSrc
    {
      public:
      public:
        ObReplayLogSrc();
        virtual ~ObReplayLogSrc();
        int init(ObLogBuffer* log_buffer, common::IObAsyncTaskSubmitter* prefetch_task_submitter,
                 common::IObServerGetter* server_getter, ObUpsRpcStub* rpc_stub,
                 const int64_t fetch_timeout, const int64_t n_blocks, const int64_t block_size_shift);
        int get_log(const common::ObLogCursor& start_cursor, int64_t& end_id,
                    char* buf, const int64_t len, int64_t& read_count);
        ObRemoteLogSrc& get_remote_log_src()
      {
        return remote_log_src_;
      }
        int prefetch_log();
        int get_prefetch_cursor(common::ObLogCursor& cursor);
        int64_t to_string(char* buf, const int64_t len) const;
      protected:
        bool is_inited() const;
        int reset_prefetch_log_buffer();
        int submit_prefetch_log_task(const common::ObLogCursor& start_cursor);
      private:
        int64_t read_pos_;
        ObLogBuffer* log_buffer_;
        volatile common::ObLogCursor prefetch_log_cursor_;
        ObRemoteLogSrc remote_log_src_;
        ObPrefetchLogBuffer prefetch_log_buffer_;
        common::IObAsyncTaskSubmitter* prefetch_log_task_submitter_;
        common::ThreadSpecificBuffer log_buffer_for_prefetch_;
        mutable common::SpinRWLock pc_lock_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_REPLAY_LOG_SRC_H__ */
