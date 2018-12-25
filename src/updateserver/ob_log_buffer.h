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
#ifndef __OB_UPDATESERVER_OB_LOG_BUFFER_H__
#define __OB_UPDATESERVER_OB_LOG_BUFFER_H__

#include "ob_ring_data_buffer.h"

namespace oceanbase
{
  namespace updateserver
  {
    // 串行地append_log(), 并发地get_log()
    class ObLogBuffer : public ObRingDataBuffer
    {
      public:
        ObLogBuffer();
        virtual ~ObLogBuffer();
        // 返回OB_DISCONTINUOUS_LOG, 需要重置。
        int append_log(const int64_t start_id, const int64_t end_id, const char* buf, const int64_t len);
        // 返回OB_DATA_NOT_SERVE, pos无效，需要重新定位,
        int get_log(const int64_t align_bits, const int64_t pos, int64_t& real_pos, int64_t& start_id, int64_t& end_id, char* buf, const int64_t len, int64_t& read_count) const;
        // 返回OB_DATA_NOT_SERVE，缓冲区中暂时没有编号为log_id的日志
        int seek(const int64_t log_id, const int64_t advised_pos, int64_t& real_pos) const;
        int reset();
        int dump_for_debug() const;
        int64_t get_start_id() const;
        int64_t get_end_id() const;
        int64_t to_string(char* buf, const int64_t len) const;
      protected:
        int get_next_entry(const int64_t pos, int64_t& next_pos, int64_t& log_id) const;
      private:
        volatile int64_t end_id_;
    };
    // 可能返回OB_DATA_NOT_SERVE
    int get_from_log_buffer(ObLogBuffer* log_buf, const int64_t align_bits, const int64_t advised_pos, int64_t& real_pos, const int64_t start_id, int64_t& end_id, char* buf, const int64_t len, int64_t& read_count);
    //add wangjiahao [Paxos ups_replication_tmplog] 20150630 :b
    // 可能返回OB_DATA_NOT_SERVE
    int get_from_log_buffer_2(ObLogBuffer* log_buf, const int64_t align_bits, const int64_t advised_pos, int64_t& real_pos, const int64_t start_id, int64_t end_id, char* buf, int64_t& read_count, const int64_t max_data_len);
    //add :e
    // OB_DISCONTINUOUS_LOG被处理，不应该返回任何错误
    int append_to_log_buffer(ObLogBuffer* log_buf, const int64_t start_id, const int64_t end_id,
                             const char* buf, const int64_t len);
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_LOG_BUFFER_H__ */
