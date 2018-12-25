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
#include "updateserver/ob_log_locator.h"
#include "updateserver/ob_located_log_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace lsync
  {
    class ObSeekableLogReader
    {
      public:
        static const int64_t DEFAULT_RESERVED_TIME_TO_SEND_PACKET = 500000;
      public:
        ObSeekableLogReader(): stop_(false), log_dir_(NULL), dio_(true),
                               is_read_old_log_(false), lsync_retry_wait_time_us_(0),
                               reserved_time_to_send_packet_(DEFAULT_RESERVED_TIME_TO_SEND_PACKET),
                               start_location_() {}
        ~ObSeekableLogReader() {}
        int init(const char* log_dir, const bool is_read_old_log, const int64_t lsync_retry_wait_tims_us);
        int get_log(const int64_t file_id, const int64_t start_id, char* buf, const int64_t len, int64_t& read_count, const int64_t timeout_us);
        void stop() { stop_ = true; }
      protected:
        bool is_inited() const;
        int get_location(const int64_t file_id, const int64_t start_id, ObLogLocation& start_location);
        int get_log(const int64_t file_id, const int64_t start_id, ObLogLocation& start_location, ObLogLocation& end_location, char* buf, const int64_t len, int64_t& read_count);
      private:
        volatile bool stop_;
        const char* log_dir_;
        bool dio_;
        bool is_read_old_log_;
        int64_t lsync_retry_wait_time_us_;
        int64_t reserved_time_to_send_packet_;
        ObLogLocation start_location_;
        ObLocatedLogReader located_log_reader_;
    };

  } // end namespace lsync
} // end namespace oceanbase
#endif // _OCEANBASE_LSYNC_OB_SEEKABLE_LOG_READER_H_

