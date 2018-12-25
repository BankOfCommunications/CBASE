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
#ifndef __OB_UPDATESERVER_OB_CACHED_POS_LOG_READER_H__
#define __OB_UPDATESERVER_OB_CACHED_POS_LOG_READER_H__
#include "common/ob_define.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObPosLogReader;
    class ObRegionLogReader;
    class ObLogBuffer;
    class ObFetchLogReq;
    class ObFetchedLog;

    class ObCachedPosLogReader
    {
      public:
        ObCachedPosLogReader();
        ~ObCachedPosLogReader();
      public:
        int init(ObPosLogReader* log_reader, ObLogBuffer* log_buffer);
        int get_log(ObFetchLogReq& req, ObFetchedLog& result);
      protected:
        bool is_inited() const;
      private:
        ObPosLogReader* log_reader_;
        ObLogBuffer* log_buffer_;
    };
    //add wangjiahao [Paxos ups_replication] 20150710 :b
    class ObCachedRegionLogReader
    {
      public:
        ObCachedRegionLogReader();
        ~ObCachedRegionLogReader();
      public:
        int init(ObRegionLogReader* log_reader, ObLogBuffer* log_buffer);
        //get log if log_buffer_ has log which start with start_id, otherwise find them in hd where log in [start_id, end_id] *MUST BE* existed
        int get_log(int64_t start_id, int64_t end_id, char *data, int64_t& data_len, int64_t max_data_len, int64_t &old_pos, bool disk_only=false);
        bool is_inited() const;
      private:
        ObRegionLogReader* log_reader_;
        ObLogBuffer* log_buffer_;
    };
    //add :e

  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_CACHED_POS_LOG_READER_H__ */
