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
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_COMMIT_LOG_RECEIVER_H_
#define OCEANBASE_UPDATESERVER_OB_COMMIT_LOG_RECEIVER_H_

#include "common/ob_log_writer.h"
#include "common/ob_base_server.h"
#include "ob_ups_slave_mgr.h"
#include "easy_io_struct.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObCommitLogReceiver
    {
      public:
        ObCommitLogReceiver();
        int init(common::ObLogWriter *log_writer, common::ObBaseServer *base_server, ObUpsSlaveMgr *slave_mgr, const int64_t time_warn_us);
        int receive_log(const int32_t version, common::ObDataBuffer& in_buff,
                        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int write_data(const char* log_data, const int64_t data_len, uint64_t &log_id, uint64_t &log_seq);

      protected:
        int start_log(common::ObDataBuffer& buff);
        int start_receiving_log();
        int end_receiving_log(const int32_t version, easy_request_t* req, const int32_t channel_id, common::ObDataBuffer& out_buff);

        inline int check_inner_stat() const
        {
          int ret = common::OB_SUCCESS;
          if (!is_initialized_)
          {
            TBSYS_LOG(ERROR, "ObLogWriter has not been initialized");
            ret = common::OB_NOT_INIT;
          }
          return ret;
        }

      protected:
        common::ObLogWriter *log_writer_;
        common::ObBaseServer *base_server_;
        ObUpsSlaveMgr *slave_mgr_;
        int64_t time_warn_us_;
        bool is_initialized_;
    };
  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_OB_COMMIT_LOG_RECEIVER_H_
