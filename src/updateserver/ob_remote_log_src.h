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
#ifndef __OB_UPDATESERVER_OB_REMOTE_LOG_SRC_H__
#define __OB_UPDATESERVER_OB_REMOTE_LOG_SRC_H__
#include "common/ob_log_cursor.h"
#include "ob_log_src.h"
#include "ob_fetched_log.h"

namespace oceanbase
{
  namespace common
  {
    class IObServerGetter;
    class ObServer;
  };
  namespace updateserver
  {
    class ObUpsRpcStub;
    class ObRemoteLogSrc
    {
      public:
        ObRemoteLogSrc();
        virtual ~ObRemoteLogSrc();
        int init(common::IObServerGetter* server_getter, ObUpsRpcStub* rpc_stub, const int64_t fetch_timeout);
        // 可能返回OB_NEED_RETRY;
        int fill_start_cursor(common::ObLogCursor& start_cursor);
        int fill_start_cursor_with_lsync_server(const common::ObServer& server, common::ObLogCursor& start_cursor);
        int fill_start_cursor_with_fetch_server(const common::ObServer& server, common::ObLogCursor& start_cursor);
        // virtual int get_log(const int64_t start_id, int64_t& end_id,
        //                     char* buf, const int64_t len, int64_t& read_count);
        int get_log(const common::ObLogCursor& start_cursor, common::ObLogCursor& end_cursor,
                    char* buf, const int64_t len, int64_t& read_count);
        int get_log_from_lsync_server(const common::ObServer& server, const common::ObLogCursor& start_cursor,
                                      common::ObLogCursor& end_cursor, char* buf, const int64_t len, int64_t& read_count);
        bool is_using_lsync() const;
        int64_t to_string(char* buf, const int64_t len) const;
        ObFetchLogReq& copy_req(ObFetchLogReq& req);
        ObFetchLogReq& update_req(ObFetchLogReq& req);
      protected:
        bool is_inited() const;
      private:
        bool registered_to_lsync_server_;
        ObFetchLogReq req_;
        volatile uint64_t req_seq_;
        common::IObServerGetter* server_getter_;
        ObUpsRpcStub* rpc_stub_;
        int64_t fetch_timeout_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_REMOTE_LOG_SRC_H__ */

