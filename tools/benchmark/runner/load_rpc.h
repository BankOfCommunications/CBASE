/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_rpc.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_RPC_H_
#define LOAD_RPC_H_

#include "common/ob_scanner.h"
#include "common/thread_buffer.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_client_manager.h"
#include "common/ob_fifo_stream.h"

namespace oceanbase
{
  namespace tools
  {
    class QueryInfo;
    class LoadRpc
    {
    public:
      LoadRpc();
      virtual ~LoadRpc();
    public:
      static const int64_t DEFAULT_VERSION = 1;
      void init(const common::ObClientManager * client, const common::ThreadSpecificBuffer * buffer);
      // sync
      virtual int send_request(const common::ObServer & server, common::FIFOPacket * packet,
          common::ObScanner & result);
      // get packet
      int fetch_query(const common::ObServer & sever, const int64_t timeout, QueryInfo & query);
    private:
      /// check inner stat
      bool check_inner_stat(void) const;
      int get_rpc_buffer(common::ObDataBuffer & data_buffer) const;
      virtual int send_request(const common::ObServer & server, common::Request * request,
          common::ObScanner & result);
    private:
      const common::ObClientManager * frame_;
      const common::ThreadSpecificBuffer * buffer_;
    };

    inline bool LoadRpc::check_inner_stat(void) const
    {
      return ((NULL != frame_) && (NULL != buffer_));
    }
  }
}


#endif // LOAD_RPC_H_

