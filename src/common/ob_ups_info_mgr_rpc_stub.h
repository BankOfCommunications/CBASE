

/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_info_mgr_rpc_stub.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_UPS_INFO_MGR_RPC_STUB_H
#define _OB_UPS_INFO_MGR_RPC_STUB_H

#include "common/ob_client_manager.h"
#include "common/ob_define.h"
#include "common/ob_ups_info.h"
#include "common/ob_result.h"

namespace oceanbase
{
  namespace common
  {
    class ObUpsInfoMgrRpcStub
    {
    public:
      ObUpsInfoMgrRpcStub();
      virtual ~ObUpsInfoMgrRpcStub();

      int init(const common::ObClientManager * client_mgr);
      virtual int fetch_server_list(const int64_t timeout, const ObServer & root_server, ObUpsList & server_list) const;

    private:
      bool check_inner_stat(void) const;
      int get_rpc_buffer(ObDataBuffer & data_buffer) const;

    private:
      static const int32_t DEFAULT_VERSION = 1;

      ThreadSpecificBuffer thread_buffer_;
      const common::ObClientManager * client_mgr_;         // rpc frame for send request

    };
  }
}

#endif /* _OB_UPS_INFO_MGR_RPC_STUB_H */


