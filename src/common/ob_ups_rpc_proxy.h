/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_rpc_proxy.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_UPS_RPC_PROXY_H
#define _OB_UPS_RPC_PROXY_H 1

#include "ob_scanner.h"
#include "ob_ups_info.h"

namespace oceanbase
{
  namespace common
  {
    class ObUpsRpcProxy
    {
      public:
        virtual ~ObUpsRpcProxy() {}
      public:
        // get data from update server
        // param  @get_param get param
        //        @scanner return result
        virtual int ups_get(const common::ObGetParam & get_param, 
                            common::ObScanner & scanner,
                            const common::ObServerType server_type = common::MERGE_SERVER,
                            const int64_t time_out = 0) = 0;

        // scan data from update server
        // param  @scan_param get param
        //        @scanner return result
        virtual int ups_scan(const common::ObScanParam & scan_param, 
                             common::ObScanner & scanner,
                             const common::ObServerType server_type = common::MERGE_SERVER,
                             const int64_t time_out = 0) = 0;

    };
  }
}

#endif /* _OB_UPS_RPC_PROXY_H */

