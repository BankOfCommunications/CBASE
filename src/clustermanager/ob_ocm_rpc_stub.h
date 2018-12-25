/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ocm_info_manager.h,v 0.1 2010/09/28 13:39:26 rongxuan Exp $
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef _OB_OCM_RPC_STUB_H
#define _OB_OCM_RPC_STUB_H

#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "ob_ocm_meta.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace clustermanager
  {
    class ObOcmRpcStub
    {
      public:
        ObOcmRpcStub();
        ~ObOcmRpcStub();
        bool check_inner_stat() const;
        int init(common::ThreadSpecificBuffer *rpc_buffer, common::ObClientManager *rpc_frame);
        int broad_cast_ocm_info(const int64_t timeout, const ObServer& ocm_server, const ObOcmMeta& ocm_meta) const;
        int get_rpc_buffer(common::ObDataBuffer &data_buffer) const;
      private:
        bool init_;
        const common::ThreadSpecificBuffer *rpc_buffer_;
        const common::ObClientManager *rpc_frame_;
    };
  }
}

#endif
