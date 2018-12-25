/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_LEASE_TASK_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_LEASE_TASK_H_

#include "common/ob_timer.h"
#include "ob_ups_rpc_stub.h"
#include "common/ob_role_mgr.h"
#include "ob_ups_log_mgr.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsLeaseTask : public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 100000L;
        ObUpsLeaseTask();
        ~ObUpsLeaseTask();
        int init(common::ObRoleMgr *role_mgr, ObUpsRpcStub * rpc_stub, common::ObServer &root_server, ObUpsLogMgr *log_mgr, const int64_t inner_port, common::ObServer &self_server, const int64_t timeout_us);
        void update_lease(int64_t lease_time);
        void revoke_lease(int64_t lease_time);
        void set_renew_time(const int64_t renew_lease_time);
        void runTimerTask(void);

        const static int64_t DEFAULT_LEASE_RETRY_TIME = 800 * 1000;
      private:
        common::ObRoleMgr *role_mgr_;
        ObUpsRpcStub *rpc_stub_;
        common::ObServer root_server_;
        ObUpsLogMgr *log_mgr_;
        int64_t self_lease_;
        int64_t inner_port_;
        common::ObServer self_addr_;
        int64_t timeout_us_;
        int64_t lease_renew_time_;
    };
  }
}
#endif 
