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

#ifndef OCEANBASE_COMMON_OB_CHECK_RUNNABLE_H_
#define OCEANBASE_COMMON_OB_CHECK_RUNNABLE_H_

#include "tbsys.h"
#include "ob_role_mgr.h"
#include "ob_define.h"
#include "ob_common_rpc_stub.h"

namespace oceanbase
{
  namespace common
  {
    class ObCheckRunnable : public tbsys::CDefaultRunnable
    {
    public:
      ObCheckRunnable();
      virtual ~ObCheckRunnable();

      virtual void run(tbsys::CThread* thread, void* arg);

      /// @brief 初始化
      /// @param [in] role_mgr role_mgr
      /// @param [in] vip vip address
      /// @param [in] check_period check period
      /// @param [in] lease_interval lease interval
      //  @param [in] rpc_stub rpc stub
      //  @param [in] master the master server addr
      //  @param [in] slave_addr the slave server addr
      int init(ObRoleMgr *role_mgr, const uint32_t vip, const int64_t check_period,
          ObCommonRpcStub* rpc_stub = NULL, ObServer* master = NULL, ObServer* slave_addr = NULL);

      // @brief  renew lease
      // @param [in] lease lease passed from Master
      int renew_lease(const ObLease& lease);

      // @brief set renew lease network timeout
      // @param [in] renew_lease_timeout renew lease network timeout
      void set_renew_lease_timeout(const int64_t renew_lease_timeout);

      // reset vip (for debug only)
      void reset_vip(const int32_t vip);

    protected:
      enum LeaseStatus
      {
        LEASE_NORMAL = 0,
        LEASE_SHOULD_RENEW = 1, // should renew lease
        LEASE_INVALID = 2,
      };

      // get lease status
      int64_t get_lease_status_();
      // renew lease
      int renew_lease_();

    protected:
      ObRoleMgr *role_mgr_;
      int64_t check_period_;
      uint32_t vip_;
      bool lease_on_;
      int64_t lease_interval_;
      int64_t renew_interval_;
      int64_t lease_time_;
      ObCommonRpcStub* rpc_stub_;
      ObServer* server_;
      ObServer* slave_addr_;
      int64_t renew_lease_timeout_;
    };
  } // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_COMMON_OB_CHECK_RUNNABLE_H_
