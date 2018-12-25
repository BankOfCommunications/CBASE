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
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "ob_ups_check_runnable.h"
#include "ob_update_server_main.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    ObUpsCheckRunnable::ObUpsCheckRunnable()
    {
    }

    ObUpsCheckRunnable::~ObUpsCheckRunnable()
    {
    }

    int ObUpsCheckRunnable::init(ObiRole *obi_role, ObRoleMgr *role_mgr,
        const int64_t check_period, ObCommonRpcStub* rpc_stub,                             
        ObServer* master, ObServer* slave_addr)
    {
      int err = OB_SUCCESS;
      if (NULL == obi_role)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argumeng, obi_role =%p", obi_role);
      }
      else
      {
        uint32_t vip = 0;
        obi_role_ = obi_role;
        err = common::ObCheckRunnable::init(role_mgr, vip, check_period, rpc_stub, master, slave_addr);
      }
      return err;
    }

    int ObUpsCheckRunnable::set_master_addr(ObServer *master)
    {
      int err = OB_SUCCESS;
      if (NULL == master)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument. master add=%p", master);
      }
      else
      {
        server_ = master;
      }
      return err;
    }

    void ObUpsCheckRunnable::run(tbsys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "start to check ups inner lease");

      while (!_stop)
      {
        if (ObiRole::MASTER == obi_role_->get_role()
            && ObRoleMgr::MASTER == role_mgr_->get_role())
        {
          //donothing
        }
        else
        {
          if (lease_on_ && (ObRoleMgr::ACTIVE == role_mgr_->get_state()
                || ObRoleMgr::NOTSYNC == role_mgr_->get_state()))
          {
            int64_t status = get_lease_status_();
            ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
            if (NULL == ups_main)                             
            {
              TBSYS_LOG(WARN, "get ups main fail");           
              err = OB_ERROR;
            }
            else
            {
              if (LEASE_INVALID == status)
              {
                TBSYS_LOG(ERROR, "ups slave invalid lease, set server to INIT state");
                ups_main->get_update_server().set_ups_inner_lease_fail();
                role_mgr_->set_state(ObRoleMgr::INIT);
              }
              else if (LEASE_SHOULD_RENEW == status)
              {
                err = renew_lease_();
                if (ObiRole::SLAVE == obi_role_->get_role()
                    && ObRoleMgr::MASTER == role_mgr_->get_role()
                    && OB_NOT_MASTER == err)
                {
                  TBSYS_LOG(INFO,  "master inst has changed the master ups.");
                  ups_main->get_update_server().set_ups_inner_lease_fail();
                  role_mgr_->set_state(ObRoleMgr::INIT);
                  //err = ups_main->get_update_server().renew_master_inst_ups();
                }
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN, "fail to renew lease. err=%d", err);
                }
              }
            }
          }
        }
        usleep(static_cast<useconds_t>(check_period_));
      }
    }
  }
}
