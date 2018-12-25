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

#include "ob_check_runnable.h"
#include "ob_lease_common.h"

using namespace oceanbase::common;

ObCheckRunnable::ObCheckRunnable()
{
  role_mgr_ = NULL;
  vip_ = 0;
  lease_on_ = false;
  lease_interval_ = 0;
  lease_time_ = 0;
  renew_interval_ = 0;
  rpc_stub_ = NULL;
  server_ = NULL;
  renew_lease_timeout_ = 0;
}

ObCheckRunnable::~ObCheckRunnable()
{
}

void ObCheckRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  int err = OB_SUCCESS;

  while (!_stop)
  {
    if (tbsys::CNetUtil::isLocalAddr(vip_))
    {
      if (ObRoleMgr::SLAVE == role_mgr_->get_role())
      {
        struct in_addr vip_addr;
        vip_addr.s_addr = vip_;
        TBSYS_LOG(INFO, "vip found on slave, vip=%s", inet_ntoa(vip_addr));
        int64_t status = LEASE_NORMAL;
        if (lease_on_)
        {
          status = get_lease_status_();
        }

        if (LEASE_INVALID != status)
        {
          role_mgr_->set_state(ObRoleMgr::SWITCHING);
        }
        else
        {
          TBSYS_LOG(ERROR, "vip found on slave, but slave lease is invalid, vip=%s", inet_ntoa(vip_addr));
        }
      }
    }
    else
    {
      if (ObRoleMgr::MASTER == role_mgr_->get_role())
      {
        struct in_addr vip_addr;
        vip_addr.s_addr = vip_;
        TBSYS_LOG(ERROR, "Incorrect vip, STOP server, vip=%s", inet_ntoa(vip_addr));
        role_mgr_->set_state(ObRoleMgr::STOP);
        stop();
      }
      else if (ObRoleMgr::SLAVE == role_mgr_->get_role() && lease_on_)
      {
        if (ObRoleMgr::ACTIVE == role_mgr_->get_state())
        {
          int64_t status = get_lease_status_();
          if (LEASE_INVALID == status)
          {
            TBSYS_LOG(ERROR, "Invalid lease, set server to INIT state");
            role_mgr_->set_state(ObRoleMgr::INIT); // set to init state, slave will re-register
          }
          else if (LEASE_SHOULD_RENEW == status)
          {
            err = renew_lease_();
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to renew_lease_, err=%d", err);
            }
          }
        }
      }
    }
    usleep(static_cast<useconds_t>(check_period_));
  }
}

int ObCheckRunnable::init(ObRoleMgr *role_mgr, const uint32_t vip, const int64_t check_period,
    ObCommonRpcStub* rpc_stub, ObServer* server, ObServer* slave_addr)
{
  int ret = OB_SUCCESS;

  if (NULL == role_mgr)
  {
    TBSYS_LOG(WARN, "Parameter is invalid[role_mgr=%p]", role_mgr);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    role_mgr_ = role_mgr;
    vip_ = vip;
    check_period_ = check_period;
    rpc_stub_ = rpc_stub;
    server_ = server;
    slave_addr_ = slave_addr;
  }

  return ret;
}

void ObCheckRunnable::reset_vip(const int32_t vip)
{
  vip_ = vip;
}

int64_t ObCheckRunnable::get_lease_status_()
{
  LeaseStatus status = LEASE_NORMAL;
  timeval time_val;
  gettimeofday(&time_val, NULL);
  int64_t cur_time_us = time_val.tv_sec * 1000 * 1000 + time_val.tv_usec;
  TBSYS_LOG(DEBUG, "lease_time_ = %ld, lease_interval_ =%ld, cur_time_us=%ld", lease_time_, lease_interval_, cur_time_us);
  if (lease_time_ + lease_interval_ < cur_time_us)
  {
    TBSYS_LOG(WARN, "Lease expired, lease_time_=%ld, lease_interval_=%ld, cur_time_us=%ld",
        lease_time_, lease_interval_, cur_time_us);
    status = LEASE_INVALID;
  }
  else if (lease_time_ + lease_interval_ < cur_time_us + renew_interval_)
  {
    TBSYS_LOG(DEBUG, "Lease will expire, lease_time_=%ld, lease_interval_=%ld, "
          "cur_time_us=%ld, renew_interval_=%ld", lease_time_, lease_interval_,
          cur_time_us, renew_interval_);
    status = LEASE_SHOULD_RENEW;
  }

  return status;
}

int ObCheckRunnable::renew_lease_()
{
  int err = OB_SUCCESS;

  if (NULL == rpc_stub_ || NULL == server_)
  {
    TBSYS_LOG(WARN, "invalid status, rpc_stub_=%p, server_=%p", rpc_stub_, server_);
    err = OB_ERROR;
  }
  else
  {
    err = rpc_stub_->renew_lease(*server_, *slave_addr_, renew_lease_timeout_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to renew lease, err=%d, timeout=%ld, server=%s", err, renew_lease_timeout_, server_->to_cstring());
    }
  }

  return err;
}

int ObCheckRunnable::renew_lease(const ObLease& lease)
{
  int err = OB_SUCCESS;

  lease_on_ = true;
  lease_time_ = lease.lease_time;
  lease_interval_ = lease.lease_interval;
  renew_interval_ = lease.renew_interval;

  TBSYS_LOG(DEBUG, "Renew lease, lease_time=%ld lease_interval=%ld renew_interval=%ld",
      lease_time_, lease_interval_, renew_interval_);

  return err;
}

void ObCheckRunnable::set_renew_lease_timeout(const int64_t renew_lease_timeout)
{
  renew_lease_timeout_ = renew_lease_timeout;
}

