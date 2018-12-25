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

#include "ob_ups_lease_task.h"
#include "common/ob_rs_ups_message.h"
using namespace oceanbase;
using namespace updateserver;

ObUpsLeaseTask::ObUpsLeaseTask()
{
  role_mgr_ = NULL;
  rpc_stub_ = NULL;
  self_lease_ = 0;
}

ObUpsLeaseTask::~ObUpsLeaseTask()
{
}
int ObUpsLeaseTask::init(common::ObRoleMgr *role_mgr, ObUpsRpcStub * rpc_stub, common::ObServer &root_server, ObUpsLogMgr *log_mgr, const int64_t inner_port, common::ObServer &self_server, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  self_lease_ = 0;
  if (NULL == role_mgr || NULL == rpc_stub || NULL == log_mgr)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument. role_mgr=%p", role_mgr);
  }
  else
  {
    role_mgr_ = role_mgr;
    rpc_stub_ = rpc_stub;
    log_mgr_ = log_mgr;
    root_server_.set_ipv4_addr(root_server.get_ipv4(), root_server.get_port());
    inner_port_ = inner_port;
    self_addr_.set_ipv4_addr(self_server.get_ipv4(), self_server.get_port());
    timeout_us_ = timeout_us;
  }
  return err;
}

void ObUpsLeaseTask::revoke_lease(int64_t lease_time)
{
  self_lease_ = lease_time;
}
void ObUpsLeaseTask::update_lease(int64_t lease_time)
{
  int64_t cur_time = tbsys::CTimeUtil::getTime();
  if (lease_time < cur_time)
  {
    TBSYS_LOG(WARN, "invalid lease. lease_time=%ld, cur_time=%ld", lease_time, cur_time);
  }
  else
  {
    self_lease_ = lease_time;
  }
}

void ObUpsLeaseTask::set_renew_time(const int64_t lease_renew_time)
{
  lease_renew_time_ = lease_renew_time;
}
void ObUpsLeaseTask::runTimerTask(void)
{
  int64_t cur_time_us = tbsys::CTimeUtil::getTime();
  //send keep_alive msg to rootserver
  int err = OB_SUCCESS;
  common::ObMsgUpsRegister msg_register;
  msg_register.log_seq_num_ = log_mgr_->get_cur_log_seq();
  msg_register.inner_port_ = static_cast<int32_t>(inner_port_);
  msg_register.addr_.set_ipv4_addr(self_addr_.get_ipv4(), self_addr_.get_port());
  int64_t lease_renew_time = 0;
  if (ObRoleMgr::SLAVE == role_mgr_->get_role())
  {
    msg_register.lease_ = 0;
  }
  else
  {
    msg_register.lease_ = self_lease_;
  }
  int64_t cluster_id = 0;
  err = rpc_stub_->ups_register(root_server_, msg_register, timeout_us_, cluster_id, lease_renew_time);
  if(OB_SUCCESS != err && OB_ALREADY_REGISTERED != err)
  {
    TBSYS_LOG(WARN, "fail to send keep_alive_msg to rootserver, err=%d", err);
  }
  else
  {
    TBSYS_LOG(DEBUG, "ups send keep alive to rootserver. time=%ld", cur_time_us);
  }

  //check lease
  if (0 != self_lease_ && self_lease_ < cur_time_us)
  {
    TBSYS_LOG(ERROR, "lease timeout, should reregister to rootserver, lease_time:%ld, cur_time:%ld", self_lease_, cur_time_us);
    role_mgr_->set_state(ObRoleMgr::INIT);
    self_lease_ = 0;
  }
  //retry to send response to rootserver
  else if (0 != self_lease_ && self_lease_ - cur_time_us < lease_renew_time_)
  {
    TBSYS_LOG(WARN, "lease will be timeout, retry to send renew lease to rootserver. lease_time:%ld, cur_time:%ld, remain lease[%ld] should big than %ld", self_lease_, cur_time_us, self_lease_ - cur_time_us, lease_renew_time_);
    err = rpc_stub_->renew_lease(root_server_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to send renew_lease to rootserver. err=%d", err);
    }
  }
}
