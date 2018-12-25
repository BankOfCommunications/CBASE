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

#include "ob_ups_fetch_lsync.h"


using namespace oceanbase::common;
using namespace oceanbase::updateserver;

ObUpsFetchLsync::ObUpsFetchLsync()
{
  rpc_stub_ = NULL;
  log_id_ = 0;
  log_seq_ = 0;
  is_initialized_ = false;
}

ObUpsFetchLsync::~ObUpsFetchLsync()
{
}

int ObUpsFetchLsync::init(const common::ObServer &lsync_server, const uint64_t log_id,
         const uint64_t log_seq, //ObUpsRpcStub *rpc_stub,
         ObUpsRpcStub *rpc_stub,
         ObCommitLogReceiver *clog_receiver, const int64_t fetch_timeout,
         common::ObRoleMgr *role_mgr)
{
  int ret = OB_SUCCESS;

  if (NULL == rpc_stub || NULL == clog_receiver)
  {
    TBSYS_LOG(WARN, "Parameters are invalid in ObUpsFetchLsync::init, log_id=%lu log_seq=%lu rpc_stub=%p clog_receiver=%p",
        log_id, log_seq, rpc_stub, clog_receiver);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    lsync_server_ = lsync_server;
    clog_receiver_ = clog_receiver;
    log_id_ = log_id;
    log_seq_ = log_seq;
    rpc_stub_ = rpc_stub;
    fetch_timeout_ = fetch_timeout;
    role_mgr_ = role_mgr;

    is_initialized_ = true;
  }

  return ret;
}

void ObUpsFetchLsync::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  int err = OB_SUCCESS;

  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsFetchLsync has not initialized");
    role_mgr_->set_state(ObRoleMgr::ERROR);
  }
  else
  {
    char* log_data = NULL;
    int64_t log_len = 0;
    while (!_stop && OB_SUCCESS == err)
    {
      TBSYS_LOG(DEBUG, "fetch lsync server: log_id_=%lu log_seq_=%lu", log_id_, log_seq_);

      err = rpc_stub_->fetch_lsync(get_lsync_server(), log_id_, log_seq_, log_data, log_len, fetch_timeout_ - TIMEOUT_DELTA);
      if (OB_SUCCESS != err && OB_RESPONSE_TIME_OUT != err && OB_NEED_RETRY != err && OB_NOT_REGISTERED != err)
      {
        TBSYS_LOG(ERROR, "fetch_lsync error, err=%d log_id_=%lu log_seq_=%lu log_data=%p log_len=%ld",
            err, log_id_, log_seq_, log_data, log_len);
        role_mgr_->set_state(ObRoleMgr::ERROR);
      }
      else if (OB_SUCCESS == err)
      {
        if (log_len > 0)
        {
          err = clog_receiver_->write_data(log_data, log_len, log_id_, log_seq_);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "ObCommitLogReceiver write_data error, err=%d log_data=%p log_len=%ld log_id_=%lu log_seq_=%lu",
                err, log_data, log_len, log_id_, log_seq_);
          }
          else
          {
            TBSYS_LOG(DEBUG, "write_data to log, log_len=%ld log_id_=%lu log_seq_=%lu", log_len, log_id_, log_seq_);
          }
        }
        else
        {
          TBSYS_LOG(INFO, "lsync returned 0 length log data");
        }
      }
      else if (OB_NOT_REGISTERED == err)
      {
        TBSYS_LOG(INFO, "The lsync server returned NOT_REGISTERED, go to register");
        role_mgr_->set_state(ObRoleMgr::INIT);
        while (ObRoleMgr::INIT == role_mgr_->get_state())
        {
          usleep(10000);
        }
        err = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "The lsync server returned err=%d", err);
        err = OB_SUCCESS;
      }
    }
  }

  TBSYS_LOG(INFO, "ObUpsFetchLsync finished, err=%d role_state=%s", err, role_mgr_->get_state_str());
}

