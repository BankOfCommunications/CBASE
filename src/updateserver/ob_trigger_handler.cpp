/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_trigger_handler.cpp,  12/13/2012 11:15:06 AM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_trigger_msg.h"
#include "ob_ups_rpc_stub.h"
#include "ob_ups_role_mgr.h"
#include "ob_trigger_handler.h"
#include "ob_sessionctx_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

ObTriggerCallback::ObTriggerCallback()
  : handler_(NULL), init_(false)
{
}

int ObTriggerCallback::init(ObTriggerHandler * handler)
{
  int ret = OB_SUCCESS;
  if (init_)
  {
    TBSYS_LOG(ERROR, "ObTriggerCallback %p has been initialized", this);
    ret = OB_INIT_TWICE;
  }
  else if (NULL == handler)
  {
    TBSYS_LOG(ERROR, "Parameter is invalid, handler is NULL");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    handler_ = handler;
    init_ = true;
  }
  return ret;
}

int ObTriggerCallback::cb_func(const bool rollback, void *data,
    BaseSessionCtx &session)
{
  UNUSED(data);
  int ret = OB_SUCCESS;
  bool need_to_trigger = true;
  if (!rollback)
  {
    RWSessionCtx * session_ctx  = dynamic_cast<RWSessionCtx *>(&session);
    if (handler_ != NULL && session_ctx != NULL)
    {
      if (ST_REPLAY == session_ctx->get_type())
      {
        RPSessionCtx* rpsession_ctx = dynamic_cast<RPSessionCtx*>(&session);
        if (NULL != rpsession_ctx && rpsession_ctx->is_replay_local_log())
        {
          need_to_trigger = false;
        }
      }
      if (need_to_trigger
          && OB_SUCCESS !=
          (ret = handler_->handle_trigger(session_ctx->get_ups_mutator())))
      {
        TBSYS_LOG(ERROR, "notify the trigger handler failed. ret=%d", ret);
      }
    }
  }
  return ret;
}

ObTriggerHandler::ObTriggerHandler()
{
}

ObTriggerHandler::~ObTriggerHandler()
{
}

int ObTriggerHandler::init(common::ObServer &rootserver, ObUpsRpcStub *rpc_stub, ObUpsRoleMgr *role_mgr)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(INFO, "init trigger");

  rootserver_ = rootserver;
  if (NULL == (rpc_stub_ = rpc_stub) ||
      NULL == (role_mgr_ = role_mgr))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "init trigger handler failed. invalid arguments. rpc_stub=%p, role_mgr=%p", rpc_stub, role_mgr);
  }
  else if (OB_SUCCESS != (err = callback_.init(this)))
  {
    TBSYS_LOG(ERROR, "ObTriggerCallback init error, err = %d", err);
  }
  return err;
}

int ObTriggerHandler::handle_trigger(ObUpsMutator &mutator) const
{
  int err = OB_SUCCESS;
  common::ObMutatorCellInfo* cell = NULL;

  if (ObUpsRoleMgr::MASTER == role_mgr_->get_role())
    // && ObUpsRoleMgr::ACTIVE == role_mgr_->get_state()) slave-master maybe not-sync state
  {
    mutator.reset_iter();
    if (OB_SUCCESS == err)
    {
      if (OB_SUCCESS != (err = mutator.next_cell()))
      {
        // nop
      }
      else if (OB_SUCCESS != (mutator.get_cell(&cell)))
      {
        TBSYS_LOG(WARN, "fail to get cell. err=%d", err);
      }
      else if (NULL != cell)
      {
        // TBSYS_LOG(INFO, "GOT a trigger event on table %lu, OB_TRIGGER_EVENT_TABLE_TID=%lu",
        //    cell->cell_info.table_id_, OB_TRIGGER_EVENT_TID);
        if (OB_TRIGGER_EVENT_TID == cell->cell_info.table_id_)
        {
          ObTriggerMsg msg;
          // got a trigger event
          TBSYS_LOG(INFO, "GOT a trigger event, check if msg valid");
          // src
          if (OB_SUCCESS != (err = cell->cell_info.value_.get_varchar(msg.src)))
          {
            TBSYS_LOG(WARN, "fail to get trigger src. err=%d", err);
          }
          else if (OB_SUCCESS != (err = next_value_(mutator, msg.type)))
          {
            TBSYS_LOG(WARN, "fail to get trigger type. err=%d", err);
            if (OB_ITER_END == err)
            {
              err = OB_ITEM_NOT_SETTED;
            }
          }
          else if (OB_SUCCESS != (err = next_value_(mutator, msg.param)))
          {
            TBSYS_LOG(WARN, "fail to get trigger param. err=%d", err);
            if (OB_ITER_END == err)
            {
              err = OB_ITEM_NOT_SETTED;
            }
          }
          else
          {
            err = handle_trigger(msg);
          }
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        else if (OB_DDL_OPERATION_TID == cell->cell_info.table_id_)
        {
          ObDdlTriggerMsg msg;
          TBSYS_LOG(INFO, "GOT a ddl trigger event, check if msg valid");

          if (cell->cell_info.row_key_.get_obj_cnt() == 1 )
          {
            err = cell->cell_info.row_key_.get_obj_ptr()->get_int(msg.version);
            if (err != OB_SUCCESS)
            {
              err = OB_ITEM_NOT_SETTED;
              TBSYS_LOG(WARN, "fail to get ddl trigger version. err=%d", err);
            }
            else if (OB_SUCCESS != (err = cell->cell_info.value_.get_int(msg.param)))
            {
              TBSYS_LOG(WARN, "fail to get trigger src. err=%d", err);
            }
            else if (OB_SUCCESS != (err = next_value_(mutator, msg.type)))
            {
              TBSYS_LOG(WARN, "fail to get trigger type. err=%d", err);
              if (OB_ITER_END == err)
              {
                err = OB_ITEM_NOT_SETTED;
              }
            }
            else
            {
              TBSYS_LOG(INFO, "succeed to get ddl trigger version. version=%ld, param =%ld", msg.version, msg.param);
              err = handle_trigger(msg);
            }
          }
          else
          {
            err = OB_INNER_STAT_ERROR;
            TBSYS_LOG(WARN, "fail to get ddl trigger version, rowkey count[%ld]. err=%d", cell->cell_info.row_key_.get_obj_cnt(),err);
          }
        }
        //add:e
      }

      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      mutator.reset_iter();
    }
  }
  return err;
}

ObTriggerCallback * ObTriggerHandler::get_trigger_callback()
{
  return &callback_;
}

int ObTriggerHandler::next_value_(ObUpsMutator &mutator, int64_t &value) const
{
  int err = OB_SUCCESS;
  common::ObMutatorCellInfo* cell = NULL;
  if (OB_SUCCESS != (err = mutator.next_cell()))
  {
    if (OB_ITER_END != err)
    {
      TBSYS_LOG(WARN, "fail to move to next cell. err=%d", err);
    }
  }
  else if (OB_SUCCESS != (err = mutator.get_cell(&cell)))
  {
    TBSYS_LOG(WARN, "fail to get cell. err=%d", err);
  }
  else if (OB_SUCCESS != (err = cell->cell_info.value_.get_int(value)))
  {
    TBSYS_LOG(WARN, "fail to get trigger src. err=%d", err);
  }
  return err;
}

int ObTriggerHandler::handle_trigger(ObTriggerMsg &msg) const
{
  TBSYS_LOG(INFO, "[TRIGGER][handle_trigger]send TriggerMsg to Rootserver. msg(src:%.*s, type:%ld, param:%ld)",
      msg.src.length(), msg.src.ptr(), msg.type, msg.param);
  return rpc_stub_->notify_rs_trigger_event(rootserver_, msg, RPC_TIMEOUT);
}

//add zhaoqiong [Schema Manager] 20150327:b
int ObTriggerHandler::handle_trigger(ObDdlTriggerMsg &msg) const
{
  TBSYS_LOG(INFO, "[TRIGGER][handle_trigger]send Ddl TriggerMsg to Rootserver. msg(version:%ld, type:%ld, param:%ld)",
      msg.version,msg.type, msg.param);
  return rpc_stub_->notify_rs_ddl_trigger_event(rootserver_, msg, RPC_TIMEOUT);
}
 //add:e

