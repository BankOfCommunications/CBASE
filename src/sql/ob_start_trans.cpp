/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_start_trans.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_start_trans.h"
#include "common/utility.h"
#include "common/ob_common_stat.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObStartTrans::open()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(rpc_);
  common::ObTransID trans_id;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  // get isolation level from session
  ObObj val;
  ObString isolation_str;
  int64_t tx_timeout_val = 0;
  int64_t tx_idle_timeout = 0;
  if (OB_SUCCESS != (ret = session->get_sys_variable_value(ObString::make_string("tx_isolation"), val)))
  {
    TBSYS_LOG(WARN, "failed to get tx_isolation value, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = val.get_varchar(isolation_str)))
  {
    TBSYS_LOG(WARN, "wrong obj type, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (ret = req_.set_isolation_by_name(isolation_str)))
  {
    TBSYS_LOG(WARN, "failed to set isolation level, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (ret = session->get_sys_variable_value(ObString::make_string("ob_tx_timeout"), val)))
  {
    TBSYS_LOG(WARN, "failed to get tx_timeout value, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = val.get_int(tx_timeout_val)))
  {
    TBSYS_LOG(WARN, "wrong obj type, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (ret = session->get_sys_variable_value(ObString::make_string("ob_tx_idle_timeout"), val)))
  {
    TBSYS_LOG(WARN, "failed to get tx_idle_timeout value, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = val.get_int(tx_idle_timeout)))
  {
    TBSYS_LOG(WARN, "wrong obj type, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if (session->get_trans_id().is_valid())
  {
    TBSYS_LOG(USER_ERROR, "already in transaction");
    // no update trans id, keep last trans id
    ret = OB_ERR_TRANS_ALREADY_STARTED;
  }
  else
  {
    req_.timeout_ = tx_timeout_val;
    req_.idle_time_ = tx_idle_timeout;
    if (OB_SUCCESS != (ret = rpc_->ups_start_trans(req_, trans_id)))
    {
      TBSYS_LOG(WARN, "ups start transaction failed, err=%d", ret);
    }
    else
    {
      session->set_trans_id(trans_id);
      session->set_curr_trans_start_time(tbsys::CTimeUtil::getTime());
      OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_COUNT);
      OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_STMT_COUNT);
    }
    TBSYS_LOG(DEBUG, "start transaction, ret=%d req=%s trans_id=%s ret=%d",
        ret, to_cstring(req_), to_cstring(trans_id), ret);
  }
  return ret;
}

PHY_OPERATOR_ASSIGN(ObStartTrans)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObStartTrans);
  rpc_ = NULL;
  req_ = o_ptr->req_;
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObStartTrans, PHY_START_TRANS);
  }
}
