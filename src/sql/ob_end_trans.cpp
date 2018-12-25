/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_end_strans.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_end_trans.h"
#include "ob_physical_plan.h"
#include "ob_result_set.h"
#include "ob_sql_session_info.h"
#include "common/ob_common_stat.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObEndTrans::open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  req_.trans_id_ = my_phy_plan_->get_result_set()->get_session()->get_trans_id(); // get trans id at runtime to support prepare commit/rollback
  int64_t begin_time_us = tbsys::CTimeUtil::getTime();
  if (!req_.trans_id_.is_valid())
  {
    TBSYS_LOG(WARN, "session_id[%ld] not in transaction",session->get_session_id());
  }
  else if (OB_SUCCESS != (ret = rpc_->ups_end_trans(req_)))
  {
    TBSYS_LOG(WARN, "failed to end ups transaction, err=%d trans=%s",
              ret, to_cstring(req_));
    if (OB_TRANS_ROLLBACKED == ret)
    {
      TBSYS_LOG(USER_ERROR, "transaction is rolled back");
    }
    // reset transaction id
    ObTransID invalid_trans;
    session->set_trans_id(invalid_trans);
  }
  else
  {
    // reset transaction id
    ObTransID invalid_trans;
    session->set_trans_id(invalid_trans);
  }
  int64_t now = tbsys::CTimeUtil::getTime();
  OB_STAT_INC(OBMYSQL, SQL_END_TRANS_TIME, now - begin_time_us);
  if (!req_.rollback_)
  {
    OB_STAT_INC(OBMYSQL, SQL_COMMIT_COUNT);
  }
  else
  {
    OB_STAT_INC(OBMYSQL, SQL_ROLLBACK_COUNT);
  }
  if (0 < session->get_curr_trans_start_time())
  {
    int64_t trans_time = now - session->get_curr_trans_start_time();
    OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_TIME, trans_time);
    TBSYS_LOG(DEBUG, "trans finished, start=%ld trans=%ld", session->get_curr_trans_start_time(), trans_time);
    session->set_curr_trans_start_time(0);
  }
  FILL_TRACE_LOG("trans_id=%s err=%d", to_cstring(req_.trans_id_), ret);
  return ret;
}

int64_t ObEndTrans::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "EndTrans(trans_id=%s, rollback=%c)\n",
                  to_cstring(req_.trans_id_), req_.rollback_?'Y':'N');
  return pos;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObEndTrans, PHY_END_TRANS);
  }
}

PHY_OPERATOR_ASSIGN(ObEndTrans)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObEndTrans);
  rpc_ = NULL;
  req_ = o_ptr->req_;
  return ret;
}
