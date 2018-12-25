/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_prepare.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_prepare.h"
#include "ob_result_set.h"
#include "ob_physical_plan.h"
#include "parse_malloc.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObPrepare::ObPrepare()
{
}

ObPrepare::~ObPrepare()
{
}

int ObPrepare::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObPrepare::open()
{
  int ret = OB_SUCCESS;
  if (stmt_name_.length() <= 0 || child_op_ == NULL)
  {
    ret = OB_ERR_GEN_PLAN;
    TBSYS_LOG(WARN, "Prepare statement is not initiated");
  }
  else
  {
    // add child plan to session
    ret = store_phy_plan_to_session();
  }
  return ret;
}

int ObPrepare::close()
{
  return OB_SUCCESS;
}

int ObPrepare::store_phy_plan_to_session()
{
  int ret = OB_SUCCESS;
  ObPhyOperator* old_main_query = my_phy_plan_->get_main_query();
  my_phy_plan_->set_main_query(child_op_);
  my_phy_plan_->remove_phy_query(old_main_query);
  TBSYS_LOG(DEBUG, "prepare main query=%p", child_op_);
  ObResultSet *result_set = my_phy_plan_->get_result_set();
  ObSQLSessionInfo *session = result_set->get_session();
  if ((ret = session->store_plan(stmt_name_, *result_set)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Store current result failed.");
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObPrepare, PHY_PREPARE);
  }
}

int64_t ObPrepare::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Prepare(stmt_name=%.*s)\n", stmt_name_.length(), stmt_name_.ptr());
  return pos;
}
