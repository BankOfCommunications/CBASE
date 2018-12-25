/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_deallocate.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_deallocate.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDeallocate::ObDeallocate()
  :stmt_id_(OB_INVALID_ID)
{
}

ObDeallocate::~ObDeallocate()
{
}

void ObDeallocate::reset()
{
  stmt_id_ = OB_INVALID_ID;
}

void ObDeallocate::reuse()
{
  stmt_id_ = OB_INVALID_ID;
}


int ObDeallocate::open()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == stmt_id_)
  {
    ret = OB_ERR_GEN_PLAN;
    TBSYS_LOG(WARN, "Deallocate statement is not initiated, stmt_id=%lu", stmt_id_);
  }
  else
  {
    // add child plan to session
    ret = delete_plan_from_session();
  }
  return ret;
}

int ObDeallocate::close()
{
  return OB_SUCCESS;
}

int ObDeallocate::delete_plan_from_session()
{
  int ret = OB_SUCCESS;
  if ((ret = my_phy_plan_->get_result_set()->get_session()->remove_plan(stmt_id_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "remove statment from session failed. ret=%d", ret);
  }
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160426:b
  /*
   * remove the plan related with ud anyway.
  */
  {
      uint64_t stmt_id;
      char t_array[stmt_name_.length()+2];

      memset(t_array, 0, sizeof(t_array));
      sprintf(t_array,"%.*s",stmt_name_.length(),stmt_name_.ptr());
      t_array[stmt_name_.length()]='.';

      ObString stmt_name = ObString::make_string(t_array);

      TBSYS_LOG(DEBUG,"gaojt-test-ud: stmt-name=%.*s",stmt_name.length(),stmt_name.ptr());

      if (my_phy_plan_->get_result_set()->get_session()->plan_exists(stmt_name, &stmt_id) != false)
      {
          if (OB_SUCCESS != (ret = my_phy_plan_->get_result_set()->get_session()->remove_plan(stmt_id)))
          {
              TBSYS_LOG(DEBUG,"fail to remove plan, ret=%d",ret);
          }
          else
          {
              TBSYS_LOG(DEBUG,"Deallocate-ud-success");
          }
      }
  }
  //add gaojt 20160426:e
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObDeallocate, PHY_DEALLOCATE);
  }
}

int64_t ObDeallocate::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Deallocate Prepare(stmt_id=%lu)\n", stmt_id_);
  return pos;
}
