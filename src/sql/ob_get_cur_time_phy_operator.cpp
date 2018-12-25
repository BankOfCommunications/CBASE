/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_get_cur_time_phy_operator.cpp
 *
 * Authors:
 *  yongle.xh <yongle.xh@alipay.com>
 *
 */
#include "ob_get_cur_time_phy_operator.h"
#include "ob_ups_executor.h"
#include "common/utility.h"
#include "common/ob_trace_log.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObGetCurTimePhyOperator::~ObGetCurTimePhyOperator()
{
  reset();
}

void ObGetCurTimePhyOperator::reset()
{
  rpc_ = NULL;
  if (NULL != ups_plan_)
  {
    if (ups_plan_->is_cons_from_assign())
    {
      ups_plan_->clear();
      ObPhysicalPlan::free(ups_plan_);
    }
    else
    {
      ups_plan_->~ObPhysicalPlan();
    }
    ups_plan_ = NULL;
  }
  rpc_ = NULL;
  //mod liuzy [datetime func] 20150909:b
  /*Exp: 将cur_time_fun_type_修改为ObArray<ObCurTimeType>类型*/
//  cur_time_fun_type_ = NO_CUR_TIME;
  cur_time_fun_type_.clear();
  //mod 20150909:e
}

int ObGetCurTimePhyOperator::open()
{
  int ret = OB_SUCCESS;
  //add liuzy [datetime func] 20150909:b
  for (int64_t idx = 0, size = cur_time_fun_type_.count(); idx < size; ++idx)
  {
    TBSYS_LOG(DEBUG, "open: idx = [%ld], size = [%ld]", idx, size);
  //add 20150909:e
    if (CUR_TIME_UPS == cur_time_fun_type_.at(idx))
    {
      ret = get_cur_time_from_ups();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to get_cur_time_from_ups, ret=%d", ret);
      }
    }
    //add wuna [datetime func] 20150902:b
    else if (CUR_DATE == cur_time_fun_type_.at(idx))
    {
      TBSYS_LOG(DEBUG, "CUR_DATE, cur_time_fun_type_=%d", cur_time_fun_type_.at(idx));
      ret = get_cur_time(CUR_DATE);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to get_cur_time_from_date, ret=%d", ret);
      }
    }
    //add 20150902:e
    //add liuzy [datetime func] 20150907:b
    else if (CUR_TIME_HMS == cur_time_fun_type_.at(idx))
    {
      TBSYS_LOG(DEBUG, "CUR_TIME_HMS, cur_time_fun_type_=%d", cur_time_fun_type_.at(idx));
      ret = get_cur_time(CUR_TIME_HMS);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to get_cur_time_from_time, ret=%d", ret);
      }
    }
    //add 20150907:e
    else
    {
      ret = get_cur_time();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to get_cur_time, ret=%d", ret);
      }
    }
  }//add 20150909:e /*Exp: end for*/
  return ret;
}

int ObGetCurTimePhyOperator::get_cur_time_from_ups()
{
  int ret = OB_SUCCESS;
  ObUpsResult ups_result;
  ObResultSet *my_result_set = NULL;
  ObResultSet *outer_result_set = NULL;
  if (NULL == rpc_ || NULL == ups_plan_ || NULL == my_phy_plan_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "rpc_=%p, ups_plan_=%p and my_phy_plan_=%p must not be NULL",
      rpc_, ups_plan_, my_phy_plan_);
  }

  // 0. set result set of ups plan
  if (OB_SUCCESS == ret)
  {
    my_result_set = my_phy_plan_->get_result_set();
    outer_result_set = my_result_set->get_session()->get_current_result_set();
    my_result_set->set_session(outer_result_set->get_session()); // be careful!

    ups_plan_->set_result_set(my_result_set);
    ups_plan_->set_curr_frozen_version(my_phy_plan_->get_curr_frozen_version());
  }

  // 1. send to ups
  int64_t remain_us = 0;
  if (OB_SUCCESS == ret)
  {
    if (my_phy_plan_->is_timeout(&remain_us))
    {
      ret = OB_PROCESS_TIMEOUT;
      TBSYS_LOG(WARN, "ups execute timeout. remain_us[%ld]", remain_us);
    }
    else if (OB_SUCCESS != (ret = rpc_->ups_plan_execute(remain_us, *ups_plan_, ups_result)))
    {
      TBSYS_LOG(WARN, "failed to execute plan on updateserver, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ups_result.get_error_code()))
    {
      TBSYS_LOG(WARN, "execute ups plan on updateserver exit with error code: %d", ret);
    }
  }
  // 2. set result set
  if (OB_SUCCESS == ret)
  {
    ObNewScanner &new_scanner = ups_result.get_scanner();
    ObRowDesc row_desc;
    ObRow row;
    if (OB_SUCCESS != (ret = row_desc.add_column_desc(OB_INVALID_ID, OB_MAX_TMP_COLUMN_ID)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, ret=%d", ret);
    }
    else
    {
      common::ObObj *cur_time;
      row.set_row_desc(row_desc);
      if (OB_SUCCESS != (ret = new_scanner.get_next_row(row)))
      {
        TBSYS_LOG(WARN, "ups plan should contain current_time row, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row.get_cell(OB_INVALID_ID, OB_MAX_TMP_COLUMN_ID, cur_time)))
      {
        TBSYS_LOG(WARN, "failed to get curren time cell:ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = outer_result_set->set_cur_time(*cur_time)))
      {
        TBSYS_LOG(WARN, "failed to set cur_time_ups of result_set:cur_time: %s ret=%d", to_cstring(*cur_time), ret);
      }
    }
  }
  return ret;
}
//mod liuzy [datetime func] 20150902:e
//int ObGetCurTimePhyOperator::get_cur_time()
int ObGetCurTimePhyOperator::get_cur_time(ObCurTimeType type)
//mod 20150902:b
{
  int ret = OB_SUCCESS;
  common::ObObj cur_time;
  ObResultSet *my_result_set = NULL;
  ObResultSet *outer_result_set = NULL;

  if (NULL == my_phy_plan_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "my_phy_plan_ must not null");
  }

  if (OB_SUCCESS == ret)
  {
    my_result_set = my_phy_plan_->get_result_set();
    outer_result_set = my_result_set->get_session()->get_current_result_set();
  }
  //add liuzy [datetime func] 20150902:b
  if (CUR_TIME_HMS == type)
  {
    time_t t = static_cast<time_t>(tbsys::CTimeUtil::getTime()/1000000L);//seconds
    struct tm gtm;
    localtime_r(&t, &gtm);
    int64_t time = (gtm.tm_hour * 3600L + gtm.tm_min * 60L + gtm.tm_sec) * 1000000L;
    cur_time.set_time(time);
  }
  //add wuna [datetime func] 20150902:b
  else if (CUR_DATE == type)
  {
    time_t t = static_cast<time_t>(tbsys::CTimeUtil::getTime()/1000000L);//seconds
    struct tm gtm;
    localtime_r(&t, &gtm);
    int64_t date = (t - gtm.tm_hour*3600L - gtm.tm_min*60L - gtm.tm_sec)*1000000L;
    cur_time.set_date(date);//to change
  }
  //add 20150902:e
  else
  {
  //add 20150902:e
    cur_time.set_precise_datetime(tbsys::CTimeUtil::getTime());
  }

  if(OB_SUCCESS != (ret = outer_result_set->set_cur_time(cur_time)))
  {
    TBSYS_LOG(WARN, "failed to set cur_time of result_set:cur_time: %s ret=%d", to_cstring(cur_time), ret);
  }
  return ret;
}

void ObGetCurTimePhyOperator::reuse()
{
  reset();
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObGetCurTimePhyOperator, PHY_CUR_TIME);
  }
}

int64_t ObGetCurTimePhyOperator::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObGetCurTimePhyOperator()\n");
  if (NULL != ups_plan_)
  {
    databuff_printf(buf, buf_len, pos, "==ups_plan_:\n");
    pos += ups_plan_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObGetCurTimePhyOperator)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObGetCurTimePhyOperator);
  reset();
  if (NULL != o_ptr->ups_plan_)
  {
    if ((ups_plan_ = ObPhysicalPlan::alloc()) == NULL)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "Can not generate new ups physical plan, ret=%d", ret);
    }
    else if ((ret = ups_plan_->assign(*o_ptr->ups_plan_)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Assign ups physical plan, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    rpc_ = NULL;
    cur_time_fun_type_ = o_ptr->cur_time_fun_type_;
  }
  return ret;
}

