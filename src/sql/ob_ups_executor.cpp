/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_executor.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_executor.h"
#include "common/utility.h"
#include "common/ob_trace_log.h"
#include "common/ob_common_stat.h"
#include "ob_bind_values.h"//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150424
#include "ob_fill_values.h"//add gaojt [Delete_Update_Function] [JHOBv0.1] 20151011
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObUpsExecutor::ObUpsExecutor()
  : rpc_(NULL), inner_plan_(NULL)
  ,insert_select_batch_num_(0)//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150423
  ,sub_query_num_(0),is_delete_update_(false),is_row_num_null_(false)//add gaojt [Delete_Update_Function] [JHOBv0.1] 20151008
{
}

ObUpsExecutor::~ObUpsExecutor()
{
  reset();
}

void ObUpsExecutor::reset()
{
  rpc_ = NULL;
  if (NULL != inner_plan_)
  {
    if (inner_plan_->is_cons_from_assign())
    {
      inner_plan_->clear();
      ObPhysicalPlan::free(inner_plan_);
    }
    else
    {
      inner_plan_->~ObPhysicalPlan();
    }
    inner_plan_ = NULL;
  }
  local_result_.clear();
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_desc_.reset();
}

void ObUpsExecutor::reuse()
{
  reset();
}

int ObUpsExecutor::open()
{
  int ret = OB_SUCCESS;
  // quiz: why there are two diffrent result sets?
  ObResultSet* outer_result_set = NULL;
  ObResultSet* my_result_set = NULL;
  ObSQLSessionInfo *session = NULL;
  ObBindValues* bind_values = NULL;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150424
  ObFillValues* fill_values = NULL;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160325
  if (rpc_ == NULL
    || inner_plan_ == NULL)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObUpsExecutor is not initiated well, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(my_phy_plan_);
    my_result_set = my_phy_plan_->get_result_set();
    outer_result_set = my_result_set->get_session()->get_current_result_set();
    my_result_set->set_session(outer_result_set->get_session()); // be careful!
    session = my_phy_plan_->get_result_set()->get_session();

    inner_plan_->set_result_set(my_result_set);
    inner_plan_->set_curr_frozen_version(my_phy_plan_->get_curr_frozen_version());
    local_result_.clear();
    // When read_only is enabled, the server permits no updates except for system tables.
    if (session->is_read_only() && my_phy_plan_->is_user_table_operation())
    {
      TBSYS_LOG(USER_ERROR, "The server is read only and no update is permitted. Ask your DBA for help.");
      ret = OB_ERR_READ_ONLY;
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    // 1. fetch static data
    ObPhyOperator* main_query = inner_plan_->get_main_query();
    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151013:b
    for (int32_t i = 0; i < inner_plan_->get_query_size(); ++i)
    {
        ObPhyOperator* aux_query = inner_plan_->get_phy_query(i);
        //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20160308:b
        if(NULL != (bind_values = dynamic_cast<ObBindValues*>(aux_query)))
        {
            break;
        }
        //add gaojt 20160308:e
        else if(NULL != (fill_values = dynamic_cast<ObFillValues*>(aux_query)))
        {
            break;
        }
    }
    //add gaojt 20151013:e
    for (int32_t i = 0; i < inner_plan_->get_query_size(); ++i)
    {
        ObPhyOperator* aux_query = inner_plan_->get_phy_query(i);
        if (aux_query != main_query)
      {
        TBSYS_LOG(DEBUG, "execute sub query %d", i);
        if (OB_SUCCESS != (ret = aux_query->open()))
        {
          TBSYS_LOG(WARN, "failed to execute sub-query, err=%d i=%d", ret, i);
          break;
        }
      }
    } // end for
    //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20160310:b
    if(NULL != bind_values)
    {
        if( 0 == bind_values->get_inserted_row_num())
        {
            is_row_num_null_ = true;
        }
    }
    //add gaojt 20160310:e
    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160325:b
    else if (NULL != fill_values)
    {
        if( fill_values->is_ud_non_row())
        {
            is_row_num_null_ = true;
        }
    }
    //add gaojt 20160325:e
  }
  // 2. send to ups
  bool start_new_trans = false;
  //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160308:b
  /*expr: fix the bug when there is not one inserting row in insert...select */
  //if (OB_LIKELY(OB_SUCCESS == ret))
  if (OB_LIKELY(OB_SUCCESS == ret) && !is_row_num_null_)
  //mod gaojt 20160308:e
  {
    start_new_trans = (!session->get_autocommit() && !session->get_trans_id().is_valid());
    inner_plan_->set_start_trans(start_new_trans);
    if (start_new_trans
        && (OB_SUCCESS != (ret = set_trans_params(session, inner_plan_->get_trans_req()))))
    {
      TBSYS_LOG(WARN, "failed to set params for transaction request, err=%d", ret);
    }
    else if (outer_result_set->is_with_rows()
        && OB_SUCCESS != (ret = make_fake_desc(outer_result_set->get_field_columns().count())))
    {
      TBSYS_LOG(WARN, "failed to get row descriptor, err=%d", ret);
    }
  }
  int64_t remain_us = 0;

  //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160308:b
  /*expr: fix the bug when there is not one inserting row in insert...select */
  //if (OB_LIKELY(OB_SUCCESS == ret))
  if (OB_LIKELY(OB_SUCCESS == ret) && !is_row_num_null_)
  //mod gaojt 20160308:e
  {
    int64_t begin_time_us = tbsys::CTimeUtil::getTime();
    if (my_phy_plan_->is_timeout(&remain_us))
    {
      ret = OB_PROCESS_TIMEOUT;
      TBSYS_LOG(WARN, "ups execute timeout. remain_us[%ld]", remain_us);
    }
    else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
    {
      TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
    }
    else if (OB_SUCCESS != (ret = rpc_->ups_plan_execute(remain_us, *inner_plan_, local_result_)))
    {
      int64_t elapsed_us = tbsys::CTimeUtil::getTime() - begin_time_us;
      OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_COUNT);
      OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_TIME, elapsed_us);

      TBSYS_LOG(WARN, "failed to execute plan on updateserver, err=%d", ret);
      if (OB_TRANS_ROLLBACKED == ret)
      {
        // when updateserver returning TRANS_ROLLBACKED, it cannot get local_result_ to fill error message
        TBSYS_LOG(USER_ERROR, "transaction is rolled back");
        // reset transaction id
        ObTransID invalid_trans;
        my_phy_plan_->get_result_set()->get_session()->set_trans_id(invalid_trans);
      }
    }
    else
    {
      int64_t elapsed_us = tbsys::CTimeUtil::getTime() - begin_time_us;
      OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_COUNT);
      OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_TIME, elapsed_us);

      ret = local_result_.get_error_code();
      if (start_new_trans && local_result_.get_trans_id().is_valid())
      {
        FILL_TRACE_LOG("ups_err=%d ret_trans_id=%s", ret, to_cstring(local_result_.get_trans_id()));
        session->set_trans_id(local_result_.get_trans_id());
        int64_t now = tbsys::CTimeUtil::getTime();
        session->set_curr_trans_start_time(now);
        OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_COUNT);
        OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_STMT_COUNT);
        TBSYS_LOG(DEBUG, "new trans start, start=%ld", now);
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ups execute plan failed, err=%d trans_id=%s",
                  ret, to_cstring(local_result_.get_trans_id()));
        if (OB_TRANS_ROLLBACKED == ret)
        {
          TBSYS_LOG(USER_ERROR, "transaction is rolled back");
          // reset transaction id
          ObTransID invalid_trans;
          my_phy_plan_->get_result_set()->get_session()->set_trans_id(invalid_trans);
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "affected_rows=%ld warning_count=%ld",
                  local_result_.get_affected_rows(), local_result_.get_warning_count());
        outer_result_set->set_affected_rows(local_result_.get_affected_rows());
        outer_result_set->set_warning_count(local_result_.get_warning_count());
        if (0 < local_result_.get_warning_count())
        {
          local_result_.reset_iter_warning();
          const char* warn_msg = NULL;
          while (NULL != (warn_msg = local_result_.get_next_warning()))
          {
            TBSYS_LOG(WARN, "updateserver warning: %s", warn_msg);
          }
        }
      }
    }
  }
  // add by maosy [Delete_Update_Function_isolation_RC] 20161228
  else if(OB_SUCCESS==ret && session->get_read_times () == EMPTY_ROW_CLEAR)
  {
      if (my_phy_plan_->is_timeout(&remain_us))
      {
        ret = OB_PROCESS_TIMEOUT;
        TBSYS_LOG(WARN, "ups execute timeout. remain_us[%ld]", remain_us);
      }
      else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
      {
          TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
      }
      else if (OB_ERR_BATCH_EMPTY_ROW == (ret = rpc_->ups_plan_execute(remain_us, *inner_plan_, local_result_)))
      {
          ret = OB_SUCCESS;
      }
      else
      {
         TBSYS_LOG(WARN,"empty packet to clear start time for batch is failed ,ret = %d",ret);
      }
  }
  // add e
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150424:b
  if(OB_SUCCESS == ret)
  {
      if(NULL != bind_values)
      {
          inserted_row_num_ = bind_values->get_inserted_row_num();
          insert_select_batch_num_++;
      }
  }
  //add gaojt 20150424:e
  // 3 close sub queries anyway
  if (OB_LIKELY(NULL != inner_plan_))
  {
    ObPhyOperator* main_query = inner_plan_->get_main_query();
    for (int32_t i = 0; i < inner_plan_->get_query_size(); ++i)
    {
        ObPhyOperator* aux_query = inner_plan_->get_phy_query(i);
      if (aux_query != main_query)
      {
        TBSYS_LOG(DEBUG, "close sub query %d", i);
        aux_query->close();
      }
    }
  }
  return ret;
}
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150423:b
void ObUpsExecutor::get_output_infor(int64_t& batch_num,int64_t& inserted_row_num)
{
    batch_num = insert_select_batch_num_;
    inserted_row_num = inserted_row_num_;
}
//add gaojt 20150423:e
int ObUpsExecutor::set_trans_params(ObSQLSessionInfo *session, common::ObTransReq &req)
{
  int ret = OB_SUCCESS;
  // get isolation level etc. from session
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
  else if (OB_SUCCESS != (ret = req.set_isolation_by_name(isolation_str)))
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
  else
  {
    req.timeout_ = tx_timeout_val;
    req.idle_time_ = tx_idle_timeout;
  }
  return ret;
}

int ObUpsExecutor::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(my_phy_plan_);
  // for session stored physical plan, my_phy_plan_->get_result_set() is the result_set who stored the plan,
  // since the two commands are in the same session, so we can get the real result_set from session.
  // for global stored physical plan, new coppied plan has itsown my_phy_plan_, so both my_phy_plan_->get_result_set()
  // and my_phy_plan_->get_result_set()->get_session()->get_current_result_set() are correct
  ObResultSet *my_result_set = my_phy_plan_->get_result_set()->get_session()->get_current_result_set();
  if (OB_UNLIKELY(!my_result_set->is_with_rows()))
  {
    ret = OB_NOT_SUPPORTED;
  }
  else if (OB_UNLIKELY(curr_row_.get_row_desc() == NULL))
  {
    curr_row_.set_row_desc(row_desc_);
  }
  if (ret == OB_SUCCESS
    && (ret = local_result_.get_scanner().get_next_row(curr_row_)) == OB_SUCCESS)
  {
    row = &curr_row_;
  }
  return ret;
}

int ObUpsExecutor::make_fake_desc(const int64_t column_num)
{
  int ret = OB_SUCCESS;
  row_desc_.reset();
  for (int64_t i = 0; ret == OB_SUCCESS && i < column_num; i++)
  {
    if ((ret = row_desc_.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID + i)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Generate row descriptor of ObUpsExecutor failed, err=%d", ret);
      break;
    }
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObUpsExecutor, PHY_UPS_EXECUTOR);
  }
}

int64_t ObUpsExecutor::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "UpsExecutor(ups_plan=");
  if (NULL != inner_plan_)
  {
    pos += inner_plan_->to_string(buf+pos, buf_len-pos);
  }
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}

PHY_OPERATOR_ASSIGN(ObUpsExecutor)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObUpsExecutor);
  reset();

  if (!my_phy_plan_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObPhysicalPlan/allocator is not set, ret=%d", ret);
  }
  else if ((inner_plan_ = ObPhysicalPlan::alloc()) == NULL)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "Can not generate new inner physical plan, ret=%d", ret);
  }
  else
  {
    // inner_plan_->set_allocator(NULL); // no longer need
    inner_plan_->set_result_set(my_phy_plan_->get_result_set());
    if ((ret = inner_plan_->assign(*o_ptr->inner_plan_)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Assign inner physical plan, ret=%d", ret);
    }
  }
  return ret;
}
