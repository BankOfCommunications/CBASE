/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_result_set.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_result_set.h"
#include "sql/ob_physical_plan.h"
#include "sql/ob_multi_logic_plan.h"
#include "parse_malloc.h"
#include "ob_sql_session_info.h"
#include "common/ob_trace_log.h"
#include "common/ob_common_stat.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObResultSet::Field::deep_copy(ObResultSet::Field &other, ObStringBuf *str_buf)
{
  int ret = OB_SUCCESS;
  if (NULL == str_buf)
  {
    TBSYS_LOG(WARN, "invalid argument str_buf = %p", str_buf);
  }
  else
  {
    if (OB_SUCCESS != (ret = str_buf->write_string(other.tname_, &tname_)))
    {
      TBSYS_LOG(WARN, "ObStringBuf write Fileld.tname_ string error, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = str_buf->write_string(other.org_tname_, &org_tname_)))
    {
      TBSYS_LOG(WARN, "ObStringBuf write Fileld.org_tname_ string error, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = str_buf->write_string(other.cname_, &cname_)))
    {
      TBSYS_LOG(WARN, "ObStringBuf write Fileld.cname_ string error, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = str_buf->write_string(other.org_cname_, &org_cname_)))
    {
      TBSYS_LOG(WARN, "ObStringBuf write Fileld.org_cname_ string error, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = str_buf->write_string(other.org_tname_, &org_tname_)))
    {
      TBSYS_LOG(WARN, "ObStringBuf write Fileld.tname_ string error, ret=%d", ret);
    }
    if (OB_SUCCESS == ret)
    {
      type_ = other.type_;
    }
  }
  return ret;
}

ObResultSet::~ObResultSet()
{
  if (!plan_from_assign_ && own_physical_plan_ && NULL != physical_plan_)
  {
    TBSYS_LOG(DEBUG, "destruct physical plan, addr=%p", physical_plan_);
    physical_plan_->~ObPhysicalPlan();
    physical_plan_ = NULL;
  }

  if (plan_from_assign_ && NULL != physical_plan_)
  {
    TBSYS_LOG(DEBUG, "destruct physical plan cons from assign, addr=%p", physical_plan_);
    physical_plan_->clear();
    ObPhysicalPlan::free(physical_plan_);
    physical_plan_ = NULL;
  }

  if (NULL != ps_trans_allocator_)
  {
    OB_ASSERT(my_session_);
    my_session_->free_transformer_mem_pool_for_ps(ps_trans_allocator_);
    ps_trans_allocator_ = NULL;
  }
  for (int64_t i = 0; i < params_.count(); i++)
  {
    ObObj *value = params_.at(i);
    value->~ObObj();
    //parse_free(value);
  }
  params_.clear();
  params_type_.clear();
  //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
  params_constraint_.clear();
  //add 20131222:e
  //add liuzy [datetime func] 20150910:b
  for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
  {
    cur_time_[i]->~ObObj();
  }
  //add 20150910:e
}

int ObResultSet::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObResultSet::open()
{
  int ret = common::OB_SUCCESS;
  ObObj val;
  int64_t plan_timeout = OB_DEFAULT_STMT_TIMEOUT;

  if (OB_UNLIKELY(NULL == physical_plan_))
  {
    if (ObBasicStmt::T_PREPARE != stmt_type_)
    {
      TBSYS_LOG(WARN, "physical_plan not init, stmt_type=%d", stmt_type_);
      ret = common::OB_NOT_INIT;
    }
  }
  else
  {
    OB_ASSERT(my_session_);
    if (my_session_->get_trans_id().is_valid())
    {
      // statement in transaction
      int64_t stmt_interval = tbsys::CTimeUtil::getTime() - my_session_->get_curr_trans_last_stmt_time();
      OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_STMT_INTERVAL, stmt_interval);
      OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_STMT_COUNT);
      TBSYS_LOG(DEBUG, "stmt begin in transaction, start=%ld last_stmt=%ld interval=%ld",
                my_session_->get_curr_trans_start_time(),
                my_session_->get_curr_trans_last_stmt_time(), stmt_interval);
    }
    // get current frozen version
    FILL_TRACE_LOG("curr_frozen_version=%s", to_cstring(my_session_->get_frozen_version()));
    physical_plan_->set_curr_frozen_version(my_session_->get_frozen_version());

    ///  query timeout for sql level
    if (OB_SUCCESS == my_session_->get_sys_variable_value(ObString::make_string(OB_QUERY_TIMEOUT_PARAM), val))
    {
      if (OB_SUCCESS != val.get_int(plan_timeout))
      {
        TBSYS_LOG(WARN, "fail to get query timeout from session, ret=%d", ret);
        plan_timeout = OB_DEFAULT_STMT_TIMEOUT; // use default
      }
    }
	//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140713 :b
  //mod liumz [extend_time_for_get_privilege]20151201:b
  if((is_need_extend_time() || physical_plan_->is_need_extend_time()) && plan_timeout<OB_DEFAULT_STMT_TIMEOUT*60L)
  //if(physical_plan_->is_need_extend_time() && plan_timeout<OB_DEFAULT_STMT_TIMEOUT*60L)
  //mod:e
	{
	  plan_timeout = OB_DEFAULT_STMT_TIMEOUT*60L;
	}
	//add 20140713 :e
    physical_plan_->set_timeout_timestamp(plan_timeout + tbsys::CTimeUtil::getTime());
    ObPhyOperator *main_query = physical_plan_->get_main_query();
    ObPhyOperator *pre_query = physical_plan_->get_pre_query();

    if (NULL != pre_query)
    {
      ret = pre_query->open();
      FILL_TRACE_LOG("pre_query finished");
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to open pre_query, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (NULL == main_query)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "main query must not be NULL");
      }
      else
      {
        ret = main_query->open();
        FILL_TRACE_LOG("main_query finished");
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to open main query, ret=%d", ret);
        }
      }
    }
  }
  set_errcode(ret);
  return ret;
}

int ObResultSet::close()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL == physical_plan_))
  {
    if (ObBasicStmt::T_PREPARE != stmt_type_)
    {
      TBSYS_LOG(WARN, "physical_plan not init, stmt_type=%d", stmt_type_);
      ret = common::OB_NOT_INIT;
    }
  }
  else
  {
    ObPhyOperator *main_query = physical_plan_->get_main_query();
    ObPhyOperator *pre_query = physical_plan_->get_pre_query();
    int tmp_ret = OB_SUCCESS;
    if (NULL != pre_query)
    {
      if (OB_SUCCESS != (tmp_ret = pre_query->close()))
      {
        ret = tmp_ret;
        TBSYS_LOG(WARN, "failed to close pre_query, ret=%d", ret);
      }
    }

    if (NULL == main_query)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "main query must not be NULL");
    }
    else if (OB_SUCCESS != (tmp_ret = main_query->close()))
    {
      ret = tmp_ret;
      TBSYS_LOG(WARN, "failed to close main_query, ret=%d", ret);
    }

    if (my_session_->get_trans_id().is_valid())
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      my_session_->set_curr_trans_last_stmt_time(now);
      TBSYS_LOG(DEBUG, "stmt end in transaction, start=%ld now=%ld", my_session_->get_curr_trans_start_time(), now);
    }
  }
  set_errcode(ret);
  return ret;
}

int ObResultSet::reset()
{
  int ret = OB_SUCCESS;

  if (!plan_from_assign_ && own_physical_plan_ && NULL != physical_plan_)
  {
    TBSYS_LOG(DEBUG, "destruct physical plan, addr=%p", physical_plan_);
    physical_plan_->~ObPhysicalPlan();
    physical_plan_ = NULL;
  }

  if (plan_from_assign_ && NULL != physical_plan_)
  {
    TBSYS_LOG(DEBUG, "destruct physical plan cons from assign, addr=%p", physical_plan_);
    physical_plan_->clear();
    ObPhysicalPlan::free(physical_plan_);
    physical_plan_ = NULL;
  }
  statement_id_ = OB_INVALID_ID;
  affected_rows_ = 0;
  statement_name_.reset();
  warning_count_ = 0;
  message_[0] = '\0';
  field_columns_.clear();
  param_columns_.clear();
  for (int64_t i = 0; i < params_.count(); i++)
  {
    ObObj *value = params_.at(i);
    value->~ObObj();
    //parse_free(value);
  }
  params_.clear();
  params_type_.clear();
  //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
  params_constraint_.clear();
  //add 20131222:e
  //mod liuzy [datetime func] 20150911:b
  //cur_time_ = NULL;
  for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
  {
    cur_time_[i]->~ObObj();
    cur_time_[i] = NULL;
  }
  errcode_ = 0;
  // don't set stmt_type_ = ObBasicStmt::T_NONE;
  return ret;
}

void ObResultSet::set_statement_name(const common::ObString name)
{
  statement_name_ = name;
}

int ObResultSet::pre_assign_params_room(const int64_t& size, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObObj *place_holder = NULL;
  Field param_field;
  param_field.type_.set_type(ObIntType); // @bug
  params_.reserve(size);
  param_columns_.reserve(size);
  //add peiouya [NotNULL_check] [JHOBv0.1] 2013122:b
  params_constraint_.reserve(size);
  //add 2013122:e
  for (int64_t i = 0; i < size; i++)
  {
    if (NULL == (place_holder = (ObObj*)alloc.alloc(sizeof(ObObj))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    }
    else
    {
      place_holder = new(place_holder) ObObj();
    }
    if (OB_SUCCESS != (ret = params_.push_back(place_holder)))
    {
      break;
    }
    else if (OB_SUCCESS != (ret = param_columns_.push_back(param_field)))
    {
      break;
    }
    //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
	else if (OB_SUCCESS != (ret = params_constraint_.push_back(true)))
	{
	  break;
	}
    //add 20131222:e
  }
  TBSYS_LOG(DEBUG, "stmt_id=%lu param_count=%ld param_msize=%ld paramc_c=%ld paramc_s=%ld",
            statement_id_, params_.count(), params_.get_data_size(),
            param_columns_.count(), param_columns_.get_data_size());
  return ret;
}

int ObResultSet::pre_assign_cur_time_room(common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  //add liuzy [datetime func] 20150910:b
  for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
  {
  //add 20150910:e
    ObObj *place_holder = NULL;
    if (NULL == (place_holder = (ObObj*)alloc.alloc(sizeof(ObObj))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      place_holder = new(place_holder) ObObj();
      cur_time_[i] = place_holder;
    }
  }//add liuzy [datetime func] 20150910 /*Exp: end for*/
  return ret;
}

int ObResultSet::get_param_idx(int64_t param_addr, int64_t &idx)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < params_.count(); ++i)
  {
    if (reinterpret_cast<ObObj *>(param_addr) == params_.at(i))
    {
      ret = OB_SUCCESS;
      idx = i;
      break;
    }
  }
  return ret;
}

//mod peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
//int ObResultSet::fill_params(const common::ObIArray<obmysql::EMySQLFieldType>& types,
//                       const common::ObIArray<common::ObObj>& values)
int ObResultSet::fill_params(const common::ObIArray<obmysql::EMySQLFieldType>& types,
                             const common::ObIArray<common::ObObj>& values, int64_t &idx)
//mod 20140306:e
{
  int ret = OB_SUCCESS;
  if (values.count() != params_.count())
  {
    ret = OB_ERR_WRONG_DYNAMIC_PARAM;
    TBSYS_LOG(USER_ERROR, "Incorrect arguments number to EXECUTE, need %ld arguments but give %ld",
              params_.count(), values.count());
  }
  else
  {
    if (0 < types.count())
    {
      params_type_.reserve(types.count());
      // bound types
      params_type_ = types;
      TBSYS_LOG(DEBUG, "stmt_id=%lu paramt_count=%ld paramt_msize=%ld",
                statement_id_, params_type_.count(), params_type_.get_data_size());
    }
	//add peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
	bool flag = true;
	for (int64_t i = 0; ret == OB_SUCCESS && i < params_.count(); i++)
	{
	  ret = get_params_constraint(i, flag);
	  if (!flag && (ObNullType == (values.at(i)).get_type()))
	  {
	    idx = i;
	    ret = OB_ERR_VARIABLE_NULL;
	  }
	}
	//add 20140306:e
    //TBSYS_LOG(DEBUG, "execute with params=%s", to_cstring(values));
    for (int64_t i = 0; ret == OB_SUCCESS && i < params_.count(); i++)
    {
      *params_.at(i) = values.at(i); // shallow copy
      TBSYS_LOG(DEBUG, "params is %p execute with params=%s", params_.at(i), to_cstring(*params_.at(i)));
    }

  }
  return ret;
}

int ObResultSet::from_prepared(const ObResultSet& stored_result_set)
{
  int ret = OB_SUCCESS;
  statement_id_ = stored_result_set.statement_id_;
  affected_rows_ = 0;

  physical_plan_ = stored_result_set.physical_plan_;
  own_physical_plan_ = false;

  field_columns_ = stored_result_set.field_columns_;
  param_columns_ = stored_result_set.param_columns_;
  params_ = stored_result_set.params_;
  //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
  params_constraint_ = stored_result_set.params_constraint_;
  //add 20131222:e
  //mod liuzy [datetime func] 20150911:b
//  cur_time_ = stored_result_set.cur_time_;
  for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
  {
    cur_time_[i] = stored_result_set.cur_time_[i];
  }
  //mod 20150911:e
  ps_trans_allocator_ = NULL;
  query_string_id_ = stored_result_set.query_string_id_;

  // Outer statement has itsown stmt_type_, it needs the plan only not the stmt_type_
  // If outer resultset with stmt_type_ = T_EXECUTE, changing to T_PREPARE will arise error
  // stmt_type_ = stored_result_set.stmt_type_;
  inner_stmt_type_ = stored_result_set.stmt_type_;
  compound_ = stored_result_set.compound_;
  return ret;
}

int ObResultSet::from_store(ObPsStoreItemValue *value, ObPsSessionInfo *info)
{
  int ret = OB_SUCCESS;
  if (NULL == value)
  {
    TBSYS_LOG(ERROR, "invalid argument value is %p", value);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    statement_id_ = value->sql_id_;
    affected_rows_ = 0;
    own_physical_plan_ = true;
    ps_trans_allocator_ = NULL;
    inner_stmt_type_ = value->inner_stmt_type_;
    compound_ = value->compound_;
    field_columns_ = value->field_columns_;
    param_columns_ = value->param_columns_;
	//add peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
	params_constraint_ = value->params_constraint_;
	//add 20140306:e
    //TODO copy params && store params
    params_ = info->params_;
    //mod liuzy [datetime func] 20150910:b
//    if (NULL != info->cur_time_)
//    {
//      cur_time_ = info->cur_time_;
//    }
    for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
    {
      if (NULL != info->cur_time_[i])
      {
        cur_time_[i] = info->cur_time_[i];
      }
    }
    //mod 20150910:e
  }
  return ret;
}
int ObResultSet::to_prepare(ObResultSet& other)
{
  int ret = OB_SUCCESS;
  other.statement_id_ = statement_id_;
  other.affected_rows_ = 0;
  other.warning_count_ = 0;
  other.statement_name_ = statement_name_;
  other.message_[0] = '\0';
  other.field_columns_ = field_columns_;
  other.param_columns_ = param_columns_;
  other.params_ = params_;
  //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
  other.params_constraint_ = params_constraint_;
  //add 20131222:e
  //mod liuzy [datetime func] 20150910:b
//  other.cur_time_ = cur_time_;
  for (int i = 0; i < ObResultSet::CUR_SIZE; ++i)
  {
    other.cur_time_[i] = cur_time_[i];
  }
  //mod 20150910:e
  other.physical_plan_ = physical_plan_;
  other.physical_plan_->set_result_set(&other); // the new ownership
  other.own_physical_plan_ = true;
  other.stmt_type_ = stmt_type_;
  other.errcode_ = OB_SUCCESS;
  other.my_session_ = my_session_;
  other.ps_trans_allocator_ = ps_trans_allocator_;
  other.query_string_id_ = query_string_id_;

  this->statement_name_.reset();
  this->physical_plan_ = NULL;
  this->own_physical_plan_ = false;
  this->ps_trans_allocator_ = NULL;
  // keep params_, field_columns_, param_columns_ etc.
  return ret;
}
