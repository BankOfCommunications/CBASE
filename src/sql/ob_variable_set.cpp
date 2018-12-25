/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_variable_set.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_variable_set.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "common/ob_obj_cast.h"
#include "common/ob_trace_log.h"
#include "common/ob_common_stat.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObVariableSet::ObVariableSet()
  : variable_nodes_(), rpc_(NULL), table_id_(OB_INVALID_ID)
  , rowkey_info_(), name_cid_(OB_INVALID_ID)
  , value_cid_(OB_INVALID_ID), value_type_(ObMinType)
  , mutator_()
{
}

ObVariableSet::~ObVariableSet()
{
  destroy_variable_nodes();
}

void ObVariableSet::reset()
{
  destroy_variable_nodes();
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  name_cid_ = OB_INVALID_ID;
  value_cid_ = OB_INVALID_ID;
  value_type_ = ObMinType;
  mutator_.reset();
}

void ObVariableSet::reuse()
{
  destroy_variable_nodes();
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  name_cid_ = OB_INVALID_ID;
  value_cid_ = OB_INVALID_ID;
  value_type_ = ObMinType;
  mutator_.clear();
}

void ObVariableSet::destroy_variable_nodes()
{
  for (int i = 0; i < variable_nodes_.count(); i++)
  {
    ObSqlExpression::free(variable_nodes_.at(i).variable_expr_);
  }
  variable_nodes_.clear();
}

int ObVariableSet::open()
{
  int ret = OB_SUCCESS;
  if (variable_nodes_.count() <= 0 || rpc_ == NULL
    || table_id_ == OB_INVALID_ID || rowkey_info_.get_size() == 0
    || name_cid_ == OB_INVALID_ID || value_cid_ == OB_INVALID_ID)
  {
    ret = OB_ERR_GEN_PLAN;
    TBSYS_LOG(WARN, "Variable set statement is not initiated");
  }
  else
  {
    mutator_.use_db_sem();
    if (OB_SUCCESS != (ret = mutator_.reset()))
    {
      TBSYS_LOG(WARN, "fail to reset mutator. ret=%d", ret);
    }
    else
    {
      // add child plan to session
      ret = process_variables_set();
    }
  }
  return ret;
}

int ObVariableSet::close()
{
  mutator_.reset();
  return OB_SUCCESS;
}

int ObVariableSet::process_variables_set()
{
  int ret = OB_SUCCESS;
  bool need_global_change = false;
  ObRow val_row;
  const ObObj *value_obj = NULL;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  for (int64_t i = 0; ret == OB_SUCCESS && i < variable_nodes_.count(); i++)
  {
    VariableSetNode& node = variable_nodes_.at(i);
    if ((ret = node.variable_expr_->calc(val_row, value_obj)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Calculate variable value failed. ret=%d", ret);
    }
    else if (!node.is_system_variable_)
    {
      // user defined tmp variable
      if ((ret = session->replace_variable(
                    node.variable_name_,
                    *value_obj)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "set variable to session plan failed. ret=%d, name=%.*s",
            ret, node.variable_name_.length(), node.variable_name_.ptr());
      }
    }
    else if (!node.is_global_)
    {
      // set session system variable
      if ((ret = session->update_system_variable(
                    node.variable_name_,
                    *value_obj)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "set variable to session plan failed. ret=%d, name=%.*s",
            ret, node.variable_name_.length(), node.variable_name_.ptr());
      }
      // special case: set autocommit
      else if (node.variable_name_ == ObString::make_string("autocommit"))
      {
        int64_t int_val = 0;
        ObObj int_obj;
        ObObj obj_type;
        obj_type.set_type(ObIntType);
        const ObObj *res_obj = NULL;
        if (OB_SUCCESS != (ret = obj_cast(*value_obj, obj_type, int_obj, res_obj)))
        {
          TBSYS_LOG(WARN, "failed to cast to int, obj=%s", to_cstring(*value_obj));
        }
        else
        {
          ret = res_obj->get_int(int_val);
          OB_ASSERT(OB_SUCCESS == ret);
          if (int_val)
          {
            ret = set_autocommit();
            OB_STAT_INC(OBMYSQL, SQL_AUTOCOMMIT_ON_COUNT);
          }
          else
          {
            ret = clear_autocommit();
            OB_STAT_INC(OBMYSQL, SQL_AUTOCOMMIT_OFF_COUNT);
          }
        }
      }
    }
    else
    {
      // set global system variable
      char key_varchar_buff[OB_MAX_ROWKEY_COLUMN_NUMBER + 1][OB_MAX_VARCHAR_LENGTH];
      ObObj casted_cells[OB_MAX_ROWKEY_COLUMN_NUMBER + 1];
      ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
      const ObObj *res_cell = NULL;
      ObRowkey rowkey;
      ObObj value;
      // construct rowkey
      for (int64_t j = 0; ret == OB_SUCCESS && j < rowkey_info_.get_size(); ++j)
      {
        uint64_t cid = OB_INVALID_ID;
        ObString varchar;
        varchar.assign_ptr(key_varchar_buff[j], OB_MAX_VARCHAR_LENGTH);
        casted_cells[j].set_varchar(varchar);

        const ObRowkeyColumn *rowkey_column = rowkey_info_.get_column(j);
        if (rowkey_column == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "Get rowkey error");
          break;
        }
        else if (OB_SUCCESS != (ret = rowkey_info_.get_column_id(j, cid)))
        {
          TBSYS_LOG(USER_ERROR, "Primary key can not be empty");
          ret = OB_ERR_GEN_PLAN;
          break;
        }
        else
        {
          ObObj data_type;
          data_type.set_type(rowkey_column->type_);
          if (cid != name_cid_)
          {
            value.set_int(0);
          }
          else
          {
            value.set_varchar(node.variable_name_);
          }
          if (OB_SUCCESS != (ret = obj_cast(value, data_type, casted_cells[j], res_cell)))
          {
            TBSYS_LOG(USER_ERROR, "Primary key can not be empty");
            ret = OB_ERR_GEN_PLAN;
            break;
          }
          else
          {
            rowkey_objs[j] = *res_cell;
          }
        }
      }
      if (ret == OB_SUCCESS)
      {
        rowkey.assign(rowkey_objs, rowkey_info_.get_size());
      }
      else
      {
        break;
      }

      // add type and value
      if (ret == OB_SUCCESS)
      {
        ObString varchar;
        varchar.assign_ptr(key_varchar_buff[OB_MAX_ROWKEY_COLUMN_NUMBER], OB_MAX_VARCHAR_LENGTH);
        ObObj &casted_cell = casted_cells[OB_MAX_ROWKEY_COLUMN_NUMBER];
        casted_cell.set_varchar(varchar);
        ObObj value_type;
        value_type.set_type(value_type_);
        if (OB_SUCCESS != (ret = obj_cast(*value_obj, value_type, casted_cell, res_cell)))
        {
          TBSYS_LOG(WARN, "Failed to cast obj, err=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = mutator_.update(table_id_, rowkey, value_cid_, *res_cell)))
        {
          TBSYS_LOG(WARN, "Failed to update cell, err=%d", ret);
          break;
        }
        else
        {
          need_global_change = true;
        }
      }
    }
  }
  // send mutator
  ObScanner scanner;
  if (OB_SUCCESS == ret && need_global_change
    && OB_SUCCESS != (ret = rpc_->ups_mutate(mutator_, false, scanner)))
  {
    TBSYS_LOG(WARN, "failed to send mutator to ups, err=%d", ret);
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObVariableSet, PHY_VARIABLE_SET);
  }
}

int64_t ObVariableSet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "VariableSet(variables=[");
  for (int64_t i = 0; i < variable_nodes_.count(); i++)
  {
    const VariableSetNode& node = variable_nodes_.at(i);
    databuff_printf(buf, buf_len, pos, "<variable name=%.*s, variable expr=",
        node.variable_name_.length(), node.variable_name_.ptr());
    pos += node.variable_expr_->to_string(buf + pos, buf_len - pos);
    if (i == variable_nodes_.count() - 1)
      databuff_printf(buf, buf_len, pos, ">");
    else
      databuff_printf(buf, buf_len, pos, ">, ");
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}

int ObVariableSet::set_autocommit()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  if (session->get_autocommit())
  {
    // do nothing
    TBSYS_LOG(DEBUG, "already in autocommit=1");
  }
  else
  {
    // autocommit=0 --> autocommit=1
    if (session->get_trans_id().is_valid())
    {
      // commit the current transaction before changing autocommit status
      ObEndTransReq req;
      req.trans_id_ = session->get_trans_id();
      req.rollback_ = false;
      if (OB_SUCCESS != (ret = rpc_->ups_end_trans(req)))
      {
        TBSYS_LOG(WARN, "failed to end ups transaction, err=%d trans=%s",
                  ret, to_cstring(req));
      }
      else
      {
        if (0 < session->get_curr_trans_start_time())
        {
          int64_t trans_time = tbsys::CTimeUtil::getTime() - session->get_curr_trans_start_time();
          OB_STAT_INC(OBMYSQL, SQL_MULTI_STMT_TRANS_TIME, trans_time);
          session->set_curr_trans_start_time(0);
        }
        TBSYS_LOG(WARN, "autocommit unfinished transaction when set autocommit=1, trans_id=%s",
                  to_cstring(req.trans_id_));
        FILL_TRACE_LOG("autocommit trans");
        // reset transaction id
        ObTransID invalid_trans;
        session->set_trans_id(invalid_trans);
      }
      // set autocommit = 1 whatever
      session->set_autocommit(true);
    }
    else
    {
      session->set_autocommit(true);
    }
  }
  return ret;
}

int ObVariableSet::clear_autocommit()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  if (!session->get_autocommit())
  {
    // do nothing
    TBSYS_LOG(DEBUG, "in autocommit=0");
  }
  else
  {
    // autocommit=1 --> autocommit=0
    session->set_autocommit(false);
  }
  return ret;
}
