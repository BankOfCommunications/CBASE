/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_sys_cnf.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_alter_sys_cnf.h"
#include "ob_sql.h"
#include "ob_direct_trigger_event_util.h"
#include "common/ob_define.h"
#include "common/ob_obj_cast.h"

using namespace oceanbase;
using namespace oceanbase::sql;

#define START_TRANSACTION 0
#define COMMIT 1
#define ROLLBACK 2

ObAlterSysCnf::ObAlterSysCnf()
{
}

ObAlterSysCnf::~ObAlterSysCnf()
{
}

void ObAlterSysCnf::reset()
{
}

void ObAlterSysCnf::reuse()
{
}

void ObAlterSysCnf::set_sql_context(ObSqlContext& context)
{
  local_context_ = context;
  local_context_.schema_manager_ = NULL;
}

int ObAlterSysCnf::open()
{
  int ret = OB_SUCCESS;
  if (sys_cnf_items_.count() <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(USER_ERROR, "No param to be changed, ret=%d", ret);
  }
  else if ((local_context_.schema_manager_ =
                local_context_.merger_schema_mgr_->get_user_schema(0)) == NULL)
  {
    ret = OB_ERROR;
    TBSYS_LOG(USER_ERROR, "Fail to get schema manager, ret=%d", ret);
  }
  else if ((ret = execute_transaction_stmt(START_TRANSACTION)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Start transaction failed, ret=%d", ret);
  }
  else
  {
    for (int64_t i = 0; i < sys_cnf_items_.count(); i++)
    {
      ObSysCnfItem& cnf_item = sys_cnf_items_.at(i);
      if (cnf_item.server_ip_.length() > 15)
      {
        ret = OB_ERROR;
        TBSYS_LOG(USER_ERROR, "Invalid server ip: %.*s",
                  cnf_item.server_ip_.length(), cnf_item.server_ip_.ptr());
        break;
      }
      else if (cnf_item.server_ip_.length() > 0)
      {
        // check ip address
        uint32_t ip = 0;
        char buf[16];
        memcpy(buf, cnf_item.server_ip_.ptr(), cnf_item.server_ip_.length());
        buf[cnf_item.server_ip_.length()] = '\0';
        if ((ip = inet_addr(buf)) == INADDR_NONE)
        {
          ret = OB_ERROR;
          TBSYS_LOG(USER_ERROR, "Invalid server ip: %.*s",
                    cnf_item.server_ip_.length(), cnf_item.server_ip_.ptr());
          break;
        }
      }
      else
      {
        cnf_item.server_ip_ = ObString::make_string("ANY"); // @see
      }
/**
mysql> desc __all_sys_config;
+--------------+--------------+----------+------+---------+-------+
| field        | type         | nullable | key  | default | extra |
+--------------+--------------+----------+------+---------+-------+
| gm_create    | createtime   |          |    0 | NULL    |       |
| gm_modify    | modifytime   |          |    0 | NULL    |       |
| cluster_id   | int          |          |    1 | NULL    |       |
| svr_type     | varchar(16)  |          |    2 | NULL    |       |
| svr_ip       | varchar(32)  |          |    3 | NULL    |       |
| svr_port     | int          |          |    4 | NULL    |       |
| name         | varchar(256) |          |    5 | NULL    |       |
| section      | varchar(256) |          |    0 | NULL    |       |
| data_type    | varchar(256) |          |    0 | NULL    |       |
| value        | varchar(256) |          |    0 | NULL    |       |
| value_strict | varchar(512) |          |    0 | NULL    |       |
| info         | varchar(512) |          |    0 | NULL    |       |
+--------------+--------------+----------+------+---------+-------+
 */
      int64_t pos = 0;
      char sql_buff[OB_MAX_VARCHAR_LENGTH];
      databuff_printf(sql_buff, OB_MAX_VARCHAR_LENGTH, pos,
                      "replace into %s"
                      "(cluster_id, svr_type, svr_ip, "
                      "svr_port, name, section, data_type, "
                      "value, value_strict, info) "
                      "values(%lu, '%s', '%.*s', %lu, '%.*s', 'unknown', '%s', '",
                      OB_ALL_SYS_CONFIG_TABLE_NAME,
                      cnf_item.cluster_id_ != OB_INVALID_ID ? cnf_item.cluster_id_ : 0,
                      print_role(cnf_item.server_type_),
                      cnf_item.server_ip_.length(), cnf_item.server_ip_.ptr(),
                      cnf_item.server_port_ != OB_INVALID_ID ? cnf_item.server_port_ : 0,
                      cnf_item.param_name_.length(), cnf_item.param_name_.ptr(),
                      ob_obj_type_str(cnf_item.param_value_.get_type()));
      if (cnf_item.param_value_.get_type() == ObNullType)
      {
        databuff_printf(sql_buff, OB_MAX_VARCHAR_LENGTH, pos, "NULL");
      }
      else
      {
        char varchar_buf[OB_MAX_VARCHAR_LENGTH];
        ObString varchar(OB_MAX_VARCHAR_LENGTH, OB_MAX_VARCHAR_LENGTH, varchar_buf);
        ObObj casted_cell;
        casted_cell.set_varchar(varchar);
        const ObObj *res_value = NULL;
        ObObj data_type;
        data_type.set_type(ObVarcharType);
        ObString val;
        if ((ret = obj_cast(cnf_item.param_value_, data_type, casted_cell, res_value)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Fail to cast obj, err=%d", ret);
          break;
        }
        else if ((ret = res_value->get_varchar(val)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Get casted value failed, err=%d", ret);
          break;
        }
        databuff_printf(sql_buff, OB_MAX_VARCHAR_LENGTH, pos, "%.*s", val.length(), val.ptr());
      }
      // add zhaoqiong [roottable tablet management]20150318:b
      if (cnf_item.param_name_ == ObString::make_string("tablet_replicas_num"))
      {
          TBSYS_LOG(INFO, "check the valid of tablet_replicas_num");
          int64_t out_val = 0;
          ObObj casted_cell;
          const ObObj * res_value= NULL;
          ObObj date_type;
          date_type.set_type(ObIntType);
          if ((ret = obj_cast(cnf_item.param_value_,date_type,casted_cell,res_value)) != OB_SUCCESS)
          {
              TBSYS_LOG(WARN, "Fail to cast obj, err=%d", ret);
              break;
          }
          else if (res_value != NULL && (ret = res_value->get_int(out_val)) != OB_SUCCESS)
          {
              TBSYS_LOG(ERROR, "get_int error [err:%d]", ret);
              break;
          }
          else if (out_val <= 0 || out_val > OB_MAX_COPY_COUNT)
          {
              TBSYS_LOG(WARN, "illegal value, err=%d", ret);
              ret = OB_ERR_ILLEGAL_VALUE;
              break;
          }
      }

      // add e
      databuff_printf(sql_buff, OB_MAX_VARCHAR_LENGTH, pos, "', 'unknown', '%.*s')",
                      cnf_item.comment_.length(), cnf_item.comment_.ptr());
      ObString sql_string(OB_MAX_VARCHAR_LENGTH, static_cast<int32_t>(pos), sql_buff);
      ObResultSet tmp_result;
      if (OB_SUCCESS != (ret = tmp_result.init()))
      {
        TBSYS_LOG(WARN, "Init temp result set failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(sql_string, tmp_result, local_context_)))
      {
        TBSYS_LOG(USER_ERROR, "Direct_execute failed, sql=%.*s ret=%d",
                  sql_string.length(), sql_string.ptr(), ret);
        break;
      }
      else if (OB_SUCCESS != (ret = tmp_result.open()))
      {
        TBSYS_LOG(WARN, "Open result set failed, sql=%.*s ret=%d",
                  sql_string.length(), sql_string.ptr(), ret);
        break;
      }
      else
      {
        /*
        if (tmp_result.get_affected_rows() <= 0)
        {
          TBSYS_LOG(WARN, "Fail to update param %.*s, sql=%.*s",
                    cnf_item.param_name_.length(), cnf_item.param_name_.ptr(),
                    sql_string.length(), sql_string.ptr());
        }
        */
        if ((ret = tmp_result.close()) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Failed to close temp result set, sql=%.*s ret=%d",
                    sql_string.length(), sql_string.ptr(), ret);
          break;
        }
      }
    }
    if (ret == OB_SUCCESS)
    {
      if ((ret = execute_transaction_stmt(COMMIT)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Commit transaction failed, ret=%d", ret);
      }
      else
      {
        // notify all servers to update config
        ObResultSet tmp_result;
        if (OB_SUCCESS != (ret = tmp_result.init()))
        {
          TBSYS_LOG(WARN, "Init temp result set failed, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = ObDirectTriggerEventUtil::refresh_new_config(tmp_result, local_context_)))
        {
          TBSYS_LOG(ERROR, "fail to refresh new config. ret=%d", ret);
        }
        // to release resouce, call open & close
        else if (OB_SUCCESS != (ret = tmp_result.open()))
        {
          TBSYS_LOG(WARN, "fail to open tmp result. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = tmp_result.close()))
        {
          TBSYS_LOG(WARN, "fail to close tmp result. ret=%d", ret);
        }
      }
    }
    else
    {
      if (execute_transaction_stmt(ROLLBACK) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Rollback transaction failed");
      }
    }
  }
  if (local_context_.schema_manager_ != NULL)
  {
    local_context_.merger_schema_mgr_->release_schema(local_context_.schema_manager_);
    local_context_.schema_manager_ = NULL;
  }
  return ret;
}

int ObAlterSysCnf::close()
{
  return OB_SUCCESS;
}

int ObAlterSysCnf::execute_transaction_stmt(const int& trans_type)
{
  int ret = OB_SUCCESS;
  ObString trans_string;
  ObResultSet tmp_result;
  switch (trans_type)
  {
    case START_TRANSACTION:
      trans_string = ObString::make_string("START TRANSACTION");
      break;
    case COMMIT:
      trans_string = ObString::make_string("COMMIT");
      break;
    case ROLLBACK:
      trans_string = ObString::make_string("ROLLBACK");
      break;
    default:
      TBSYS_LOG(WARN, "Wrong transaction type, trans_type=%d", trans_type);
      ret = OB_ERROR;
      break;
  }
  if (ret != OB_SUCCESS)
  {
  }
  else if ((ret = tmp_result.init()) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Init result set failed, ret=%d", ret);
  }
  else if ((ret = ObSql::direct_execute(trans_string, tmp_result, local_context_)) != OB_SUCCESS)
  {
    TBSYS_LOG(USER_ERROR, "Execute transaction stmt failed, sql=%.*s ret=%d",
              trans_string.length(), trans_string.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "Open temp result set failed, sql=%.*s ret=%d",
              trans_string.length(), trans_string.ptr(), ret);
  }
  else
  {
    if (tmp_result.is_with_rows())
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "Transaction stmt should not return rows, ret=%d", ret);
    }
    if (tmp_result.close() != OB_SUCCESS)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "Failed to close tmp result set, ret=%d", ret);
    }
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObAlterSysCnf, PHY_ALTER_SYS_CNF);
  }
}

int64_t ObAlterSysCnf::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "AlterSystem(params=[");
  for (int64_t i = 0; i < sys_cnf_items_.count(); ++i)
  {
    databuff_printf(buf, buf_len, pos, "param %ld (", i);
    pos += sys_cnf_items_.at(i).to_string(buf + pos, buf_len - pos);
    if (i != sys_cnf_items_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, "), ");
    }
    else
    {
      databuff_printf(buf, buf_len, pos, ")");
    }
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}
