/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@alipay.com>
 */

#include "common/ob_row.h"
#include "common/ob_privilege_type.h"
#include "common/ob_define.h"
#include "common/ob_encrypted_helper.h"
#include "common/ob_strings.h"
#include "common/ob_trigger_msg.h"
#include "common/ob_array.h"
#include "common/ob_obj_cast.h"
#include "common/ob_string.h"
#include "common/ob_inner_table_operator.h"
#include "sql/ob_priv_executor.h"
#include "sql/ob_sql.h"
#include "sql/ob_grant_stmt.h"
#include "sql/ob_create_user_stmt.h"
#include "sql/ob_drop_user_stmt.h"
#include "sql/ob_revoke_stmt.h"
#include "sql/ob_lock_user_stmt.h"
#include "sql/ob_set_password_stmt.h"
#include "sql/ob_rename_user_stmt.h"
//add wenghaixing [database manage]20150609
#include "sql/ob_create_db_stmt.h"
#include "sql/ob_use_db_stmt.h"
#include "sql/ob_drop_database_stmt.h"
//add e
using namespace oceanbase;
using namespace oceanbase::sql;
//add wenghaixing [database manage]20150702
using namespace oceanbase::common;
//add e

void ObPrivExecutor::reset()
{
  stmt_ = NULL;
  context_ = NULL;
  result_set_out_ = NULL;
  page_arena_.free();
}

void ObPrivExecutor::reuse()
{
  stmt_ = NULL;
  context_ = NULL;
  result_set_out_ = NULL;
  page_arena_.reuse();
}

int ObPrivExecutor::start_transaction()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("START TRANSACTION");
  ret = execute_stmt_no_return_rows(start_thx);
  return ret;
}
int ObPrivExecutor::commit()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("COMMIT");
  ret = execute_stmt_no_return_rows(start_thx);
  if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = insert_trigger())))
  {
    TBSYS_LOG(ERROR, "insert trigger  failed,ret=%d", ret);
  }
  return ret;
}
int ObPrivExecutor::rollback()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("ROLLBACK");
  ret = execute_stmt_no_return_rows(start_thx);
  return ret;
}
int ObPrivExecutor::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  UNUSED(row_desc);
  return OB_NOT_SUPPORTED;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObPrivExecutor, PHY_PRIV_EXECUTOR);
  }
}
int64_t ObPrivExecutor::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObPrivExecutor(stmt_type=%d)\n", stmt_->get_stmt_type());
  return pos;
}
ObPrivExecutor::ObPrivExecutor()
:context_(NULL), result_set_out_(NULL), page_arena_(4 * 1024)
{
}
ObPrivExecutor::~ObPrivExecutor()
{
}
void ObPrivExecutor::set_stmt(const ObBasicStmt *stmt)
{
  stmt_ = stmt;
}
void ObPrivExecutor::set_context(ObSqlContext *context)
{
  context_ = context;
  result_set_out_ = context_->session_info_->get_current_result_set();
}
int ObPrivExecutor::get_next_row(const ObRow *&row)
{
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}
int ObPrivExecutor::open()
{
  int ret = OB_SUCCESS;
  context_->disable_privilege_check_ = true;
  context_->schema_manager_ = context_->merger_schema_mgr_->get_user_schema(0);
  if (context_->schema_manager_ == NULL)
  {
    ret = OB_SCHEMA_ERROR;
    TBSYS_LOG(WARN, "get schema error, ret=%d", ret);
  }
  else
  {
    switch(stmt_->get_stmt_type())
    {
      case ObBasicStmt::T_CREATE_USER:
        ret = do_create_user(stmt_);
        break;
      case ObBasicStmt::T_DROP_USER:
        ret = do_drop_user(stmt_);
        break;
      case ObBasicStmt::T_SET_PASSWORD:
        ret = do_set_password(stmt_);
        break;
      case ObBasicStmt::T_LOCK_USER:
        ret = do_lock_user(stmt_);
        break;
      case ObBasicStmt::T_GRANT:
        ret = do_grant_privilege_v3(stmt_);
        break;
      case ObBasicStmt::T_REVOKE:
        ret = do_revoke_privilege_v2(stmt_);
        break;
      case ObBasicStmt::T_RENAME_USER:
        ret = do_rename_user(stmt_);
        break;
      //add wenghaixing [database manage]20150609
      case ObBasicStmt::T_CREATE_DATABASE:
        ret = do_create_db(stmt_);
        break;
      case ObBasicStmt::T_USE_DATABASE:
        ret = do_use_db(stmt_);
        break;
      case ObBasicStmt::T_DROP_DATABASE:
        ret = do_drop_db(stmt_);
        break;
      //add e
      default:
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(DEBUG, "not a privilege-related sql, ret=%d", ret);
        break;
    };
    int err = context_->merger_schema_mgr_->release_schema(context_->schema_manager_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "release schema failed,ret=%d", err);
    }
    context_->disable_privilege_check_ = false;
  }
  return ret;
}
int ObPrivExecutor::close()
{
  return OB_SUCCESS;
}
int ObPrivExecutor::get_all_columns_by_user_name(const ObString &user_name, UserInfo &user_info)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char select_users_buff[512];
  ObString select_user;
  // 13个整数列
  databuff_printf(select_users_buff, 512, pos, "SELECT user_id,priv_all,priv_alter,priv_create,priv_create_user,\
      priv_delete, priv_drop, priv_grant_option,priv_insert, priv_update, priv_select, priv_replace,is_locked,pass_word,info FROM \
      __all_user WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
  if (pos >= 511)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    select_user.assign_ptr(select_users_buff, static_cast<ObString::obstr_size_t>(pos));
    ObResultSet result2;
    context_->session_info_->set_current_result_set(&result2);
    if (OB_SUCCESS != (ret = result2.init()))
    {
      TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_user, result2, *context_)))
    {
      TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_user.length(), select_user.ptr(), ret);
      result_set_out_->set_message(result2.get_message());
    }
    else if (OB_SUCCESS != (ret = result2.open()))
    {
      TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_user.length(), select_user.ptr(), ret);
    }
    else
    {
      OB_ASSERT(result2.is_with_rows() == true);
      const ObRow* row = NULL;
      ret = result2.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_ERR_USER_NOT_EXIST;
        result_set_out_->set_message("user not exist");
        TBSYS_LOG(WARN, "user not exist");
      }
      else if (OB_SUCCESS == ret)
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        // 13个整数列
        int field_count = 13;
        int i = 0;
        for (i = 0;i < field_count; ++i)
        {
          int64_t field_value = 0;
          if (OB_SUCCESS != (ret = row->raw_get_cell(i, pcell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
            break;
          }
          else if (pcell->get_type() != ObIntType)
          {
            if (pcell->get_type() == ObNullType)
            {
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
              break;
            }
          }
          else if (OB_SUCCESS != (ret = pcell->get_int(field_value)))
          {
            TBSYS_LOG(WARN, "get value from cell failed, ret=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = user_info.field_values_.push_back(field_value)))
          {
            TBSYS_LOG(WARN, "push field value to array failed, ret=%d", ret);
            break;
          }
        }
        // 得到两个string字段，password和comment
        if (OB_SUCCESS == ret)
        {
          ObString tmp;
          if (OB_SUCCESS != (ret = row->raw_get_cell(i, pcell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
          }
          else if (pcell->get_type() != ObVarcharType)
          {
            if (pcell->get_type() == ObNullType)
            {
              user_info.password_ = ObString::make_string("");
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "type is not expected, type=%d, ret=%d", pcell->get_type(), ret);
            }
          }
          else if (OB_SUCCESS != (ret = pcell->get_varchar(tmp)))
          {
            TBSYS_LOG(WARN, "get value from cell failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = ob_write_string(page_arena_, tmp, user_info.password_)))
          {
            TBSYS_LOG(WARN, "ob_write_string failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = row->raw_get_cell(i + 1, pcell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
          }
          else if (pcell->get_type() != ObVarcharType)
          {
            if (pcell->get_type() == ObNullType)
            {
              user_info.comment_ = ObString::make_string("");
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "type is not expected, type=%d, ret=%d", pcell->get_type(), ret);
            }
          }
          else if (OB_SUCCESS != (ret = pcell->get_varchar(tmp)))
          {
            TBSYS_LOG(WARN, "get value from cell failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = ob_write_string(page_arena_, tmp, user_info.comment_)))
          {
            TBSYS_LOG(WARN, "ob_write_string failed, ret=%d", ret);
          }
        }
      }
      int err = result2.close();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
      }
      result2.reset();
    }
    context_->session_info_->set_current_result_set(result_set_out_);
  }
  return ret;
}
void ObPrivExecutor::construct_replace_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges)
{
  ObString replace_prefix = ObString::make_string("REPLACE INTO __all_table_privilege(user_id, table_id,");
  databuff_printf(buf, buf_size, pos, "%.*s", replace_prefix.length(), replace_prefix.ptr());
  int i = 0;
  for (i = 0;i < privileges->count();++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      databuff_printf(buf, buf_size, pos, "priv_all,");
    }
    else if (privilege == OB_PRIV_ALTER)
    {
      databuff_printf(buf, buf_size, pos, "priv_alter,");
    }
    else if (privilege == OB_PRIV_CREATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_create,");
    }
    else if (privilege == OB_PRIV_CREATE_USER)
    {
      databuff_printf(buf, buf_size, pos, "priv_create_user,");
    }
    else if (privilege == OB_PRIV_DELETE)
    {
      databuff_printf(buf, buf_size, pos, "priv_delete,");
    }
    else if (privilege == OB_PRIV_DROP)
    {
      databuff_printf(buf, buf_size, pos, "priv_drop,");
    }
    else if (privilege == OB_PRIV_GRANT_OPTION)
    {
      databuff_printf(buf, buf_size, pos, "priv_grant_option,");
    }
    else if (privilege == OB_PRIV_INSERT)
    {
      databuff_printf(buf, buf_size, pos, "priv_insert,");
    }
    else if (privilege == OB_PRIV_UPDATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_update,");
    }
    else if (privilege == OB_PRIV_SELECT)
    {
      databuff_printf(buf, buf_size, pos, "priv_select,");
    }
    else if (privilege == OB_PRIV_REPLACE)
    {
      databuff_printf(buf, buf_size, pos, "priv_replace,");
    }
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ") VALUES (%ld, %lu,", user_id, table_id);
  for (i = 0;i < privileges->count();++i)
  {
    databuff_printf(buf, buf_size, pos, "1,");
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ")");
}

void ObPrivExecutor::construct_replace_db_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges)
{
  ObString replace_prefix = ObString::make_string("REPLACE INTO __all_database_privilege(user_id, db_id,");
  databuff_printf(buf, buf_size, pos, "%.*s", replace_prefix.length(), replace_prefix.ptr());
  int i = 0;
  for (i = 0;i < privileges->count();++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      databuff_printf(buf, buf_size, pos, "priv_all,");
      //add liumz,
      databuff_printf(buf, buf_size, pos, "priv_alter,");
      databuff_printf(buf, buf_size, pos, "priv_create,");
      databuff_printf(buf, buf_size, pos, "priv_create_user,");
      databuff_printf(buf, buf_size, pos, "priv_delete,");
      databuff_printf(buf, buf_size, pos, "priv_drop,");
      databuff_printf(buf, buf_size, pos, "priv_insert,");
      databuff_printf(buf, buf_size, pos, "priv_update,");
      databuff_printf(buf, buf_size, pos, "priv_select,");
      databuff_printf(buf, buf_size, pos, "priv_replace,");
      //add:e
    }
    else if (privilege == OB_PRIV_ALTER)
    {
      databuff_printf(buf, buf_size, pos, "priv_alter,");
    }
    else if (privilege == OB_PRIV_CREATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_create,");
    }
    else if (privilege == OB_PRIV_CREATE_USER)
    {
      databuff_printf(buf, buf_size, pos, "priv_create_user,");
    }
    else if (privilege == OB_PRIV_DELETE)
    {
      databuff_printf(buf, buf_size, pos, "priv_delete,");
    }
    else if (privilege == OB_PRIV_DROP)
    {
      databuff_printf(buf, buf_size, pos, "priv_drop,");
    }
    else if (privilege == OB_PRIV_GRANT_OPTION)
    {
      databuff_printf(buf, buf_size, pos, "priv_grant_option,");
    }
    else if (privilege == OB_PRIV_INSERT)
    {
      databuff_printf(buf, buf_size, pos, "priv_insert,");
    }
    else if (privilege == OB_PRIV_UPDATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_update,");
    }
    else if (privilege == OB_PRIV_SELECT)
    {
      databuff_printf(buf, buf_size, pos, "priv_select,");
    }
    else if (privilege == OB_PRIV_REPLACE)
    {
      databuff_printf(buf, buf_size, pos, "priv_replace,");
    }
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ") VALUES (%ld, %ld,", user_id, db_id);
  for (i = 0;i < privileges->count();++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      databuff_printf(buf, buf_size, pos, "1,1,1,1,1,1,1,1,1,1,");

    }
    else
    {
      databuff_printf(buf, buf_size, pos, "1,");
    }
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ")");
}

void ObPrivExecutor::construct_replace_table_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, int64_t db_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges)
{
  ObString replace_prefix = ObString::make_string("REPLACE INTO __all_table_privilege(user_id, db_id, table_id,");
  databuff_printf(buf, buf_size, pos, "%.*s", replace_prefix.length(), replace_prefix.ptr());
  int i = 0;
  for (i = 0;i < privileges->count();++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      databuff_printf(buf, buf_size, pos, "priv_all,");
      //add liumz,
      databuff_printf(buf, buf_size, pos, "priv_alter,");
      databuff_printf(buf, buf_size, pos, "priv_create,");
      databuff_printf(buf, buf_size, pos, "priv_create_user,");
      databuff_printf(buf, buf_size, pos, "priv_delete,");
      databuff_printf(buf, buf_size, pos, "priv_drop,");
      databuff_printf(buf, buf_size, pos, "priv_insert,");
      databuff_printf(buf, buf_size, pos, "priv_update,");
      databuff_printf(buf, buf_size, pos, "priv_select,");
      databuff_printf(buf, buf_size, pos, "priv_replace,");
      //add:e
    }
    else if (privilege == OB_PRIV_ALTER)
    {
      databuff_printf(buf, buf_size, pos, "priv_alter,");
    }
    else if (privilege == OB_PRIV_CREATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_create,");
    }
    else if (privilege == OB_PRIV_CREATE_USER)
    {
      databuff_printf(buf, buf_size, pos, "priv_create_user,");
    }
    else if (privilege == OB_PRIV_DELETE)
    {
      databuff_printf(buf, buf_size, pos, "priv_delete,");
    }
    else if (privilege == OB_PRIV_DROP)
    {
      databuff_printf(buf, buf_size, pos, "priv_drop,");
    }
    else if (privilege == OB_PRIV_GRANT_OPTION)
    {
      databuff_printf(buf, buf_size, pos, "priv_grant_option,");
    }
    else if (privilege == OB_PRIV_INSERT)
    {
      databuff_printf(buf, buf_size, pos, "priv_insert,");
    }
    else if (privilege == OB_PRIV_UPDATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_update,");
    }
    else if (privilege == OB_PRIV_SELECT)
    {
      databuff_printf(buf, buf_size, pos, "priv_select,");
    }
    else if (privilege == OB_PRIV_REPLACE)
    {
      databuff_printf(buf, buf_size, pos, "priv_replace,");
    }
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ") VALUES (%ld, %ld, %lu,", user_id, db_id, table_id);
  for (i = 0;i < privileges->count();++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      databuff_printf(buf, buf_size, pos, "1,1,1,1,1,1,1,1,1,1,");
    }
    else
    {
      databuff_printf(buf, buf_size, pos, "1,");
    }
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ")");
}

/**
 * @synopsis  construct_update_expressions
 *
 * @param buf
 * @param buf_size
 * @param pos 从pos开始写数据
 * @param privileges 不能为NULL并且必须至少有一个元素
 * @param is_grant 如果true，则构造grant语句中的update，如果false，则构造revoke语句中的update
 * @returns
 */
void ObPrivExecutor::construct_update_expressions(char *buf, int buf_size, int64_t &pos, const ObArray<ObPrivilegeType> *privileges, bool is_grant)
{
  int ret = OB_SUCCESS;
  int64_t priv_count = privileges->count();
  // 如果GRANT OPTION以外的权限被revoke，all权限位需要被重置为0
  bool flag = false;
  int64_t i = 0;
  for (i = 0;i < priv_count;++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      if (is_grant)
      {
        databuff_printf(buf, buf_size, pos, "priv_all=1,");
        //add liumz,
        databuff_printf(buf, buf_size, pos, "priv_alter=1,");
        databuff_printf(buf, buf_size, pos, "priv_create=1,");
        databuff_printf(buf, buf_size, pos, "priv_create_user=1,");
        databuff_printf(buf, buf_size, pos, "priv_delete=1,");
        databuff_printf(buf, buf_size, pos, "priv_drop=1,");
        //databuff_printf(buf, buf_size, pos, "priv_grant_option=1,");
        databuff_printf(buf, buf_size, pos, "priv_insert=1,");
        databuff_printf(buf, buf_size, pos, "priv_update=1,");
        databuff_printf(buf, buf_size, pos, "priv_select=1,");
        databuff_printf(buf, buf_size, pos, "priv_replace=1,");
        //add:e
      }
      else
      {
        databuff_printf(buf, buf_size, pos, "priv_alter=0,");
        databuff_printf(buf, buf_size, pos, "priv_create=0,");
        databuff_printf(buf, buf_size, pos, "priv_create_user=0,");
        databuff_printf(buf, buf_size, pos, "priv_delete=0,");
        databuff_printf(buf, buf_size, pos, "priv_drop=0,");
        databuff_printf(buf, buf_size, pos, "priv_grant_option=0,");
        databuff_printf(buf, buf_size, pos, "priv_insert=0,");
        databuff_printf(buf, buf_size, pos, "priv_update=0,");
        databuff_printf(buf, buf_size, pos, "priv_select=0,");
        databuff_printf(buf, buf_size, pos, "priv_replace=0,");
        flag = true;
      }
    }
    else if (privilege == OB_PRIV_ALTER)
    {
      databuff_printf(buf, buf_size, pos, "priv_alter=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_CREATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_create=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_CREATE_USER)
    {
      databuff_printf(buf, buf_size, pos, "priv_create_user=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_DELETE)
    {
      databuff_printf(buf, buf_size, pos, "priv_delete=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_DROP)
    {
      databuff_printf(buf, buf_size, pos, "priv_drop=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_GRANT_OPTION)
    {
      databuff_printf(buf, buf_size, pos, "priv_grant_option=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_INSERT)
    {
      databuff_printf(buf, buf_size, pos, "priv_insert=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_UPDATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_update=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_SELECT)
    {
      databuff_printf(buf, buf_size, pos, "priv_select=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_REPLACE)
    {
      databuff_printf(buf, buf_size, pos, "priv_replace=%d,", is_grant ? 1 : 0);
      flag = true;
    }
  }
  // revoke语句使用
  if (true == flag && !is_grant)
  {
    databuff_printf(buf, buf_size, pos, "priv_all=0,");
  }
  if (pos >= buf_size - 1)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    //回退最后一个逗号
    pos = pos - 1;
  }
}
int ObPrivExecutor::insert_trigger()
{
  int ret = OB_SUCCESS;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  ObString sql;
  ObServer server;
  server.set_ipv4_addr(tbsys::CNetUtil::getLocalAddr(NULL), 0);
  char buf[OB_MAX_SQL_LENGTH] = "";
  sql.assign(buf, sizeof(buf));
  ret = ObInnerTableOperator::update_all_trigger_event(sql, timestamp, server, UPDATE_PRIVILEGE_TIMESTAMP_TRIGGER, 0);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get update all trigger event sql failed:ret[%d]", ret);
  }
  else
  {
    ret = execute_stmt_no_return_rows(sql);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "execute_stmt_no_return_rows failed:sql[%.*s], ret[%d]", sql.length(), sql.ptr(), ret);
    }
  }
  return ret;
}
int ObPrivExecutor::revoke_all_priv_from_table_privs_by_user_name(const ObString &user_name)
{
  int ret = OB_SUCCESS;
  int64_t user_id = -1;
  ObArray<uint64_t> table_ids;
  if (OB_SUCCESS != (ret = get_user_id_by_user_name(user_name, user_id)))
  {
    TBSYS_LOG(ERROR, "get user id by user name failed, user_name=%.*s, ret=%d", user_name.length(), user_name.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = get_table_ids_by_user_id(user_id, table_ids)))
  {
    TBSYS_LOG(ERROR, "get table ids from user id failed, user_id=%ld, ret=%d", user_id, ret);
  }
  else
  {
    int i = 0;
    for (i = 0;i < table_ids.count();++i)
    {
      uint64_t tid = table_ids.at(i);
      if (OB_SUCCESS != (ret = reset_table_privilege(user_id, tid, NULL)))
      {
        TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%lu, table_id=%lu, ret=%d", user_id, tid, ret);
        break;
      }
    }
  }
  return ret;
}
//add liumz, [bugfix: grant table level priv, revoke global level priv]20150902:b
int ObPrivExecutor::revoke_priv_from_table_privs_by_user_name_v2(const ObString &user_name, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  int64_t user_id = -1;
  ObArray<int64_t> db_ids;
  ObArray<uint64_t> table_ids;
  if (OB_SUCCESS != (ret = get_user_id_by_user_name(user_name, user_id)))
  {
    TBSYS_LOG(ERROR, "get user id by user name failed, user_name=%.*s, ret=%d", user_name.length(), user_name.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = get_table_ids_by_user_id_v2(user_id, db_ids, table_ids)))
  {
    TBSYS_LOG(ERROR, "get table ids from user id failed, user_id=%ld, ret=%d", user_id, ret);
  }
  else
  {
    int i = 0;
    for (i = 0;i < table_ids.count();++i)
    {
      int64_t db_id = db_ids.at(i);
      uint64_t tid = table_ids.at(i);
      if (OB_SUCCESS != (ret = reset_table_privilege_v2(user_id, db_id, tid, privileges)))
      {
        TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%lu, table_id=%lu, ret=%d", user_id, tid, ret);
        break;
      }
    }
  }
  return ret;
}
//add:e
int ObPrivExecutor::revoke_all_priv_from_db_privs_by_user_name(const ObString &user_name)
{
  int ret = OB_SUCCESS;
  int64_t user_id = -1;
  ObArray<int64_t> db_ids;
  if (OB_SUCCESS != (ret = get_user_id_by_user_name(user_name, user_id)))
  {
    TBSYS_LOG(ERROR, "get user id by user name failed, user_name=%.*s, ret=%d", user_name.length(), user_name.ptr(), ret);
  }
  else
  {
    if (OB_SUCCESS != (ret = get_db_ids_by_user_id(user_id, db_ids)))
    {
      TBSYS_LOG(ERROR, "get db ids from user id failed, user_id=%ld, ret=%d", user_id, ret);
    }
    else
    {
      for (int64_t db_index = 0;db_index < db_ids.count() && OB_LIKELY(OB_SUCCESS == ret);db_index++)
      {
        ObArray<uint64_t> table_ids;
        int64_t db_id = db_ids.at(db_index);
        if (OB_SUCCESS != (ret = reset_db_privilege(user_id, db_id, NULL)))
        {
          TBSYS_LOG(ERROR, "revoke privilege from __all_database_privilege failed, user_id=%ld, db_id=%ld, ret=%d", user_id, db_id, ret);
        }
        else if (OB_SUCCESS != (ret = get_table_ids_by_user_db_id(user_id, db_id, table_ids)))
        {
          TBSYS_LOG(ERROR, "get table ids from user id and db id failed, user_id=%ld, db_id=%ld ret=%d", user_id, db_id, ret);
        }
        else
        {
          for (int64_t table_index = 0; table_index < table_ids.count() && OB_LIKELY(OB_SUCCESS == ret);table_index++)
          {
            uint64_t tid = table_ids.at(table_index);
            if (OB_SUCCESS != (ret = reset_table_privilege_v2(user_id, db_id, tid, NULL)))
            {
              TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%ld, db_id=%ld, table_id=%lu, ret=%d", user_id, db_id, tid, ret);
            }
          }//table for
        }
      }//db for
    }
  }
  return ret;
}
int ObPrivExecutor::revoke_priv_from_db_privs_by_user_db_id(int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  if (OB_SUCCESS != (ret = reset_db_privilege(user_id, db_id, privileges)))
  {
    TBSYS_LOG(ERROR, "revoke database privs from user_id=%ld failed, db_id=%ld, ret=%d", user_id, db_id, ret);
  }
  else if (OB_SUCCESS != (ret = get_table_ids_by_user_db_id(user_id, db_id, table_ids)))
  {
    TBSYS_LOG(ERROR, "get table ids from __all_table_privilege by user id and db_id failed, user_id=%ld, db_id=%ld, ret=%d", user_id, db_id, ret);
  }
  for(int64_t table_index = 0; table_index < table_ids.count() && OB_LIKELY(OB_SUCCESS == ret); table_index++)
  {
    uint64_t tid = table_ids.at(table_index);
    if (OB_SUCCESS != (ret = reset_table_privilege_v2(user_id, db_id, tid, privileges)))
    {
      TBSYS_LOG(ERROR, "revoke table privs from user_id=%ld failed, db_id=%ld, tid=%lu, ret=%d", user_id, db_id, tid, ret);
    }
  }
  return ret;
}

int ObPrivExecutor::revoke_all_priv_from_users_by_user_name(const ObString &user_name)
{
  int ret = OB_SUCCESS;
  char update_users_buff[512];
  ObString update_users;
  int64_t pos = 0;
  databuff_printf(update_users_buff, 512, pos, "UPDATE __all_user SET priv_all=0, priv_alter=0,priv_create=0,priv_create_user=0,priv_delete=0,priv_drop=0,priv_grant_option=0,priv_insert=0, priv_update=0,priv_select=0,priv_replace=0 WHERE user_name=");
  databuff_printf(update_users_buff, 512, pos, "'%.*s'", user_name.length(), user_name.ptr());
  if (pos >= 511)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    update_users.assign_ptr(update_users_buff, static_cast<ObString::obstr_size_t>(pos));
    ret = execute_stmt_no_return_rows(update_users);
  }
  return ret;
}
int ObPrivExecutor::reset_db_privilege(int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString update_priv;
  char update_db_buff[512];
  // indicates revoke all privileges grant_option
  if (NULL == privileges)
  {
    databuff_printf(update_db_buff, 512, pos, "UPDATE __all_database_privilege SET priv_all=0, priv_alter=0,priv_create=0,priv_create_user=0,priv_delete=0,priv_drop=0,priv_grant_option=0,priv_insert=0, priv_update=0,priv_select=0,priv_replace=0 WHERE user_id=%ld and db_id=%ld", user_id, db_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_db_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  else
  {
    databuff_printf(update_db_buff, 512, pos, "UPDATE __all_database_privilege SET ");
    construct_update_expressions(update_db_buff, 512, pos, privileges, false);
    databuff_printf(update_db_buff, 512, pos, "WHERE user_id=%ld and db_id=%ld", user_id, db_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_db_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = execute_stmt_no_return_rows(update_priv);
  }
  return ret;
}
int ObPrivExecutor::reset_table_privilege_v2(int64_t user_id, int64_t db_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString update_priv;
  char update_table_buff[512];
  // indicates revoke all privileges grant_option
  if (NULL == privileges)
  {
    databuff_printf(update_table_buff, 512, pos, "UPDATE __all_table_privilege SET priv_all=0, priv_alter=0,priv_create=0,priv_create_user=0,priv_delete=0,priv_drop=0,priv_grant_option=0,priv_insert=0, priv_update=0,priv_select=0,priv_replace=0 WHERE user_id=%ld and db_id=%ld and table_id=%lu", user_id, db_id, table_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_table_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  else
  {
    databuff_printf(update_table_buff, 512, pos, "UPDATE __all_table_privilege SET ");
    construct_update_expressions(update_table_buff, 512, pos, privileges, false);
    databuff_printf(update_table_buff, 512, pos, " WHERE user_id=%ld and db_id=%ld and table_id=%lu", user_id, db_id, table_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_table_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = execute_stmt_no_return_rows(update_priv);
  }
  return ret;
}
int ObPrivExecutor::reset_table_privilege(int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString update_priv;
  char update_table_buff[512];
  // indicates revoke all privileges grant_option
  if (NULL == privileges)
  {
    databuff_printf(update_table_buff, 512, pos, "UPDATE __all_table_privilege SET priv_all=0, priv_alter=0,priv_create=0,priv_create_user=0,priv_delete=0,priv_drop=0,priv_grant_option=0,priv_insert=0, priv_update=0,priv_select=0,priv_replace=0 WHERE user_id=%ld and table_id=%lu", user_id, table_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_table_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  else
  {
    databuff_printf(update_table_buff, 512, pos, "UPDATE __all_table_privilege SET ");
    construct_update_expressions(update_table_buff, 512, pos, privileges, false);
    databuff_printf(update_table_buff, 512, pos, "WHERE user_id=%ld and table_id=%lu", user_id, table_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_table_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = execute_stmt_no_return_rows(update_priv);
  }
  return ret;
}

//add liumz, [multi_database.priv_management]:20150608:b
int ObPrivExecutor::get_db_ids_by_user_id(int64_t user_id, ObArray<int64_t> &db_ids)
{
  int ret = OB_SUCCESS;
  char select_db_id_buff[512];
  int cnt = snprintf(select_db_id_buff, 512, "SELECT db_id from __all_database_privilege where user_id=%ld", user_id);
  ObString select_db_id;
  select_db_id.assign_ptr(select_db_id_buff, cnt);
  ObResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_db_id, tmp_result, *context_)))
  {
    context_->session_info_->get_current_result_set()->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_db_id.length(), select_db_id.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_db_id.length(), select_db_id.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == true);
    const ObRow* row = NULL;
    while (true)
    {
      ret = tmp_result.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            int64_t db_id = -1;
            if (OB_SUCCESS != (ret = pcell->get_int(db_id)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
            else
            {
              if (OB_SUCCESS != (ret = db_ids.push_back(db_id)))
              {
                TBSYS_LOG(WARN, "push db id=%ld to array failed, ret=%d", db_id, ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(db_id) failed, ret=%d", ret);
        }
      }
    }// while
  }
  return ret;
}

int ObPrivExecutor::get_table_ids_by_user_db_id(int64_t user_id, int64_t db_id, ObArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  char select_table_id_buff[512];
  int cnt = snprintf(select_table_id_buff, 512, "SELECT table_id from __all_table_privilege where user_id=%ld and db_id=%ld", user_id, db_id);
  ObString select_table_id;
  select_table_id.assign_ptr(select_table_id_buff, cnt);
  ObResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_table_id, tmp_result, *context_)))
  {
    context_->session_info_->get_current_result_set()->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == true);
    const ObRow* row = NULL;
    while (true)
    {
      ret = tmp_result.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            int64_t tid = -1;
            if (OB_SUCCESS != (ret = pcell->get_int(tid)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
            else
            {
              table_id = static_cast<uint64_t>(tid);
              if (OB_SUCCESS != (ret = table_ids.push_back(table_id)))
              {
                TBSYS_LOG(WARN, "push table id=%lu to array failed, ret=%d", table_id, ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(table_id) failed, ret=%d", ret);
        }
      }
    }// while
  }
  return ret;
}
//add:e

int ObPrivExecutor::get_table_ids_by_user_id(int64_t user_id, ObArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  char select_table_id_buff[512];
  int cnt = snprintf(select_table_id_buff, 512, "SELECT table_id from __all_table_privilege where user_id=%ld", user_id);
  ObString select_table_id;
  select_table_id.assign_ptr(select_table_id_buff, cnt);
  ObResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_table_id, tmp_result, *context_)))
  {
    context_->session_info_->get_current_result_set()->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == true);
    const ObRow* row = NULL;
    while (true)
    {
      ret = tmp_result.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            int64_t tid = -1;
            if (OB_SUCCESS != (ret = pcell->get_int(tid)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
            else
            {
              table_id = static_cast<uint64_t>(tid);
              if (OB_SUCCESS != (ret = table_ids.push_back(table_id)))
              {
                TBSYS_LOG(WARN, "push table id=%lu to array failed, ret=%d", table_id, ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(table_id) failed, ret=%d", ret);
        }
      }
    }// while
  }
  return ret;
}

//add liumz, [bugfix: grant table level priv, revoke global level priv]20150902:b
int ObPrivExecutor::get_table_ids_by_user_id_v2(int64_t user_id, ObArray<int64_t> &db_ids, ObArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  char select_table_id_buff[512];
  int cnt = snprintf(select_table_id_buff, 512, "SELECT table_id, db_id from __all_table_privilege where user_id=%ld", user_id);
  ObString select_table_id;
  select_table_id.assign_ptr(select_table_id_buff, cnt);
  ObResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_table_id, tmp_result, *context_)))
  {
    context_->session_info_->get_current_result_set()->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == true);
    const ObRow* row = NULL;
    while (true)
    {
      ret = tmp_result.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        if (OB_SUCCESS == ret)
        {
          ret = row->raw_get_cell(0, pcell, table_id, column_id);
          if (pcell->get_type() == ObIntType)
          {
            int64_t tid = -1;
            if (OB_SUCCESS != (ret = pcell->get_int(tid)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
            else
            {
              table_id = static_cast<uint64_t>(tid);
              if (OB_SUCCESS != (ret = table_ids.push_back(table_id)))
              {
                TBSYS_LOG(WARN, "push table id=%lu to array failed, ret=%d", table_id, ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(table_id) failed, ret=%d", ret);
        }

        if (OB_SUCCESS == ret)
        {
          ret = row->raw_get_cell(1, pcell, table_id, column_id);
          if (pcell->get_type() == ObIntType)
          {
            int64_t db_id = -1;
            if (OB_SUCCESS != (ret = pcell->get_int(db_id)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
            else
            {
              if (OB_SUCCESS != (ret = db_ids.push_back(db_id)))
              {
                TBSYS_LOG(WARN, "push db id=%ld to array failed, ret=%d", db_id, ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(db_id) failed, ret=%d", ret);
        }

      }
    }// while
  }
  return ret;
}
//add:e

int ObPrivExecutor::execute_update_user(const ObString &update_user)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(update_user, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", update_user.length(), update_user.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", update_user.length(), update_user.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int64_t affected_rows = tmp_result.get_affected_rows();
    if (affected_rows == 0)
    {
      TBSYS_LOG(WARN, "user not exist, sql=%.*s", update_user.length(), update_user.ptr());
      result_set_out_->set_message("user not exist");
      ret = OB_ERR_USER_NOT_EXIST;
    }
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(&tmp_result);
  return ret;
}

//add wenghaixing [database manage]20150616
int ObPrivExecutor::execute_delete_database(const ObString &delete_db)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if(OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret = %d", ret);
  }
  else if(OB_SUCCESS != (ret = ObSql::direct_execute(delete_db, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d",delete_db.length(),delete_db.ptr(), ret);
  }
  else if(OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", delete_db.length(), delete_db.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int64_t affected_rows = tmp_result.get_affected_rows();
    if(affected_rows == 0)
    {
      TBSYS_LOG(WARN,"delete db failed,sql = %.*s", delete_db.length(), delete_db.ptr());
      result_set_out_->set_message("delete db failed");
      ret = OB_ERR_DATABASE_NOT_EXSIT;
    }
    int err = tmp_result.close();
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}
//add e
int ObPrivExecutor::execute_delete_user(const ObString &delete_user)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(delete_user, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", delete_user.length(), delete_user.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", delete_user.length(), delete_user.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int64_t affected_rows = tmp_result.get_affected_rows();
    if (affected_rows == 0)
    {
      TBSYS_LOG(WARN, "delete user failed, sql=%.*s", delete_user.length(), delete_user.ptr());
      result_set_out_->set_message("delete user failed");
      ret = OB_ERR_USER_NOT_EXIST;
    }
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}
int ObPrivExecutor::execute_stmt_no_return_rows(const ObString &stmt)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(stmt, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}
int ObPrivExecutor::get_user_id_by_user_name(const ObString &user_name, int64_t &user_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString select_user_id_prefix = ObString::make_string("select user_id from __all_user where user_name = '");
  char select_user_id_buff[512];
  ObString select_user_id;
  databuff_printf(select_user_id_buff, 512, pos, "select user_id from __all_user where user_name = '%.*s'", user_name.length(), user_name.ptr());
  if (pos >= 511)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    select_user_id.assign_ptr(select_user_id_buff, static_cast<ObString::obstr_size_t>(pos));
    ObResultSet result2;
    context_->session_info_->set_current_result_set(&result2);
    if (OB_SUCCESS != (ret = result2.init()))
    {
      TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_user_id, result2, *context_)))
    {
      TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
      result_set_out_->set_message(result2.get_message());
    }
    else if (OB_SUCCESS != (ret = result2.open()))
    {
      TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
    }
    else
    {
      OB_ASSERT(result2.is_with_rows() == true);
      const ObRow* row = NULL;
      ret = result2.get_next_row(row);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(WARN, "user_name: %.*s not exists, ret=%d", user_name.length(), user_name.ptr(), ret);
          ret = OB_ERR_USER_NOT_EXIST;
          result_set_out_->set_message("user not exists");
        }
        else
        {
          TBSYS_LOG(WARN, "next row from ObResultSet failed,ret=%d", ret);
        }
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            if (OB_SUCCESS != (ret = pcell->get_int(user_id)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from __all_user row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(user_id) from __all_user failed, ret=%d", ret);
        }
      }
      int err = result2.close();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
      }
      result2.reset();
    }
    context_->session_info_->set_current_result_set(result_set_out_);
  }
  return ret;
}
//add liumz, [multi_database.priv_management]:20150608:b
int ObPrivExecutor::get_db_id_by_db_name(const ObString &db_name, int64_t &db_id)
{
  //add liumz, [bugfix: grant global level priv, sys table]20150902
  if (db_name.length() == 0)
  {
    db_id = -1;//sys database
    return OB_SUCCESS;
  }
  //add:e
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString select_db_id_prefix = ObString::make_string("select db_id from __all_database where db_name = '");
  char select_db_id_buff[512];
  ObString select_db_id;
  databuff_printf(select_db_id_buff, 512, pos, "select db_id from __all_database where db_name = '%.*s'", db_name.length(), db_name.ptr());
  if (pos >= 511)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    select_db_id.assign_ptr(select_db_id_buff, static_cast<ObString::obstr_size_t>(pos));
    ObResultSet result2;
    context_->session_info_->set_current_result_set(&result2);
    if (OB_SUCCESS != (ret = result2.init()))
    {
      TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_db_id, result2, *context_)))
    {
      TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_db_id.length(), select_db_id.ptr(), ret);
      result_set_out_->set_message(result2.get_message());
    }
    else if (OB_SUCCESS != (ret = result2.open()))
    {
      TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_db_id.length(), select_db_id.ptr(), ret);
    }
    else
    {
      OB_ASSERT(result2.is_with_rows() == true);
      const ObRow* row = NULL;
      ret = result2.get_next_row(row);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(WARN, "db_name: %.*s not exists, ret=%d", db_name.length(), db_name.ptr(), ret);
          ret = OB_ERR_DATABASE_NOT_EXSIT;
          result_set_out_->set_message("db not exists");
        }
        else
        {
          TBSYS_LOG(WARN, "next row from ObResultSet failed,ret=%d", ret);
        }
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            if (OB_SUCCESS != (ret = pcell->get_int(db_id)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from __all_database row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(db_id) from __all_database failed, ret=%d", ret);
        }
      }
      int err = result2.close();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
      }
      result2.reset();
    }
    context_->session_info_->set_current_result_set(result_set_out_);
  }
  return ret;
}
//add:e
int ObPrivExecutor::do_revoke_privilege(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObRevokeStmt *revoke_stmt = dynamic_cast<const ObRevokeStmt*>(stmt);
  if (OB_UNLIKELY(NULL == revoke_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast revoke stmt failed, ret=%d", ret);
  }
  else
  {
    // start transaction
    // user_name->user_id:   select user_id from __users for update where user_name = 'user1';
    // update __users表
    // 根据user_id 查__table_privileges表，得到所有table_id
    // 根据user_id和table_id update __table_privileges表
    // commit
    // insert trigger table

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      ObString user_name;
      const ObStrings *users = revoke_stmt->get_users();
      int64_t user_count = users->count();
      int64_t user_id = -1;
      int i = 0;
      for (i = 0;i < user_count;++i)
      {
        // step2: username->user_id select user_id from __users for update where user_name = 'user1'
        user_id = -1;
        pos = 0;
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from grant stmt failed, ret=%d", ret);
          break;
        }
        else
        {
          // user_name->user_id
          ret = get_user_id_by_user_name(user_name, user_id);
        }
        // so far, we got user_id
        // step3 : update __users and  __table_privileges
        if (OB_SUCCESS == ret)
        {
          char update_table_priv_buff[512];
          ObString update_table_priv;
          update_table_priv.assign_ptr(update_table_priv_buff, 512);

          char update_priv_buff[512];
          ObString update_priv;

          uint64_t table_id = revoke_stmt->get_table_id();
          // revoke xx,xx on * from user
          // revoke xx, xx on table_name from user
          if ((OB_NOT_EXIST_TABLE_TID == table_id) || (table_id != OB_INVALID_ID))
          {
            // step 3.1: 在__users表中清除全局权限
            const ObArray<ObPrivilegeType> *privileges = revoke_stmt->get_privileges();
            databuff_printf(update_priv_buff, 512, pos, "UPDATE __all_user SET ");
            construct_update_expressions(update_priv_buff, 512, pos, privileges, false);
            databuff_printf(update_priv_buff, 512, pos, " WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
            if (pos >= 511)
            {
              // overflow
              ret = OB_BUF_NOT_ENOUGH;
              TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
            }
            else
            {
              //update __users表
              update_priv.assign_ptr(update_priv_buff, static_cast<ObString::obstr_size_t>(pos));
              ret = execute_stmt_no_return_rows(update_priv);
            }
            if (OB_SUCCESS == ret)
            {
              // revoke xx,xx on * from user
              if (OB_NOT_EXIST_TABLE_TID == table_id)
              {
                // select table_id from __table_privileges where user_id =
                // 从__table_privileges表中reset局部权限，将与user_name这个用户相关的所有的记录的这几个权限全部清 0
                ObArray<uint64_t> table_ids;
                ret = get_table_ids_by_user_id(user_id, table_ids);
                if (OB_SUCCESS == ret)
                {
                  char replace_table_priv_buff[512];
                  ObString replace_table_priv;
                  replace_table_priv.assign_buffer(replace_table_priv_buff, 512);
                  for (int i = 0;i < table_ids.count();++i)
                  {
                    uint64_t tid = table_ids.at(i);
                    if (OB_SUCCESS != (ret = reset_table_privilege(user_id, tid, privileges)))
                    {
                      TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%lu, table_id=%lu, ret=%d", user_id, tid, ret);
                      break;
                    }
                  }
                }
              }
              //revoke xx, xx on table_name from user
              else
              {
                if (OB_SUCCESS != (ret = reset_table_privilege(user_id, table_id, privileges)))
                {
                  TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%ld, table_id=%lu, ret=%d", user_id, table_id, ret);
                }
              }
            }
          }
          // revoke ALL PRIVILEGES, GRANT OPTION from user, only support this syntax
          // e.g. revoke select from user is not a valid syntax
          else if (OB_INVALID_ID == table_id)
          {
            if (OB_SUCCESS != (ret = revoke_all_priv_from_users_by_user_name(user_name)))
            {
              TBSYS_LOG(ERROR, "revoke all users priv from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
            }
            else if (OB_SUCCESS != (ret = revoke_all_priv_from_table_privs_by_user_name(user_name)))
            {
              TBSYS_LOG(ERROR, "revoke all table privs from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
            }
          }
        }
      }// for (i = 0;i < user_count;++i)
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
      if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = insert_trigger())))
      {
        TBSYS_LOG(ERROR, "insert trigger  failed,ret=%d", ret);
      }
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

int ObPrivExecutor::do_revoke_privilege_v2(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObRevokeStmt *revoke_stmt = dynamic_cast<const ObRevokeStmt*>(stmt);
  if (OB_UNLIKELY(NULL == revoke_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast revoke stmt failed, ret=%d", ret);
  }
  else
  {
    // start transaction
    // user_name->user_id:   select user_id from __users for update where user_name = 'user1';
    // update __users表
    // 根据user_id 查__table_privileges表，得到所有table_id
    // 根据user_id和table_id update __table_privileges表
    // commit
    // insert trigger table

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      ObString user_name;
      const ObStrings *users = revoke_stmt->get_users();
      int64_t user_count = users->count();
      int64_t user_id = -1;
      int i = 0;
      for (i = 0;i < user_count && OB_LIKELY(OB_SUCCESS == ret);++i)
      {
        // step2: username->user_id select user_id from __users for update where user_name = 'user1'
        user_id = -1;
        pos = 0;
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from grant stmt failed, ret=%d", ret);
          break;
        }
        else
        {
          // user_name->user_id
          ret = get_user_id_by_user_name(user_name, user_id);
        }
        // so far, we got user_id
        // step3 : update __users and  __table_privileges
        if (OB_SUCCESS == ret)
        {
          char update_priv_buff[512];
          ObString update_priv;

          uint64_t table_id = revoke_stmt->get_table_id();
          // revoke xx,xx on * from user; global level priv
          // revoke xx,xx on db_name.* from user; db level priv
          // revoke xx,xx on table_name from user; table level priv
          // case A: revoke ALL PRIVILEGES, GRANT OPTION from user之外的revoke操作
          if (table_id != OB_INVALID_ID)
          {
            // step 3.1: 在__users表中清除全局权限
            const ObArray<ObPrivilegeType> *privileges = revoke_stmt->get_privileges();
            databuff_printf(update_priv_buff, 512, pos, "UPDATE __all_user SET ");
            construct_update_expressions(update_priv_buff, 512, pos, privileges, false);
            databuff_printf(update_priv_buff, 512, pos, " WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
            if (pos >= 511)
            {
              // overflow
              ret = OB_BUF_NOT_ENOUGH;
              TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
            }
            else
            {
              //update __users表
              update_priv.assign_ptr(update_priv_buff, static_cast<ObString::obstr_size_t>(pos));
              ret = execute_stmt_no_return_rows(update_priv);
            }
            if (OB_SUCCESS == ret)
            {
              //case1: 全局权限
              if (revoke_stmt->is_global_priv() && (OB_NOT_EXIST_TABLE_TID == table_id))
              {
                //revoke xx,xx on * from user
                //for all dbs and all tables under the db
                ObArray<int64_t> db_ids;
                db_ids.push_back(-1);//add liumz, [bugfix: revoke gloabl level priv, sys table]20150902
                if (OB_SUCCESS != (ret = get_db_ids_by_user_id(user_id, db_ids)))
                {
                  TBSYS_LOG(ERROR, "get db ids from __all_database_privilege by user id failed, username=%.*s, ret=%d", user_name.length(), user_name.ptr(), ret);
                }
                for(int64_t db_index = 0; db_index < db_ids.count() && OB_LIKELY(OB_SUCCESS == ret); db_index++)
                {
                  int64_t db_id = db_ids.at(db_index);
                  //for one db & all tables under the db
                  if (OB_SUCCESS != (ret = revoke_priv_from_db_privs_by_user_db_id(user_id, db_id, privileges)))
                  {
                    TBSYS_LOG(ERROR, "revoke db&table privs from username=%.*s failed, db_id=%ld, ret=%d", user_name.length(), user_name.ptr(), db_id, ret);
                  }
                }//outer for
				//add liumz, [bugfix: grant table level priv, revoke global level priv]20150902:b
                if (OB_SUCCESS == ret && OB_SUCCESS != (ret = revoke_priv_from_table_privs_by_user_name_v2(user_name, privileges)))
                {
                  TBSYS_LOG(ERROR, "revoke table privs from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
                }
				//add:e
              }
              else if (!revoke_stmt->is_global_priv())
              {
                int64_t db_id = -1;
                const ObString &db_name = revoke_stmt->get_db_name();
                //case2: 库级权限
                if (OB_NOT_EXIST_TABLE_TID == table_id)
                {
                  //for one db & all tables under the db
                  //db_name->db_id
                  if (OB_SUCCESS != (ret = get_db_id_by_db_name(db_name, db_id)))
                  {
                    TBSYS_LOG(ERROR, "get db_id by db_name failed, ret=%d", ret);
                  }
                  else if (OB_SUCCESS != (ret = revoke_priv_from_db_privs_by_user_db_id(user_id, db_id, privileges)))
                  {
                    TBSYS_LOG(ERROR, "revoke db&table privs from username=%.*s failed, db_id=%ld, ret=%d", user_name.length(), user_name.ptr(), db_id, ret);
                  }
                }
                //case3: 表级权限
                else if (OB_INVALID_ID != table_id)
                {
                  //for one table
                  if (!IS_SYS_TABLE_ID(table_id))
                  {
                    if (OB_SUCCESS != (ret = get_db_id_by_db_name(db_name, db_id)))
                    {
                      TBSYS_LOG(ERROR, "get db_id by db_name failed, ret=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = reset_db_privilege(user_id, db_id, privileges)))
                    {
                      TBSYS_LOG(ERROR, "revoke database privs from username=%.*s failed, db_id=%ld, ret=%d", user_name.length(), user_name.ptr(), db_id, ret);
                    }
                  }
                  if (OB_SUCCESS == ret)
                  {
                    if (OB_SUCCESS != (ret = reset_table_privilege_v2(user_id, db_id, table_id, privileges)))
                    {
                      TBSYS_LOG(ERROR, "revoke table privs from username=%.*s failed, db_id=%ld, tid=%lu, ret=%d", user_name.length(), user_name.ptr(), db_id, table_id, ret);
                    }
                  }
                }
                else
                {
                  //shouldn't be here!
                }
              }
            }
          }
          // case B: revoke ALL PRIVILEGES, GRANT OPTION from user, only support this syntax
          // e.g. revoke select from user is not a valid syntax
          else if (OB_INVALID_ID == table_id)
          {
            if (OB_SUCCESS != (ret = revoke_all_priv_from_users_by_user_name(user_name)))
            {
              TBSYS_LOG(ERROR, "revoke all users priv from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
            }
            else if (OB_SUCCESS != (ret = revoke_all_priv_from_db_privs_by_user_name(user_name)))
            {
              TBSYS_LOG(ERROR, "revoke all database privs and table privs from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
            }
			//add liumz, [bugfix: grant table level priv, revoke global level priv]20150902:b
            else if (OB_SUCCESS != (ret = revoke_priv_from_table_privs_by_user_name_v2(user_name, NULL)))
            {
              TBSYS_LOG(ERROR, "revoke all table privs from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
            }
			//add:e
          }
        }
      }// for (i = 0;i < user_count;++i)
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
      //del liumz, [multi_database.priv_management]:20150627:b
      /*if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = insert_trigger())))
      {
        TBSYS_LOG(ERROR, "insert trigger  failed,ret=%d", ret);
      }*/
      //del:e
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

int ObPrivExecutor::do_grant_privilege(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObGrantStmt *grant_stmt = dynamic_cast<const ObGrantStmt*>(stmt);
  if (OB_UNLIKELY(NULL == grant_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast grant stmt failed, ret=%d", ret);
  }
  else
  {
    // start transaction
    // user_name->user_id:   select user_id from __users for update where user_name = 'user1';
    //
    // 看看有没有 on *  1. 如果有*，直接修改__users表 2. 如果没有* , 直接修改__table_privileges表
    // commit

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      char select_user_id_buff[512];
      ObString user_name;
      ObString select_user_id;
      ObString select_user_id_prefix = ObString::make_string("select user_id from __all_user where user_name = '");
      const ObStrings *users = grant_stmt->get_users();
      int64_t user_count = users->count();
      int64_t user_id = -1;
      int i = 0;
      for (i = 0;i < user_count;++i)
      {
        // step2: username->user_id select user_id from __users for update where user_name = 'user1'
        user_id = -1;
        pos = 0;
        select_user_id.assign_buffer(select_user_id_buff, 512);
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from grant stmt failed, ret=%d", ret);
          break;
        }
        else
        {
          // user_name->user_id
          ret = get_user_id_by_user_name(user_name, user_id);
        }
        // so far, we got user_id, used by __table_privileges
        // step3 : update __users or __table_privileges
        if (OB_SUCCESS == ret)
        {
          char update_priv_buff[512];
          ObString update_priv;
          //update_priv.assign_buffer(update_priv_buff, 512);
          uint64_t table_id = grant_stmt->get_table_id();
          // grant xx,xx on * to user
          if (OB_NOT_EXIST_TABLE_TID == table_id)
          {
            ObString update_priv_suffix = ObString::make_string(" WHERE user_name='");
            const ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
            databuff_printf(update_priv_buff, 512, pos, "UPDATE __all_user SET ");
            construct_update_expressions(update_priv_buff, 512, pos, privileges, true);
            databuff_printf(update_priv_buff, 512, pos, " WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
          }// grant xx,xx on * to user
          // grant xx, xx on table_name to user
          else
          {
            // only one table id
            const ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
            construct_replace_stmt(update_priv_buff, 512, pos, user_id, table_id, privileges);

          }// grant xx, xx on table_name to user
          if (pos >= 511)
          {
            // overflow
            ret = OB_BUF_NOT_ENOUGH;
            TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
          }
          else
          {
            // execute this statement
            update_priv.assign_ptr(update_priv_buff, static_cast<ObString::obstr_size_t>(pos));
            ret = execute_stmt_no_return_rows(update_priv);
          }
        }
      }// for (i = 0;i < user_count;++i)
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

int ObPrivExecutor::do_grant_privilege_v2(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObGrantStmt *grant_stmt = dynamic_cast<const ObGrantStmt*>(stmt);
  if (OB_UNLIKELY(NULL == grant_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast grant stmt failed, ret=%d", ret);
  }
  else
  {
    // start transaction
    // user_name->user_id:   select user_id from __all_user for update where user_name = 'user1';
    // db_name->db_id: select db_id from __all_database for update where db_name = 'db1';
    // 看看有没有 on *  1. 如果只有*，直接修改__all_user表 2. 如果是db_name.*，直接修改__all_database_privilege 3. 如果没有* , 直接修改__all_table_privilege表
    // commit

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      char select_user_id_buff[512];
      ObString user_name;
      ObString select_user_id;
      ObString select_user_id_prefix = ObString::make_string("select user_id from __all_user where user_name = '");
      const ObStrings *users = grant_stmt->get_users();
      int64_t user_count = users->count();
      int64_t user_id = -1;
      int i = 0;
      for (i = 0;i < user_count && OB_LIKELY(OB_SUCCESS == ret);++i)
      {
        // step2: username->user_id select user_id from __users for update where user_name = 'user1'
        user_id = -1;
        pos = 0;
        select_user_id.assign_buffer(select_user_id_buff, 512);
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from grant stmt failed, ret=%d", ret);
          break;
        }
        else
        {
          // user_name->user_id
          ret = get_user_id_by_user_name(user_name, user_id);
        }
        // so far, we got user_id, used by __table_privileges
        // step3 : update __users or __table_privileges
        if (OB_SUCCESS == ret)
        {
          char update_priv_buff[512];
          ObString update_priv;
          //update_priv.assign_buffer(update_priv_buff, 512);
          uint64_t table_id = grant_stmt->get_table_id();
          const ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
          // grant xx,xx on * to user
          // case1: 全局权限
          if (grant_stmt->is_global_priv() && (OB_NOT_EXIST_TABLE_TID == table_id))
          {
            databuff_printf(update_priv_buff, 512, pos, "UPDATE __all_user SET ");
            construct_update_expressions(update_priv_buff, 512, pos, privileges, true);
            databuff_printf(update_priv_buff, 512, pos, " WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
          }
          else if (!grant_stmt->is_global_priv())
          {            
            int64_t db_id = -1;
            const ObString &db_name = grant_stmt->get_db_name();
            //db_name->db_id
            if (OB_SUCCESS != (ret = get_db_id_by_db_name(db_name, db_id)))
            {
              TBSYS_LOG(ERROR, "get db_id by db_name failed, ret=%d", ret);
            }
            //grant xx,xx on db_name.* to user
            //case2: 库级权限
            else if (OB_NOT_EXIST_TABLE_TID == table_id)
            {
              construct_replace_db_stmt(update_priv_buff, 512, pos, user_id, db_id, privileges);
            }
            //grant xx,xx on db_name.table_name to user
            //case3: 表级权限
            else if (OB_INVALID_ID != table_id)
            {           
              construct_replace_table_stmt(update_priv_buff, 512, pos, user_id, db_id, table_id, privileges);
            }
            else
            {
              //shouldn't be here!
            }
          }
          if (OB_SUCCESS == ret)
          {
            if (pos >= 511)
            {
              // overflow
              ret = OB_BUF_NOT_ENOUGH;
              TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
            }
            else
            {
              // execute this statement
              update_priv.assign_ptr(update_priv_buff, static_cast<ObString::obstr_size_t>(pos));
              ret = execute_stmt_no_return_rows(update_priv);
            }
          }
        }//end if
      }// for (i = 0;i < user_count;++i)
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

int ObPrivExecutor::do_grant_privilege_v3(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObGrantStmt *grant_stmt = dynamic_cast<const ObGrantStmt*>(stmt);
  if (OB_UNLIKELY(NULL == grant_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast grant stmt failed, ret=%d", ret);
  }
  else
  {
    // start transaction
    // user_name->user_id:   select user_id from __all_user for update where user_name = 'user1';
    // db_name->db_id: select db_id from __all_database for update where db_name = 'db1';
    // 看看有没有 on *  1. 如果只有*，直接修改__all_user表 2. 如果是db_name.*，直接修改__all_database_privilege 3. 如果没有* , 直接修改__all_table_privilege表
    // commit

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      char select_user_id_buff[512];
      ObString user_name;
      ObString select_user_id;
      ObString select_user_id_prefix = ObString::make_string("select user_id from __all_user where user_name = '");
      const ObStrings *users = grant_stmt->get_users();
      int64_t user_count = users->count();
      int64_t user_id = -1;
      int i = 0;
      for (i = 0;i < user_count && OB_LIKELY(OB_SUCCESS == ret);++i)
      {
        // step2: username->user_id select user_id from __users for update where user_name = 'user1'
        user_id = -1;
        pos = 0;
        select_user_id.assign_buffer(select_user_id_buff, 512);
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from grant stmt failed, ret=%d", ret);
          break;
        }
        else
        {
          // user_name->user_id
          ret = get_user_id_by_user_name(user_name, user_id);
        }
        // so far, we got user_id, used by __table_privileges
        // step3 : update __users or __table_privileges
        if (OB_SUCCESS == ret)
        {
          char update_priv_buff[512];
          ObString update_priv;
          //update_priv.assign_buffer(update_priv_buff, 512);
          uint64_t table_id = grant_stmt->get_table_id();
          const ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
          // grant xx,xx on * to user
          // case1: 全局权限
          if (grant_stmt->is_global_priv() && (OB_NOT_EXIST_TABLE_TID == table_id))
          {
            databuff_printf(update_priv_buff, 512, pos, "UPDATE __all_user SET ");
            construct_update_expressions(update_priv_buff, 512, pos, privileges, true);
            databuff_printf(update_priv_buff, 512, pos, " WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
            if (pos >= 511)
            {
              ret = OB_BUF_NOT_ENOUGH;
              TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
            }
            else
            {
              // execute this statement
              update_priv.assign_ptr(update_priv_buff, static_cast<ObString::obstr_size_t>(pos));
              if (OB_SUCCESS != (ret = execute_stmt_no_return_rows(update_priv)))
              {
                TBSYS_LOG(WARN, "grant global privs to [%.*s] failed ,ret=%d", user_name.length(), user_name.ptr(), ret);
              }
              else if (OB_SUCCESS != (ret = grant_db_privs_by_user_id(user_id, privileges)))
              {
                TBSYS_LOG(WARN, "grant privs on all databases to [%.*s] failed ,ret=%d", user_name.length(), user_name.ptr(), ret);
              }
            }
          }
          else if (!grant_stmt->is_global_priv())
          {
            int64_t db_id = -1;
            const ObString &db_name = grant_stmt->get_db_name();
            //grant xx,xx on db_name.* to user
            //case2: 库级权限
            if (OB_NOT_EXIST_TABLE_TID == table_id)
            {              
              //db_name->db_id
              if (OB_SUCCESS != (ret = get_db_id_by_db_name(db_name, db_id)))
              {
                TBSYS_LOG(ERROR, "get db_id by db_name[%.*s] failed, ret=%d", db_name.length(), db_name.ptr(), ret);
              }
              else if (OB_SUCCESS != (ret = grant_db_privs_by_user_id_db_name(user_id, db_name, privileges)))
              {
                TBSYS_LOG(WARN, "grant privs on [%.*s.*] to [%.*s] failed, ret=%d",
                          db_name.length(), db_name.ptr(), user_name.length(), user_name.ptr(), ret);
              }
            }
            //grant xx,xx on db_name.table_name to user
            //case3: 表级权限
            else if (OB_INVALID_ID != table_id)
            {
              if (!IS_SYS_TABLE_ID(table_id))
              {
                if (OB_SUCCESS != (ret = get_db_id_by_db_name(db_name, db_id)))
                {
                  TBSYS_LOG(ERROR, "get db_id by db_name[%.*s] failed, ret=%d", db_name.length(), db_name.ptr(), ret);
                }
              }
              if (OB_SUCCESS == ret && OB_SUCCESS != (ret = set_table_privilege(user_id, db_id, table_id, privileges)))
              {
                TBSYS_LOG(WARN, "grant table privs to [%.*s] failed, table_id=%lu, ret=%d", user_name.length(), user_name.ptr(), table_id, ret);
              }
            }
            else
            {
              //shouldn't be here!
            }
          }
        }//end if
      }// for (i = 0;i < user_count;++i)
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

//add liumz, [multi_database.priv_management]:20150617:b
int ObPrivExecutor::get_all_db_names(ObStrings &db_names)
{
  int ret = OB_SUCCESS;
  char select_db_name_buff[512];
  int cnt = snprintf(select_db_name_buff, 512, "SELECT db_name FROM __all_database");
  ObString select_db_name;
  select_db_name.assign_ptr(select_db_name_buff, cnt);
  ObResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_db_name, tmp_result, *context_)))
  {
    context_->session_info_->get_current_result_set()->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_db_name.length(), select_db_name.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_db_name.length(), select_db_name.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == true);
    const ObRow* row = NULL;
    while (true)
    {
      ret = tmp_result.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObVarcharType)
          {
            ObString db_name;
            if (OB_SUCCESS != (ret = pcell->get_varchar(db_name)))
            {
              TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
            }
            else
            {
              if (OB_SUCCESS != (ret = db_names.add_string(db_name)))
              {
                TBSYS_LOG(WARN, "push db_name[%.*s] to array failed, ret=%d", db_name.length(), db_name.ptr(), ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(db_id) failed, ret=%d", ret);
        }
      }
    }// while
  }
  return ret;
}

int ObPrivExecutor::get_all_table_ids_by_db_name(const ObString &db_name, ObArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  char select_db_name_buff[512];
  int cnt = snprintf(select_db_name_buff, 512, "SELECT table_id FROM __first_tablet_entry WHERE db_name='%.*s'", db_name.length(), db_name.ptr());
  ObString select_db_name;
  select_db_name.assign_ptr(select_db_name_buff, cnt);
  ObResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_db_name, tmp_result, *context_)))
  {
    context_->session_info_->get_current_result_set()->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_db_name.length(), select_db_name.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_db_name.length(), select_db_name.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == true);
    const ObRow* row = NULL;
    while (true)
    {
      ret = tmp_result.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            int64_t tid = -1;
            if (OB_SUCCESS != (ret = pcell->get_int(tid)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
            else
            {
              table_id = static_cast<uint64_t>(tid);
              if (OB_SUCCESS != (ret = table_ids.push_back(table_id)))
              {
                TBSYS_LOG(WARN, "push table id=%lu to array failed, ret=%d", table_id, ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(table_id) failed, ret=%d", ret);
        }
      }
    }// while
  }
  return ret;
}

int ObPrivExecutor::grant_db_privs_by_user_id(int64_t user_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  ObStrings db_names;
  db_names.add_string(ObString::make_string(""));//add liumz, [bugfix: grant global level priv, sys table]20150902
  if (OB_SUCCESS != (ret = get_all_db_names(db_names)))
  {
    TBSYS_LOG(ERROR, "get all db names from __all_database failed, ret=%d", ret);
  }
  for(int64_t db_index = 0; db_index < db_names.count() && OB_LIKELY(OB_SUCCESS == ret); db_index++)
  {
    ObString db_name;
    if (OB_SUCCESS != (ret = db_names.get_string(db_index, db_name)))
    {
      TBSYS_LOG(WARN, "get_string() failed, ret=%d", ret);
    }
    //for one db & all tables under the db
    else if (OB_SUCCESS != (ret = grant_db_privs_by_user_id_db_name(user_id, db_name, privileges)))
    {
      TBSYS_LOG(ERROR, "grant db&table privs to user_id=%ld failed, db_name=%.*s, ret=%d", user_id, db_name.length(), db_name.ptr(), ret);
    }
  }
  return ret;
}

int ObPrivExecutor::grant_db_privs_by_user_id_db_name(int64_t user_id, const ObString &db_name, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  int64_t db_id = -1;
  if (OB_SUCCESS != (ret = get_db_id_by_db_name(db_name, db_id)))
  {
    TBSYS_LOG(ERROR, "get db id from __all_database by db_name failed, db_name=%.*s, ret=%d", db_name.length(), db_name.ptr(), ret);
  }
  else if (-1 != db_id/*ignore sys db*/ && OB_SUCCESS != (ret = set_db_privilege(user_id, db_id, privileges)))
  {
    TBSYS_LOG(ERROR, "grant database privs to user_id=%ld failed, db_id=%ld, ret=%d", user_id, db_id, ret);
  }
  else if (OB_SUCCESS != (ret = get_all_table_ids_by_db_name(db_name, table_ids)))
  {
    TBSYS_LOG(ERROR, "get table ids from __first_tablet_entry by db_name failed, db_name=%.*s, ret=%d", db_name.length(), db_name.ptr(), ret);
  }
  for(int64_t table_index = 0; table_index < table_ids.count() && OB_LIKELY(OB_SUCCESS == ret); table_index++)
  {
    uint64_t tid = table_ids.at(table_index);
    if (OB_SUCCESS != (ret = set_table_privilege(user_id, db_id, tid, privileges)))
    {
      TBSYS_LOG(ERROR, "grant table privs to user_id=%ld, db_id=%ld, tid=%lu, ret=%d", user_id, db_id, tid, ret);
    }
  }
  return ret;
}

int ObPrivExecutor::set_db_privilege(int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString replace_priv;
  char replace_db_buff[512];
  if (NULL == privileges)
  {
    databuff_printf(replace_db_buff, 512, pos,
                    "REPLACE INTO __all_database_privilege(user_id,db_id,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace) values(%ld,%ld,1,1,1,1,1,1,1,1,1,1,1)", user_id, db_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      replace_priv.assign_ptr(replace_db_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  else
  {
    construct_replace_db_stmt(replace_db_buff, 512, pos, user_id, db_id, privileges);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      replace_priv.assign_ptr(replace_db_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = execute_stmt_no_return_rows(replace_priv);
  }
  return ret;
}

int ObPrivExecutor::set_table_privilege(int64_t user_id, int64_t db_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString replace_priv;
  char replace_db_buff[512];
  if (NULL == privileges)
  {
    databuff_printf(replace_db_buff, 512, pos,
                    "REPLACE INTO __all_table_privilege(user_id,db_id,table_id,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace) values(%ld,%ld,%ld,1,1,1,1,1,1,1,1,1,1,1)", user_id, db_id, table_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      replace_priv.assign_ptr(replace_db_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  else
  {
    construct_replace_table_stmt(replace_db_buff, 512, pos, user_id, db_id, table_id, privileges);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      replace_priv.assign_ptr(replace_db_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = execute_stmt_no_return_rows(replace_priv);
  }
  return ret;
}
//add:e

int ObPrivExecutor::do_drop_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDropUserStmt *drop_user_stmt = dynamic_cast<const ObDropUserStmt*>(stmt); 
  //add wenghaixing [database manage -> clean privilege]20150908
  const ObPrivilege::NameUserMap* user_name_map = (*(context_->pp_privilege_))->get_username_map_const();
  ObPrivilege::User user;
  uint64_t user_id = OB_INVALID_ID;
  ObArray<uint64_t> user_id_list;
  ObArray<int64_t> db_id_list;
  ObArray<uint64_t> table_id_list;
  //add e
  if (OB_UNLIKELY(NULL == drop_user_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast drop user stmt failed, ret=%d", ret);
  }
  else
  {
    // START TRANSACTION
    // delete from __users where user_name = 'user1'
    // commit;
    // insert trigger

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    // step 3: delete user
    else
    {
      const ObStrings *users = drop_user_stmt->get_users();
      int64_t user_count = users->count();
      ObString user_name;
      int i = 0;
      for (i = 0;i < user_count; ++i)
      {
        pos = 0;
        char delete_user_buff[512];
        ObString delete_user;
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from drop user stmt failed, ret=%d", ret);
          break;
        }
        else if (user_name == OB_ADMIN_USER_NAME)
        {
          ret = OB_ERR_NO_PRIVILEGE;
          result_set_out_->set_message("no privilege");
          TBSYS_LOG(ERROR, "drop admin failed, can't drop admin");
          break;
        }
        else
        {
          databuff_printf(delete_user_buff, 512, pos, "delete from __all_user where user_name = '%.*s'", user_name.length(), user_name.ptr());
          if (pos >= 511)
          {
            // overflow
            ret = OB_BUF_NOT_ENOUGH;
            TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
          }
          else
          {
            delete_user.assign_ptr(delete_user_buff, static_cast<ObString::obstr_size_t>(pos));
            //add wenghaixing [database manage -> clean privilege]20150908

            int err = OB_SUCCESS;
            if(hash::HASH_NOT_EXIST == (err = user_name_map->get(user_name,user)))
            {
              TBSYS_LOG(INFO,"cannot find user");
              err = OB_ERROR;
            }
            else if(hash::HASH_EXIST == err)
            {
              user_id_list.push_back(user.user_id_);
              err = OB_SUCCESS;
            }
            if(OB_SUCCESS == err)
            {
              db_id_list.clear();
              table_id_list.clear();
              if(OB_SUCCESS != (err = get_table_ids_by_user_id_v2(user.user_id_,db_id_list,table_id_list)))
              {
                TBSYS_LOG(WARN, "failed to get list of user[%ld]",user.user_id_);
              }
              else if(OB_SUCCESS != (err = clean_user_privilege(user.user_id_,db_id_list,table_id_list)))
              {
                TBSYS_LOG(WARN, "failed to clean user privilege,ret = %d", ret);
              }
              else
              {
                TBSYS_LOG(INFO, "clean user privileges");
              }

            }
            //add e
            ret = execute_delete_user(delete_user);
          }
        }
      }
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
    //add wenghaixing [database manage -> clean privilege]20150908
    if(OB_SUCCESS == ret)
    {
      start_transaction();
      for(int64_t i = 0; i < user_id_list.count();i++)
      {
        if(OB_SUCCESS != (ret = clean_database_privilege(user_id_list.at(i), 1)))
        {
          TBSYS_LOG(WARN,"clean database privilege failed user_id[%ld],ret[%d]",user_id, ret);
          break;
        }
        else
        {
          TBSYS_LOG(INFO,"clean privilege for user[%ld]",user_id_list.at(i));
        }
      }
      if(OB_SUCCESS == ret)
      {
        ret = commit();
      }
      else
      {
        int err = rollback();
        if(OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "rollback failed, ret = %d",err);
        }
      }
    }
    //add e
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);

  return ret;
}
int ObPrivExecutor::do_rename_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  const ObRenameUserStmt *rename_user_stmt = dynamic_cast<const ObRenameUserStmt*>(stmt);
  const common::ObStrings* rename_infos = rename_user_stmt->get_rename_infos();
  int64_t pos = 0;
  // step 1: start transaction
  // step 2: select * from __users where user_name =
  // step 3: delete from __users where user_name =
  // step 4: insert into __users() values()
  // step 5: commit
  // step 6: insert trigger table

  // step 1: start transaction and insert trigger
  if (OB_SUCCESS != (ret = start_transaction()))
  {
    TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
  }
  else
  {
    OB_ASSERT(rename_infos->count() % 2 == 0);
    ObString from_user;
    ObString to_user;
    for (int i = 0;i < rename_infos->count(); i = i + 2)
    {
      UserInfo user_info;
      pos = 0;
      if (OB_SUCCESS != (ret = rename_infos->get_string(i, from_user)))
      {
      }
      else if (OB_SUCCESS != (ret = rename_infos->get_string(i + 1, to_user)))
      {
      }
      else
      {
        ret = get_all_columns_by_user_name(from_user, user_info);
      }
      // delete
      if (OB_SUCCESS == ret)
      {
        char delete_user_buff[128];
        ObString delete_user;
        databuff_printf(delete_user_buff, 128, pos, "DELETE FROM __all_user WHERE user_name='%.*s'", from_user.length(), from_user.ptr());
        if (pos >= 127)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          delete_user.assign_ptr(delete_user_buff, static_cast<ObString::obstr_size_t>(pos));
          ret = execute_delete_user(delete_user);
        }
      }
      // insert
      if (OB_SUCCESS == ret)
      {
        pos = 0;
        char insert_user_buff[1024];
        ObString insert_user;
        databuff_printf(insert_user_buff, 1024, pos, "INSERT INTO __all_user(user_id,priv_all,priv_alter,priv_create,priv_create_user,\
                  priv_delete,priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace,is_locked,user_name,pass_word,info) \
                  VALUES(%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,'%.*s','%.*s', '%.*s')", user_info.field_values_.at(0), user_info.field_values_.at(1),
                    user_info.field_values_.at(2), user_info.field_values_.at(3),user_info.field_values_.at(4),
                    user_info.field_values_.at(5), user_info.field_values_.at(6),user_info.field_values_.at(7),
                    user_info.field_values_.at(8), user_info.field_values_.at(9),user_info.field_values_.at(10),
                    user_info.field_values_.at(11),user_info.field_values_.at(12),to_user.length(), to_user.ptr(),
                    user_info.password_.length(), user_info.password_.ptr(),
                    user_info.comment_.length(), user_info.comment_.ptr());
        if (pos >= 639)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          insert_user.assign_ptr(insert_user_buff, static_cast<ObString::obstr_size_t>(pos));
          TBSYS_LOG(INFO, "insert_user=%.*s", insert_user.length(), insert_user.ptr());
          ret = execute_stmt_no_return_rows(insert_user);
        }
      }
    }// for each from user and to user
  }
  if (OB_SUCCESS == ret)
  {
    ret = commit();
  }
  else
  {
    // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
    // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
    int err = rollback();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_set_password(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char encrypted_pass_buff[SCRAMBLE_LENGTH * 2 + 1];
  ObString encrypted_pass;
  encrypted_pass.assign_ptr(encrypted_pass_buff, SCRAMBLE_LENGTH * 2 + 1);
  ObString empty_user;
  ObString user;
  ObString pwd;
  const ObSetPasswordStmt *set_password_stmt = dynamic_cast<const ObSetPasswordStmt*>(stmt);
  if (OB_UNLIKELY(NULL == set_password_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", ret);
  }
  else
  {
    const common::ObStrings *user_pass = set_password_stmt->get_user_password();
    OB_ASSERT(user_pass->count() == 2);
    if (OB_SUCCESS != (ret = user_pass->get_string(0, user)))
    {
    }
    else if (OB_SUCCESS != (ret = user_pass->get_string(1, pwd)))
    {
    }
    else
    {
      if (user == empty_user)
      {
        user = context_->session_info_->get_user_name();
      }
      if (pwd.length() == 0)
      {
        ret = OB_ERR_PASSWORD_EMPTY;
        result_set_out_->set_message("password must not be empty");
        TBSYS_LOG(WARN, "password must not be empty");
      }
      else
      {
        ObEncryptedHelper::encrypt(encrypted_pass, pwd);
        encrypted_pass.assign_ptr(encrypted_pass_buff, SCRAMBLE_LENGTH * 2);
      }
      // got user and password
    }
    // step 1: start transaction
    // step 2: update __users
    // step 3: commit
    // step 4: insert trigger table
    if (OB_SUCCESS == ret)
    {
      // step 1: start transaction and insert trigger
      if (OB_SUCCESS != (ret = start_transaction()))
      {
        TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
      }
      else
      {
        char update_user_buff[512];
        ObString update_user;
        databuff_printf(update_user_buff, 512, pos, "UPDATE __all_user SET pass_word='%.*s' WHERE user_name='%.*s'",
            encrypted_pass.length(), encrypted_pass.ptr(), user.length(), user.ptr());
        if (pos >= 511)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          update_user.assign_ptr(update_user_buff, static_cast<ObString::obstr_size_t>(pos));
          ret = execute_update_user(update_user);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = commit();
      }
      else
      {
        // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
        // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
        int err = rollback();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
        }
        //if (OB_ERR_USER_NOT_EXIST == ret)
        //{
          //ret = OB_USER_NOT_EXIST;
        //}
      }
      //int64_t affected_rows = context_->session_info_->get_current_result_set()->get_affected_rows();
      //if (affected_rows == 0)
      //{
        //context_->session_info_->get_current_result_set()->set_message("user not exist");
        //TBSYS_LOG(WARN, "user %.*s not exists, ret=%d", user.length(), user.ptr(), ret);
      //}
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_lock_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObLockUserStmt *lock_user_stmt = dynamic_cast<const ObLockUserStmt*>(stmt);
  if (OB_UNLIKELY(NULL == lock_user_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast lock user stmt failed, ret=%d", ret);
  }
  else
  {
    // step1: start transaction
    // step2: update __users
    // step3: commit
    // step4: insert trigger table

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      const common::ObStrings* user = lock_user_stmt->get_user();
      OB_ASSERT(user->count() == 1);
      ObString user_name;
      if (OB_SUCCESS != (ret = user->get_string(0, user_name)))
      {
        TBSYS_LOG(WARN, "get user from lock user stmt failed, ret=%d", ret);
      }
      else
      {
        // step 2
        char update_user_buff[128];
        ObString update_user;
        databuff_printf(update_user_buff, 128, pos, "UPDATE __all_user SET is_locked=%d WHERE user_name='%.*s'",
            lock_user_stmt->is_locked(), user_name.length(), user_name.ptr());
        if (pos >= 127)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          update_user.assign_ptr(update_user_buff, static_cast<ObString::obstr_size_t>(pos));
          //mod liumz, [bugfix: lock user not exist]20150828:b
          //ret = execute_stmt_no_return_rows(update_user);
          ret = execute_update_user(update_user);
          //mod:e
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_create_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // hold return value of result.close()
  int err = OB_SUCCESS;
  const ObCreateUserStmt *create_user_stmt = dynamic_cast<const ObCreateUserStmt*>(stmt);
  if (OB_UNLIKELY(NULL == create_user_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast create user stmt failed, ret=%d", ret);
  }
  else
  {

     // step 1: START TRANSACTION;
     // step 2: insert trigger table
     // step 3: select value1 from __all_sys_stat where cluster_role = 0 and cluster_id = 0 and server_type = 0 and server_role = 0 and server_ipv4 = 0 and
     //         server_ipv6_high = 0 and server_ipv6_low = 0 and server_port = 0 and table_id = 0 and name = 'ob_max_user_id' for update
     // step 4: 对明文密码进行加密
     // step 5: insert into __users
     //         insert into __users
     // step 6: UPDATE __all_sys_stat set value1 = value1 + 用户个数 where cluster_role = 0 and cluster_id = 0 and server_type = 0 and server_role = 0
     //         and server_ipv4 = 0 and server_ipv6_high = 0 and server_ipv6_low = 0 and server_port = 0 and table_id = 0 and name = 'ob_max_user_id'
     // step 7: COMMIT;

    int64_t current_user_id = -1;
    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    // step2: get user id
    else
    {
      ObString select_user_id = ObString::make_string("select value from __all_sys_stat where cluster_id = 0 and name = 'ob_max_user_id'");
      ObResultSet result2;
      context_->session_info_->set_current_result_set(&result2);
      if (OB_SUCCESS != (ret = result2.init()))
      {
        TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_user_id, result2, *context_)))
      {
        TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
        result_set_out_->set_message(result2.get_message());
      }
      else if (OB_SUCCESS != (ret = result2.open()))
      {
        TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
      }
      else
      {
        OB_ASSERT(result2.is_with_rows() == true);
        const ObRow* row = NULL;
        ret = result2.get_next_row(row);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "next row from ObResultSet failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
        }
        else
        {
          ObExprObj in;
          ObExprObj out;
          const ObObj *pcell = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ret = row->raw_get_cell(0, pcell, table_id, column_id);
          if (OB_SUCCESS == ret)
          {
            in.assign(*pcell);
            ObObjCastParams params;
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObVarcharType][ObIntType](params, in, out)))
            {
              TBSYS_LOG(ERROR, "varchar cast to int failed, ret=%d", ret);
            }
            else
            {
              current_user_id = out.get_int();
            }
          }
          else
          {
            TBSYS_LOG(WARN, "raw get cell(ob_max_user_id) from row failed, ret=%d", ret);
          }
        }
        err = result2.close();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
        }
        result2.reset();
      }
      context_->session_info_->set_current_result_set(result_set_out_);
    }
    // step 3: insert __users
    // INSERT INTO __users(is_locked,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,priv_drop,priv_grant_option,priv_insert,
    // priv_update,priv_select,priv_replace,user_id,user_name,pass_word) values(0,0,0,0,0,0,0,0,0,0,0,0,value1+1,'user1',
    // '经过加密后的user1的password'
    if (OB_SUCCESS == ret)
    {
      const ObStrings * users = create_user_stmt->get_users();
      int64_t count = users->count();
      OB_ASSERT(count % 2 == 0);
      //int64_t user_count = count / 2;
      int i = 0;
      ObString insert_user_prefix = ObString::make_string("INSERT INTO __all_user(info,is_locked,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,\
               priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace,user_id,user_name,pass_word) values('', 0,0,0,0,0,0,0,0,0,0,0,0,");
      ObString user_name;
      ObString pass_word;
      ObString empty_pwd = ObString::make_string("");
      for (i = 0;i < count; i = i + 2)
      {
        pos = 0;
        char insert_user_buff[512];
        ObString insert_user;
        insert_user.assign_buffer(insert_user_buff, 512);
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(WARN, "get user from create user stmt failed, ret=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = users->get_string(i + 1, pass_word)))
        {
          TBSYS_LOG(WARN, "get password from create user stmt failed, ret=%d", ret);
          break;
        }
        else if (pass_word == empty_pwd)
        {
          ret = OB_ERR_PASSWORD_EMPTY;
          result_set_out_->set_message("password must not be empty");
          TBSYS_LOG(WARN, "password must not be empty");
          break;
        }
        else
        {
          char scrambled[SCRAMBLE_LENGTH * 2 + 1];
          memset(scrambled, 0, sizeof(scrambled));
          ObString stored_pwd;
          stored_pwd.assign_ptr(scrambled, SCRAMBLE_LENGTH * 2 + 1);
          ObEncryptedHelper::encrypt(stored_pwd, pass_word);
          stored_pwd.assign_ptr(scrambled, SCRAMBLE_LENGTH * 2);
          databuff_printf(insert_user_buff, 512, pos, "INSERT INTO __all_user(info,is_locked,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,\
            priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace,user_id,user_name,pass_word) \
            VALUES('', 0,0,0,0,0,0,0,0,0,0,0,0,%ld, '%.*s', '%.*s')", current_user_id + i / 2 + 1,
              user_name.length(), user_name.ptr(), stored_pwd.length(), stored_pwd.ptr());
          if (pos >= 511)
          {
            // overflow
            ret = OB_BUF_NOT_ENOUGH;
            TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
          }
          else
          {
            insert_user.assign_ptr(insert_user_buff, static_cast<ObString::obstr_size_t>(pos));
            ret = execute_stmt_no_return_rows(insert_user);
            if (OB_SUCCESS != ret)
            {
              ret = OB_ERR_USER_EXIST;
              result_set_out_->set_message("user exists");
              TBSYS_LOG(WARN, "create user failed, user:%.*s exists",user_name.length(), user_name.ptr());
              break;
            }
          }
        }
      }// for
      current_user_id += count/2;
    }
    // step 4: update __all_sys_stat
    if (OB_SUCCESS == ret)
    {
      pos = 0;
      char update_max_user_id_buff[512];
      ObString update_max_user_id;
      databuff_printf(update_max_user_id_buff, 512, pos, "UPDATE __all_sys_stat set value = '%ld' \
          where cluster_id = 0 and name = 'ob_max_user_id'", current_user_id);
      if (pos >= 511)
      {
        // overflow
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
      }
      else
      {
        update_max_user_id.assign_ptr(update_max_user_id_buff, static_cast<ObString::obstr_size_t>(pos));
        ret = execute_stmt_no_return_rows(update_max_user_id);
      }
    }
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

//add wenghaixing [database name]20150609
int ObPrivExecutor::do_create_db(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int err = OB_SUCCESS;
  int64_t current_db_id = -1;
  const ObCreateDbStmt *create_db_stmt = dynamic_cast<const ObCreateDbStmt*>(stmt);
  ObString db_name;
  if (OB_UNLIKELY(NULL == create_db_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast create db stmt failed!");
  }
  else
  {
    if(OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed, ret=%d", ret);
    }
    else
    {
      ObString select_db_id = ObString::make_string("select value from __all_sys_stat where cluster_id = 0 and name = 'ob_max_database_id'");
      ObResultSet result2;
      context_->session_info_->set_current_result_set(&result2);
      if (OB_SUCCESS != (ret = result2.init()))
      {
        TBSYS_LOG(WARN, "init result set failed, ret = %d", ret);
      }
      else if(OB_SUCCESS != (ret = ObSql::direct_execute(select_db_id, result2, *context_)))
      {
        TBSYS_LOG(WARN, "direct_execute failed, sql = %.*s ret = %d",select_db_id.length(), select_db_id.ptr(), ret);
        result_set_out_->set_message(result2.get_message());
      }
      else if(OB_SUCCESS != (ret = result2.open()))
      {
        TBSYS_LOG(WARN, "open result set failed, sql =%.*s ret = %d", select_db_id.length(), select_db_id.ptr(), ret );
      }
      else
      {
        OB_ASSERT(true == result2.is_with_rows());
        const ObRow* row = NULL;
        ret = result2.get_next_row(row);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "next row from ObResultSet failed, sql = %.*s ret = %d", select_db_id.length(), select_db_id.ptr(), ret);
        }
        else
        {
          ObExprObj in;
          ObExprObj out;
          const ObObj *pcell = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ret = row->raw_get_cell(0, pcell, table_id, column_id);
          if(OB_SUCCESS == ret)
          {
            in.assign(*pcell);
            ObObjCastParams params;
            if(OB_SUCCESS != (ret = OB_OBJ_CAST[ObVarcharType][ObIntType](params, in, out)))
            {
              TBSYS_LOG(ERROR, "varchar cast to int failed, ret = %d", ret);
            }
            else
            {
              current_db_id = out.get_int();
            }
          }
          else
          {
            TBSYS_LOG(WARN, "raw get cell ob_max_database_id from row failed, ret = %d", ret);
          }
        }
      }
      err = result2.close();
      if(OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
      }
      result2.reset();
    }
    context_->session_info_->set_current_result_set(result_set_out_);
  }
  if (OB_SUCCESS == ret)
  {
    const ObStrings* database = create_db_stmt->get_database();
    int64_t count = database->count();
    OB_ASSERT(count == 1);
    //ObString insert_db_prefix = ObString::make_string("INSERT INTO __all_database ");
    char insert_db_buff[512] = {0};
    ObString insert_db;
    insert_db.assign_buffer(insert_db_buff,512);
    if(OB_SUCCESS != (ret = database->get_string(0, db_name)))
    {
      TBSYS_LOG(WARN, "get database from create db stmt failed, ret = %d", ret);
    }
    
    else
    {
      databuff_printf(insert_db_buff, 512, pos,"INSERT INTO __all_database(db_name,db_id,stat)VALUES('%.*s',%ld,%ld)",db_name.length(),db_name.ptr(),current_db_id+1,(int64_t)1);
      if (pos >= 511)
      {
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "privilege buff overflow, ret = %d", ret);
      }
      else
      {
        insert_db.assign_ptr(insert_db_buff, static_cast<ObString::obstr_size_t>(pos));
        ret = execute_stmt_no_return_rows(insert_db);
        if(OB_SUCCESS != ret)
        {
          ret = OB_ERR_ALREADY_EXISTS;
          result_set_out_->set_message("db is already exists");
          TBSYS_LOG(WARN, "create db failed,db:%.*s exists",db_name.length(), db_name.ptr());

        }
      }
    }
    current_db_id ++;
    //update _all_sys_stat
    if(OB_SUCCESS == ret)
    {
      pos = 0;
      char update_max_db_id_buff[512]={0};
      ObString update_max_db_id;
      databuff_printf(update_max_db_id_buff, 512, pos, "UPDATE __all_sys_stat set value = '%ld' \
                      where cluster_id = 0 and name = 'ob_max_database_id'", current_db_id);
      if(pos >= 511)
      {
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "privilege buffer overflow, ret = %d", ret);
      }
      else
      {
        update_max_db_id.assign_ptr(update_max_db_id_buff, static_cast<ObString::obstr_size_t>(pos));
        ret = execute_stmt_no_return_rows(update_max_db_id);
      }
    }
    if(OB_SUCCESS == ret)
    {
      ret = grant_all_db_priv(current_db_id);
    }
    if(OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      int err = rollback();
      if(OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed, ret = %d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  if(OB_SUCCESS == ret && context_->session_info_->is_regrant_priv())
  {
    if(OB_SUCCESS != (ret = grant_all_db_priv_global_user(db_name)))
    {
      TBSYS_LOG(WARN, "grant al db priv failed,ret[%d]", ret);
    }
  }
  return ret;
}

int ObPrivExecutor::grant_all_db_priv(const int64_t &db_id)
{
  int ret = OB_SUCCESS;
  char insert_db_priv_buff[512] = {0};
  ObString insert_db_priv;
  const ObString user_name = context_->session_info_->get_user_name();
  common::ObPrivilege::User user;
  int64_t pos = 0;
  if(OB_SUCCESS != (ret = (*(context_->pp_privilege_))->is_user_exist(user_name, user)))
  {
    TBSYS_LOG(WARN, "check user exist failed, ret [%d]", ret);
  }
  else
  {
    insert_db_priv.assign_buffer(insert_db_priv_buff, 512);
    databuff_printf(insert_db_priv_buff, 512, pos,"REPLACE INTO __all_database_privilege(user_id,db_id,priv_all,priv_alter,priv_create_user,priv_create,priv_delete,"
                    "priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace,is_locked)VALUES(%ld,%ld,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,% d)"
                    ,user.user_id_, db_id, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0);
    if (pos >= 511)
    {
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buff overflow, ret = %d", ret);
    }
    else
    {
      insert_db_priv.assign_ptr(insert_db_priv_buff, static_cast<ObString::obstr_size_t>(pos));
      ret = execute_stmt_no_return_rows(insert_db_priv);
      if(OB_SUCCESS != ret)
      {
        ret = OB_INNER_STAT_ERROR;
        result_set_out_->set_message("grant owner database priv failed!");
        TBSYS_LOG(WARN, "grant owner database failed");
      }
    }
  }
  return ret;
}

int ObPrivExecutor::grant_all_db_priv_global_user(const ObString &db_name)
{
  int ret = OB_SUCCESS;
  int64_t buf_size = 512;
  char grant_buff[512] = {0};
  int64_t pos = 0;
  ObPrivilege::NameUserMap *user_pirv_map = const_cast<ObPrivilege*>((*(context_->pp_privilege_)))->get_username_map();
  ObPrivilege::NameUserMap::const_iterator iter = user_pirv_map->begin();
  ObPrivilege::User user;
  ObString user_name;
  ObBitSet<> privileges;
  ObString grant_sql;
  for(; iter != user_pirv_map->end(); ++ iter)
  {
    user = iter->second;
    if(!user.privileges_.is_empty())
    {
      memset(grant_buff, 0, 512);
      pos = 0;
      user_name = iter->first;
      privileges = user.privileges_;
      databuff_printf(grant_buff, buf_size, pos, "GRANT ");
      if (privileges.has_member(OB_PRIV_ALL))
      {
        databuff_printf(grant_buff, buf_size, pos, "ALL PRIVILEGES,");
        if (privileges.has_member(OB_PRIV_GRANT_OPTION))
        {
          databuff_printf(grant_buff, buf_size, pos, "GRANT OPTION,");
        }
      }
      else
      {
        if (privileges.has_member(OB_PRIV_ALTER))
        {
          databuff_printf(grant_buff, buf_size, pos, "ALTER,");
        }
        //mod liumz, [multi_database.UT_bugfix]20150715:b
        if (privileges.has_member(OB_PRIV_CREATE))
        {
          databuff_printf(grant_buff, buf_size, pos, "CREATE,");
        }
        if (privileges.has_member(OB_PRIV_CREATE_USER))
        {
          databuff_printf(grant_buff, buf_size, pos, "CREATE USER,");
        }
        if (privileges.has_member(OB_PRIV_DELETE))
        {
          databuff_printf(grant_buff, buf_size, pos, "DELETE,");
        }
        if (privileges.has_member(OB_PRIV_DROP))
        {
          databuff_printf(grant_buff, buf_size, pos, "DROP,");
        }
        if (privileges.has_member(OB_PRIV_GRANT_OPTION))
        {
          databuff_printf(grant_buff, buf_size, pos, "GRANT OPTION,");
        }
        if (privileges.has_member(OB_PRIV_INSERT))
        {
          databuff_printf(grant_buff, buf_size, pos, "INSERT,");
        }
        if (privileges.has_member(OB_PRIV_UPDATE))
        {
          databuff_printf(grant_buff, buf_size, pos, "UPDATE,");
        }
        if (privileges.has_member(OB_PRIV_SELECT))
        {
          databuff_printf(grant_buff, buf_size, pos, "SELECT,");
        }
        if (privileges.has_member(OB_PRIV_REPLACE))
        {
          databuff_printf(grant_buff, buf_size, pos, "REPLACE,");
        }
        //mod:e
      }
      pos = pos - 1;
      databuff_printf(grant_buff, buf_size, pos, " ON \"%.*s\".* TO '%.*s'", db_name.length(), db_name.ptr(), user_name.length(), user_name.ptr());
      if(pos >= 511)
      {
        TBSYS_LOG(WARN, "buff for grant sql is not enough");
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        grant_sql.assign_ptr(grant_buff, static_cast<ObString::obstr_size_t>(pos));
      }
      if(OB_SUCCESS == ret)
      {
        //add liuxiao, [multi_database.UT_bugfix]20150827:b
        //不在这了关闭权限检查会在建库时由于共用一个权限检查标志位导致grant第二个用户权限时进入检查分支
        //建库时是不应该进入的
        context_->disable_privilege_check_ = true;
        //add e
        if(OB_SUCCESS != (ret = execute_stmt_no_return_rows(grant_sql)))
        {
          TBSYS_LOG(WARN, "execute grant sql failed,sql = %.*s, ret[%d]", grant_sql.length(), grant_sql.ptr(),ret);
          break;
        }
      }
    }
  }
  return ret;
}


int ObPrivExecutor::do_use_db(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  const ObUseDbStmt *use_db_stmt = dynamic_cast<const ObUseDbStmt*>(stmt);
  if(OB_UNLIKELY(NULL == use_db_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast use db stmt failed!") ;
  }
  else
  {
    //start a empty transaction, to promise use db only in one time
    if(OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed, ret=%d", ret);
    }
    else
    {
      //pick out database name and check if user can use it
      const ObStrings* database = use_db_stmt->get_database();
      int64_t count = database->count();
      OB_ASSERT(count == 1);
      const ObString user = context_->session_info_->get_user_name();
      ObString db_name;
      if(OB_SUCCESS != (ret = database->get_string(0,db_name)))
      {
        TBSYS_LOG(WARN, "get database name failed, ret = [%d]", ret);
      }
      else if(OB_SUCCESS != (ret = db_can_be_connect(user, db_name)))
      {
        TBSYS_LOG(WARN, "user [%.*s] has no priveleges to use db [%.*s] ret [%d]", user.length(), user.ptr(), db_name.length(), db_name.ptr(), ret);
      }
      else if(OB_SUCCESS != (ret = context_->session_info_->set_db_name(db_name)))
      {
        TBSYS_LOG(WARN, "change database failed, ret [%d]", ret);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    ret = commit();
  }
  else
  {
    int err = rollback();
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "rollback failed, ret = %d", err);
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

int ObPrivExecutor::do_drop_db(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObString db_name;
  uint64_t db_id = OB_INVALID_ID;
  //1.take db name from stmt
  const ObDropDbStmt *drop_db_stmt = dynamic_cast<const ObDropDbStmt*>(stmt);
  if (OB_UNLIKELY(NULL == drop_db_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast drop db stmt failed!");
  }
  else
  {
    const ObStrings *db = drop_db_stmt->get_database();
    if(OB_SUCCESS != (ret = db->get_string(0, db_name)))
    {
      TBSYS_LOG(WARN, "get db name from statemen failed, ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = (*(context_->pp_privilege_))->is_db_exist(db_name,db_id)))
    {
      TBSYS_LOG(WARN, "database cannot be droped, ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = db_can_be_drop(db_name)))
    {
      TBSYS_LOG(WARN, "database cannot be droped, ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed, ret[%d]", ret);
    }
    else
    {
      ObString delete_database;
      char delete_db_buff[512];
      int64_t pos = 0;
      databuff_printf(delete_db_buff, 512, pos, "delete from __all_database where db_name = '%.*s'", db_name.length(), db_name.ptr());
      if(pos >= 511)
      {
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "privilege buffer overflow,ret[%d]", ret);
      }
      else
      {
        delete_database.assign_ptr(delete_db_buff, static_cast<ObString::obstr_size_t>(pos));
        ret = execute_delete_database(delete_database);
      }
    }

    if(OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      int err = rollback();
      if(OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed, ret = %d",err);
      }
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  //2.check if drop db has one or more table,if exists, cannot drop it

  //3.drop db,and delete record in sys table
  //4.set db name with null string
  //mod liuxiao, [multi_database.UT_bugfix]20150828:b
  if((OB_SUCCESS == ret) && context_->session_info_->get_db_name() == db_name)
  {
    ObString null_db;
    context_->session_info_->set_db_name(null_db);
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = clean_database_privilege(db_id)))
    {
      TBSYS_LOG(WARN,"clean database privilege failed,db_id[%ld],ret[%d]",db_id,ret);
    }
  }
  /*ObString null_db;
  if(OB_SUCCESS == ret)
  {
    context_->session_info_->set_db_name(null_db);
  }*/
  //mod e

  return ret;
}

int ObPrivExecutor::db_can_be_drop(const ObString &db_name)
{
  //add liumz, [bugfix: default db can not be dropped!!!]20150917:b
  if (db_name == OB_DEFAULT_DB_NAME)
  {
    return OB_ERR_DROP_DEFAULT_DB;
  }
  //add:e
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int err = OB_SUCCESS;
  char select_db_buff[512] = {0};
  ObString sql_get_db;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  databuff_printf(select_db_buff, 512, pos, "SELECT /*+read_consistency(WEAK) */ * FROM __first_tablet_entry WHERE db_name = '%.*s'", db_name.length(), db_name.ptr());
  if (pos >= 511)
  {
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buff overflow, ret = %d", ret);
  }
  else
  {
    sql_get_db.assign_ptr(select_db_buff, static_cast<ObString::obstr_size_t>(pos));
    if(OB_SUCCESS != (ret = result.init()))
    {
      TBSYS_LOG(WARN, "init result set failed, ret = %d", ret);
    }
    else if(OB_SUCCESS != (ret = ObSql::direct_execute(sql_get_db, result, *context_)))
    {
      TBSYS_LOG(WARN, "direct_execute failed, sql = %.*s ret = %d",sql_get_db.length(), sql_get_db.ptr(), ret);
      result_set_out_->set_message(result.get_message());
    }
    else if(OB_SUCCESS != (ret = result.open()))
    {
      TBSYS_LOG(WARN, "open result set failed, sql =%.*s ret = %d", sql_get_db.length(), sql_get_db.ptr(), ret );
    }
    else if(result.is_with_rows())
    {
      const ObRow* row = NULL;
      if(OB_ITER_END == (ret = result.get_next_row(row)))
      {
        ret = OB_SUCCESS;
      }
      else if(OB_SUCCESS == ret)
      {
        ret = OB_ERR_STILL_HAS_TABLE_IN_DATABASE;
      }
    }
  }
  err = result.close();
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
  }
  result.reset();
  return ret;
}

int ObPrivExecutor::db_can_be_connect(const ObString &user_name, const ObString &db_name)
{
  int ret = OB_SUCCESS;
  //uint64_t user_id = OB_INVALID_ID;
  uint64_t db_id = OB_INVALID_ID;
  common::ObPrivilege::User user;
  if(OB_SUCCESS != (ret = (*(context_->pp_privilege_))->is_user_exist(user_name, user)))
  {
    TBSYS_LOG(WARN, "check user exist failed, ret [%d]", ret);
  }
  else if(OB_SUCCESS != (ret = sql_get_db_id(db_name, db_id)))
  {
    TBSYS_LOG(WARN, "get db_id failed, ret[%d]", ret);
  }
  else
  {
    ret = (*(context_->pp_privilege_))->user_has_global_priv(user);
  }
  if(OB_ERR_NO_ACCESS_TO_DATABASE == ret)
  {
    //second check if database privilege has user priv
    ObString sql_get_db_priv;
    char select_db_pri_buff[512] = {0};
    int64_t pos = 0;
    databuff_printf(select_db_pri_buff, 512, pos, "SELECT /*+read_consistency(WEAK) */dp.user_id,dp.db_id,dp.priv_all,dp.priv_alter,dp.priv_create,dp.priv_create_user,dp.priv_delete,dp.priv_drop,dp.priv_grant_option,dp.priv_insert,dp.priv_update,dp.priv_select,dp.priv_replace,dp.is_locked FROM __all_database_privilege dp WHERE dp.user_id=%ld AND dp.db_id = %ld ", user.user_id_, db_id);
    if(pos >= 511)
    {
      TBSYS_LOG(WARN, "privilege buff over flow");
      ret = OB_BUF_NOT_ENOUGH;
    }
    else
    {
      sql_get_db_priv.assign_ptr(select_db_pri_buff, static_cast<ObString::obstr_size_t>(pos));
      if(OB_SUCCESS != (ret = direct_execute_sql_db_priv_is_valid(sql_get_db_priv)))
      {
        ObString sql_get_table_priv;
        char select_table_priv_buff[512] = {0};
        pos = 0;
        databuff_printf(select_table_priv_buff, 512, pos, "SELECT /*+read_consistency(WEAK) */t.user_id,t.table_id,t.priv_all,t.priv_alter,t.priv_create,t.priv_create_user,t.priv_delete,t.priv_drop,t.priv_grant_option,t.priv_insert,t.priv_update,t.priv_select, priv_replace FROM __all_table_privilege t WHERE t.user_id = %ld AND t.db_id = %ld", user.user_id_, db_id);
        if(pos >= 512)
        {
          TBSYS_LOG(WARN, "privilege buff overflow");
          ret = OB_BUF_NOT_ENOUGH;
        }
        else
        {
          sql_get_table_priv.assign_ptr(select_table_priv_buff, static_cast<ObString::obstr_size_t>(pos));
          ret = direct_execute_sql_table_priv_is_valid(sql_get_table_priv);
          //TBSYS_LOG(ERROR, "test::whx exec ret[%d],sql[%.*s]", ret, sql_get_table_priv.length(),sql_get_table_priv.ptr());
        }
      }
    }
  }
  return ret;
}

int ObPrivExecutor::sql_get_db_id(const ObString &db_name, uint64_t &db_id) const
{
  int ret = OB_SUCCESS;
  ObString sql_select_database;
  char sql_buf[512] = {0};
  int64_t pos = 0;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  databuff_printf(sql_buf, 512, pos, "SELECT /*+read_consistency(WEAK) */ db_id FROM __all_database WHERE db_name = '%.*s'", db_name.length(), db_name.ptr());
  if(pos >= 511)
  {
    TBSYS_LOG(WARN, "privilege buff over flow");
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    sql_select_database.assign_ptr(sql_buf, static_cast<ObString::obstr_size_t>(pos));
    if(OB_SUCCESS != (ret = result.init()))
    {
      TBSYS_LOG(WARN, "init result set failed, ret = %d", ret);
    }
    else if(OB_SUCCESS != (ret = ObSql::direct_execute(sql_select_database, result, *context_)))
    {
      TBSYS_LOG(WARN, "direct execute failed, sql = %.*s", sql_select_database.length(), sql_select_database.ptr());
      result_set_out_->set_message(result.get_message());
    }
    else if(OB_SUCCESS != (ret = result.open()))
    {
      TBSYS_LOG(WARN, "open result set failed, sql = %.*s, ret = %d", sql_select_database.length(), sql_select_database.ptr(), ret);
    }
    else if(result.is_with_rows())
    {
      const ObRow* row = NULL;
      const ObObj* obj = NULL;
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      ret = result.get_next_row(row);
      if(OB_SUCCESS == ret)
      {
        int64_t tmp_db = OB_INVALID_ID;
        if(OB_SUCCESS != (ret = row->raw_get_cell(0, obj, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "get cell failed, ret = %d", ret);
        }
        else if(OB_SUCCESS != (ret = obj->get_int(tmp_db)))
        {
          TBSYS_LOG(WARN, "get int failed, ret = %d", ret);
        }
        else
        {
          db_id = tmp_db;
        }
      }
      else if(OB_ITER_END == ret)
      {
        TBSYS_LOG(WARN, "database not exist,db_name = %.*s", db_name.length(), db_name.ptr());
        ret = OB_ERR_DATABASE_NOT_EXSIT;
      }
    }
    int err = OB_SUCCESS;
    err = result.close();
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
    }
    result.reset();
  }
  return ret;
}

int ObPrivExecutor::direct_execute_sql_has_result(const ObString &sql)
{
  int ret = OB_SUCCESS;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if(OB_SUCCESS != (ret = result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed,ret = %d", ret);
  }
  else if(OB_SUCCESS != (ret = ObSql::direct_execute(sql, result, *context_)))
  {
    TBSYS_LOG(WARN, "direct_execute failed, sql = %.*s", sql.length(),sql.ptr());
    result_set_out_->set_message(result.get_message());
  }
  else if(OB_SUCCESS != (ret = result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql =%.*s ret = %d", sql.length(), sql.ptr(), ret );
  }
  else if(result.is_with_rows())
  {
    //const ObRow* row = NULL;
    if(0 == result.get_affected_rows())
    {
      ret = OB_ITER_END;
    }
  }
  int err = OB_SUCCESS;
  err = result.close();
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
  }
  result.reset();
  return ret;
}

int ObPrivExecutor::direct_execute_sql_db_priv_is_valid(const ObString &sql)
{
  int ret = OB_SUCCESS;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if(OB_SUCCESS != (ret = result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed,ret = %d", ret);
  }
  else if(OB_SUCCESS != (ret = ObSql::direct_execute(sql, result, *context_)))
  {
    TBSYS_LOG(WARN, "direct_execute failed, sql = %.*s", sql.length(),sql.ptr());
    result_set_out_->set_message(result.get_message());
  }
  else if(OB_SUCCESS != (ret = result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql =%.*s ret = %d", sql.length(), sql.ptr(), ret );
  }
  else if(result.is_with_rows())
  {
    const ObRow* row = NULL;
    {
      bool is_empty = true;
      int err = OB_SUCCESS;
      while(OB_SUCCESS == result.get_next_row(row))
      {
        ObPrivilege priv;
        if(OB_SUCCESS != (err = priv.db_result_to_priv_empty(*row, is_empty)))
        {
          TBSYS_LOG(WARN, "db_result_to_priv_empty func exec failed, err[%d]", err);
          break;
        }
        else if(!is_empty)
        {
          break;
        }
      }
      if(is_empty)
      {
        ret = OB_ERR_NO_ACCESS_TO_DATABASE;
      }
    }
  }
  int err = OB_SUCCESS;
  err = result.close();
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
  }
  result.reset();
  return ret;
}

int ObPrivExecutor::direct_execute_sql_table_priv_is_valid(const ObString &sql)
{
  int ret = OB_SUCCESS;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if(OB_SUCCESS != (ret = result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed,ret = %d", ret);
  }
  else if(OB_SUCCESS != (ret = ObSql::direct_execute(sql, result, *context_)))
  {
    TBSYS_LOG(WARN, "direct_execute failed, sql = %.*s", sql.length(),sql.ptr());
    result_set_out_->set_message(result.get_message());
  }
  else if(OB_SUCCESS != (ret = result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql =%.*s ret = %d", sql.length(), sql.ptr(), ret );
  }
  else if(result.is_with_rows())
  {
    const ObRow* row = NULL;
    bool is_empty = true;
    int err = OB_SUCCESS;
    while(OB_SUCCESS == result.get_next_row(row))
    {
      ObPrivilege priv;
      //TBSYS_LOG(ERROR, "test::whx row [%s]", to_cstring(*row));
      if(OB_SUCCESS != (err = priv.table_result_to_priv_empty(*row, is_empty)))
      {
        TBSYS_LOG(WARN, "db_result_to_priv_empty func exec failed, err[%d]", err);
        break;
      }
      else if(!is_empty)
      {
        break;
      }
    }
    if(is_empty)
    {
      ret = OB_ERR_NO_ACCESS_TO_DATABASE;
    }
  }
  int err = OB_SUCCESS;
  err = result.close();
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
  }
  result.reset();
  return ret;
}
//add e
/*============================================================
  * @author                 kindaich(kindaichweng@gmail.com)
  * @date
  * @DataBaseManage
  * @common                 when drop a database,we should clean
  *                         privilege with __all_database_privilege
  *                         and __all_table_privilege
  ============================================================*/
int ObPrivExecutor::clean_database_privilege(const uint64_t flag_id, int flag)
{
  int ret = OB_SUCCESS;
  const ObPrivilege::UserDbPriMap *db_pri_map = (*(context_->pp_privilege_))->get_user_database_privilege_map_const();
  ObPrivilege::UserIdDatabaseId user_db;
  //const char* sql = "delete from __all_database_privilege where ";
  ObString sql_delete_priv;
  char sql_buf[512];
  int64_t pos = 0;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  uint64_t id_left = OB_INVALID_ID;
  uint64_t id_right = OB_INVALID_ID;
  if(0 == flag)start_transaction();//if flag != 0,then start transaction will put outside
  for( ObPrivilege::UserDbPriMap::const_iterator iter = db_pri_map->begin();iter != db_pri_map->end();++iter)
  {

    user_db = iter->first;
    if(flag == 0)
    {
      id_left = user_db.dbid_;
    }
    else
    {
      id_left = user_db.userid_;
    }
    id_right = flag_id;
    if(id_left == id_right)
    {
      memset(sql_buf, 0, 512);
      pos = 0;
      databuff_printf(sql_buf,512,pos,"DELETE FROM __all_database_privilege WHERE user_id = %ld and db_id = %ld;",user_db.userid_,user_db.dbid_);
      if(pos >=511)
      {
        TBSYS_LOG(WARN, "privilege buff over flow");
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        sql_delete_priv.assign_ptr(sql_buf,static_cast<ObString::obstr_size_t>(pos));
        if(OB_SUCCESS != (ret = result.init()))
        {
          TBSYS_LOG(WARN,"init result set failed,%d",ret);
        }
        else if(OB_SUCCESS != (ret = ObSql::direct_execute(sql_delete_priv,result,*context_)))
        {
          TBSYS_LOG(WARN, "direct execute failed, sql = %.*s",sql_delete_priv.length(),sql_delete_priv.ptr());
          result_set_out_->set_message(result.get_message());
          break;
        }
        else if(OB_SUCCESS != (ret = result.open()))
        {
          TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", sql_delete_priv.length(), sql_delete_priv  .ptr(), ret);
          break;
        }
        else
        {
          OB_ASSERT(result.is_with_rows() == false);
          int64_t affected_rows = result.get_affected_rows();
          TBSYS_LOG(INFO," clean database privileges affect [%ld] rows", affected_rows);
        }
        int err = OB_SUCCESS;
        err = result.close();
        if(OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
          break;
        }
        result.reset();
      }
    }

  }
  if(0 == flag)
  {
    if(OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      int err = rollback();
      if(OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed, ret = %d",err);
      }
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

/*============================================================
  * @author                 kindaich(kindaichweng@gmail.com)
  * @date
  * @DataBaseManage
  * @common                 there will not be start_transaction
  *                         or end transaction
  ============================================================*/
int ObPrivExecutor::clean_user_privilege(const uint64_t &user_id, const ObArray<int64_t> &db_list, const ObArray<uint64_t> &table_list)
{
  int ret = OB_SUCCESS;
  ObString sql_delete_priv;
  char sql_buf[512];
  int64_t pos = 0;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if(db_list.count() != table_list.count())
  {
    ret = OB_ERROR;
  }
  if(OB_SUCCESS == ret)
  {
    for(int64_t i = 0; i < db_list.count();i++)
    {
      memset(sql_buf, 0, 512);
      pos = 0;
      databuff_printf(sql_buf, 512, pos, "DELETE FROM __all_table_privilege WHERE user_id = %ld and db_id = %ld and table_id = %ld", user_id, db_list.at(i), table_list.at(i));
      if(pos >= 511)
      {
        TBSYS_LOG(WARN, "privilege buff over flow");
        ret = OB_BUF_NOT_ENOUGH;
        break;
      }
      else
      {
        sql_delete_priv.assign_ptr(sql_buf,static_cast<ObString::obstr_size_t>(pos));
        TBSYS_LOG(WARN, "test::whx, clean user table privilege, sql = %.*s",sql_delete_priv.length(),sql_delete_priv.ptr());
        if(OB_SUCCESS != (ret = result.init()))
        {
          TBSYS_LOG(WARN, "init result set failed,%d",ret);
        }
        else if(OB_SUCCESS != (ret = ObSql::direct_execute(sql_delete_priv,result,*context_)))
        {
          TBSYS_LOG(WARN, "direct execute failed,sql = %.*s",sql_delete_priv.length(),sql_delete_priv.ptr());
          break;
        }
        else if(OB_SUCCESS != (ret = result.open()))
        {
          TBSYS_LOG(WARN, "open result set failed,sql = %.*s",sql_delete_priv.length(),sql_delete_priv.ptr());
          break;
        }
        else
        {
          int64_t affected_rows = result.get_affected_rows();
          TBSYS_LOG(INFO," clean user privileges affect [%ld] rows", affected_rows);
        }
        int err = OB_SUCCESS;
        err = result.close();
        if(OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to close result set, err = %d", err);
          break;
        }
        result.reset();
      }
    }
  }
  return ret;
}
