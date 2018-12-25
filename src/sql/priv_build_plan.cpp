/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * priv_build_plan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "priv_build_plan.h"
#include "ob_logical_plan.h"
#include "parse_malloc.h"
#include "ob_create_user_stmt.h"
#include "ob_drop_user_stmt.h"
#include "ob_set_password_stmt.h"
#include "ob_rename_user_stmt.h"
#include "ob_lock_user_stmt.h"
#include "ob_grant_stmt.h"
#include "ob_schema_checker.h"
#include "ob_revoke_stmt.h"
//add wenghaixing [database manage]20150608
#include "ob_create_db_stmt.h"
#include "ob_use_db_stmt.h"
#include "ob_drop_database_stmt.h"
#include "sql/ob_sql_context.h"
//add e
using namespace oceanbase::common;
using namespace oceanbase::sql;

int resolve_create_user_stmt(ResultPlan* result_plan,
                             ParseNode* node,
                             uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_CREATE_USER && node->num_child_ > 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObCreateUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObString empty_str = ObString::make_string("");
    for (int i = 0; i < node->num_child_; ++i)
    {
      ParseNode *user_pass = node->children_[i];
      OB_ASSERT(2 == user_pass->num_child_);
      ObString user = ObString::make_string(user_pass->children_[0]->str_value_);
      ObString password = ObString::make_string(user_pass->children_[1]->str_value_);
      //add liumz, [bugfix:create user '' identified by 'password']20150819:b
      //modify by liuxiao, [bugfix:username length<=]20150825:b
      //if (user.length() <= 0)
      if (user.length() <= 0 || user.length()>= OB_MAX_USERNAME_LENGTH || user == empty_str)
      //modify:e
      {
        if(user.length() <= 0 || user == empty_str)
        {
          PARSER_LOG("username must not be empty");
        }
        else
        {
          PARSER_LOG("username is too long");
        }
        ret = OB_ERR_USER_EMPTY;
        break;
      }
      //add:e
      //add liuxiao, [bugfix:password length<=]20150825:b
      if (password.length() <= 0 || password.length() >= OB_MAX_PASSWORD_LENGTH || password == empty_str)
      {
        if(password.length() <= 0 || password == empty_str)
        {
          PARSER_LOG("password must not be empty");
        }
        else
        {
          PARSER_LOG("password is too long");
        }
        ret = OB_ERR_WRONG_PASSWORD;
        break;
      }
      //add:e
      if (OB_SUCCESS != (ret = stmt->add_user(user, password)))
      {
        PARSER_LOG("Failed to add user to CreateUserStmt");
        break;
      }
    } // end for
  }
  return ret;
}

int resolve_drop_user_stmt(ResultPlan* result_plan,
                           ParseNode* node,
                           uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_DROP_USER && node->num_child_ > 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObDropUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    for (int i = 0; i < node->num_child_; ++i)
    {
      ObString user = ObString::make_string(node->children_[i]->str_value_);
      if (OB_SUCCESS != (ret = stmt->add_user(user)))
      {
        PARSER_LOG("Failed to add user to DropUserStmt");
        break;
      }
    }
  }
  return ret;
}

int resolve_set_password_stmt(ResultPlan* result_plan,
                              ParseNode* node,
                              uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_SET_PASSWORD && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObSetPasswordStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObString user;
    if (NULL != node->children_[0])
    {
      user.assign_ptr(const_cast<char*>(node->children_[0]->str_value_),
                      static_cast<int32_t>(strlen(node->children_[0]->str_value_)));
      //add liumz, [bugfix:set password for '' = 'password' | alter user '' identified by 'password']20150819:b
      if (user.length() <= 0)
      {
        PARSER_LOG("User not exist");
        ret = OB_USER_NOT_EXIST;
      }
      //add:e
    }
    if (OB_SUCCESS == ret)
    {
      ObString password = ObString::make_string(node->children_[1]->str_value_);
      //mod liuxiao, [bugfix:password length limit]20150831:b
      if(password.length() == 0)
      {
        PARSER_LOG("password can not be empty");
        ret = OB_ERROR;
      }
      else if(password.length() >= OB_MAX_PASSWORD_LENGTH)
      {
        PARSER_LOG("password is too long");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = stmt->set_user_password(user, password)))
      {
        PARSER_LOG("Failed to set UserPasswordStmt");
      }
      /*if (OB_SUCCESS != (ret = stmt->set_user_password(user, password)))
      {
        PARSER_LOG("Failed to set UserPasswordStmt");
      }*/
      //mod:e
    }
  }
  return ret;
}

int resolve_rename_user_stmt(ResultPlan* result_plan,
                             ParseNode* node,
                             uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_RENAME_USER && node->num_child_ > 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObRenameUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    for (int i = 0; i < node->num_child_; ++i)
    {
      ParseNode *rename_info = node->children_[i];
      OB_ASSERT(2 == rename_info->num_child_ && T_RENAME_INFO == rename_info->type_);

      ObString from_user = ObString::make_string(rename_info->children_[0]->str_value_);
      ObString to_user = ObString::make_string(rename_info->children_[1]->str_value_);
      //add liumz, [bugfix: rename user 'user' to '']20150828:b
      //add liuxiao, [bugfix: rename user name length]20150831:b
      if (to_user.length() >= OB_MAX_USERNAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("user name is too long");
        break;
      }
      //add:e
      if (to_user.length() <= 0)
      {
        ret = OB_ERR_USER_EMPTY;
        PARSER_LOG("invalid user name");
        break;
      }
      //add:e
      if (OB_SUCCESS != (ret = stmt->add_rename_info(from_user, to_user)))
      {
        PARSER_LOG("Failed to add user to RenameUserStmt");
        break;
      }
    } // end for
  }
  return ret;
}

int resolve_lock_user_stmt(ResultPlan* result_plan,
                           ParseNode* node,
                           uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_LOCK_USER && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLockUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObString user = ObString::make_string(node->children_[0]->str_value_);
    bool locked = (0 != node->children_[1]->value_);
    if (OB_SUCCESS != (ret = stmt->set_lock_info(user, locked)))
    {
      PARSER_LOG("Failed to set lock info for LockUserStmt");
    }
  }
  return ret;
}

static int get_table_id(ResultPlan* result_plan, const ObString &table_name, uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != result_plan);
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  ObSchemaChecker* schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);

  if (logical_plan == NULL)
  {
    ret = OB_ERR_UNEXPECTED;
    PARSER_LOG("unexpected branch, logical_plan is NULL");
  }
  else if (NULL == schema_checker)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    PARSER_LOG("unexpected branch, schema_checker is NULL");
  }
  //add liumz, [multi_database.sql]:20150613
  else
  {
    table_id = schema_checker->get_table_id(table_name);
  }
  //add:e
  /*else if (OB_INVALID_ID == (table_id = schema_checker->get_table_id(table_name)))
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    PARSER_LOG("Table `%.*s' does not exist", table_name.length(), table_name.ptr());
  }*/
  return ret;
}

//add liumz, [multi_database.sql]:20150613
int construct_db_table_name(ResultPlan *&result_plan, const ObString &db_name, ObString &table_name)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObString db_table_name;
  int64_t pos = 0;
  int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
  char db_table_name_buf[buf_len];
  databuff_printf(db_table_name_buf, buf_len, pos, "%.*s", db_name.length(), db_name.ptr());
  databuff_printf(db_table_name_buf, buf_len, pos, ".");
  databuff_printf(db_table_name_buf, buf_len, pos, "%.*s", table_name.length(), table_name.ptr());
  if (pos >= buf_len)
  {
    ret = OB_ERR_TABLE_NAME_LENGTH;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Table name too long");
  }
  else
  {
    db_table_name.assign_ptr(db_table_name_buf, static_cast<ObString::obstr_size_t>(pos));
    if (OB_SUCCESS != (ret = ob_write_string(*name_pool, db_table_name, table_name)))
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not make space for table name %.*s", db_table_name.length(), db_table_name.ptr());
    }
  }
  return ret;
}

int write_db_table_name(const ObString &db_name, const ObString &table_name, ObString &db_table_name)
{
  int ret = OB_SUCCESS;
  if (db_name.length() != db_table_name.write(db_name.ptr(), db_name.length()))
  {
    ret = OB_ERROR;
  }
  else if (1 != db_table_name.write(".", 1))
  {
    ret = OB_ERROR;
  }
  else if (table_name.length() != db_table_name.write(table_name.ptr(), table_name.length()))
  {
    ret = OB_ERROR;
  }
  return ret;
}
//add:e

int resolve_grant_stmt(ResultPlan* result_plan,
                       ParseNode* node,
                       uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_GRANT && node->num_child_ == 3);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObGrantStmt *stmt = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  //add liumz, [multi_database.priv_management.grant_priv]:20150608:b
  else if (session_info == NULL)
  {
    ret = OB_ERR_SESSION_UNSET;
    PARSER_LOG("Session not set");
  }
  //add:e
  else
  {
    ParseNode *privileges_node = node->children_[0];
    ParseNode *priv_level = node->children_[1];
    ParseNode *users_node = node->children_[2];
    OB_ASSERT(NULL != privileges_node);
    OB_ASSERT(NULL != priv_level);
    OB_ASSERT(NULL != users_node);
    OB_ASSERT(privileges_node->num_child_ > 0);
    for (int i = 0; i < privileges_node->num_child_; ++i)
    {
      OB_ASSERT(T_PRIV_TYPE == privileges_node->children_[i]->type_);
      if (OB_SUCCESS != (ret = stmt->add_priv(static_cast<ObPrivilegeType>(privileges_node->children_[i]->value_))))
      {
        PARSER_LOG("Failed to add privilege");
        break;
      }
    }
    if (OB_SUCCESS == ret)
    {
      //add liumz, [multi_database.priv_management.grant_priv]:20150608:b
      if (0 == priv_level->num_child_)
      {
        //priv_level: *, means global priv
        stmt->set_global_priv(true);
        if (OB_SUCCESS != (ret = stmt->set_table_id(OB_NOT_EXIST_TABLE_TID)))
        {
          PARSER_LOG("Failed to set table id");
        }
      }
      else
      {
        OB_ASSERT(2 == priv_level->num_child_);
        OB_ASSERT(NULL != priv_level->children_[1]);
        ObString db_name;
        //priv_level: db_name.*, means db priv
        if (T_STAR == priv_level->children_[1]->type_)
        {
          OB_ASSERT(NULL != priv_level->children_[0]);
          OB_ASSERT(T_IDENT == priv_level->children_[0]->type_);
          if ((ret = ob_write_string(*name_pool, ObString::make_string(priv_level->children_[0]->str_value_), db_name)) != OB_SUCCESS)
          {
            PARSER_LOG("Can not malloc space for db name");
          }
          else
          {
            stmt->set_db_name(db_name);
            if (OB_SUCCESS != (ret = stmt->set_table_id(OB_NOT_EXIST_TABLE_TID)))
            {
              PARSER_LOG("Failed to set table id");
            }
          }
        }
        else
        {
          //priv_level: db_name.relation_name, means table priv
          //OB_ASSERT(T_IDENT == priv_level->children_[0]->type_);
          OB_ASSERT(T_IDENT == priv_level->children_[1]->type_);
          uint64_t table_id = OB_INVALID_ID;
          ObString table_name = ObString::make_string(priv_level->children_[1]->str_value_);
          if (NULL == priv_level->children_[0])
          {
            //use db_name in session default

            db_name = session_info->get_db_name();
          }
          else
          {            
            if ((ret = ob_write_string(*name_pool, ObString::make_string(priv_level->children_[0]->str_value_), db_name)) != OB_SUCCESS)
            {
              PARSER_LOG("Can not malloc space for db name");
            }
          }
          if (OB_SUCCESS == ret)
          {           
            stmt->set_db_name(db_name);
            if (OB_SUCCESS != (ret = get_table_id(result_plan, table_name, table_id)))
            {
              PARSER_LOG("Failed to get table id");
            }
            else
            {
              if (!IS_SYS_TABLE_ID(table_id))
              {
                ObString db_table_name;
                int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
                char name_buf[buf_len];
                db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
                if(OB_SUCCESS != (ret = write_db_table_name(db_name, table_name, db_table_name)))
                {
                  PARSER_LOG("Table name \"%.*s.%.*s\" too long", db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
                }
                else if (OB_SUCCESS != (ret = get_table_id(result_plan, db_table_name, table_id)))
                {
                  PARSER_LOG("Failed to get table id");
                }
                else if (OB_INVALID_ID == table_id)
                {
                  ret = OB_ERR_TABLE_UNKNOWN;
                  PARSER_LOG("Unkown table \"%.*s.%.*s\" ", db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
                }
              }
            }
            if (OB_SUCCESS == ret && OB_SUCCESS != (ret = stmt->set_table_id(table_id)))
            {
              PARSER_LOG("Failed to set table id");
            }
          }//end if
        }
      }
      //add:e
      //del liumz, [multi_database.priv_management.grant_priv]:20150608:b
      /*if (0 == priv_level->num_child_)
      {
        if (OB_SUCCESS != (ret = stmt->set_table_id(OB_NOT_EXIST_TABLE_TID)))
        {
          PARSER_LOG("Failed to set table id");
        }
      }
      else
      {
        OB_ASSERT(1 == priv_level->num_child_);
        // table name -> table id
        uint64_t table_id = OB_INVALID_ID;
        if (OB_SUCCESS != (ret = get_table_id(result_plan,
                                              ObString::make_string(priv_level->children_[0]->str_value_),
                                              table_id)))
        {
        }
        else
        {
          if (OB_SUCCESS != (ret = stmt->set_table_id(table_id)))
          {
            PARSER_LOG("Failed to set table id");
          }
        }
      }*/
      //del:e
    } // end if
    if (OB_SUCCESS == ret)
    {
      OB_ASSERT(users_node->num_child_ > 0);
      for (int i = 0; i < users_node->num_child_; ++i)
      {
        if (OB_SUCCESS != (ret = stmt->add_user(ObString::make_string(users_node->children_[i]->str_value_))))
        {
          PARSER_LOG("Failed to add user");
          break;
        }
      }
    }
  }
  return ret;
}

int resolve_revoke_stmt(ResultPlan* result_plan,
                        ParseNode* node,
                        uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_REVOKE && node->num_child_ == 3);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObRevokeStmt *stmt = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  //add liumz, [multi_database.priv_management.grant_priv]:20150608:b
  else if (session_info == NULL)
  {
    ret = OB_ERR_SESSION_UNSET;
    PARSER_LOG("Session not set");
  }
  //add:e
  else
  {
    ParseNode *privileges_node = node->children_[0];
    ParseNode *priv_level = node->children_[1];
    ParseNode *users_node = node->children_[2];
    OB_ASSERT(NULL != privileges_node);
    OB_ASSERT(NULL != users_node);
    OB_ASSERT(privileges_node->num_child_ > 0);
    for (int i = 0; i < privileges_node->num_child_; ++i)
    {
      OB_ASSERT(T_PRIV_TYPE == privileges_node->children_[i]->type_);
      if (OB_SUCCESS != (ret = stmt->add_priv(static_cast<ObPrivilegeType>(privileges_node->children_[i]->value_))))
      {
        PARSER_LOG("Failed to add privilege");
        break;
      }
    }
    if (OB_SUCCESS == ret)
    {
      //add liumz, [multi_database.priv_management.revoke_priv]:20150608:b
      if (NULL == priv_level)
      {
        //REVOKE ALL PRIVILEGES, GRANT OPTION FROM user, table_id == OB_INVALID_ID
      }
      //mod liumz, [multi_database.UT_bugfix]20150710:b
      //if (0 == priv_level->num_child_)
      else if (0 == priv_level->num_child_)
      //mod:e
      {
        //priv_level: *, means global priv
        stmt->set_global_priv(true);
        if (OB_SUCCESS != (ret = stmt->set_table_id(OB_NOT_EXIST_TABLE_TID)))
        {
          PARSER_LOG("Failed to set table id");
        }
      }
      else if (1 == priv_level->num_child_)
      {
        //not support yet
      }
      else
      {
        OB_ASSERT(2 == priv_level->num_child_);
        OB_ASSERT(NULL != priv_level->children_[1]);
        ObString db_name;
        //priv_level: db_name.*, means db priv
        if (T_STAR == priv_level->children_[1]->type_)
        {
          OB_ASSERT(NULL != priv_level->children_[0]);
          OB_ASSERT(T_IDENT == priv_level->children_[0]->type_);
          if ((ret = ob_write_string(*name_pool, ObString::make_string(priv_level->children_[0]->str_value_), db_name)) != OB_SUCCESS)
          {
            PARSER_LOG("Can not malloc space for db name");
          }
          else
          {
            stmt->set_db_name(db_name);
            if (OB_SUCCESS != (ret = stmt->set_table_id(OB_NOT_EXIST_TABLE_TID)))
            {
              PARSER_LOG("Failed to set table id");
            }
          }
        }
        else
        {
          //priv_level: db_name.relation_name, means table priv
          //OB_ASSERT(T_IDENT == priv_level->children_[0]->type_);
          OB_ASSERT(T_IDENT == priv_level->children_[1]->type_);
          uint64_t table_id = OB_INVALID_ID;
          ObString table_name = ObString::make_string(priv_level->children_[1]->str_value_);
          if (NULL == priv_level->children_[0])
          {
            //use db_name in session default
            db_name = session_info->get_db_name();
          }
          else
          {
            if ((ret = ob_write_string(*name_pool, ObString::make_string(priv_level->children_[0]->str_value_), db_name)) != OB_SUCCESS)
            {
              PARSER_LOG("Can not malloc space for db name");
            }
          }
          if (OB_SUCCESS == ret)
          {           
            stmt->set_db_name(db_name);
            if (OB_SUCCESS != (ret = get_table_id(result_plan, table_name, table_id)))
            {
              PARSER_LOG("Failed to get table id");
            }
            else
            {
              if (!IS_SYS_TABLE_ID(table_id))
              {
                ObString db_table_name;
                int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
                char name_buf[buf_len];
                db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
                if(OB_SUCCESS != (ret = write_db_table_name(db_name, table_name, db_table_name)))
                {
                  PARSER_LOG("Table name \"%.*s.%.*s\" too long", db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
                }
                else if (OB_SUCCESS != (ret = get_table_id(result_plan, db_table_name, table_id)))
                {
                  PARSER_LOG("Failed to get table id");
                }
                else if (OB_INVALID_ID == table_id)
                {
                  ret = OB_ERR_TABLE_UNKNOWN;
                  PARSER_LOG("Unkown table \"%.*s.%.*s\" ", db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
                }
              }              
            }
            if (OB_SUCCESS == ret && OB_SUCCESS != (ret = stmt->set_table_id(table_id)))
            {
              PARSER_LOG("Failed to set table id");
            }
          }//end if
        }
      }
      //add:e
      /*uint64_t table_id = OB_INVALID_ID;
      if (NULL == priv_level)
      {
      }
      else if (0 == priv_level->num_child_)
      {
        table_id = OB_NOT_EXIST_TABLE_TID;
      }
      else
      {
        OB_ASSERT(1 == priv_level->num_child_);
        // table name -> table id
        if (OB_SUCCESS != (ret = get_table_id(result_plan,
                                              ObString::make_string(priv_level->children_[0]->str_value_),
                                              table_id)))
        {
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = stmt->set_table_id(table_id)))
        {
          PARSER_LOG("Failed to set table id");
        }
      }*/
    } // end if
    if (OB_SUCCESS == ret)
    {
      OB_ASSERT(users_node->num_child_ > 0);
      for (int i = 0; i < users_node->num_child_; ++i)
      {
        //add liumz, [multi_database.priv_management.revoke_priv]:20160516:b
        if (ObString::make_string(users_node->children_[i]->str_value_) == OB_ADMIN_USER_NAME)
        {
          ret = OB_ERR_NO_PRIVILEGE;
          PARSER_LOG("Can't revoke privileges from admin!");
          break;
        }
        //add:e
        else if (OB_SUCCESS != (ret = stmt->add_user(ObString::make_string(users_node->children_[i]->str_value_))))
        {
          PARSER_LOG("Failed to add user");
          break;
        }
      }
    }
  }
  return ret;
}

//add wenghaixing [database manage]20150608
int resolve_create_db_stmt(ResultPlan *result_plan, ParseNode *node, uint64_t &query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_CREATE_DATABASE && node->num_child_ == 1);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObCreateDbStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ParseNode *database_node = node->children_[0];
    OB_ASSERT(NULL != database_node);
    //OB_ASSERT(database_node->num_child_ > 0);
    //for(int i = 0; i < database_node->num_child_; ++i)
    //{
      //OB_ASSERT( T_OP_NAME_FIELD == database_node->type_);
      ObString db_name = ObString::make_string(database_node->str_value_);
      if (db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)//mod liumz, [name_length]
      {
        ret = OB_INVALID_ARGUMENT;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "database name length cannot be greater than %ld", OB_MAX_DATBASE_NAME_LENGTH - 1);
      }
      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = stmt->add_database(db_name)))
      {
        PARSER_LOG("Failed to add database");
      }
    //}
  }
  return ret;
}
int resolve_use_db_stmt(ResultPlan *result_plan, ParseNode *node, uint64_t &query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_USE_DATABASE && node->num_child_ == 1);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObUseDbStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ParseNode *database_node = node->children_[0];
    OB_ASSERT(NULL != database_node);
    //OB_ASSERT(T_OP_NAME_FIELD == database_node->type_);
    if(OB_SUCCESS != (ret = stmt->add_database(ObString::make_string(database_node->str_value_))))
    {
      PARSER_LOG("Failed to add database");
    }
  }
  return ret;
}

int resolve_drop_db_stmt(ResultPlan *result_plan, ParseNode *node, uint64_t &query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_DROP_DATABASE && node->num_child_ == 1);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObDropDbStmt *stmt = NULL;
  if(OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ParseNode *database_node = node->children_[0];
    OB_ASSERT(NULL != database_node);
    //mod liuxiao, [multi_database.UT_bugfix]20150828:b
    ObString db_name=ObString::make_string(database_node->str_value_);
    if(OB_SUCCESS == (ObString::make_string(database_node->str_value_).compare(OB_DEFAULT_DATABASE_NAME)))
    {
      PARSER_LOG("default database 'oceanbase' can not be droped!");
      ret=OB_ERROR;
    }
    else if(OB_SUCCESS != (ret = stmt->add_database(ObString::make_string(database_node->str_value_))))
    {
      PARSER_LOG("Faiked to add database");
    }
    //if(OB_SUCCESS != (ret = stmt->add_database(ObString::make_string(database_node->str_value_))))
    //{
    //  PARSER_LOG("Faiked to add database");
    //}
    //mod e
  }
  return ret;
}
//add e
