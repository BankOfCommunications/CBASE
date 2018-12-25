/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_grant_stmt.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_grant_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObGrantStmt::ObGrantStmt(const ObGrantStmt &other)
  :ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}
ObGrantStmt& ObGrantStmt::operator=(const ObGrantStmt &other)
{
  if (this == &other)
  {
  }
  else
  {
    int ret = OB_SUCCESS;
    const common::ObStrings* users = other.get_users();
    int i = 0;
    for (i = 0;i < users->count();i++)
    {
      ObString user_name;
      if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
      {
        break;
      }
      else if (OB_SUCCESS != (ret = this->add_user(user_name)))
      {
        break;
      }
    }
    this->table_id_ = other.get_table_id();
    this->privileges_ = *(other.get_privileges());
    //add liumz, [multi_database.priv_management.grant_priv]:20150615:b
    this->db_name_ = other.get_db_name();
    this->is_global_priv_ = other.is_global_priv();
    //add:e
  }
  return *this;
}
ObGrantStmt::ObGrantStmt()
  :ObBasicStmt(ObBasicStmt::T_GRANT), table_id_(OB_INVALID_ID), is_global_priv_(false)
{
}

ObGrantStmt::~ObGrantStmt()
{
}

void ObGrantStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObGrantStmt id=%d>\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "Privileges := ");
  for (int64_t i = 0; i < privileges_.count(); ++i)
  {
    fprintf(fp, "%s ",  ob_priv_type_str(privileges_.at(i)));
  }
  fprintf(fp, "\n");
  print_indentation(fp, level + 1);
  fprintf(fp, "TableId := %lu",  table_id_);
  fprintf(fp, "\n");
  print_indentation(fp, level + 1);
  fprintf(fp, "Users := ");
  common::ObString user;
  for (int64_t i = 0; i < users_.count(); i++)
  {
    if (common::OB_SUCCESS != users_.get_string(i, user))
    {
      break;
    }
    fprintf(fp, "%.*s ", user.length(), user.ptr()); // password
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObGrantStmt>\n");
}


int ObGrantStmt::add_user(const common::ObString &user)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = users_.add_string(user)))
  {
    TBSYS_LOG(WARN, "failed to add string, err=%d", ret);
  }
  return ret;
}

//add liumz, [multi_database.priv_management.grant_priv]:20150608:b
void ObGrantStmt::set_db_name(const common::ObString &db_name)
{
  db_name_ = db_name;
}
const ObString & ObGrantStmt::get_db_name() const
{
  return db_name_;
}
void ObGrantStmt::set_global_priv(bool is_global)
{
  is_global_priv_ = is_global;
}
bool ObGrantStmt::is_global_priv() const
{
  return is_global_priv_;
}
//add:e

int ObGrantStmt::add_priv(ObPrivilegeType priv)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = privileges_.push_back(priv)))
  {
    TBSYS_LOG(WARN, "failed to push to array, err=%d", ret);
  }
  return ret;
}

int ObGrantStmt::set_table_id(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid table id");
  }
  else
  {
    table_id_ = table_id;
  }
  return ret;
}
const ObStrings* ObGrantStmt::get_users() const
{
  return &users_;
}
uint64_t ObGrantStmt::get_table_id() const
{
  return table_id_;
}
const ObArray<ObPrivilegeType>* ObGrantStmt::get_privileges() const
{
  return &privileges_;
}



