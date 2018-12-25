/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_set_password_stmt.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_set_password_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObSetPasswordStmt::ObSetPasswordStmt(const ObSetPasswordStmt &other)
  :ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}
ObSetPasswordStmt& ObSetPasswordStmt::operator=(const ObSetPasswordStmt &other)
{
  if (this == &other)
  {
  }
  else
  {
    int ret = OB_SUCCESS;
    const common::ObStrings* user_password = other.get_user_password();
    OB_ASSERT(user_password->count() == 2);
    ObString user_name;
    ObString pass_word;
    if (OB_SUCCESS != (ret = user_password->get_string(0, user_name)))
    {
      TBSYS_LOG(WARN, "get user_name from set password stmt failed,ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = user_password->get_string(1, pass_word)))
    {
      TBSYS_LOG(WARN, "get pass_word from set password stmt failed,ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = this->set_user_password(user_name, pass_word)))
    {
      TBSYS_LOG(WARN, "add user pass to set pass stmt failed,ret=%d", ret);
    }
  }
  return *this;
}
ObSetPasswordStmt::ObSetPasswordStmt()
  :ObBasicStmt(ObBasicStmt::T_SET_PASSWORD)
{
}

ObSetPasswordStmt::~ObSetPasswordStmt()
{
}

void ObSetPasswordStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObSetPasswordStmt id=%d>\n", index);
  common::ObString user;
  for (int64_t i = 0; i < user_pass_.count(); i++)
  {
    if (common::OB_SUCCESS != user_pass_.get_string(i, user))
    {
      break;
    }
    if (i == 0)
    {
      print_indentation(fp, level + 1);
      fprintf(fp, "UserPassword := (%.*s", user.length(), user.ptr()); // user
    }
    else
    {
      fprintf(fp, ", %.*s)", user.length(), user.ptr()); // password
    }
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObSetPasswordStmt>\n");
}

int ObSetPasswordStmt::set_user_password(const common::ObString &user, const common::ObString &password)
{
  int ret = OB_SUCCESS;
  if (0 != user_pass_.count())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "should be only set once");
  }
  else if (OB_SUCCESS != (ret = user_pass_.add_string(user)))
  {
    TBSYS_LOG(WARN, "failed to add string, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = user_pass_.add_string(password)))
  {
    TBSYS_LOG(WARN, "failed to add string, err=%d", ret);
  }
  return ret;
}
const ObStrings* ObSetPasswordStmt::get_user_password() const
{
  return &user_pass_;
}
