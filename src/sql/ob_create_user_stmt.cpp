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

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_strings.h"
#include "ob_create_user_stmt.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

const common::ObStrings* ObCreateUserStmt::get_users() const
{
  return &users_;
}
ObCreateUserStmt::ObCreateUserStmt(const ObCreateUserStmt &other)
: ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}
ObCreateUserStmt& ObCreateUserStmt::operator=(const ObCreateUserStmt &other)
{
  if (this == &other)
  {
  }
  else
  {
    int ret = OB_SUCCESS;
    const common::ObStrings* users = other.get_users();
    int i = 0;
    for (i = 0;i < users->count();i = i + 2)
    {
      ObString user_name;
      ObString pass_word;
      if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
      {
        break;
      }
      else if (OB_SUCCESS != (ret = users->get_string(i + 1, pass_word)))
      {
        break;
      }
      else if (OB_SUCCESS != (ret = this->add_user(user_name, pass_word)))
      {
        break;
      }
    }
  }
  return *this;
}

ObCreateUserStmt::ObCreateUserStmt()
  :ObBasicStmt(ObBasicStmt::T_CREATE_USER)
{
}

ObCreateUserStmt::~ObCreateUserStmt()
{
}

int ObCreateUserStmt::add_user(const common::ObString &user, const common::ObString &password)
{
  int ret = common::OB_SUCCESS;
  if (common::OB_SUCCESS != (ret = users_.add_string(user)))
  {
    TBSYS_LOG(WARN, "failed to add user, err=%d", ret);
  }
  else if (common::OB_SUCCESS != (ret = users_.add_string(password)))
  {
    TBSYS_LOG(WARN, "failed to add password, err=%d", ret);
  }
  return ret;
}

void ObCreateUserStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObCreateUserStmt id=%d>\n", index);
  OB_ASSERT(users_.count() % 2 == 0);
  common::ObString user;
  common::ObString password;
  for (int64_t i = 0; i < users_.count()/2; i++)
  {
    if (common::OB_SUCCESS != users_.get_string(i*2, user))
    {
      break;
    }
    else if (common::OB_SUCCESS != users_.get_string(i*2+1, password))
    {
      break;
    }
    if (i == 0)
    {
      print_indentation(fp, level + 1);
      fprintf(fp, "Users := (%.*s, %.*s)", user.length(), user.ptr(),
              password.length(), password.ptr());
    }
    else
    {
      fprintf(fp, ", (%.*s, %.*s)", user.length(), user.ptr(),
              password.length(), password.ptr());
    }
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObCreateUserStmt>\n");

}
