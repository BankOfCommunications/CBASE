/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_drop_user_stmt.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_drop_user_stmt.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
  
  
ObDropUserStmt::ObDropUserStmt(const ObDropUserStmt &other)
:ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}
ObDropUserStmt& ObDropUserStmt::operator=(const ObDropUserStmt &other)
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
  }
  return *this;
}
ObDropUserStmt::ObDropUserStmt()
  :ObBasicStmt(ObBasicStmt::T_DROP_USER)
{
}

ObDropUserStmt::~ObDropUserStmt()
{
}

const ObStrings* ObDropUserStmt::get_users() const
{
  return &users_;
}
void ObDropUserStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObDropUserStmt id=%d>\n", index);
  common::ObString user;
  for (int64_t i = 0; i < users_.count(); i++)
  {
    if (common::OB_SUCCESS != users_.get_string(i, user))
    {
      break;
    }
    if (i == 0)
    {
      print_indentation(fp, level + 1);
      fprintf(fp, "Users := (%.*s)", user.length(), user.ptr());
    }
    else
    {
      fprintf(fp, ", (%.*s)", user.length(), user.ptr());
    }
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObDropUserStmt>\n");
}

int ObDropUserStmt::add_user(const common::ObString &user)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = users_.add_string(user)))
  {
    TBSYS_LOG(WARN, "failed to add user to DropUserStmt, err=%d", ret);
  }
  return ret;
}
