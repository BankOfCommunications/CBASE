/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rename_user_stmt.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_rename_user_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObRenameUserStmt::ObRenameUserStmt(const ObRenameUserStmt &other)
  :ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}
ObRenameUserStmt& ObRenameUserStmt::operator=(const ObRenameUserStmt &other)
{
  if (this == &other)
  {
  }
  else
  {
    int ret = OB_SUCCESS;
    const common::ObStrings* rename_infos = other.get_rename_infos();
    int i = 0;
    for (i = 0;i < rename_infos->count();i = i + 2)
    {
      ObString from_user;
      ObString to_user;
      if (OB_SUCCESS != (ret = rename_infos->get_string(i, from_user)))
      {
        TBSYS_LOG(WARN, "get from user failed, ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = rename_infos->get_string(i + 1, to_user)))
      {
        TBSYS_LOG(WARN, "get to user failed, ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = this->add_rename_info(from_user, to_user)))
      {
        TBSYS_LOG(WARN, "add from user and to user  failed, ret=%d", ret);
        break;
      }
    }
  }
  return *this;
}
ObRenameUserStmt::ObRenameUserStmt()
  :ObBasicStmt(ObBasicStmt::T_RENAME_USER)
{
}

ObRenameUserStmt::~ObRenameUserStmt()
{
}

void ObRenameUserStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObRenameUserStmt id=%d>\n", index);
  common::ObString from_user;
  common::ObString to_user;
  for (int64_t i = 0; i < rename_infos_.count()/2; i++)
  {
    if (common::OB_SUCCESS != rename_infos_.get_string(i*2, from_user))
    {
      break;
    }
    else if (common::OB_SUCCESS != rename_infos_.get_string(i*2+1, to_user))
    {
      break;
    }
    if (i == 0)
    {
      print_indentation(fp, level + 1);
      fprintf(fp, "UserInfos := (%.*s -> %.*s)", from_user.length(), from_user.ptr(),
              to_user.length(), to_user.ptr());
    }
    else
    {
      fprintf(fp, ", (%.*s -> %.*s)", from_user.length(), from_user.ptr(),
              to_user.length(), to_user.ptr());
    }
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObRenameUserStmt>\n");
}

int ObRenameUserStmt::add_rename_info(const common::ObString &from_user, const common::ObString &to_user)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = rename_infos_.add_string(from_user)))
  {
    TBSYS_LOG(WARN, "failed to add string, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rename_infos_.add_string(to_user)))
  {
    TBSYS_LOG(WARN, "failed to add string, err=%d", ret);
  }
  return ret;
}
const ObStrings* ObRenameUserStmt::get_rename_infos() const
{
  return &rename_infos_;
}
