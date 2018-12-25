/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lock_user_stmt.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_lock_user_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObLockUserStmt::ObLockUserStmt(const ObLockUserStmt &other)
:ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}

ObLockUserStmt& ObLockUserStmt::operator=(const ObLockUserStmt &other)
{
  if (this == &other)
  {
  }
  else
  {
    int ret = OB_SUCCESS;
    const common::ObStrings* users = other.get_user();
    int i = 0;
    for (i = 0;i < users->count();i++)
    {
      ObString user_name;
      if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
      {
        TBSYS_LOG(WARN, "get user name failed,ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = this->set_lock_info(user_name, other.is_locked())))
      {
        TBSYS_LOG(WARN, "add user failed, ret=%d", ret);
        break;
      }
    }
  }
  return *this;
}
ObLockUserStmt::ObLockUserStmt()
  :ObBasicStmt(ObBasicStmt::T_LOCK_USER), locked_(false)
{
}

ObLockUserStmt::~ObLockUserStmt()
{
}

void ObLockUserStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObLockUserStmt id=%d>\n", index);
  common::ObString user;
  if (common::OB_SUCCESS != user_.get_string(0, user))
  {
    TBSYS_LOG(WARN, "failed to get user");
  }
  else
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "LockInfo := (%.*s, %s)",
            user.length(), user.ptr(), locked_?"LOCKED":"UNLOCKED");
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObLockUserStmt>\n");
}

bool ObLockUserStmt::is_locked() const
{
  return locked_;
}
const ObStrings* ObLockUserStmt::get_user() const
{
  return &user_;
}
int ObLockUserStmt::set_lock_info(const common::ObString &user, bool locked)
{
  int ret = OB_SUCCESS;
  if (0 != user_.count())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "should be set only once");
  }
  else if (OB_SUCCESS != (ret = user_.add_string(user)))
  {
    TBSYS_LOG(WARN, "failed to add string, err=%d", ret);
  }
  else
  {
    locked_ = locked;
  }
  return ret;
}
