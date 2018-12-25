/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lock_user_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_LOCK_USER_STMT_H
#define _OB_LOCK_USER_STMT_H 1
#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
namespace oceanbase
{
  namespace sql
  {
    class ObLockUserStmt: public ObBasicStmt
    {
      public:
        ObLockUserStmt();
        virtual ~ObLockUserStmt();
        virtual void print(FILE* fp, int32_t level, int32_t index);

        int set_lock_info(const common::ObString &user, bool locked);
        const common::ObStrings* get_user() const;
        bool is_locked() const;
      private:
        // types and constants
      public:
        ObLockUserStmt(const ObLockUserStmt &other);
        ObLockUserStmt& operator=(const ObLockUserStmt &other);
        // function members
      private:
        // data members
        common::ObStrings user_;
        bool locked_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_LOCK_USER_STMT_H */
