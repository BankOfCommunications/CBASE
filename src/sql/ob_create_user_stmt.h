/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_user_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_CREATE_USER_STMT_H
#define _OB_CREATE_USER_STMT_H 1

#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
#include "common/ob_define.h"
namespace oceanbase
{
  namespace sql
  {
    class ObCreateUserStmt: public ObBasicStmt
    {
      public:
        ObCreateUserStmt();
        virtual ~ObCreateUserStmt();

        virtual void print(FILE* fp, int32_t level, int32_t index);

        int add_user(const common::ObString &user, const common::ObString &password);
        const common::ObStrings* get_users() const;
      private:
        // types and constants
      public:
        ObCreateUserStmt(const ObCreateUserStmt &other);
        ObCreateUserStmt& operator=(const ObCreateUserStmt &other);
        // function members
      private:
        // data members
        common::ObStrings users_; // (user1, pass1, user2, pass2, ...)
    };
  } // end namespace sql
} // end namespace oceanbase
#endif /* _OB_CREATE_USER_STMT_H */
