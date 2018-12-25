/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_set_password_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SET_PASSWORD_STMT_H
#define _OB_SET_PASSWORD_STMT_H 1
#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSetPasswordStmt: public ObBasicStmt
    {
      public:
        ObSetPasswordStmt();
        virtual ~ObSetPasswordStmt();
        virtual void print(FILE* fp, int32_t level, int32_t index);
        int set_user_password(const common::ObString &user, const common::ObString &password);
        const common::ObStrings* get_user_password() const;
      private:
        // types and constants
      public:
        ObSetPasswordStmt(const ObSetPasswordStmt &other);
        ObSetPasswordStmt& operator=(const ObSetPasswordStmt &other);
        // function members
      private:
        // data members
        common::ObStrings user_pass_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SET_PASSWORD_STMT_H */
