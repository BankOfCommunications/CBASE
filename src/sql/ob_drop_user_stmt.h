/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_drop_user_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_DROP_USER_STMT_H
#define _OB_DROP_USER_STMT_H 1
#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDropUserStmt: public ObBasicStmt
    {
      public:
        ObDropUserStmt();
        virtual ~ObDropUserStmt();
        virtual void print(FILE* fp, int32_t level, int32_t index);

        int add_user(const common::ObString &user);
        const common::ObStrings* get_users() const;
      private:
        // types and constants
      public:
        ObDropUserStmt(const ObDropUserStmt &other);
        ObDropUserStmt& operator=(const ObDropUserStmt &other);
        // function members
      private:
        // data members
        common::ObStrings users_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_DROP_USER_STMT_H */
