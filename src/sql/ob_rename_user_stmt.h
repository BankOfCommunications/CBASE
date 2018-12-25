/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rename_user_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RENAME_USER_STMT_H
#define _OB_RENAME_USER_STMT_H 1
#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
namespace oceanbase
{
  namespace sql
  {
    class ObRenameUserStmt: public ObBasicStmt
    {
      public:
        ObRenameUserStmt();
        virtual ~ObRenameUserStmt();
        virtual void print(FILE* fp, int32_t level, int32_t index);
        int add_rename_info(const common::ObString &from_user, const common::ObString &to_user);
        const common::ObStrings* get_rename_infos() const;
      private:
        // types and constants
      public:
        ObRenameUserStmt(const ObRenameUserStmt &other);
        ObRenameUserStmt& operator=(const ObRenameUserStmt &other);
        // function members
      private:
        // data members
        common::ObStrings rename_infos_; // (from1, to1), (from2, to2)
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RENAME_USER_STMT_H */
