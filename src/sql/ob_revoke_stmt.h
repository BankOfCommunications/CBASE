/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_revoke_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_REVOKE_STMT_H
#define _OB_REVOKE_STMT_H 1
#include "ob_basic_stmt.h"
#include "common/ob_strings.h"
#include "common/ob_array.h"
#include "common/ob_privilege_type.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace sql
  {
    class ObRevokeStmt: public ObBasicStmt
    {
      public:
        ObRevokeStmt();
        virtual ~ObRevokeStmt();
        virtual void print(FILE* fp, int32_t level, int32_t index);

        int add_user(const common::ObString &user);
        int add_priv(const ObPrivilegeType priv);
        int set_table_id(uint64_t table_id);
        uint64_t get_table_id() const;
        //add liumz, [multi_database.priv_management.revoke_priv]:20150608:b
        void set_db_name(const common::ObString &db_name);
        const common::ObString & get_db_name() const;
        void set_global_priv(bool is_global);
        bool is_global_priv() const;
        //add:e
        const common::ObStrings* get_users() const;
        const common::ObArray<ObPrivilegeType>* get_privileges() const;
      private:
        // types and constants
      public:
        ObRevokeStmt(const ObRevokeStmt &other);
        ObRevokeStmt& operator=(const ObRevokeStmt &other);
        // function members
      private:
        // data members
        common::ObArray<ObPrivilegeType> privileges_;
        uint64_t table_id_;     // 0 means `*', INVALID_ID means ALL
        common::ObStrings users_;
        //add liumz, [multi_database.priv_management.revoke_priv]:20150608:b
        common::ObString db_name_;
        bool is_global_priv_;
        //add:e
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_REVOKE_STMT_H */
