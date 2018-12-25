/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_grant_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_GRANT_STMT_H
#define _OB_GRANT_STMT_H 1
#include "common/ob_strings.h"
#include "ob_basic_stmt.h"
#include "common/ob_privilege_type.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace sql
  {
    class ObGrantStmt: public ObBasicStmt
    {
      public:
        ObGrantStmt();
        virtual ~ObGrantStmt();
        virtual void print(FILE* fp, int32_t level, int32_t index);

        int add_user(const common::ObString &user);
        int add_priv(const ObPrivilegeType priv);
        int set_table_id(uint64_t table_id);
        //add liumz, [multi_database.priv_management.grant_priv]:20150608:b
        void set_db_name(const common::ObString &db_name);
        const common::ObString & get_db_name() const;
        void set_global_priv(bool is_global);
        bool is_global_priv() const;
        //add:e
        const common::ObStrings* get_users() const;
        uint64_t get_table_id() const;
        const common::ObArray<ObPrivilegeType>* get_privileges() const;
      private:
        // types and constants
      public:
        ObGrantStmt(const ObGrantStmt &other);
        ObGrantStmt& operator=(const ObGrantStmt &other);
        // function members
      private:
        // data members
        common::ObArray<ObPrivilegeType> privileges_;
        uint64_t table_id_;
        common::ObStrings users_;
        //add liumz, [multi_database.priv_management.grant_priv]:20150608:b
        common::ObString db_name_;
        bool is_global_priv_;
        //add:e
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_GRANT_STMT_H */
