/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@alipay.com>
 */

#ifndef OB_PRIV_EXECUTOR_H_
#define OB_PRIV_EXECUTOR_H_

#include "common/ob_array.h"
#include "common/page_arena.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_basic_stmt.h"
#include "sql/ob_result_set.h"
#include "sql/ob_no_children_phy_operator.h"
//add wenghaixing [database manage]20150619
#include "common/ob_privilege.h"
//add e

class ObTestPrivExecutor_test_construct_replace_db_stmt_Test;
class ObTestPrivExecutor_test_construct_replace_table_stmt_Test;
class ObTestPrivExecutor_test_construct_update_expressions_Test;
namespace oceanbase
{
  namespace sql
  {
    class ObPrivExecutor : public ObNoChildrenPhyOperator
    {
      friend class ::ObTestPrivExecutor_test_construct_replace_db_stmt_Test;
      friend class ::ObTestPrivExecutor_test_construct_replace_table_stmt_Test;
      friend class ::ObTestPrivExecutor_test_construct_update_expressions_Test;
      public:
        ObPrivExecutor();
        virtual ~ObPrivExecutor();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual ObPhyOperatorType get_type() const { return PHY_PRIV_EXECUTOR; }
        virtual int get_next_row(const ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        void set_stmt(const ObBasicStmt *stmt);
        void set_context(ObSqlContext *context);
      private:
        int do_revoke_privilege(const ObBasicStmt *stmt);
        int do_grant_privilege(const ObBasicStmt *stmt);
        //add liumz, [multi_database.priv_management]:20150608:b
        int do_grant_privilege_v2(const ObBasicStmt *stmt);
        int do_grant_privilege_v3(const ObBasicStmt *stmt);
        int do_revoke_privilege_v2(const ObBasicStmt *stmt);
        //add:e
        int do_drop_user(const ObBasicStmt *stmt);
        int do_create_user(const ObBasicStmt *stmt);
        int do_lock_user(const ObBasicStmt *stmt);
        int do_set_password(const ObBasicStmt *stmt);
        int do_rename_user(const ObBasicStmt *stmt);
        //add wenghaixing [database manage]20150609
        int do_create_db(const ObBasicStmt *stmt);
        int do_use_db(const ObBasicStmt *stmt);
        int do_drop_db(const ObBasicStmt *stmt);
        int db_can_be_drop(const ObString &db_name);
        int db_can_be_connect(const ObString& user_name, const ObString &db_name);
        int direct_execute_sql_has_result(const ObString& sql);
        int direct_execute_sql_db_priv_is_valid(const ObString &sql);
        int direct_execute_sql_table_priv_is_valid(const ObString &sql);
        int sql_get_db_id(const ObString &db_name, uint64_t &db_id)const;
        //when create database,should grant all priv to owner
        int grant_all_db_priv(const int64_t &db_id);
        //when create database, grant all priv to user who has global priv if param is_grant_priv is true
        int grant_all_db_priv_global_user(const ObString &db_name);
        //add e
        //add wenghaixing [database manage->clean privilege]20150907
        int clean_database_privilege(const uint64_t flag_id, int flag = 0);// 0 for db_id,1 for user_id
        int clean_user_privilege(const uint64_t &user_id, const ObArray<int64_t> &db_list,const ObArray<uint64_t> &table_list);
        //add e
      private:
        struct UserInfo
        {
          UserInfo():password_(), comment_(){}
          // 存user_id, 11个权限和1个is_lock字段
          ObArray<int64_t> field_values_;
          ObString password_;
          ObString comment_;
        };
        const ObBasicStmt *stmt_;
        ObSqlContext *context_;
        ObResultSet *result_set_out_;
        common::PageArena<char> page_arena_;
      private:
        // 执行insert，delete，replace等不需要返回行的语句,result是整个权限语句的resultset，
        // 一旦stmt执行出错，用于接住stmt的错误信息
        int execute_stmt_no_return_rows(const ObString &stmt);
        int get_user_id_by_user_name(const ObString &user_name, int64_t &user_id);
        //add liumz, [multi_database.priv_management]:20150608:b
        int get_db_id_by_db_name(const ObString &db_name, int64_t &db_id);
        int get_db_ids_by_user_id(int64_t user_id, ObArray<int64_t> &db_ids);
        int get_table_ids_by_user_db_id(int64_t user_id, int64_t db_id, ObArray<uint64_t> &table_ids);
        int reset_db_privilege(int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges);
        int reset_table_privilege_v2(int64_t user_id, int64_t db_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges);
        int revoke_priv_from_db_privs_by_user_db_id(int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges);
        int revoke_all_priv_from_db_privs_by_user_name(const ObString &user_name);
        //add:e
        //add liumz, [multi_database.priv_management]:20150617:b
        int get_all_db_names(ObStrings &db_names);
        int get_all_table_ids_by_db_name(const ObString &db_name, ObArray<uint64_t> &table_ids);
        int grant_db_privs_by_user_id(int64_t user_id, const ObArray<ObPrivilegeType> *privileges);
        int grant_db_privs_by_user_id_db_name(int64_t user_id, const ObString &db_name, const ObArray<ObPrivilegeType> *privileges);
        int set_db_privilege(int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges);
        int set_table_privilege(int64_t user_id, int64_t db_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges);
        //add:e
        int get_table_ids_by_user_id(int64_t user_id, ObArray<uint64_t> &table_ids);
        int get_table_ids_by_user_id_v2(int64_t user_id, ObArray<int64_t> &db_ids, ObArray<uint64_t> &table_ids);//add liumz, [bugfix: grant table level priv, revoke global level priv]20150902
        int reset_table_privilege(int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges);
        int revoke_all_priv_from_users_by_user_name(const ObString &user_name);
        int revoke_all_priv_from_table_privs_by_user_name(const ObString &user_name);
        int revoke_priv_from_table_privs_by_user_name_v2(const ObString &user_name, const ObArray<ObPrivilegeType> *privileges);//add liumz, [bugfix: grant table level priv, revoke global level priv]20150902
        int execute_delete_user(const ObString &delete_user);
        int execute_update_user(const ObString &update_user);
        int insert_trigger();
        void construct_update_expressions(char *buf, int buf_size, int64_t &pos, const ObArray<ObPrivilegeType> *privileges, bool is_grant);
        void construct_replace_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges);
        //add liumz, [multi_database.priv_management]:20150608:b
        void construct_replace_db_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, int64_t db_id, const ObArray<ObPrivilegeType> *privileges);
        void construct_replace_table_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, int64_t db_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges);
        //add:e
        //add wenghaixing [database manage]20150616
        int execute_delete_database(const ObString &delete_db);
        //add e
        int get_all_columns_by_user_name(const ObString &user_name, UserInfo &user_info);
        int start_transaction();
        // warning:comit then replace into trigger event inner table
        int commit();
        int rollback();
    };
  }// namespace sql
} // namespace oceanbase
#endif
