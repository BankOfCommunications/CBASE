/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SQL_H
#define _OB_SQL_H 1
#include "common/ob_string.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_logical_plan.h"
#include "sql/ob_multi_phy_plan.h"
#include "obmysql/ob_mysql_global.h" // for EMySQLFieldType
namespace oceanbase
{
  namespace sql
  {
    struct ObStmtPrepareResult;
    // this class is the main interface for sql module
    class ObSql
    {
      public:
        /**
         * execute the SQL statement directly
         *
         * @param stmt [in]
         * @param result [out]
         *
         * @return oceanbase error code defined in ob_define.h
         */
        //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
//        static int direct_execute(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context);
        static int direct_execute(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ParseResult* ud_select_parse_result = NULL);
        //mod gaojt 20160418:e
        /**
         * prepare the SQL statement for later execution
         * @see stmt_execute()
         * @param stmt [in]
         * @param result [out]
         * @param context [out]
         *
         * @return oceanbase error code defined in ob_define.h
         */
        static int stmt_prepare(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context);
        /**
         * execute the prepared statement
         *
         * @param stmt_id [in] statement handler id returned by stmt_prepare()
         * @param params [in] parameters for binding
         * @param result [out]
         *
         * @return oceanbase error code defined in ob_define.h
         */
        static int stmt_execute(const uint64_t stmt_id,
                                const common::ObIArray<obmysql::EMySQLFieldType> &params_type,
                                const common::ObIArray<common::ObObj> &params,
                                ObResultSet &result, ObSqlContext &context);
        /**
         * close the prepared statement
         *
         * @param stmt_id [in] statement handler id returned by stmt_prepare()
         * @param context [in]
         *
         * @return oceanbase error code defined in ob_define.h
         */
        static int stmt_close(const uint64_t stmt_id, ObSqlContext &context);
      private:
        // types and constants
      private:
        ObSql(){}
        ~ObSql(){}
        // disallow copy
        ObSql(const ObSql &other);
        ObSql& operator=(const ObSql &other);
        static int generate_logical_plan(const common::ObString &stmt, ObSqlContext & context, ResultPlan  &logical_plan, ObResultSet & result);
        static int generate_physical_plan(ObSqlContext & context, ResultPlan &result_plan, ObMultiPhyPlan & multi_phy_plan, ObResultSet & result);
        static int do_grant_privilege(const ObBasicStmt *stmt, ObSqlContext & context, ObResultSet &result);
        static void clean_result_plan(ResultPlan &result_plan);

        static int do_real_prepare(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ObPsStoreItem *item, bool &do_prepare);
        static int copy_plan_from_store(ObResultSet *result, ObPsStoreItem *item, ObSqlContext *context, uint64_t sql_id);
        /**
         * copy fields/params && physical paln to ObPsStoreItem
         */
        static int copy_plan_to_store(ObResultSet *result, ObPsStoreItem *item);
        static int copy_params_to_plan(ObResultSet *result, const common::ObArray<obmysql::EMySQLFieldType> &params_type,
                                       const common::ObArray<common::ObObj> &params);
        static int try_rebuild_plan(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ObPsStoreItem *item, bool &flag, bool substitute);
        static int copy_physical_plan(ObPhysicalPlan& new_plan, ObPhysicalPlan& old_plan, ObSqlContext *context = NULL);
        static int set_execute_context(ObPhysicalPlan& plan, ObSqlContext& context);
        static bool need_rebuild_plan(const common::ObSchemaManagerV2 *schema_manager, ObPsStoreItem *item);
        //add liuzy [sequence] [JHOBv0.1] 20150327:b
        static int sequence_alter_rebuild_parse(ObSqlContext & context,ParseResult & parse_result, ResultPlan  &result_plan, ObResultSet & result);
        //add 20150327:e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
        static int generate_logical_plan_ud(const common::ObString &stmt,ParseResult ud_select_parse_result, ObSqlContext & context, ResultPlan  &result_plan, ObResultSet & result);
        //add gaojt 20160418:e
        // function members

        // for temp use to deal with special statment
        // todo: use standard sql parser to deal with this purpose
        // @return
        //  true: hook success
        //  false: not hooked
        static bool process_special_stmt_hook(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context);
        static int do_privilege_check(const common::ObString & username, const ObPrivilege **pp_privilege, ObLogicalPlan *plan);
        static bool no_enough_memory();
      private:
        // data members
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_H */
