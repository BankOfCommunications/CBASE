/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@taobao.com>
 */

#ifndef _OB_GET_PRIVILEGE_TASK_H_
#define _OB_GET_PRIVILEGE_TASK_H_

#include "common/ob_timer.h"
#include "common/ob_privilege_manager.h"
#include "ob_ms_sql_proxy.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObGetPrivilegeTask : public common::ObTimerTask
    {
      public:
        ObGetPrivilegeTask();
        ~ObGetPrivilegeTask();
        void runTimerTask();
        void init(ObPrivilegeManager *privilege_mgr, ObMsSQLProxy *sql_proxy, const int64_t privilege_version);
        sql::ObSQLSessionInfo & get_session_info();
        sql::ObSqlContext & get_sql_context();
      private:
        int load_user_table(common::ObPrivilege *p_privilege);
        int load_privilege_table(common::ObPrivilege *p_privilege);
        //add wenghaixing [database manage]20150607
        int load_database_table(common::ObPrivilege *p_privilege);
        int load_database_priv_table(common::ObPrivilege *p_privilege);
        int load_table_database(common::ObPrivilege *p_privilege);
        //add e
      private:
        ObPrivilegeManager *privilege_mgr_;
        ObMsSQLProxy *sql_proxy_;
        int64_t privilege_version_;
        static const ObString &get_users;
        static const ObString & get_table_privileges;
        //add wenghaixing [database manage]20150607
        static const ObString &get_databases;
        static const ObString &get_database_privilege;
        static const ObString &get_first_tablet_entry;
        //add e


    };
  }// namespace mergeserver
}// namespace oceanbase






#endif /* _OB_GET_PRIVILEGE_TASK_H_ */
