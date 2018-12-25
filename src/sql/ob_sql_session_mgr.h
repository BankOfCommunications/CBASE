/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_session_mgr.h is for what ...
 *
 * Version: ***: ob_sql_session_mgr.h  Sat Apr 27 17:08:31 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_SESSION_MGR_H_
#define OB_SQL_SESSION_MGR_H_

#include "ob_sql_session_info_callback.h"
#include "common/ob_allocator.h"
#include "common/ob_new_scanner.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLServer;
  }
  namespace sql
  {
    typedef int32_t ObSQLSessionKey;
    class ObSQLSessionMgr: public common::ObTimerTask
    {
    public:
      static const int64_t SCHEDULE_PERIOD = 1000*1000; //1s
      ObSQLSessionMgr();
      ~ObSQLSessionMgr();

      int get(ObSQLSessionKey key, ObSQLSessionInfo *&info);
      int set(ObSQLSessionKey key, ObSQLSessionInfo *&info, int flag = 0);
      int create(int64_t hash_bucket);
      int erase(ObSQLSessionKey key);
      int64_t get_session_count() const;
      void set_sql_server(obmysql::ObMySQLServer *server);
      const ObServer* get_server() const;
      int get_scanner(ObNewScanner &scanner);
      int kill_session(uint32_t session_id, bool is_query);
      virtual void runTimerTask();
      void check_session_timeout();

    private:
      common::hash::ObHashMap<ObSQLSessionKey, ObSQLSessionInfo*> session_map_;
      //ObSQLSessionInfoMap session_map_;
      obmysql::ObMySQLServer *sql_server_; /*  */

    };
  }
}
#endif
