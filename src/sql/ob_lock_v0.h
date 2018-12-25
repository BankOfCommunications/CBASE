/**
 * nwpu
 *
 * Authors:
 *   gaojintao
 */
#ifndef _OB_LOCK_V0_H
#define _OB_LOCK_V0_H 1
#include "obmysql/ob_mysql_result_set.h"
//#include "ob_result_set.h"
#include "ob_sql_context.h"
namespace oceanbase
{
  namespace sql
  {
    class ObLockV0
    {
      public:
        ObLockV0(ObSqlContext *sql_context);
        ~ObLockV0() {}
      public:
            int lock_by_for_update(ObRow *row,
                                   const uint64_t &table_id,
                                   bool is_lock,
                                   ObMySQLResultSet & mysql_result);
      private:
            ObSqlContext *sql_context_;
    };
  }; // end namespace sql
}; // end namespace oceanbase

#endif
