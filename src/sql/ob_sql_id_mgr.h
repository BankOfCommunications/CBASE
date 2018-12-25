/*
 * (C) 2013-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_sql_id_mgr.h$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@alipay.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_SQL_ID_MGR
#define OCEANBASE_SQL_OB_SQL_ID_MGR

#include <common/ob_string.h>
#include <common/hash/ob_hashmap.h>
#include <common/ob_string_buf.h>
#include "common/ob_new_scanner.h"
namespace oceanbase
{
  namespace sql
  {
    typedef common::ObString SQLIdKey;
    typedef int64_t SQLIdValue;
    struct SQLStat
    {
      common::ObString sql_;
      int64_t prepare_count_;
      int64_t execute_count_;
      int64_t execute_time_;
      int64_t last_active_timestamp_;
      int64_t slow_count_;
      int64_t create_timestamp_;
      SQLStat()
        :prepare_count_(0),
         execute_count_(0),
         execute_time_(0),
         last_active_timestamp_(0),
         slow_count_(0),
         create_timestamp_(0)
      {
      }
    };

    class ObSQLIdMgr
    {
      public:
        static const int64_t SQL_ID_MAX_BUCKET_NUM = 1L << 20;
        typedef common::hash::ObHashMap<SQLIdKey, SQLIdValue> SqlIdMap;
        typedef common::hash::ObHashMap<SQLIdValue, SQLStat> IdSqlMap;
      public:
        ObSQLIdMgr();
        ~ObSQLIdMgr();
        int init();
        int get_sql_id (const common::ObString & sql, int64_t & sql_id);
        int64_t allocate_new_id();
        int get_sql(int64_t sql_id, common::ObString &sql) const;

        // statistics
        int stat_prepare(int64_t sql_id);
        int stat_execute(int64_t sql_id, int64_t elapsed_us, bool is_slow);
        int get_scanner(const common::ObServer &addr, common::ObNewScanner &scanner) const;
        void print() const;
      protected:
        tbsys::CThreadMutex lock_;
        int64_t               last_sql_id_;
        SqlIdMap              sql2id_map_;
        IdSqlMap id2sql_map_;
        common::ObStringBuf   sql_str_buf_;
    };
  } // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_SQL_ID_MGR
