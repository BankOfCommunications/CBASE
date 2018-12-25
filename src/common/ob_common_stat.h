/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_mysql_stat.cpp,  03/14/2013 05:13:14 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   Sql perf statistics
 *
 */
#ifndef __OB_COMMON_STAT__
#define __OB_COMMON_STAT__

#include "common/ob_define.h"
#include "common/ob_statistics.h"

namespace oceanbase
{
  namespace common
  {
    /* rootserver */
    enum
    {
      INDEX_SUCCESS_GET_COUNT = 0,
      INDEX_SUCCESS_SCAN_COUNT,
      INDEX_FAIL_GET_COUNT,
      INDEX_FAIL_SCAN_COUNT,
      INDEX_GET_OBI_ROLE_COUNT,
      INDEX_MIGRATE_COUNT,
      INDEX_COPY_COUNT,
      INDEX_GET_SCHMEA_COUNT,
      INDEX_REPORT_VERSION_COUNT,
      INDEX_ALL_TABLE_COUNT,
      INDEX_ALL_TABLET_COUNT,
      INDEX_ALL_ROW_COUNT,
      INDEX_ALL_DATA_SIZE,
      ROOTSERVER_STAT_MAX,
    };
    /* updateserver */
    enum
    {
      UPS_STAT_GET_COUNT = 0,
      UPS_STAT_SCAN_COUNT,
      UPS_STAT_TRANS_COUNT,
      UPS_STAT_APPLY_COUNT,
      UPS_STAT_BATCH_COUNT,
      UPS_STAT_MERGE_COUNT,

      UPS_STAT_GET_QTIME,
      UPS_STAT_SCAN_QTIME,
      UPS_STAT_APPLY_QTIME,

      UPS_STAT_GET_TIMEU,
      UPS_STAT_SCAN_TIMEU,
      UPS_STAT_TRANS_TIMEU,
      UPS_STAT_TRANS_WTIME, // packet wait
      UPS_STAT_TRANS_HTIME, // trans handle
      UPS_STAT_TRANS_CTIME, // wait commit thread
      UPS_STAT_TRANS_FTIME, // wait flush
      UPS_STAT_TRANS_RTIME, // wait response
      UPS_STAT_APPLY_TIMEU,
      UPS_STAT_BATCH_TIMEU,
      UPS_STAT_MERGE_TIMEU,

      UPS_STAT_MEMORY_TOTAL,
      UPS_STAT_MEMORY_LIMIT,
      UPS_STAT_MEMTABLE_TOTAL,
      UPS_STAT_MEMTABLE_USED,
      UPS_STAT_TOTAL_LINE,

      UPS_STAT_ACTIVE_MEMTABLE_LIMIT,
      UPS_STAT_ACTICE_MEMTABLE_TOTAL,
      UPS_STAT_ACTIVE_MEMTABLE_USED,
      UPS_STAT_ACTIVE_TOTAL_LINE,

      UPS_STAT_FROZEN_MEMTABLE_LIMIT,
      UPS_STAT_FROZEN_MEMTABLE_TOTAL,
      UPS_STAT_FROZEN_MEMTABLE_USED,
      UPS_STAT_FROZEN_TOTAL_LINE,

      UPS_STAT_APPLY_FAIL_COUNT,
      UPS_STAT_TBSYS_DROP_COUNT,
      UPS_STAT_PACKET_LONG_WAIT_COUNT,

      UPS_STAT_COMMIT_LOG_SIZE,
      UPS_STAT_COMMIT_LOG_ID,
      UPS_STAT_CLOG_SYNC_COUNT,
      UPS_STAT_CLOG_SYNC_DELAY,
      UPS_STAT_SLOW_CLOG_SYNC_COUNT,
      UPS_STAT_SLOW_CLOG_SYNC_DELAY,
      UPS_STAT_LAST_REPLAY_CLOG_TIME,

      UPS_STAT_FROZEN_VERSION,

      UPS_STAT_APPLY_ROW_COUNT,
      UPS_STAT_APPLY_ROW_UNMERGED_CELL_COUNT,

      UPS_STAT_LL_GET_COUNT,
      UPS_STAT_LL_SCAN_COUNT,
      UPS_STAT_LL_APPLY_COUNT,
      UPS_STAT_LL_TRANS_COUNT,
      UPS_STAT_LL_APPLY_QTIME,

      UPS_STAT_NL_GET_COUNT,
      UPS_STAT_NL_SCAN_COUNT,
      UPS_STAT_NL_APPLY_COUNT,
      UPS_STAT_NL_TRANS_COUNT,
      UPS_STAT_NL_APPLY_QTIME,

      UPS_STAT_HL_GET_COUNT,
      UPS_STAT_HL_SCAN_COUNT,
      UPS_STAT_HL_APPLY_COUNT,
      UPS_STAT_HL_TRANS_COUNT,
      UPS_STAT_HL_APPLY_QTIME,

      UPS_STAT_LL_GET_TIMEU,
      UPS_STAT_LL_SCAN_TIMEU,
      UPS_STAT_LL_APPLY_TIMEU,
      UPS_STAT_LL_TRANS_TIMEU,

      UPS_STAT_NL_GET_TIMEU,
      UPS_STAT_NL_SCAN_TIMEU,
      UPS_STAT_NL_APPLY_TIMEU,
      UPS_STAT_NL_TRANS_TIMEU,

      UPS_STAT_HL_GET_TIMEU,
      UPS_STAT_HL_SCAN_TIMEU,
      UPS_STAT_HL_APPLY_TIMEU,
      UPS_STAT_HL_TRANS_TIMEU,

      UPS_STAT_LOCK_WAIT_TIME,
      UPS_STAT_LOCK_SUCC_COUNT,
      UPS_STAT_LOCK_FAIL_COUNT,

      UPS_STAT_HL_QUEUED_COUNT,
      UPS_STAT_NL_QUEUED_COUNT,
      UPS_STAT_LL_QUEUED_COUNT,
      UPS_STAT_HT_QUEUED_COUNT,
      UPS_STAT_TRANS_COMMIT_QUEUED_COUNT,
      UPS_STAT_TRANS_RESPONSE_QUEUED_COUNT,

      UPS_STAT_GET_BYTES,
      UPS_STAT_SCAN_BYTES,

      UPS_STAT_DML_REPLACE_COUNT,
      UPS_STAT_DML_INSERT_COUNT,
      UPS_STAT_DML_UPDATE_COUNT,
      UPS_STAT_DML_DELETE_COUNT,

      UPDATESERVER_STAT_MAX,
    };
    /* chunkserver */
    enum
    {
      /* meta stat index */
      INDEX_CS_SERVING_VERSION = 0,
      INDEX_META_OLD_VER_TABLETS_NUM,
      INDEX_META_OLD_VER_MERGED_TABLETS_NUM,
      INDEX_META_NEW_VER_TABLETS_NUM,

      INDEX_MU_DEFAULT,
      INDEX_MU_NETWORK,
      INDEX_MU_THREAD_BUFFER,
      INDEX_MU_TABLET,
      INDEX_MU_BI_CACHE,
      INDEX_MU_BLOCK_CACHE,
      INDEX_MU_BI_CACHE_UNSERVING,
      INDEX_MU_BLOCK_CACHE_UNSERVING,
      INDEX_MU_JOIN_CACHE,
      INDEX_MU_SSTABLE_ROW_CACHE,
      INDEX_MU_MERGE_BUFFER,
      INDEX_MU_MERGE_SPLIT_BUFFER,

      INDEX_META_REQUEST_COUNT,
      INDEX_META_REQUEST_COUNT_PER_SECOND,
      INDEX_META_QUEUE_WAIT_TIME,

      /* table stat index */
      INDEX_GET_COUNT,
      INDEX_SCAN_COUNT,

      INDEX_GET_TIME,
      INDEX_SCAN_TIME,

      INDEX_GET_BYTES,
      INDEX_SCAN_BYTES,

      // cs version error
      FAIL_CS_VERSION_COUNT,

      QUERY_QUEUE_COUNT,
      QUERY_QUEUE_TIME,

      OPEN_FILE_COUNT,
      CLOSE_FILE_COUNT,

      // inc query
      INC_QUERY_GET_COUNT,
      INC_QUERY_GET_TIME,
      INC_QUERY_SCAN_COUNT,
      INC_QUERY_SCAN_TIME,

      CHUNKSERVER_STAT_MAX,
    };
    /* sstable */
    enum
    {
      INDEX_BLOCK_INDEX_CACHE_HIT = 0,
      INDEX_BLOCK_INDEX_CACHE_MISS,

      INDEX_BLOCK_CACHE_HIT,
      INDEX_BLOCK_CACHE_MISS,

      INDEX_DISK_IO_READ_NUM,
      INDEX_DISK_IO_READ_BYTES,

      INDEX_DISK_IO_WRITE_NUM,
      INDEX_DISK_IO_WRITE_BYTES,

      INDEX_SSTABLE_ROW_CACHE_HIT,
      INDEX_SSTABLE_ROW_CACHE_MISS,

      INDEX_SSTABLE_GET_ROWS,
      INDEX_SSTABLE_SCAN_ROWS,

      SSTABLE_STAT_MAX,
    };
    /* mergeserver */
    enum
    {
      // ms_get
      NB_GET_OP_COUNT = 0,
      NB_GET_OP_TIME,
      // ms_scan
      NB_SCAN_OP_COUNT,
      NB_SCAN_OP_TIME,

      // ms_sql_get
      SQL_GET_EVENT_COUNT,
      SQL_GET_EVENT_TIME,
      // ms_sql_scan
      SQL_SCAN_EVENT_COUNT,
      SQL_SCAN_EVENT_TIME,
      // ups_execute
      SQL_UPS_EXECUTE_COUNT,
      SQL_UPS_EXECUTE_TIME,

     /* memory usage statistics*/
      MS_MEMORY_LIMIT,
      MS_MEMORY_TOTAL,
      MS_SQL_MU_PARSER,
      MS_SQL_MU_TRANSFORMER,
      MS_SQL_MU_PS_PLAN,
      MS_SQL_MU_RPC_REQUEST,
      MS_SQL_MU_ARRAY,
      MS_SQL_MU_EXPR,
      MS_SQL_MU_ROW_STORE,
      MS_SQL_MU_SESSION,

      MERGESERVER_STAT_MAX,
    };
    /* common */
    enum
    {
      STAT_ROW_DESC_SLOW_FIND_COUNT,
      // cache hit
      HIT_CS_CACHE_COUNT,
      MISS_CS_CACHE_COUNT,
      // rpc bytes
      RPC_BYTES_IN,
      RPC_BYTES_OUT,

      COMMON_STAT_MAX
    };
    /* sql */
    enum
    {
      SQL_GRANT_PRIVILEGE_COUNT = 0,
      SQL_REVOKE_PRIVILEGE_COUNT,
      SQL_SHOW_GRANTS_COUNT,

      SQL_CREATE_USER_COUNT,
      SQL_DROP_USER_COUNT,
      SQL_LOCK_USER_COUNT,
      SQL_SET_PASSWORD_COUNT,
      SQL_RENAME_USER_COUNT,

      SQL_CREATE_TABLE_COUNT,
      SQL_DROP_TABLE_COUNT,
      SQL_TRUNCATE_TABLE_COUNT, /*add zhaoqiong [Truncate Table]:20160318*/

      SQL_PS_ALLOCATOR_COUNT,
      SQL_INSERT_CACHE_HIT,
      SQL_INSERT_CACHE_MISS,
      SQL_UPDATE_CACHE_HIT,
      SQL_UPDATE_CACHE_MISS,
      SQL_QUERY_CACHE_HIT,
      SQL_QUERY_CACHE_MISS,

      SQL_DISTINCT_STMT_COUNT,
      SQL_PS_COUNT,
      //add wenghaixing [database manage]20150616
      SQL_CREATE_DB_COUNT,
      SQL_DROP_DB_COUNT,
      //add e
      //add liumz, [multi_database.alter_table]20150709:b
      SQL_ALTER_TABLE_COUNT,
      //add:e
      SQL_STAT_MAX
    };
    /* obmysql */
    enum
    {
      SUCC_QUERY_COUNT = 0,
      FAIL_QUERY_COUNT,
      SUCC_PREPARE_COUNT,
      FAIL_PREPARE_COUNT,
      SUCC_EXEC_COUNT,
      FAIL_EXEC_COUNT,
      SUCC_CLOSE_STMT_COUNT,
      FAIL_CLOSE_STMT_COUNT,
      SUCC_LOGIN_COUNT,
      FAIL_LOGIN_COUNT,
      SUCC_LOGOUT_COUNT,
      LOGIN_TIME,

      SQL_COMPOUND_COUNT,
      SQL_COMPOUND_TIME,

      SQL_SELECT_COUNT,
      SQL_SELECT_TIME,

      SQL_INSERT_COUNT,
      SQL_INSERT_TIME,

      SQL_REPLACE_COUNT,
      SQL_REPLACE_TIME,

      SQL_UPDATE_COUNT,
      SQL_UPDATE_TIME,

      SQL_DELETE_COUNT,
      SQL_DELETE_TIME,

      SQL_QUERY_BYTES,

      SQL_COMMIT_COUNT,
      SQL_ROLLBACK_COUNT,
      SQL_END_TRANS_TIME,
      SQL_AUTOCOMMIT_ON_COUNT,
      SQL_AUTOCOMMIT_OFF_COUNT,

      SQL_CMD_RECEIVED_COUNT,
      SQL_CMD_PROCESS_COUNT,
      SQL_CMD_WAIT_TIME_MS,

      SQL_MULTI_STMT_TRANS_COUNT,
      SQL_MULTI_STMT_TRANS_TIME,
      SQL_MULTI_STMT_TRANS_STMT_INTERVAL,
      SQL_MULTI_STMT_TRANS_STMT_COUNT,

      OBMYSQL_STAT_MAX
    };


    class ObStatSingleton
    {
      public:
        static void init(ObStatManager *mgr)
        {
          mgr_ = mgr;
          // register_stat(mgr);
        }
        static ObStatManager * get_instance()
        {
          return mgr_;
        }
      private:
        ObStatSingleton() {}
        ObStatSingleton(const ObStatSingleton &) {}
      public:
        static const char *rs_map[];
        static const char *ups_map[];
        static const char *cs_map[];
        static const char *ms_map[];
        static const char *common_map[];
        static const char *sql_map[];
        static const char *obmysql_map[];
        static const char *sstable_map[];
      private:
        static ObStatManager *mgr_;
    };
  }
}

#endif
