/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_define.h is for what ...
 *
 * Version: ***: ob_sql_define.h  Wed Nov 21 10:58:59 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_DEFINE_H_
#define OB_SQL_DEFINE_H_

#include "libobsql.h"

/* LOG */
#define OB_SQL_LOG(level, format_str, ...) TBSYS_LOG(level, "LIBOBSQL: " format_str, ##__VA_ARGS__)

#define CALL_REAL(func, arg...) ((*(g_func_set.real_##func))(arg))

#define CALL_MYSQL_REAL(mysql, func, arg...) ((*(g_func_set.real_##func))(((ObSQLMySQL *)mysql)->conn_->mysql_, ##arg))

#define CALL_STMT_REAL(stmt, func, arg...) ((*(g_func_set.real_##func))(stmt, ##arg))

#define CALL_MYSQL_REAL_WITH_JUDGE_NO_RETVAL(mysql, func, arg...)  \
  do \
  { \
    if (NULL == mysql) \
    { \
      return; \
    } \
    else if (OB_SQL_MAGIC == ((ObSQLMySQL *)mysql)->magic_) \
    { \
      ObSQLMySQL *ob_mysql = ((ObSQLMySQL *)mysql); \
      if (NULL == ob_mysql->conn_) \
      { \
        ObSQLConn *conn = acquire_conn_random(&g_group_ds, ob_mysql); \
        if (NULL == conn) \
        { \
          TBSYS_LOG(WARN, "%s() fail, no connection available now", __FUNCTION__); \
          return; \
        } \
        else \
        { \
          ob_mysql->conn_ = conn; \
          ob_mysql->rconn_ = conn; \
          ob_mysql->wconn_ = conn; \
        } \
      } \
      MYSQL *real_mysql = ob_mysql->conn_->mysql_; \
      return (*(g_func_set.real_##func))(real_mysql, ##arg); \
    } \
    else \
    { \
      return (*(g_func_set.real_##func))((MYSQL *)mysql, ##arg); \
    } \
  }while(0)

#define CALL_MYSQL_REAL_WITH_JUDGE(mysql, errcode, func, arg...)  \
  do \
  { \
    if (NULL == mysql) \
    { \
      return errcode; \
    } \
    else if (OB_SQL_MAGIC == ((ObSQLMySQL *)mysql)->magic_) \
    { \
      ObSQLMySQL *ob_mysql = ((ObSQLMySQL *)mysql); \
      if (NULL == ob_mysql->conn_) \
      { \
        return errcode; \
      } \
      MYSQL *real_mysql = ob_mysql->conn_->mysql_; \
      return (*(g_func_set.real_##func))(real_mysql, ##arg); \
    } \
    else \
    { \
      return (*(g_func_set.real_##func))((MYSQL *)mysql, ##arg); \
    } \
  }while(0)

/* CONSTS */
#define OB_SQL_CONNECTION_INFO_PRINT_INTERVAL (5 * 1000 * 1000)

const uint32_t OB_SQL_INVALID_CLUSTER_ID = UINT32_MAX;

const int64_t OB_SQL_OPT_CONNECT_TIMEOUT = 2;     // 2 seconds
const int64_t OB_SQL_OPT_READ_TIMEOUT = OB_SQL_OPT_CONNECT_TIMEOUT;
const int64_t OB_SQL_OPT_WRITE_TIMEOUT = OB_SQL_OPT_CONNECT_TIMEOUT;

#define OB_SQL_CONFIG_BUFFER_SIZE 1024*512 /* 512k */
#define OB_SQL_IP_BUFFER_SIZE 32
#define OB_SQL_BUFFER_NUM 16
#define OB_SQL_CONFIG_NUM 2
#define OB_SQL_SLOT_PER_MS 100
#define OB_SQL_MAGIC 0xF0ECAB

#define OB_SQL_DEFAULT_MIN_CONN_SIZE  1
#define OB_SQL_DEFAULT_MAX_CONN_SIZE  8

#define OB_SQL_CONFIG_DEFAULT_NAME "/home/admin/libobsql.conf"
#define OB_SQL_CONFIG_ENV "OB_SQL_CONFIG"
#define OB_SQL_CONFIG_LOG "logfile"
#define OB_SQL_CONFIG_URL "initurl"
#define OB_SQL_CONFIG_LOG_LEVEL "loglevel"
#define OB_SQL_CONFIG_MIN_CONN  "minconn"
#define OB_SQL_CONFIG_MAX_CONN  "maxconn"
#define OB_SQL_CONFIG_USERNAME "username"
#define OB_SQL_CONFIG_PASSWD "passwd"
#define OB_SQL_CONFIG_IP "ip"
#define OB_SQL_CONFIG_PORT "port" 

#define OB_SQL_BEG_TRANSACTION "begin"
#define OB_SQL_START_TRANSACTION "start"
#define OB_SQL_COMMIT "commit"
#define OB_SQL_ROLLBACK "rollback"
#define OB_SQL_CREATE "create"
#define OB_SQL_DROP "drop"
#define OB_SQL_SELECT "select"
//#define OB_SQL_REPLACE_OP "repalce"
//#define OB_SQL_INSERT_OP "insert"
//#define OB_SQL_UPDATE_OP "update"

#define OB_SQL_UPDATE_INTERVAL_USEC             (5L * 1000L * 1000L)    /* 5 seconds */
#define OB_SQL_UPDATE_FAIL_WAIT_INTERVAL_USEC   (1L * 1000L * 1000L)    /* 1 second */
#define OB_SQL_RECYCLE_INTERVAL 45
#define OB_SQL_MAX_URL_LENGTH 2048
#define OB_SQL_QUERY_STR_LENGTH 1024
#define OB_SQL_SLOT_NUM 100

// Query cluster information
#define OB_SQL_QUERY_CLUSTER "select cluster_id,cluster_role,cluster_flow_percent,cluster_vip,cluster_port,read_strategy from __all_cluster order by cluster_role ASC"

/** SQL QUERY for mergeserver list */
#define OB_SQL_QUERY_SERVER "select svr_ip,svr_port from __all_server where svr_type='mergeserver' and cluster_id="
#define OB_SQL_CLIENT_VERSION "select /*+ client(libobsql) client_version(4.0.1) mysql_driver(18.0) */ 'client_version'"

/*  */
#define OB_SQL_MAX_CLUSTER_NUM 50
#define OB_SQL_MAX_MS_NUM  1024

#define OB_SQL_MAX_SERVER_BLACK_LIST_SIZE OB_SQL_MAX_MS_NUM

#define OB_SQL_QUERY_RETRY_TIME 3

#define OB_SQL_AHEAD_MASK 0xffff
#define OB_SQL_OFFSET_SHIFT 32

#define OB_SQL_BUCKET_PER_SERVER 100

//client info
#define OB_CLIENT_INFO  "oceanbasae V4.0 client"
#define OB_CLIENT_VERSION 400010 //means 4.0.1
#define OB_SQL_CLIENT_VERSION_51_MIN
//#define OB_SQL_CLIENT_VERSION_55_MIN 50500
#define OB_SQL_CLIENT_VERSION_55_MIN 60000
#define OB_SQL_USER "admin"
#define OB_SQL_PASS "admin"
#define OB_SQL_DB ""
#define OB_SQL_DEFAULT_MMAP_THRESHOLD 64*1024+128
#endif
