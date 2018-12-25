/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_data_source.h is for what ...
 *
 * Version: ***: ob_sql_data_source.h  Wed Jan  9 14:47:21 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_DATA_SOURCE_H_
#define OB_SQL_DATA_SOURCE_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START
#include "ob_sql_server_info.h"
#include "ob_sql_list.h"
#include <mysql/mysql.h>

struct ob_sql_connection_pool;
struct ob_sql_server_pool;
#include "ob_sql_cluster_info.h"
typedef struct ob_sql_connection
{
  MYSQL *mysql_;                 /* real mysql conn */
  ObServerInfo server_;          /* connection server */
  ObSQLListNode *node_;          /* conn belongs to */

  // NOTE: If is_conn_pool_valid_ == false, pool_ is invalid and may be recycled
  bool is_conn_pool_valid_;      /* whether the connection pool is valid */
  ob_sql_connection_pool *pool_; /* mysql pool used when give back conn*/
} ObSQLConn;

//到一个MergeServer的Connection Pool
//全局配置中的minConnection/maxConnection都是针对这个结构
typedef struct ob_sql_connection_pool
{
  ob_sql_server_pool *cluster_; /* cluster data source belongs to */
  ObServerInfo server_;         /* connection server */
  ObSQLConnList conn_list_;     /* conection list free/used */
  pthread_mutex_t mutex_;       /* protected conn list for multithread update */

  bool has_free_conns() { return 0 < conn_list_.free_list_.size_; }
} ObDataSource;

OB_SQL_CPP_END
#endif
