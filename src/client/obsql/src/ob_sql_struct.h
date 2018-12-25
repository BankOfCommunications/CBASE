/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_struct.h is for what ...
 *
 * Version: ***: ob_sql_struct.h  Mon Dec  3 13:43:18 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_STRUCT_H_
#define OB_SQL_STRUCT_H_
#include "ob_sql_define.h"

OB_SQL_CPP_START
#include <mysql/mysql.h>
#include <stdint.h>
#include "ob_sql_list.h"
#include "ob_sql_define.h"
#include "ob_sql_server_info.h"
#include "ob_sql_cluster_info.h"
#include "ob_sql_data_source.h"

typedef enum enum_sql_type
{
  OB_SQL_UNKNOWN = 1,
  OB_SQL_BEGIN_TRANSACTION,
  OB_SQL_END_TRANSACTION,
  OB_SQL_CONSISTENCE_REQUEST,
  OB_SQL_DDL,
  OB_SQL_WRITE,
  OB_SQL_READ,
} ObSQLType;

typedef struct ob_sql_rs_list
{
  ObServerInfo entrance_[2][OB_SQL_MAX_CLUSTER_NUM*2];
  int size_[2];
  int using_;
} ObSQLRsList;

typedef struct ob_sql_select_table
{
  // master_count_用于记录必须访问主集群的请求在主集群上的连接的数量
  // 当其它请求获取连接时，优先访问备集群，如果访问主集群，必须将该值减为0
  volatile uint32_t master_count_;

  // Data fields
  ObClusterInfo *master_cluster_;
  volatile uint32_t cluster_index_;
  ObClusterInfo *clusters_[OB_SQL_SLOT_NUM];

  // 重置data域，除了master_count_
  void reset_data()
  {
    master_cluster_ = NULL;
    cluster_index_ = 0;
    memset(clusters_, 0, sizeof(clusters_));
  }
} ObSQLSelectTable;

typedef struct ob_sql_hash_item
{
  uint32_t hashvalue_;
  ObDataSource* server_;       /* merge server means a connection pool */
} ObSQLHashItem;

typedef struct ob_sql_select_ms_table
{
  int64_t slot_num_;
  int64_t offset_;
  ObSQLHashItem *items_;
} ObSQLSelectMsTable;

typedef struct ob_sql_cluster_config
{
  ObSQLClusterType cluster_type_;
  uint32_t cluster_id_;
  int16_t flow_weight_;
  int16_t server_num_;
  ObServerInfo server_;         /* listen ms info */
  ObServerInfo merge_server_[OB_SQL_MAX_MS_NUM];
// NOTE: Disable updating MS select table
// As this module has BUG which will cause "Core Dump" in consishash_mergeserver_select.
// wanhong.wwh 2014-04-09
#ifdef ENABLE_SELECT_MS_TABLE
  ObSQLSelectMsTable *table_;
#endif
  uint32_t read_strategy_;
} ObSQLClusterConfig;

typedef struct ob_sql_global_config
{
  bool read_slave_only_;
  int16_t min_conn_size_;
  int16_t max_conn_size_;
  int16_t cluster_size_;
  int16_t ms_table_inited_;
  ObSQLClusterConfig clusters_[OB_SQL_MAX_CLUSTER_NUM];
} ObSQLGlobalConfig;

/**
 * 这个数据结构用来重新解释用户持有的MYSQLhandle
 * 通过这一层转换把用户持有的对象转换到实际的MYSQL上
 */
typedef struct ob_sql_mysql
{
  ObSQLConn* wconn_;
  ObSQLConn* rconn_;
  ObSQLConn* conn_;
  my_bool is_consistence_;
  int64_t magic_;               /*  */
  my_bool alloc_;               /* mem alloc by obsql */
  my_bool in_transaction_;
  my_bool has_stmt_;
  my_bool retry_;
  ObServerInfo last_ds_;
  struct charset_info_st *charset; /* compatible with mysql mysql_init会设置这个值*/
  char buffer[sizeof(MYSQL) - 3*sizeof(ObSQLConn*) - sizeof(my_bool) - sizeof(int64_t) - sizeof(my_bool) - sizeof(my_bool) - sizeof(my_bool) - sizeof(my_bool) -sizeof(ObServerInfo) - sizeof(char*)];
} ObSQLMySQL;

struct ObSQLServerBlackList
{
  uint32_t ip_array_[OB_SQL_MAX_SERVER_BLACK_LIST_SIZE];
  volatile int32_t size_;     // 被多线程读取和修改

  ObSQLServerBlackList();

  bool exist(const uint32_t ip);
  int add(const uint32_t ip);
  void del(const uint32_t ip);
};

OB_SQL_CPP_END
#endif
