/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_cluster_info.h is for what ...
 *
 * Version: ***: ob_sql_cluster_info.h  Wed Jan  9 19:30:02 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_CLUSTER_INFO_H_
#define OB_SQL_CLUSTER_INFO_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START
#include "ob_sql_server_info.h"
#include "ob_sql_data_source.h"
#include "ob_sql_list.h"
#include <mysql/mysql.h>
#include <pthread.h>

typedef enum ob_sql_cluster_type
{
  MASTER_CLUSTER = 1,
  SLAVE_CLUSTER,
} ObSQLClusterType;

//一个集群中的MS list
typedef struct ob_sql_server_pool
{
  int16_t flow_weight_;        /* flow weight */
  int32_t size_;               /* datasource number */
  uint32_t cluster_id_;
  ObSQLClusterType cluster_type_;
  ObServerInfo cluster_addr_;   /* cluster address: LMS */
  ObDataSource *dslist_[OB_SQL_MAX_MS_NUM];     // NOTE: data source pointer array
  uint32_t read_strategy_;
} ObClusterInfo;
OB_SQL_CPP_END
#endif
