/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_group_data_source.h is for what ...
 *
 * Version: ***: ob_sql_group_data_source.h  Wed Jan  9 16:17:32 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_GROUP_DATA_SOURCE_H_
#define OB_SQL_GROUP_DATA_SOURCE_H_


#include "ob_sql_define.h"

OB_SQL_CPP_START
#include "ob_sql_struct.h"
#include "ob_sql_cluster_info.h"

typedef struct ob_sql_cluster_pool
{
  int32_t min_conn_;            /* min conn per server */
  int32_t max_conn_;            /* max conn per server */
  int32_t size_;                /* cluster number */
  //ob_sql_list_t cluster_list_;
  ObClusterInfo *clusters_[OB_SQL_MAX_CLUSTER_NUM];     // NOTE: cluster pointer array
  pthread_rwlock_t rwlock_;
} ObGroupDataSource;

/** 根据获得的配置文件以及MS list来更新全局的连接池
 *  调用之前拿到了全局读写锁
 *  ObGroupDataSource
 *     ----------  ObClusterInfo
 *                     ----------  ObDataSource
 */
int update_group_ds(ObSQLGlobalConfig *config, ObGroupDataSource *gds);

void destroy_group_ds(ObGroupDataSource *gds);

ObClusterInfo *get_master_cluster(ObGroupDataSource *gds);
OB_SQL_CPP_END
#endif
