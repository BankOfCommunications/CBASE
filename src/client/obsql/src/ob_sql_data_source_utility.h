/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_data_source_method.h is for what ...
 *
 * Version: ***: ob_sql_data_source_method.h  Fri Jan 18 11:33:05 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_DATA_SOURCE_METHOD_H_
#define OB_SQL_DATA_SOURCE_METHOD_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START
#include "ob_sql_cluster_info.h"
#include "ob_sql_data_source.h"

/**
 * 在指定的集群里面创建一个datasource
 * @param conns   连接的个数
 * @param server  Ms信息
 * @param cluster 集群信息
 *
 * @return int    OB_SQL_SUCCESS if build success
 *                else return OB_SQL_ERROR
 */
int add_data_source(int32_t conns, ObServerInfo server, ObClusterInfo *cluster);

/// Delete data source from cluster data source list
/// @param cluster specific cluster
/// @param sindex data source index in cluster data source list
void delete_data_source(ObClusterInfo *cluster, int32_t sindex);

// Destroy all connections in datasource
void destroy_data_source(ObDataSource *ds);

/// Recycle specific data source
/// 1. Release every node of free list: close MYSQL connection and free node memory
/// 2. Set connection pool of every connection on used list invalid
void recycle_data_source(ObDataSource *ds);

// Update data source
// If data source connection number is little than normal value, try to create new connection.
// If new connection is created successfully, delete it from black list
int update_data_source(ObDataSource &ds);

int create_real_connection(ObDataSource *pool);
OB_SQL_CPP_END
#endif
