/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_cluster_select.h is for what ...
 *
 * Version: ***: ob_sql_cluster_select.h  Tue Nov 20 10:33:59 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_CLUSTER_SELECT_H_
#define OB_SQL_CLUSTER_SELECT_H_
#include "ob_sql_define.h"

OB_SQL_CPP_START

#include "ob_sql_struct.h"
#include "ob_sql_group_data_source.h"

//分配空间以及初始化表格
//为用户的mysql handler 增加一个选择表
int init_table(ObSQLGlobalConfig *config);
int update_select_table(ObGroupDataSource *gds);

/**
 * 根据选择表 选择一个集群 
 * 调用这个函数的之前先要获取g_config_rwlock的读锁
 *
 */
ObClusterInfo* select_cluster(ObSQLMySQL *mysql);

// Destroy g_table
void destroy_select_table();
OB_SQL_CPP_END
#endif
