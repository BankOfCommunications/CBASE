/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_cluster_config.h is for what ...
 *
 * Version: ***: ob_sql_cluster_config.h  Wed Jan  9 15:38:42 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_CLUSTER_CONFIG_H_
#define OB_SQL_CLUSTER_CONFIG_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START

#include "ob_sql_struct.h"

/**
 * 更新集群地址列表
 *
 * @param cluster_address 采用逗号分隔的集群地址列表字符串
 *
 * @return int  成功返回OB_SQL_SUCCESS, 否则返回OB_SQL_ERROR
 * */
int update_cluster_address_list(const char *cluster_address);

void reset_cluster_address_list();

/**
 * 根据fake ms列表, 通过标准SQL获取集群的信息
 * 1  select cluster_id,cluster_role,cluster_flow_percent,cluster_vip,cluster_port,read_strategy from __all_cluster
 * 2  select svr_ip,svr_port from __all_server where svr_type='mergeserver' and cluster_id=xxx
 * fake ms列表是从配置文件，以及配置服务器上读取的
 *
 * @return int  成功返回OB_SQL_SUCCESS, 否则返回OB_SQL_ERROR
 */
int get_ob_config();

/**
 * 根据获取到的新集群信息来更新客户端的配置以及GroupDataSource
 * 1、获取g_config_rwlock的写锁, 保证没有线程在使用当前的配置
 * 2、交互g_config_using  g_config_update指针
 * 3、根据配置的变化情况分别更新ObGroupDataSource
 *    基于集群流量的集群选择标，用于一致性HASH的MergeServer选择表
 *
 * @return int  成功返回OB_SQL_SUCCESS, 否则返回OB_SQL_ERROR
 */
int do_update();

/// Delete cluster from ObSQLGlobalConfig
/// @param config target config
/// @param index target cluster index
void delete_cluster_from_config(ObSQLGlobalConfig &config, const int index);

// Destroy global config: g_config_update and g_config_using
void destroy_global_config();
OB_SQL_CPP_END
#endif
