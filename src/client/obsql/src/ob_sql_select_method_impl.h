/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_default_select.h is for what ...
 *
 * Version: ***: ob_sql_default_select.h  Mon Nov 26 14:01:39 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_DEFAULT_SELECT_H_
#define OB_SQL_DEFAULT_SELECT_H_

#include "ob_sql_define.h"
OB_SQL_CPP_START
#include "ob_sql_struct.h"
#include "ob_sql_select_method.h"
#include "ob_sql_cluster_info.h"
#include "ob_sql_data_source.h"

/**
 * 根据global config中ms table来做一致性hash选择
 * 调用这个函数的之前先要获取g_config_rwlock的读锁
 * consistence hash 
 */
ObDataSource* consishash_mergeserver_select(ObClusterInfo *pool, const char* sql,
                                      unsigned long length);

/* random */
ObSQLConn* random_conn_select(ObDataSource *pool);
ObDataSource* random_mergeserver_select(ObClusterInfo *pool, ObSQLMySQL *mysql);
OB_SQL_CPP_END
#endif
