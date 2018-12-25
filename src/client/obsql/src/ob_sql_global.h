/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_global.h is for what ...
 *
 * Version: ***: ob_sql_global.h  Mon Dec  3 13:56:40 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_GLOBAL_H_
#define OB_SQL_GLOBAL_H_

#include "ob_sql_struct.h"
#include "ob_sql_select_method_impl.h"
#include "ob_sql_group_data_source.h"
#include "ob_sql_real_func.h"
#include <pthread.h>

/* one is using, the other is for update */
extern ObSQLGlobalConfig g_config_array[OB_SQL_CONFIG_NUM];
extern ObSQLGlobalConfig *g_config_using;
extern ObSQLGlobalConfig *g_config_update;
extern ObGroupDataSource g_group_ds;
extern ObSQLSelectTable *g_table;
extern pthread_rwlock_t g_config_rwlock;
extern ObSQLSelectMethodSet g_default_method;
extern ObSQLList g_delete_ms_list;
//extern ObSQLRsList rslist;
extern pthread_rwlock_t g_rslist_rwlock;
extern ObServerInfo g_rslist[OB_SQL_MAX_CLUSTER_NUM*2];
extern int32_t g_rsnum;
extern FuncSet g_func_set;
extern volatile int8_t g_inited;
extern ObSQLConfig g_sqlconfig;
extern ObSQLServerBlackList g_ms_black_list;
#endif
