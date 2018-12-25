#include "ob_sql_global.h"

/* 全局配置 集群机器列表在客户端的缓存 */
ObSQLGlobalConfig g_config_array[OB_SQL_CONFIG_NUM];
ObSQLGlobalConfig *g_config_using = g_config_array + 0;
ObSQLGlobalConfig *g_config_update = g_config_array + 1;

/* 真实连接的连接池 */
ObGroupDataSource g_group_ds;

/* 用于集群选择的对照标 */
ObSQLSelectTable *g_table;

/* 读写锁保护global_config && global_table */
pthread_rwlock_t g_config_rwlock = PTHREAD_RWLOCK_INITIALIZER;

/* 默认连接选择策略 consistence hash */
ObSQLSelectMethodSet g_default_method;

//ObSQLRsList rslist;
pthread_rwlock_t g_rslist_rwlock = PTHREAD_RWLOCK_INITIALIZER;
ObServerInfo g_rslist[OB_SQL_MAX_CLUSTER_NUM * 2];
int32_t g_rsnum;

FuncSet g_func_set;

//obsql init flag
volatile int8_t g_inited = 0;

ObSQLConfig g_sqlconfig;

ObSQLServerBlackList g_ms_black_list;
