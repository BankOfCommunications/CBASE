/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *   Author XiuMing     2014-03-09
 *   Email: wanhong.wwh@alibaba-inc.com
 *     -some work detail if you want
 *
 */

#include "libobsql.h"
#include "ob_sql_global.h"
#include "common/ob_define.h"
#include "ob_sql_define.h"
#include "ob_sql_update_worker.h"
#include "ob_sql_util.h"
#include "ob_sql_cluster_select.h"    // destroy_select_table
#include "ob_sql_cluster_config.h"    // destroy_global_config
#include "common/ob_malloc.h"
#include <string.h>
#include <malloc.h>

using namespace std;
using namespace oceanbase::common;

// Static functions
static void pre_init_();
static int init_sql_config_(const ObSQLConfig &config);
static void destroy_sql_config_();
static void print_config_(const ObSQLConfig &config);

int ob_sql_init(const ObSQLConfig &config)
{
  int ret = OB_SQL_SUCCESS;

  OB_SQL_LOG(INFO, "*********************** LIBOBSQL START ***********************");

  pre_init_();

  if (OB_SQL_SUCCESS != (ret = init_sql_config_(config)))
  {
    OB_SQL_LOG(ERROR, "init_sql_config_ fail, ret=%d", ret);
  }
  else if (OB_SQL_SUCCESS != (ret = update_cluster_address_list(config.cluster_address_)))
  {
    OB_SQL_LOG(ERROR, "init_cluster_address_list fail, cluster_address='%s', ret=%d", config.cluster_address_, ret);
  }
  //加载libmysqlclient中的函数
  else if (OB_SQL_SUCCESS != (ret = init_func_set(&g_func_set)))
  {
    OB_SQL_LOG(ERROR, "load real mysql function symbol from libmysqlclient failed, ret=%d", ret);
  }
  // Call mysql_library_init before create connection pool
  else if (0 != g_func_set.real_mysql_server_init(0, NULL, NULL))
  {
    OB_SQL_LOG(ERROR, "mysql_server_init fail, real_mysql_server_init=%p", g_func_set.real_mysql_server_init);
  }
  else
  {
    g_inited = 1;

    //从配置服务器获取配置 初始化连接池 集群选择表
    if (OB_SQL_SUCCESS != (ret = start_update_worker()))
    {
      OB_SQL_LOG(ERROR, "start_update_worker fail, ret=%d", ret);
    }
  }

  return ret;
}

void ob_sql_destroy()
{
  OB_SQL_LOG(INFO, "libobsql begins to destroy");

  g_inited = 0;

  stop_update_worker();
  destroy_select_table();         /* g_table */
  destroy_group_ds(&g_group_ds);  /* g_group_ds */
  destroy_global_config();        /* g_config_update and g_config_using */
  reset_cluster_address_list();   /* g_rsnum and g_rslist */

  // NOTE: Call mysql_thread_end before destroy_func_set
  if (NULL != g_func_set.real_mysql_thread_end)
  {
    g_func_set.real_mysql_thread_end();
    OB_SQL_LOG(INFO, "%s(): Thread %lu call mysql_thread_end", __FUNCTION__, pthread_self());
  }

  // NOTE: Call mysql_server_end before exit
  if (NULL != g_func_set.real_mysql_server_end)
  {
    g_func_set.real_mysql_server_end();
    OB_SQL_LOG(INFO, "%s() call mysql_server_end", __FUNCTION__);
  }

  destroy_func_set(&g_func_set);  /* g_func_set */
  destroy_sql_config_();          /* g_sqlconfig */

  OB_SQL_LOG(INFO, "*********************** LIBOBSQL END ***********************");
}

int ob_sql_update_cluster_address(const char *cluster_address)
{
  int ret = OB_SQL_SUCCESS;

  if (! g_inited)
  {
    TBSYS_LOG(WARN, "libobsql is not inited, can not update cluster address");
    ret = OB_SQL_ERROR;
  }
  else if (NULL == cluster_address || 0 >= strlen(cluster_address))
  {
    OB_SQL_LOG(ERROR, "invalid argument: cluster_address=%p", cluster_address);
    ret = OB_SQL_INVALID_ARGUMENT;
  }
  // Update cluster address list
  else if (OB_SQL_SUCCESS != (ret = update_cluster_address_list(cluster_address)))
  {
    // NOTE: 更新cluster address失败属于正常、频繁事件（使用了try lock），此处仅WARN报警
    OB_SQL_LOG(WARN, "update_cluster_address_list fail, cluster_address=%s, ret=%d", cluster_address, ret);
  }
  else
  {
    // NOTE: Update g_sqlconfig information
    snprintf(g_sqlconfig.cluster_address_, sizeof(g_sqlconfig.cluster_address_), "%s", cluster_address);
  }

  return ret;
}

static void pre_init_()
{
  ::mallopt(M_MMAP_THRESHOLD, OB_SQL_DEFAULT_MMAP_THRESHOLD);

#ifdef NEED_INIT_MEMORY_POOL
  ob_init_memory_pool();
#endif
}

static int init_sql_config_(const ObSQLConfig &config)
{
  int ret = OB_SQL_INVALID_ARGUMENT;

  // NOTE: password can be empty
  if (0 == strlen(config.username_))
  {
    OB_SQL_LOG(ERROR, "invalid argument, user name is empty");
  }
  else if (0 == strlen(config.cluster_address_))
  {
    OB_SQL_LOG(ERROR, "invalid argument, cluster address is empty");
  }
  else
  {
    g_sqlconfig = config;

    g_sqlconfig.min_conn_ = (0 >= config.min_conn_ ? OB_SQL_DEFAULT_MIN_CONN_SIZE : config.min_conn_);
    g_sqlconfig.max_conn_ = (0 >= config.max_conn_ ? OB_SQL_DEFAULT_MAX_CONN_SIZE : config.max_conn_);

    print_config_(g_sqlconfig);

    //set min max conn
    g_config_using->min_conn_size_ = static_cast<int16_t>(g_sqlconfig.min_conn_);
    g_config_using->max_conn_size_ = static_cast<int16_t>(g_sqlconfig.max_conn_);
    g_config_update->min_conn_size_ = static_cast<int16_t>(g_sqlconfig.min_conn_);
    g_config_update->max_conn_size_ = static_cast<int16_t>(g_sqlconfig.max_conn_);

    ret = OB_SQL_SUCCESS;
  }

  return ret;
}

static void destroy_sql_config_()
{
  (void) memset(&g_sqlconfig, 0, sizeof(g_sqlconfig));
}

static void print_config_(const ObSQLConfig &config)
{
  OB_SQL_LOG(INFO, "=================== LIBOBSQL CONFIG BEGIN ===================");
  OB_SQL_LOG(INFO, "cluster_address=%s", config.cluster_address_);
  OB_SQL_LOG(INFO, "username=%s", config.username_);
  OB_SQL_LOG(INFO, "password=%s", config.passwd_);
  OB_SQL_LOG(INFO, "min_conn=%d", config.min_conn_);
  OB_SQL_LOG(INFO, "max_conn=%d", config.max_conn_);
  OB_SQL_LOG(INFO, "read_slave_only=%s", config.read_slave_only_ ? "true" : "false");
  OB_SQL_LOG(INFO, "=================== LIBOBSQL CONFIG END ====================");
}
