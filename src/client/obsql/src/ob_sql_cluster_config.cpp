#include "ob_sql_cluster_config.h"
#include "ob_sql_cluster_select.h"
#include "ob_sql_global.h"
#include "ob_sql_util.h"
#include "ob_sql_ms_select.h"         // destroy_ms_select_table
#include "tblog.h"
#include "common/ob_server.h"
#include "ob_sql_cluster_info.h"  // ObSQLClusterType
#include "ob_sql_update_worker.h"   // clear_update_flag
#include <mysql/mysql.h>
#include <string.h>

/// Delete cluster from specific configuration
void delete_cluster_from_config(ObSQLGlobalConfig &config, const int index)
{
  if (0 <= index && index < config.cluster_size_)
  {
    TBSYS_LOG(INFO, "delete cluster={id=%d,addr=%s} from config",
        config.clusters_[index].cluster_id_, get_server_str(&config.clusters_[index].server_));

    for (int idx = index + 1; idx < config.cluster_size_; idx++)
    {
      config.clusters_[idx - 1] = config.clusters_[idx];
    }

    config.cluster_size_--;
  }
}

static void insert_rs_list(uint32_t ip, uint32_t port)
{
  int index = 0;
  int exist = 0;
  for (; index < g_rsnum; ++index)
  {
    if (ip == g_rslist[index].ip_ && port == g_rslist[index].port_)
    {
      exist = 1;
      break;
    }
  }

  int64_t rslist_array_size = sizeof(g_rslist) / sizeof(g_rslist[0]);

  if (0 == exist && g_rsnum < rslist_array_size)
  {
    g_rslist[g_rsnum].ip_ = ip;
    g_rslist[g_rsnum].port_ = port;
    g_rsnum++;

    TBSYS_LOG(INFO, "insert cluster address: %s, size=%d", get_server_str(g_rslist + g_rsnum - 1), g_rsnum);
  }
}

static int update_cluster_info_(MYSQL *mysql, ObSQLGlobalConfig *gconfig, ObServerInfo *server)
{
  OB_ASSERT(NULL != mysql && NULL != gconfig && NULL != server);

  int ret = OB_SQL_SUCCESS;
  MYSQL_RES *results = NULL;
  MYSQL_ROW record;
  uint64_t cluster_num = 0;

  if (0 != (ret = CALL_REAL(mysql_query, mysql, OB_SQL_QUERY_CLUSTER)))
  {
    TBSYS_LOG(ERROR, "do query '%s' on server %s fail, err=%d, %s",
        OB_SQL_QUERY_CLUSTER, get_server_str(server), CALL_REAL(mysql_errno, mysql), CALL_REAL(mysql_error, mysql));
    ret = OB_SQL_ERROR;
  }
  else if (NULL == (results = CALL_REAL(mysql_store_result, mysql)))
  {
    TBSYS_LOG(ERROR, "store result failed, query is %s, errno=%u, %s", OB_SQL_QUERY_CLUSTER,
        CALL_REAL(mysql_errno, mysql), CALL_REAL(mysql_error, mysql));
    ret = OB_SQL_ERROR;
  }
  else if (0 >= (cluster_num = CALL_REAL(mysql_num_rows, results)))
  {
    TBSYS_LOG(WARN, "get empty cluster information from server %s", get_server_str(server));
  }
  else
  {
    int64_t cluster_index = 0;
    gconfig->cluster_size_ = 0;

    while (OB_SQL_MAX_CLUSTER_NUM > cluster_index && NULL != (record = CALL_REAL(mysql_fetch_row, results)))
    {
      if (NULL == record[0]    /* cluster_id */
          || NULL == record[1] /* cluster_role */
          || NULL == record[3] /* cluster_vip */
          || NULL == record[4] /* cluster_port */
         )
      {
        TBSYS_LOG(WARN, "cluster info are not complete: cluster_id=%s, cluster_role=%s, cluster_vip=%s, "
            "cluster_port=%s",
            NULL == record[0] ? "NULL" : record[0], NULL == record[1] ? "NULL" : record[1],
            NULL == record[3] ? "NULL" : record[3], NULL == record[4] ? "NULL" : record[4]);

        // Continue to next row
        continue;
      }
      else
      {
        ObSQLClusterConfig *cconfig = gconfig->clusters_ + cluster_index++;

        cconfig->cluster_id_ = static_cast<uint32_t>(atoll(record[0]));
        cconfig->cluster_type_ = (1 == atoll(record[1])) ? MASTER_CLUSTER : SLAVE_CLUSTER;
        cconfig->flow_weight_ = NULL == record[2] ? static_cast<int16_t>(0) : static_cast<int16_t>(atoll(record[2]));
        cconfig->server_.ip_ = oceanbase::common::ObServer::convert_ipv4_addr(record[3]);
        cconfig->server_.port_ = static_cast<uint32_t>(atoll(record[4]));
        cconfig->read_strategy_ = NULL == record[5] ? 0 : static_cast<uint32_t>(atoll(record[5]));

        int16_t old_flow_weight = cconfig->flow_weight_;
        if (g_sqlconfig.read_slave_only_)
        {
          if (MASTER_CLUSTER == cconfig->cluster_type_)
          {
            cconfig->flow_weight_ = 0;
          }
          else if (0 == old_flow_weight)
          {
            cconfig->flow_weight_ = 1;
          }
        }

        // insert cluster ip/port to rslist
        // NOTE: This function will modify g_rslist
        insert_rs_list(cconfig->server_.ip_, cconfig->server_.port_);

        TBSYS_LOG(INFO, "add cluster: cluster={id=%d,addr=%s}, flow={old=%d,new=%d}, type=%d, read_strategy=%u",
            cconfig->cluster_id_, get_server_str(&cconfig->server_),
            old_flow_weight, cconfig->flow_weight_, cconfig->cluster_type_, cconfig->read_strategy_);
      }
    }

    gconfig->cluster_size_ = static_cast<int16_t>(cluster_index);

    TBSYS_LOG(INFO, "update cluster info from %s success, valid cluster num=%ld/%ld",
        get_server_str(server), cluster_index, cluster_num);
  }

  if (NULL != results)
  {
    CALL_REAL(mysql_free_result, results);
    results = NULL;
  }

  return ret;
}

static int update_mslist_(MYSQL *mysql, ObSQLClusterConfig *cluster, ObServerInfo *server)
{
  OB_ASSERT(NULL != mysql && NULL != cluster && NULL != server);

  int ret = OB_SQL_SUCCESS;
  uint64_t ms_num = 0;
  MYSQL_ROW record;
  MYSQL_RES *results = NULL;
  char querystr[OB_SQL_QUERY_STR_LENGTH];

  snprintf(querystr, OB_SQL_QUERY_STR_LENGTH, "%s%u", OB_SQL_QUERY_SERVER, cluster->cluster_id_);

  if (OB_SQL_SUCCESS != (ret = CALL_REAL(mysql_query, mysql, querystr)))
  {
    TBSYS_LOG(WARN, "do query '%s' from %s failed, errno=%d, %s",
        querystr, get_server_str(server), CALL_REAL(mysql_errno, mysql), CALL_REAL(mysql_error, mysql));
    ret = OB_SQL_ERROR;
  }
  else if (NULL == (results = CALL_REAL(mysql_store_result, mysql)))
  {
    TBSYS_LOG(WARN, "store result failed, query is '%s' errno=%u, %s", querystr,
        CALL_REAL(mysql_errno, mysql), CALL_REAL(mysql_error, mysql));
    ret = OB_SQL_ERROR;
  }
  else if (0 >= (ms_num = CALL_REAL(mysql_num_rows, results)))
  {
    TBSYS_LOG(WARN, "cluster (ID=%u, LMS=%s) mslist is empty", cluster->cluster_id_, get_server_str(&(cluster->server_)));
  }
  else
  {
    int64_t index = 0;
    cluster->server_num_ = 0;

    while (OB_SQL_MAX_MS_NUM > index && NULL != (record = CALL_REAL(mysql_fetch_row, results)))
    {
      if (NULL == record[0] || NULL == record[1])
      {
        TBSYS_LOG(WARN, "queried meergeserver info is not complete: ip=%p, port=%p", record[0], record[1]);
      }
      else
      {
        cluster->merge_server_[index].ip_ = oceanbase::common::ObServer::convert_ipv4_addr(record[0]);
        cluster->merge_server_[index].port_ = static_cast<uint32_t>(atoll(record[1]));
        index++;
      }
    }

    cluster->server_num_ = static_cast<int16_t>(index);

    TBSYS_LOG(INFO, "update cluster mslist: cluster={ID=%u,LMS=%s}, valid ms num=%d/%lu",
        cluster->cluster_id_, get_server_str(&(cluster->server_)), cluster->server_num_, ms_num);
  }

  if (NULL != results)
  {
    (*g_func_set.real_mysql_free_result)(results);
    results = NULL;
  }

  return ret;
}

/**
 * 读取OceanBase集群信息
 *
 * @param server   listen ms information
 *
 * @return int     return OB_SQL_SUCCESS if get cluster information from listen ms success
 *                                       else return OB_SQL_ERROR
 *
 */
static int get_cluster_config(ObServerInfo *server)
{
  OB_ASSERT(NULL != server);

  int ret = OB_SQL_SUCCESS;
  MYSQL * mysql = NULL;
  const char *ip = get_ip(server);
  const char *user = g_sqlconfig.username_;
  const char *passwd = g_sqlconfig.passwd_;
  uint32_t port = server->port_;

  if (NULL == (mysql = CALL_REAL(mysql_init, NULL)))
  {
    TBSYS_LOG(ERROR, "mysql init failed");
    ret = OB_SQL_ERROR;
  }
  else if (OB_SQL_SUCCESS != (ret = set_mysql_options(mysql)))
  {
    TBSYS_LOG(ERROR, "set_mysql_options fail, ret=%d, mysql=%p", ret, mysql);
  }
  else if (NULL == (mysql = CALL_REAL(mysql_real_connect, mysql, ip, user, passwd, OB_SQL_DB, port, NULL, 0)))
  {
    TBSYS_LOG(ERROR, "failed to connect to server %s, errno=%d, %s",
        get_server_str(server), CALL_REAL(mysql_errno, mysql), CALL_REAL(mysql_error, mysql));
    ret = OB_SQL_ERROR;
  }
  else if (OB_SQL_SUCCESS != (ret = update_cluster_info_(mysql, g_config_update, server)))
  {
    TBSYS_LOG(ERROR, "update_cluster_info_ fail, server=%s, mysql=%p, ret=%d",
        get_server_str(server), mysql, ret);
  }
  else
  {
    for (int index = 0; index < g_config_update->cluster_size_ && OB_SQL_SUCCESS == ret; ++index)
    {
      ObSQLClusterConfig *cluster = g_config_update->clusters_ + index;

      if (OB_SQL_SUCCESS != (ret = update_mslist_(mysql, cluster, server)))
      {
        TBSYS_LOG(WARN, "update mslist from %s fail, mysql=%p, cluster=[ID=%u, LMS=%s], index=%d, cluster_size=%d",
            get_server_str(server), mysql, cluster->cluster_id_,
            get_server_str(&(cluster->server_)), index, g_config_update->cluster_size_);
        break;
      }

      // NOTE: Delete cluster which has no MS
      if (0 >= cluster->server_num_)
      {
        TBSYS_LOG(WARN, "no mergeserver avaliable in cluster %d, addr=%s. Delete it from configuration",
            g_config_update->clusters_[index].cluster_id_, get_server_str(&(cluster->server_)));

        delete_cluster_from_config(*g_config_update, index);
        index--;
      }
    }
  }

  if (NULL != mysql)
  {
    CALL_REAL(mysql_close, mysql);
    mysql = NULL;
  }

  if (OB_SQL_SUCCESS == ret && 0 >= g_config_update->cluster_size_)
  {
    TBSYS_LOG(WARN, "no valid cluster updated from %s, total cluster address num=%d", get_server_str(server), g_rsnum);
    ret = OB_SQL_ERROR;
  }

  TBSYS_LOG(INFO, "update cluster config from %s, ret=%d, cluster size=%d, LMS num=%d, "
      "mysql_init(real=%p, MYSQL_FUNC=%p)",
      get_server_str(server), ret, g_config_update->cluster_size_, g_rsnum,
      g_func_set.real_mysql_init, MYSQL_FUNC(mysql_init));

  return ret;
}

/* swap g_config_using && g_config_update */
static void swap_config()
{
  ObSQLGlobalConfig * tmp = g_config_using;
  g_config_using = g_config_update;
  g_config_update = tmp;
}

/**
 * 集群个数，流量配置，集群类型是否变化
 */
static bool is_cluster_changed()
{
  bool ret = false;
  if (g_config_update->cluster_size_ != g_config_using->cluster_size_)
  {
    ret = true;
  }
  else
  {
    for (int cindex = 0; cindex < g_config_using->cluster_size_; cindex++)
    {
      if (g_config_update->clusters_[cindex].cluster_id_ != g_config_using->clusters_[cindex].cluster_id_
          ||g_config_update->clusters_[cindex].flow_weight_ != g_config_using->clusters_[cindex].flow_weight_
          ||g_config_update->clusters_[cindex].cluster_type_ != g_config_using->clusters_[cindex].cluster_type_)
      {
        ret = true;
        break;
      }
    }
  }
  return ret;
}

//cluster 没变化
//判断cluster 里面的mslist是否变化
static bool is_mslist_changed(bool cluster_changed)
{
  bool ret = false;

  if (cluster_changed)
  {
    ret = true;
  }
  else
  {
    for (int cindex = 0; false == ret && cindex < g_config_using->cluster_size_; cindex++)
    {
      if (g_config_update->clusters_[cindex].server_num_ != g_config_using->clusters_[cindex].server_num_)
      {
        ret = true;
        break;
      }
      else
      {
        for (int sindex = 0; sindex < g_config_using->clusters_[cindex].server_num_; ++sindex)
        {
          if ((g_config_update->clusters_[cindex].merge_server_[sindex].ip_
                != g_config_using->clusters_[cindex].merge_server_[sindex].ip_)
              || (g_config_update->clusters_[cindex].merge_server_[sindex].port_
                != g_config_using->clusters_[cindex].merge_server_[sindex].port_))
          {
            ret = true;
            break;
          }
        } // end for

        if (true == ret)
        {
          break;
        }
      }
    } // end for
  }

  return ret;
}

static int rebuild_tables()
{
  int ret = OB_SQL_SUCCESS;
  bool cluster_changed = is_cluster_changed();
  bool mslist_changed = is_mslist_changed(cluster_changed);

  /// 如果黑名单中有机器连接有问题，应该强制更新数据源:
  /// 1. 如果机器挂了，则将机器从连接池中删除
  /// 2. 如果机器启动好了，则将其从黑名单中删除
  bool force_update = 0 < g_ms_black_list.size_;

  // First update ObGroupDataSource if needed
  if (force_update || mslist_changed || cluster_changed)
  {
    TBSYS_LOG(INFO, "update data source: mslist_changed=%s, cluster_changed=%s, force_update=%s, black list size=%d",
        mslist_changed ? "true" : "false", cluster_changed ? "true" : "false",
        force_update ? "true" : "false", g_ms_black_list.size_);

    if (OB_SQL_SUCCESS != (ret = update_group_ds(g_config_using, &g_group_ds)))
    {
      TBSYS_LOG(ERROR, "update group data source fail, ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "update data source success");
    }

    // 打印更新后的配置
    dump_config(g_config_using, "USING");

    // 更新集群选择表, 保证集群选择表信息最新
    //
    // NOTE: 由于在更新连接池过程中，cluster信息可能会变化，例如会删除没有有效MS的cluster,
    // 因此每次更新完连接池都必须更新cluster的select table，避免在select table中引用到无效的cluster指针
    ret = update_select_table(&g_group_ds);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update cluster select table failed");
    }
    else
    {
      TBSYS_LOG(INFO, "update cluster select table success");
    }

    // 打印select table
    dump_table();
  }

// NOTE: Disable updating MS select table
// As this module has BUG which will cause "Core Dump" in consishash_mergeserver_select.
// wanhong.wwh 2014-04-09
#ifdef ENABLE_SELECT_MS_TABLE
  if (OB_SQL_SUCCESS == ret && mslist_changed)
  {
    ret = update_ms_select_table();
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update ms select table failed");
    }
    else
    {
      TBSYS_LOG(DEBUG, "update ms select table success");
    }
  }
#endif

  return ret;
}

// 根据rslist 从Oceanbase 获取ms列表 流量信息
// 更新g_config_update的配置
int get_ob_config()
{
  int ret = OB_SQL_SUCCESS;
  bool cluster_config_updated = false;

  TBSYS_LOG(DEBUG, "get_ob_config: rsnum is %d", g_rsnum);

  // NOTE: 对g_rslist_rwlock加写锁，保护g_rslist和g_rsnum
  int lock_ret = 0;
  if (0 != (lock_ret = pthread_rwlock_wrlock(&g_rslist_rwlock)))
  {
    TBSYS_LOG(ERROR, "%s acquire write lock on g_rslist_rwlock fail, ret=%d", __FUNCTION__, lock_ret);
    ret = OB_SQL_ERROR;
  }
  else
  {
    if (0 >= g_rsnum)
    {
      TBSYS_LOG(ERROR, "no cluster address available, g_rsnum=%d", g_rsnum);
      ret = OB_SQL_ERROR;
    }
    else
    {
      // 重试所有的LMS，直到更新g_config_update成功
      for (int index = 0; index < g_rsnum; ++index)
      {
        ret = get_cluster_config(g_rslist + index);
        if (OB_SQL_SUCCESS == ret)
        {
          cluster_config_updated = true;
          dump_config(g_config_update, "FETCHED");
          break;
        }
        else
        {
          TBSYS_LOG(WARN, "update cluster config from %s failed, index=%d, cluster address num=%d",
              get_server_str(g_rslist + index), index, g_rsnum);
        }
      }
    }

    if (OB_SQL_SUCCESS == ret && ! cluster_config_updated)
    {
      ret = OB_SQL_ERROR;
    }

    pthread_rwlock_unlock(&g_rslist_rwlock);
  }

  return ret;
}

void destroy_global_config()
{
  TBSYS_LOG(INFO, "destroy g_config_update %p", g_config_update);
#ifdef ENABLE_SELECT_MS_TABLE
  destroy_ms_select_table(g_config_update);    /* g_config_update->clusters_[].table_ */
#endif
  (void)memset(g_config_update, 0, sizeof(ObSQLGlobalConfig));

  TBSYS_LOG(INFO, "destroy g_config_using %p", g_config_using);
#ifdef ENABLE_SELECT_MS_TABLE
  destroy_ms_select_table(g_config_using);    /* g_config_using->clusters_[].table_ */
#endif
  (void)memset(g_config_using, 0, sizeof(ObSQLGlobalConfig));
}

int do_update()
{
  int ret = OB_SQL_SUCCESS;

  // 对g_config_rwlock加写锁，用于保护g_config_using和g_group_ds
  int lock_ret = 0;
  if (0 != (lock_ret = pthread_rwlock_wrlock(&g_config_rwlock))) // wlock for update global config
  {
    TBSYS_LOG(ERROR, "%s() acquire write lock on g_config_rwlock fail, ret=%d", __FUNCTION__, lock_ret);
    ret = OB_SQL_ERROR;
  }
  else
  {
    swap_config();
    ret = rebuild_tables();
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "do_update fail: fail to rebuild table");
    }
    else
    {
      TBSYS_LOG(INFO, "update global config success");
    }

    pthread_rwlock_unlock(&g_config_rwlock); // unlock
  }
  return ret;
}

/// 根据给定的集群地址列表字符串，更新集群地址
/// 集群地址列表字符串采用逗号分隔
int update_cluster_address_list(const char *cluster_address)
{
  int ret = OB_SQL_SUCCESS;
  char buf[OB_SQL_MAX_CLUSTER_ADDRESS_LEN];
  const char *CLUSTER_ADDRESS_DELIMITOR = ",";

  // NOTE: 对g_rslist_rwlock加写锁，保护g_rslist和g_rsnum
  int lock_ret = 0;
  if (0 != (lock_ret = pthread_rwlock_trywrlock(&g_rslist_rwlock)))
  {
    TBSYS_LOG(WARN, "%s() try to acquire write lock on g_rslist_rwlock fail, ret=%d", __FUNCTION__, lock_ret);
    ret = OB_SQL_ERROR;
  }
  else
  {
    if (NULL == cluster_address || 0 >= strlen(cluster_address))
    {
      ret = OB_SQL_INVALID_ARGUMENT;
    }
    else if (0 >= snprintf(buf, sizeof(buf), "%s", cluster_address))
    {
      TBSYS_LOG(ERROR, "snprintf cluster_address(%s) to buffer fail, buffer length=%ld",
          cluster_address, sizeof(buf));
      ret = OB_SQL_INVALID_ARGUMENT;
    }
    else
    {
      char *ipaddr = NULL;
      char *saveptr = NULL;
      char *buf_ptr = NULL;
      bool cluster_address_parsed = false;

      int64_t g_rslist_size = sizeof(g_rslist) / sizeof(g_rslist[0]);

      buf_ptr = buf;
      while (NULL != (ipaddr = strtok_r(buf_ptr, CLUSTER_ADDRESS_DELIMITOR, &saveptr)))
      {
        oceanbase::common::ObServer server;

        // Parse "IP:PORT" to ObServer
        if (0 != (ret = server.parse_from_cstring(ipaddr)))
        {
          TBSYS_LOG(WARN, "ObServer.parse_from_cstring(ipaddr=%s) fail, ret=%d", ipaddr, ret);
        }
        else
        {
          // NOTE: Do not reset until valid cluster address is parsed
          if (! cluster_address_parsed)
          {
            reset_cluster_address_list();

            cluster_address_parsed = true;
          }

          if (g_rsnum >= g_rslist_size)
          {
            TBSYS_LOG(WARN, "libobsql cluster address array is full, size=%d, ignore the left. cluster_address='%s'",
                g_rsnum, cluster_address);
            break;
          }
          else
          {
            g_rslist[g_rsnum].ip_ = server.get_ipv4();
            g_rslist[g_rsnum].port_ = server.get_port();

            g_rsnum++;
          }
        }

        TBSYS_LOG(INFO, "LIBOBSQL: refresh cluster address '%s' from '%s', size=%d", ipaddr, cluster_address, g_rsnum);

        // NOTE: Tell strtok_r to return next matched token
        buf_ptr = NULL;
      }

      if (! cluster_address_parsed)
      {
        TBSYS_LOG(ERROR, "no valid cluster address in '%s'", cluster_address);
        ret = OB_SQL_ERROR;
      }
      else
      {
        TBSYS_LOG(INFO, "LIBOBSQL: refresh cluster address count=%d, clusterAddress=%s", g_rsnum, cluster_address);
        ret = OB_SQL_SUCCESS;
      }
    }

    pthread_rwlock_unlock(&g_rslist_rwlock);
  }

  return ret;
}

void reset_cluster_address_list()
{
  memset(g_rslist, 0, sizeof(g_rslist));
  g_rsnum = 0;
}
