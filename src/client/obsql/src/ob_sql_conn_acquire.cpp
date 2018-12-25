#include "tblog.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "ob_sql_conn_acquire.h"
#include "ob_sql_util.h"
#include "ob_sql_data_source_utility.h"
#include "ob_sql_select_method_impl.h"
#include "ob_sql_global.h"
#include "ob_sql_update_worker.h"         // wakeup_update_worker
#include <stdio.h>
#include <stddef.h>
#include <string.h>

using namespace oceanbase::common;

/* 连接池的互斥锁 */
pthread_mutex_t pool_mutex;

void print_connection_info(ObGroupDataSource *gds)
{
  //防止g_group_ds被修改
  int lock_ret = 0;
  if (0 != (lock_ret = pthread_rwlock_rdlock(&g_config_rwlock)))
  {
    TBSYS_LOG(WARN, "%s() acquire read lock on g_config_rwlock fail, ret=%d", __FUNCTION__, lock_ret);
  }
  else
  {
    pthread_mutex_lock(&pool_mutex);
    if (NULL != gds)
    {
      ObClusterInfo *scluster = NULL;
      ObDataSource *ds = NULL;
      for (int32_t index = 0; index < gds->size_; ++index)
      {
        scluster = gds->clusters_[index];

        TBSYS_LOG(INFO, "[CONN INFO] CLUSTER[%d]=[%s], MS=%d, FLOW=%d",
            scluster->cluster_id_, get_server_str(&scluster->cluster_addr_), scluster->size_, scluster->flow_weight_);

        for (int32_t i = 0; i < scluster->size_; i++)
        {
          ds = scluster->dslist_[i];
          TBSYS_LOG(INFO, "[CONN INFO] MS[%d][%d]=[%s] FREE=%d USED=%d",
              scluster->cluster_id_, i+1,
              get_server_str(&ds->server_),
              ds->conn_list_.free_list_.size_,
              ds->conn_list_.used_list_.size_);
        }
      }
    }

    TBSYS_LOG(INFO, "[BLACK LIST] MS BLACK LIST SIZE=%d", g_ms_black_list.size_);

    for (int32_t index = 0; index < g_ms_black_list.size_; index++)
    {
      ObServerInfo ms;
      ms.ip_ = g_ms_black_list.ip_array_[index];
      ms.port_ = 0;

      TBSYS_LOG(INFO, "[BLACK LIST] MS[%d]=%s", index + 1, get_server_str(&ms));
    }
    pthread_mutex_unlock(&pool_mutex);
    pthread_rwlock_unlock(&g_config_rwlock);
  }
}

static ObDataSource* select_ds(ObClusterInfo *cluster, const char* sql, unsigned long length, ObSQLMySQL *mysql)
{
  UNUSED(sql);
  UNUSED(length);

  ObDataSource *ds = NULL;
  if (NULL == cluster || NULL == mysql)
  {
    TBSYS_LOG(WARN, "invalid argument: cluster=%p, mysql=%p", cluster, mysql);
  }
  else
  {
    // NOTE: Use random select method directly
    ds = random_mergeserver_select(cluster, mysql);

    if (NULL == ds)
    {
      TBSYS_LOG(WARN, "no valid MS available in cluster={id=%d,addr=%s,type=%d}, MS NUM=%d",
          cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->cluster_type_, cluster->size_);
    }
    else
    {
      TBSYS_LOG(DEBUG, "selected MS=%s. free=%d, used=%d, is_black=%s, retry=%s, last_ds=%s",
          get_server_str(&ds->server_), ds->conn_list_.free_list_.size_, ds->conn_list_.used_list_.size_,
          g_ms_black_list.exist(ds->server_.ip_) ? "Y" : "N",
          mysql->retry_ ? "Y" : "N", get_server_str(&mysql->last_ds_));
    }
  }

  return ds;
}

static ObSQLConn* select_conn(ObDataSource *server)
{
  ObSQLConn* conn = NULL;
  if (NULL == server)
  {
    TBSYS_LOG(WARN, "invalid argument server is %p", server);
  }
  else
  {
    conn = random_conn_select(server);
  }

  if (NULL == conn)
  {
    TBSYS_LOG(WARN, "MS[%s] has no free connections, used=%d, free=%d",
        get_server_str(&server->server_), server->conn_list_.used_list_.size_, server->conn_list_.free_list_.size_);
  }

  return conn;
}

 /** 从一个集群中选择一条连接
  * 1、使用一致性hash找mergeserver，然后获取一条连接
  * 2、如果上面的ms的连接用光了， 遍历集群中所有连接选一条出来
  *
  * 一致性hash选择ms 如果ms上的连接全忙则round-robbin选择一条连接
  */
ObSQLConn* acquire_conn(ObClusterInfo *cluster, const char* sql, unsigned long length, ObSQLMySQL *mysql)
{
  ObSQLConn *real_conn = NULL;
  if (NULL == cluster || NULL == mysql)
  {
    TBSYS_LOG(WARN, "invalid argument: cluster=%p, mysql=%p", cluster, mysql);
  }
  else
  {
    pthread_mutex_lock(&pool_mutex);
    ObDataSource *server = select_ds(cluster, sql, length, mysql);
    if (NULL != server)
    {
      mysql->last_ds_ = server->server_;
      real_conn = select_conn(server);
      if (NULL == real_conn)
      {
        TBSYS_LOG(WARN, "cluster(id=%u,addr=%s) has no free connections, MS NUM=%d",
            cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_);
      }
      else
      {
        TBSYS_LOG(DEBUG, "acquired real conn=%p, MS=%s, cluster={ID=%d,LMS=%s}, used=%d, free=%d",
            real_conn, get_server_str(&server->server_), cluster->cluster_id_,
            get_server_str(&cluster->cluster_addr_),
            server->conn_list_.used_list_.size_, server->conn_list_.free_list_.size_);
      }
    }

    pthread_mutex_unlock(&pool_mutex);
  }

  return real_conn;
}

ObSQLConn* acquire_conn_random(ObGroupDataSource *gds, ObSQLMySQL* mysql)
{
  int32_t index = 0, lock_ret = 0;
  ObClusterInfo *scluster = NULL;
  ObDataSource *ds = NULL;
  ObSQLConn *real_conn = NULL;
  if (NULL == gds || NULL == mysql)
  {
    TBSYS_LOG(WARN, "invalid argument gds=%p, mysql=%p", gds, mysql);
  }
  else if (0 != (lock_ret = pthread_rwlock_rdlock(&g_config_rwlock)))
  {
    TBSYS_LOG(ERROR, "%s() acquire read lock on g_config_rwlock fail, ret=%d", __FUNCTION__, lock_ret);
  }
  else
  {
    pthread_mutex_lock(&pool_mutex);
    for (index = 0; index < gds->size_; ++index)
    {
      scluster = gds->clusters_[index];
      ds = random_mergeserver_select(scluster, mysql);
      if (NULL != ds)
      {
        real_conn = random_conn_select(ds);
        if (NULL == real_conn)
        {
          TBSYS_LOG(WARN, "cluster={id=%d,LMS=%s} has no avaliable connections",
              scluster->cluster_id_, get_server_str(&scluster->cluster_addr_));
        }
        else
        {
          TBSYS_LOG(INFO, "random selected connection: MS=%s, conn=%p, mysql=%p",
              get_server_str(&real_conn->server_), real_conn, real_conn->mysql_);
          break;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "get ms failed from cluster={id=%d,LMS=%s}",
            scluster->cluster_id_, get_server_str(&scluster->cluster_addr_));
      }
    }
    pthread_mutex_unlock(&pool_mutex);
    pthread_rwlock_unlock(&g_config_rwlock);
  }
  return real_conn;
}

static bool is_conn_valid_(ObSQLConn *conn)
{
  return (NULL != conn && NULL != conn->node_ && NULL != conn->mysql_
      && ((conn->is_conn_pool_valid_ && NULL != conn->pool_) || (! conn->is_conn_pool_valid_)));
}

// Close current connection and free it
static int close_conn_(ObSQLConn *conn)
{
  OB_ASSERT(is_conn_valid_(conn));

  TBSYS_LOG(DEBUG, "close connection: MS=%s, conn=%p, real mysql=%p, node=%p, "
      "is_conn_pool_valid=%s, pool=%p, error=%d %s",
      get_server_str(&conn->server_), conn, conn->mysql_, conn->node_,
      conn->is_conn_pool_valid_ ? "true" : "false", conn->pool_,
      CALL_REAL(mysql_errno, conn->mysql_), CALL_REAL(mysql_error, conn->mysql_));

  int ret = OB_SQL_SUCCESS;

  // Delete connection from data source if connection pool is valid
  if (conn->is_conn_pool_valid_)
  {
    ObSQLList *used_list = &conn->pool_->conn_list_.used_list_;

    if (OB_SQL_SUCCESS != (ret = ob_sql_list_del(used_list, conn->node_)))
    {
      TBSYS_LOG(ERROR, "delete connection node from used list fail, used_list=%p, node=%p, ret=%d",
          used_list, conn->node_, ret);
    }
  }

  if (OB_SQL_SUCCESS == ret)
  {
    // Close real mysql connection
    CALL_REAL(mysql_close, conn->mysql_);
    conn->mysql_ = NULL;

    ObSQLListNode *node = conn->node_;

    // Clear memory
    (void)memset(node, 0, sizeof(ObSQLListNode));
    (void)memset(conn, 0, sizeof(ObSQLConn));

    // Free memory of ObSQLListNode and ObSQLConn
    // NOTE: ObSQLListNode memory and ObSQLConn memory are contiguous and are allocated together.
    // Here we only need free ObSQLListNode. ObSQLConn will be freed at the same time.
    // So conn memory can not be accessed after free.
    // See create_real_connection()
    ob_free(node);

    conn = NULL;
    node = NULL;
  }

  return ret;
}

// Recycle connection and reuse it
// Delete it from used list and add it into free list
static int recycle_conn_(ObSQLConn *conn)
{
  OB_ASSERT(is_conn_valid_(conn) && conn->is_conn_pool_valid_);

  int ret = OB_SQL_SUCCESS;
  ObDataSource *ds = conn->pool_;

  TBSYS_LOG(DEBUG, "recycle connection: MS=%s, conn=%p, real mysql=%p, node=%p, "
      "is_conn_pool_valid=%s, pool=%p, error=%d %s",
      get_server_str(&conn->server_), conn, conn->mysql_, conn->node_,
      conn->is_conn_pool_valid_ ? "true" : "false", conn->pool_,
      CALL_REAL(mysql_errno, conn->mysql_), CALL_REAL(mysql_error, conn->mysql_));

  if (OB_SQL_SUCCESS != (ret = ob_sql_list_del(&ds->conn_list_.used_list_, conn->node_)))
  {
    TBSYS_LOG(ERROR, "ob_sql_list_del fail, ret=%d, node=%p, used_list=%p, size=%d, MS=[%s], real mysql=%p",
        ret, conn->node_, &ds->conn_list_.used_list_, ds->conn_list_.used_list_.size_,
        get_server_str(&conn->server_), conn->mysql_);
  }
  else if (OB_SQL_SUCCESS != (ret = ob_sql_list_add_tail(&ds->conn_list_.free_list_, conn->node_)))
  {
    TBSYS_LOG(ERROR, "ob_sql_list_add_tail fail, ret=%d, node=%p, used_list=%p, size=%d, MS=[%s], real mysql=%p",
        ret, conn->node_, &ds->conn_list_.free_list_, ds->conn_list_.free_list_.size_,
        get_server_str(&conn->server_), conn->mysql_);
  }

  return ret;
}

int release_conn(ObSQLConn *conn)
{
  int ret = OB_SQL_SUCCESS;
  int lock_ret = 0;

  if (NULL == conn)
  {
    TBSYS_LOG(ERROR, "invalid arguments: conn=%p", conn);
    ret = OB_SQL_INVALID_ARGUMENT;
  }
  else if (! is_conn_valid_(conn))
  {
    TBSYS_LOG(ERROR, "invalid connection: conn=%p, MS=%s, node=%p, real mysql=%p, pool=%p, is_conn_pool_valid=%s",
        conn, get_server_str(&conn->server_), conn->node_, conn->mysql_,
        conn->pool_, conn->is_conn_pool_valid_ ? "true" : "false");

    ret = OB_SQL_INVALID_ARGUMENT;
  }
  else if (0 != (lock_ret = pthread_rwlock_rdlock(&g_config_rwlock)))
  {
    TBSYS_LOG(ERROR, "%s() acquire read lock on g_config_rwlock fail, ret=%d, "
        "conn=%p, MS=%s, node=%p, real mysql=%p, pool=%p, is_conn_pool_valid=%s",
        __FUNCTION__, lock_ret, conn, get_server_str(&conn->server_), conn->node_, conn->mysql_,
        conn->pool_, conn->is_conn_pool_valid_ ? "true" : "false");

    ret = OB_SQL_ERROR;
  }
  else
  {
    pthread_mutex_lock(&pool_mutex);

    ObDataSource *ds = conn->pool_;
    bool is_conn_pool_valid = conn->is_conn_pool_valid_;
    int mysql_eno = CALL_REAL(mysql_errno, conn->mysql_);
    const char *ms_addr = get_server_str(&conn->server_);

    TBSYS_LOG(DEBUG, "release conn: MS=[%s], conn=%p, is_conn_pool_valid=%s, pool=%p, node=%p, "
        "real mysql=%p, used=%d, free=%d, mysql_errno=%d",
        ms_addr, conn, conn->is_conn_pool_valid_ ? "true" : "false", conn->pool_, conn->node_, conn->mysql_,
        NULL == conn->pool_ ? 0 : conn->pool_->conn_list_.used_list_.size_,
        NULL == conn->pool_ ? 0 : conn->pool_->conn_list_.free_list_.size_,
        mysql_eno);

    if (! is_conn_pool_valid || 0 != mysql_eno)
    {
      // Close current connection if connection pool is invalid or connection has been in error state
      if (OB_SQL_SUCCESS != (ret = close_conn_(conn)))
      {
        TBSYS_LOG(ERROR, "close_conn_ fail, conn=%p, MS=%s, ret=%d", conn, ms_addr, ret);
      }
      else
      {
        conn = NULL;
      }

      if (OB_SQL_SUCCESS == ret && is_conn_pool_valid && 0 != mysql_eno)
      {
        TBSYS_LOG(INFO, "create new connection to %s, free=%d, used=%d, old errno=%d",
            ms_addr, ds->conn_list_.free_list_.size_, ds->conn_list_.used_list_.size_, mysql_eno);

        // NOTE: It is normal to fail to create real connection as server may be down
        // So we do not return error
        if (OB_SQL_SUCCESS != create_real_connection(ds))
        {
          TBSYS_LOG(WARN, "fail to add new connection to pool=%p, MS=%s, free=%d, used=%d",
              ds, ms_addr, ds->conn_list_.free_list_.size_, ds->conn_list_.used_list_.size_);

          if (! g_ms_black_list.exist(ds->server_.ip_))
          {
            // NOTE: Add MS to black list on ERROR
            TBSYS_LOG(INFO, "add MS=%s to BLACK LIST, size=%d", ms_addr, g_ms_black_list.size_);
            g_ms_black_list.add(ds->server_.ip_);

            // NOTE: Wakeup update worker
            wakeup_update_worker();
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "create connection success for MS=%s after release the old. free=%d, used=%d",
              ms_addr, ds->conn_list_.free_list_.size_, ds->conn_list_.used_list_.size_);
        }
      }
    }
    else
    {
      // Recycle normal connection
      if (OB_SQL_SUCCESS != (ret = recycle_conn_(conn)))
      {
        TBSYS_LOG(ERROR, "fail to recycle connection, conn=%p, MS=%s, ret=%d", conn, ms_addr, ret);
      }
      else
      {
        conn = NULL;
      }
    }

    TBSYS_LOG(DEBUG, "after release_conn: MS=[%s], free=[%d], used=[%d], ret=%d",
        ms_addr, NULL == ds ? 0 : ds->conn_list_.free_list_.size_,
        NULL == ds ? 0 : ds->conn_list_.used_list_.size_, ret);

    pthread_mutex_unlock(&pool_mutex);
    pthread_rwlock_unlock(&g_config_rwlock);
  }

  return ret;
}
