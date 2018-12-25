#include "tblog.h"
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "ob_sql_select_method_impl.h"
#include "ob_sql_ms_select.h"
#include "ob_sql_util.h"
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <errno.h>

static bool is_same_server(ObDataSource *ds, ObServerInfo *server)
{
  bool ret = false;
  if (NULL != ds && NULL != server)
  {
    if (ds->server_.port_ == server->port_
        && ds->server_.ip_ == server->ip_)
    {
      ret = true;
    }
  }
  return ret;
}

// NOTE: Random select a MS which has free connections
ObDataSource* random_mergeserver_select(ObClusterInfo *cluster, ObSQLMySQL *mysql)
{
  ObDataSource* ds = NULL;

  if (NULL == cluster || mysql == NULL)
  {
    TBSYS_LOG(WARN, "invalid argument, cluster=%p, mysql=%p", cluster, mysql);
  }
  else if (0 >= cluster->size_)
  {
    TBSYS_LOG(WARN, "no MS in cluster[id=%d,addr=%s,type=%d], MS NUM=%d",
        cluster->cluster_id_, get_server_str(&cluster->cluster_addr_),
        cluster->cluster_type_, cluster->size_);
  }
  else
  {
    // Random select a MS
    long int r = random() % (cluster->size_);
    ds = cluster->dslist_[r];

    TBSYS_LOG(DEBUG, "SELECT_MS: random offset=%ld", r);

    // Retry if MS has no free connections or is in black list or is same with last MS if need retry
    if (!ds->has_free_conns() || g_ms_black_list.exist(ds->server_.ip_)
        || (mysql->retry_ && is_same_server(ds, &mysql->last_ds_)))
    {
      TBSYS_LOG(INFO, "SELECT_MS: MS=%s invalid. free=%d, used=%d, is_black=%s, retry=%s, last_ds=%s",
          get_server_str(&ds->server_), ds->conn_list_.free_list_.size_, ds->conn_list_.used_list_.size_,
          g_ms_black_list.exist(ds->server_.ip_) ? "Y" : "N",
          mysql->retry_ ? "Y" : "N", get_server_str(&mysql->last_ds_));

      ObDataSource *base_ds = ds;
      ds = NULL;

      if (1 < cluster->size_)
      {
        for (int32_t i = 0; i < cluster->size_; i++)
        {
          int32_t index = (int32_t)((++r) % cluster->size_);
          ObDataSource *tmp_ds = cluster->dslist_[index];

          if (base_ds == tmp_ds)
          {
            continue;
          }

          bool valid = tmp_ds->has_free_conns()
              && ! g_ms_black_list.exist(tmp_ds->server_.ip_)
              && ((! mysql->retry_) || (! is_same_server(ds, &mysql->last_ds_)));

          TBSYS_LOG(INFO, "SELECT_MS: MS=%s %s. free=%d, used=%d, is_black=%s, retry=%s, last_ds=%s, index=%d",
              get_server_str(&tmp_ds->server_), valid ? "valid" : "invalid",
              tmp_ds->conn_list_.free_list_.size_, tmp_ds->conn_list_.used_list_.size_,
              g_ms_black_list.exist(tmp_ds->server_.ip_) ? "Y" : "N",
              mysql->retry_ ? "Y" : "N", get_server_str(&mysql->last_ds_), index);

          if (valid)
          {
            ds = tmp_ds;
            break;
          }
        } // end for
      }
    }
  }

  return ds;
}

#if 0
// FIXME: consishash select method will core dump
/* consisten hash */
ObDataSource* consishash_mergeserver_select(ObClusterInfo *pool, const char* sql, unsigned long length)
{
  ObDataSource* datasource = NULL;
  uint32_t hashval = 0;
  hashval = oceanbase::common::murmurhash2(sql, static_cast<int32_t>(length), hashval);
  TBSYS_LOG(INFO, "hashval of this query is %u", hashval);
  int32_t index = 0;

  for (index = 0; index < g_config_using->cluster_size_; ++index)
  {
    if (pool->cluster_id_ == g_config_using->clusters_[index].cluster_id_)
    {
      ObSQLSelectMsTable *table = g_config_using->clusters_[index].table_;
      datasource = find_ds_by_val(table->items_, table->slot_num_, hashval);
      break;
    }
  }

  // TODO: Add MS black list control HERE

  return datasource;
}
#endif

/* random */
ObSQLConn* random_conn_select(ObDataSource *pool)
{
  ObSQLListNode *node = NULL;
  ObSQLConn* conn = NULL;

  TBSYS_LOG(DEBUG, "before random_conn_select: MS=%s, free=%d, used=%d",
      get_server_str(&pool->server_), pool->conn_list_.free_list_.size_, pool->conn_list_.used_list_.size_);

  node = pool->conn_list_.free_list_.head_;
  //conn = ob_sql_list_get_first(&pool->conn_list_.free_conn_list_, ObSQLConn, conn_list_node_);
  /* end TODO */
  if (NULL != node)
  {
    if (0 == pthread_mutex_lock(&pool->mutex_))
    {
      //ob_sql_list_get(conn, &pool->free_conn_list_, conn_list_node_, r);
      ob_sql_list_del(&pool->conn_list_.free_list_, node);
      ob_sql_list_add_tail(&pool->conn_list_.used_list_, node);
      conn = (ObSQLConn*)node->data_;
      pthread_mutex_unlock(&pool->mutex_);
    }
    else
    {
      TBSYS_LOG(ERROR, "lock pool->mutex_ %p failed, code id %d, mesg is %s", &pool->mutex_, errno, strerror(errno));
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "ob_sql_list_get_first(pool(%p) free conn list) is null", pool);
  }

  TBSYS_LOG(DEBUG, "after random_conn_select: MS=%s, free=%d, used=%d, node=%p",
      get_server_str(&pool->server_), pool->conn_list_.free_list_.size_, pool->conn_list_.used_list_.size_, node);

  return conn;
}
