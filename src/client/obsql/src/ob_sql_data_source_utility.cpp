#include "common/ob_define.h"
#include "ob_sql_data_source_utility.h"
#include "ob_sql_util.h"
#include "ob_sql_global.h"
#include "ob_sql_conn_acquire.h"
#include "common/ob_malloc.h"
#include "tblog.h"

using namespace oceanbase::common;
//static int copy_ds(ObDataSource *dest, ObDataSource *src)
//{
//  int ret = OB_SQL_SUCCESS;
//  if (NULL == dest || NULL == src)
//  {
//    TBSYS_LOG(ERROR, "invalid argument dest is %p, src is %p", dest, src);
//    ret = OB_SQL_ERROR;
//  }
//  else
//  {
//    ObSQLConn *cconn = NULL;
//    ObSQLConn *nconn = NULL;
//    dest->cluster_ = src->cluster_;
//    dest->server_ = src->server_;
//    //for conn from src to dest
//    ob_sql_list_init(&dest->conn_list_.free_conn_list_);
//    ob_sql_list_init(&dest->conn_list_.used_conn_list_);
//    ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.used_conn_list_, conn_list_node_)
//    {
//      ob_sql_list_del(&cconn->conn_list_node_);
//      ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->conn_list_.used_conn_list_);
//    }
//    ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.free_conn_list_, conn_list_node_)
//    {
//      ob_sql_list_del(&cconn->conn_list_node_);
//      ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->conn_list_.free_conn_list_);
//    }
//  }
//  return ret;
//}
/**
 * 在连接池里面创建一条新的连接
 */
int create_real_connection(ObDataSource *pool)
{
  int ret = OB_SQL_SUCCESS;
  ObSQLListNode * node = NULL;

  if (NULL == pool)
  {
    TBSYS_LOG(ERROR, "invalid data source argument: pool=%p", pool);
    ret = OB_SQL_ERROR;
  }
  else
  {
    node = reinterpret_cast<ObSQLListNode *>(ob_malloc(sizeof(ObSQLConn)+sizeof(ObSQLListNode), ObModIds::LIB_OBSQL));
    if (NULL == node)
    {
      TBSYS_LOG(ERROR, "alloc mem for ObSQLConn list node failed");
      ret = OB_SQL_ERROR;
    }
    else
    {
      memset(node, 0, sizeof(ObSQLConn)+sizeof(ObSQLListNode));

      node->data_ = ((char*)node) + sizeof(ObSQLListNode);
      ObSQLConn *conn = (ObSQLConn*)node->data_;
      char ipbuffer[OB_SQL_IP_BUFFER_SIZE];

      ret = get_server_ip(&pool->server_, ipbuffer, OB_SQL_IP_BUFFER_SIZE);
      if (ret != OB_SQL_SUCCESS)
      {
        TBSYS_LOG(ERROR,"can not get server ip address server(%u, %u)", pool->server_.ip_, pool->server_.port_);
      }
      else if (NULL == (conn->mysql_ = CALL_REAL(mysql_init, conn->mysql_)))
      {
        TBSYS_LOG(ERROR, "init mysql handler failed");
        ret = OB_SQL_ERROR;
      }
      else if (OB_SQL_SUCCESS != (ret = set_mysql_options(conn->mysql_)))
      {
        TBSYS_LOG(ERROR, "set_mysql_options fail, ret=%d, mysql=%p", ret, conn->mysql_);
      }
      else if (NULL == (conn->mysql_ = CALL_REAL(mysql_real_connect, conn->mysql_,
              ipbuffer,
              g_sqlconfig.username_,
              g_sqlconfig.passwd_,
              OB_SQL_DB,
              pool->server_.port_,
              NULL, 0)))
      {
        TBSYS_LOG(ERROR, "failed to connect to server %s, errno=%d, %s",
            get_server_str(&(pool->server_)), CALL_REAL(mysql_errno, conn->mysql_), CALL_REAL(mysql_error, conn->mysql_));
        ret = OB_SQL_ERROR;
      }
      else
      {
        conn->pool_ = pool;
        conn->node_ = node;
        conn->server_ = pool->server_;
        conn->is_conn_pool_valid_ = true;
        //client version to server
        //(*g_func_set.real_mysql_query)(conn->mysql_, OB_SQL_CLIENT_VERSION);

        ob_sql_list_add_tail(&pool->conn_list_.free_list_, node);

        TBSYS_LOG(DEBUG, "create new connection success: MS=%s, node=%p, conn=%p, mysql=%p, ds=%p, free=%d, used=%d",
            get_server_str(&pool->server_), node, conn, conn->mysql_, pool,
            pool->conn_list_.free_list_.size_, pool->conn_list_.used_list_.size_);
      }
    }
  }

  if (OB_SQL_SUCCESS != ret)
  {
    if (NULL != node)
    {
      ob_free(node);
      node = NULL;
    }

    TBSYS_LOG(ERROR, "create new connection fail, ds=%p, MS=%s",
        pool, NULL == pool ? "NULL" : get_server_str(&pool->server_));
  }

  return ret;
}

int add_data_source(int32_t conns, ObServerInfo server, ObClusterInfo *cluster)
{
  int ret = OB_SQL_SUCCESS;
  ObDataSource* ds = NULL;

  if (NULL == cluster || 0 >= conns)
  {
    TBSYS_LOG(ERROR, "invalid arguments: conns=%d, cluster=%p", conns, cluster);
    ret = OB_SQL_INVALID_ARGUMENT;
  }
  else
  {
    TBSYS_LOG(INFO, "add data source MS=%s: cluster={id=%d,addr=%s}, MS NUM=%d, conns=%d",
        get_server_str(&server), cluster->cluster_id_,
        get_server_str(&cluster->cluster_addr_),
        cluster->size_, conns);

    // Alloc new ObDataSource
    ds = static_cast<ObDataSource *>(ob_malloc(sizeof(ObDataSource), ObModIds::LIB_OBSQL));
    if (NULL == ds)
    {
      TBSYS_LOG(ERROR, "alloc memory for ObDataSource fail");
      ret = OB_SQL_ERROR;
    }
    else
    {
      (void)memset(ds, 0, sizeof(ObDataSource));

      pthread_mutex_init(&(ds->mutex_), NULL);
      ds->cluster_ = cluster;
      ds->server_ = server;
      ob_sql_list_init(&ds->conn_list_.free_list_, OBSQLCONN);
      ob_sql_list_init(&ds->conn_list_.used_list_, OBSQLCONN);
      //创建真正的连接
      for (int32_t index = 0; index < conns && OB_SQL_SUCCESS == ret; ++index)
      {
        ret = create_real_connection(ds);
        if (OB_SQL_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "create real connection to %s failed", get_server_str(&server));

          // NOTE: Recycle un-used data source
          recycle_data_source(ds);
          ds = NULL;
          break;
        }
      }

      if (OB_SQL_SUCCESS == ret)
      {
        cluster->dslist_[cluster->size_++] = ds;

        TBSYS_LOG(DEBUG, "after add data source MS=%s: cluster={id=%d,addr=%s}, MS NUM=%d, conns=%d",
            get_server_str(&server), cluster->cluster_id_,
            get_server_str(&cluster->cluster_addr_), cluster->size_, conns);
      }
    }
  }

  return ret;
}

static void release_conn_list_node_(ObSQLListNode *node)
{
  OB_ASSERT(NULL != node);

  ObSQLConn *conn = (ObSQLConn *)node->data_;

  if (NULL != conn)
  {
    if (NULL != conn->mysql_)
    {
      CALL_REAL(mysql_close, conn->mysql_);
      conn->mysql_ = NULL;
    }

    (void)memset(conn, 0, sizeof(ObSQLConn));
  }

  (void)memset(node, 0, sizeof(ObSQLListNode));

  // Free memory of ObSQLListNode and ObSQLConn
  // NOTE: ObSQLListNode memory and ObSQLConn memory are contiguous and are allocated together.
  // Here we only need free ObSQLListNode. ObSQLConn will be freed at the same time.
  ob_free(node);
  node = NULL;
}

/// Recycle specific data source
/// 1. Release every node of free list: close MYSQL connection and free node memory
/// 2. Set connection pool of every connection on used list invalid
void recycle_data_source(ObDataSource *ds)
{
  if (NULL != ds)
  {
    ObSQLListNode *node = NULL;
    ObSQLListNode *next_node = NULL;

    TBSYS_LOG(INFO, "recycle data source: ds=%p, MS=%s, free=%d, used=%d",
        ds, get_server_str(&ds->server_), ds->conn_list_.free_list_.size_, ds->conn_list_.used_list_.size_);

    // Recycle free list
    node = ds->conn_list_.free_list_.head_;
    while (NULL != node)
    {
      next_node = node->next_;
      release_conn_list_node_(node);
      node = next_node;
    }

    // Recycle used list
    node = ds->conn_list_.used_list_.head_;
    while (NULL != node)
    {
      next_node = node->next_;

      ObSQLConn *conn = (ObSQLConn *)node->data_;
      if (NULL != conn)
      {
        // Set connection pool to be invalid
        // NOTE: Keep all other information valid as connections are still using.
        conn->is_conn_pool_valid_ = false;
        conn->pool_ = NULL;
      }

      node = next_node;
    }

    // Clear data source
    pthread_mutex_destroy(&ds->mutex_);
    (void)memset(ds, 0, sizeof(ObDataSource));

    ob_free(ds);
    ds = NULL;
  }
}

// Destroy all connections in datasource
void destroy_data_source(ObDataSource *ds)
{
  if (NULL != ds)
  {
    ObSQLListNode *node = NULL;
    ObSQLListNode *next_node = NULL;

    TBSYS_LOG(INFO, "destroy data source: ds=%p, MS=%s, free=%d, used=%d",
        ds, get_server_str(&ds->server_), ds->conn_list_.free_list_.size_, ds->conn_list_.used_list_.size_);

    // Recycle free list
    node = ds->conn_list_.free_list_.head_;
    while (NULL != node)
    {
      next_node = node->next_;
      release_conn_list_node_(node);
      node = next_node;
    }

    // Recycle used list
    node = ds->conn_list_.used_list_.head_;
    while (NULL != node)
    {
      next_node = node->next_;
      release_conn_list_node_(node);
      node = next_node;
    }

    // Clear data source
    pthread_mutex_destroy(&ds->mutex_);
    (void)memset(ds, 0, sizeof(ObDataSource));

    ob_free(ds);
    ds = NULL;
  }
}

/// Delete data source from cluster data source list
/// @param cluster specific cluster
/// @param sindex data source index in cluster data source list
void delete_data_source(ObClusterInfo *cluster, int32_t sindex)
{
  TBSYS_LOG(INFO, "delete data source: cluster=%p, index=%d", cluster, sindex);

  if (NULL != cluster && 0 <= sindex && sindex < cluster->size_)
  {
    ObDataSource *ds = cluster->dslist_[sindex];
    if (NULL == ds)
    {
      TBSYS_LOG(WARN, "cluster %p has NULL ObDataSource, cluster={id=%d,addr=%s}, MS NUM=%d",
          cluster, cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_);
    }
    else
    {
      TBSYS_LOG(INFO, "delete data source: cluster={id=%d,addr=%s}, MS=%s, MS NUM=%d",
          cluster->cluster_id_, get_server_str(&cluster->cluster_addr_),
          get_server_str(&ds->server_), cluster->size_);

      // Delete un-used MS from black list
      g_ms_black_list.del(ds->server_.ip_);

      // Recycle un-used MS
      recycle_data_source(ds);
      ds = NULL;
      cluster->dslist_[sindex] = NULL;
    }

    for (int index = sindex + 1; index < cluster->size_; index++)
    {
      cluster->dslist_[index - 1] = cluster->dslist_[index];
    }

    cluster->size_--;

    TBSYS_LOG(DEBUG, "after delete_data_source: cluster=[%d,%s], size=%d",
        cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_);
  }
}

// Update data source
// If data source connection number is little than normal value, try to create new connection.
// If new connection is created successfully, delete it from black list
int update_data_source(ObDataSource &ds)
{
  int ret = OB_SQL_SUCCESS;
  int32_t conn_size = ds.conn_list_.free_list_.size_ + ds.conn_list_.used_list_.size_;

  if (conn_size < g_config_using->max_conn_size_)
  {
    TBSYS_LOG(INFO, "MS=%s conn size (used=%d,free=%d) < max_conn_size %d, try to create new connection",
        get_server_str(&ds.server_), ds.conn_list_.used_list_.size_,
        ds.conn_list_.free_list_.size_, g_config_using->max_conn_size_);

    // Create new connection
    for (int index = conn_size; index < g_config_using->max_conn_size_; index++)
    {
      if (OB_SQL_SUCCESS != (ret = create_real_connection(&ds)))
      {
        TBSYS_LOG(ERROR, "create real connection to %s failed", get_server_str(&ds.server_));
        break;
      }
    }

    if (OB_SQL_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "Add new connection to %s success, used=%d, free=%d, max_conn_size=%d. Delete it from black list",
          get_server_str(&ds.server_), ds.conn_list_.used_list_.size_,
          ds.conn_list_.free_list_.size_, g_config_using->max_conn_size_);

      // MS is normal. Delete it from black list
      g_ms_black_list.del(ds.server_.ip_);
    }
  }

  return ret;
}
