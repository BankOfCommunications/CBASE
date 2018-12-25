#include "common/ob_define.h"
#include "tblog.h"
#include "common/ob_malloc.h"
#include "ob_sql_conn_acquire.h"
#include "ob_sql_mysql_adapter.h"
#include "ob_sql_cluster_select.h"
#include "ob_sql_parser.h"
#include "ob_sql_util.h"
#include "ob_sql_data_source_utility.h"

using namespace oceanbase::common;

// Declaration of static functions
static MYSQL* select_connection(ObSQLMySQL *ob_mysql, const char *q, unsigned long length, ObSQLType *stype);

void MYSQL_FUNC(mysql_set_consistence)(MYSQL_TYPE *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->is_consistence_ = 1;
}

void MYSQL_FUNC(mysql_unset_consistence)(MYSQL_TYPE *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->is_consistence_ = 0;
}

my_bool is_consistence(ObSQLMySQL *mysql)
{
  return mysql->is_consistence_;
}

my_bool need_retry(ObSQLMySQL *mysql)
{
  my_bool ret = 1;
  if (is_in_transaction(mysql))
  {
    ret = 0;
  }
  return ret;
}

void MYSQL_FUNC(mysql_set_retry)(ObSQLMySQL *mysql)
{
  mysql->retry_ = 1;
}

void MYSQL_FUNC(mysql_unset_retry)(ObSQLMySQL *mysql)
{
  mysql->retry_ = 0;
}

my_bool is_retry(ObSQLMySQL *mysql)
{
  return mysql->retry_;
}

void MYSQL_FUNC(mysql_set_in_transaction)(MYSQL_TYPE *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->in_transaction_ = 1;
}

void MYSQL_FUNC(mysql_unset_in_transaction)(MYSQL_TYPE *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->in_transaction_ = 0;
}

my_bool is_in_transaction(ObSQLMySQL *mysql)
{
  return mysql->in_transaction_;
}

int STDCALL MYSQL_FUNC(mysql_server_init)(int argc __attribute__((unused)),
			      char **argv __attribute__((unused)),
			      char **groups __attribute__((unused)))
{
  if (0 == g_inited) //not inited
  {
    TBSYS_LOG(ERROR, "mysql_server_init fail: libobsql has not been inited");
    return 1;//error
  }
  else
  {
    return CALL_REAL(mysql_server_init, argc, argv, groups);
  }
}

void STDCALL MYSQL_FUNC(mysql_server_end)(void)
{
  if (g_inited)
  {
    CALL_REAL(mysql_server_end);
  }
}

//do nothing here same as libmysql
MYSQL_PARAMETERS *STDCALL MYSQL_FUNC(mysql_get_parameters)(void)
{
  return CALL_REAL(mysql_get_parameters);
}

my_bool STDCALL MYSQL_FUNC(mysql_thread_init)(void)
{
  my_bool ret = 0;

  if (g_inited)
  {
    ret = CALL_REAL(mysql_thread_init);
  }
  else
  {
    TBSYS_LOG(ERROR, "mysql_thread_init fail: libobsql has not been inited");
    ret = 0;
  }

  return ret;
}

void STDCALL MYSQL_FUNC(mysql_thread_end)(void)
{
  if (g_inited)
  {
    CALL_REAL(mysql_thread_end);
  }
}

my_ulonglong STDCALL MYSQL_FUNC(mysql_num_rows)(MYSQL_RES *result)
{
  return CALL_REAL(mysql_num_rows, result);
}

unsigned int STDCALL MYSQL_FUNC(mysql_num_fields)(MYSQL_RES *result)
{
  return CALL_REAL(mysql_num_fields, result);
}

my_bool STDCALL MYSQL_FUNC(mysql_eof)(MYSQL_RES *res)
{
  return CALL_REAL(mysql_eof, res);
}

MYSQL_FIELD *STDCALL MYSQL_FUNC(mysql_fetch_field_direct)(MYSQL_RES *res, unsigned int fieldnr)
{
  return CALL_REAL(mysql_fetch_field_direct, res, fieldnr);
}

MYSQL_FIELD * STDCALL MYSQL_FUNC(mysql_fetch_fields)(MYSQL_RES *res)
{
  return CALL_REAL(mysql_fetch_field, res);
}

MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_row_tell)(MYSQL_RES *res)
{
  return CALL_REAL(mysql_row_tell, res);
}

MYSQL_FIELD_OFFSET STDCALL MYSQL_FUNC(mysql_field_tell)(MYSQL_RES *res)
{
  return CALL_REAL(mysql_field_tell, res);
}

unsigned int STDCALL MYSQL_FUNC(mysql_field_count)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_field_count);
}

my_ulonglong STDCALL MYSQL_FUNC(mysql_affected_rows)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_affected_rows);
}

my_ulonglong STDCALL MYSQL_FUNC(mysql_insert_id)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_insert_id);
}

unsigned int STDCALL MYSQL_FUNC(mysql_errno)(MYSQL_TYPE *mysql)
{
  unsigned int ret = 0;

  if (NULL != mysql && NULL != ((ObSQLMySQL*)mysql)->conn_)
  {
    ret = CALL_MYSQL_REAL(mysql, mysql_errno);
  }
  else
  {
    ret = 2006;
  }

  return ret;
}

const char * STDCALL MYSQL_FUNC(mysql_error)(MYSQL_TYPE *mysql)
{
  const char *ret = NULL;

  if (NULL != mysql && NULL != ((ObSQLMySQL*)mysql)->conn_)
  {
    ret = CALL_MYSQL_REAL(mysql, mysql_error);
  }
  else
  {
    ret = "MySQL server has gone away";
  }

  return ret;
}

const char *STDCALL MYSQL_FUNC(mysql_sqlstate)(MYSQL_TYPE *mysql)
{
  const char *ret = NULL;

  if (NULL != mysql && NULL != ((ObSQLMySQL*)mysql)->conn_)
  {
    ret = CALL_MYSQL_REAL(mysql, mysql_sqlstate);
  }
  else
  {
    ret = "HY000";
  }

  return ret;
}

unsigned int STDCALL MYSQL_FUNC(mysql_warning_count)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_warning_count);
}

const char * STDCALL MYSQL_FUNC(mysql_info)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_info);
}

unsigned long STDCALL MYSQL_FUNC(mysql_thread_id)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_thread_id);
}

const char * STDCALL MYSQL_FUNC(mysql_character_set_name)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_character_set_name);
}

int STDCALL MYSQL_FUNC(mysql_set_character_set)(MYSQL_TYPE *mysql, const char *csname)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_set_character_set, csname);
}

MYSQL_TYPE * STDCALL MYSQL_FUNC(mysql_init)(MYSQL_TYPE *mysql)
{
  int flag = OB_SQL_SUCCESS;
  TBSYS_LOG(DEBUG, "mysql init called");
  if (0 == g_inited) //not inited
  {
    TBSYS_LOG(INFO, "not inited");
    return NULL;
  }

  // Call mysql_server_init instead of real mysql_init
  if (0 != CALL_REAL(mysql_server_init, 0, NULL, NULL))
  {
    TBSYS_LOG(ERROR, "mysql_server_init fail, real_mysql_server_init=%p", g_func_set.real_mysql_server_init);
    flag = OB_SQL_ERROR;
  }
  else
  {
    if (NULL == mysql)
    {
      mysql = (MYSQL_TYPE *)ob_malloc(sizeof(MYSQL_TYPE), ObModIds::LIB_OBSQL);
      if (NULL == mysql)
      {
        TBSYS_LOG(ERROR, "alloc mem for MYSQL failed");
        flag = OB_SQL_ERROR;
      }
      else
      {
        memset(mysql, 0, sizeof(MYSQL_TYPE));
        ((ObSQLMySQL*)mysql)->alloc_ = 1;
      }
    }
    else
    {
      memset(mysql,0, sizeof(MYSQL_TYPE));
    }

    // NOTE Initialize ObSQLMySQL
    if (OB_SQL_SUCCESS == flag)
    {
      ((ObSQLMySQL*)mysql)->magic_ = OB_SQL_MAGIC;
      //((ObSQLMySQL*)mysql)->charset = default_client_charset_info;
    }
  }

  return mysql;
}

my_bool STDCALL MYSQL_FUNC(mysql_ssl_set)(MYSQL_TYPE *mysql, const char *key, const char *cert, const char *ca, const char *capath, const char *cipher)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_ssl_set, key, cert, ca, capath, cipher);
}

my_bool STDCALL MYSQL_FUNC(mysql_change_user)(MYSQL_TYPE *mysql, const char *user, const char *passwd, const char *db)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_change_user, user, passwd, db);
}

//do nothing if it is obsql mysql handler
MYSQL_TYPE * STDCALL MYSQL_FUNC(mysql_real_connect)(MYSQL_TYPE *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long clientflag)
{
  MYSQL_TYPE *ret = NULL;

  if (NULL != mysql && OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
  {
    // Select a valid connection
    // NOTE: If no connection available, return NULL
    if (NULL == select_connection((ObSQLMySQL*)mysql, "select connect", sizeof("select connect"), NULL))
    {
      TBSYS_LOG(ERROR, "fail to connect to \"%s:%u\", can not find a valid connection, ob_mysql=%p", host, port, mysql);
      ret = NULL;
    }
    else
    {
      ret = mysql;
    }
  }
  else
  {
    ret = (MYSQL_TYPE *)CALL_REAL(mysql_real_connect, ((MYSQL *)mysql), host, user, passwd, db, port, unix_socket, clientflag);
  }

  return ret;
}

int STDCALL MYSQL_FUNC(mysql_select_db)(MYSQL_TYPE *mysql, const char *db)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_select_db, db);
}

//wrapper of mysql_reql_query
int STDCALL MYSQL_FUNC(mysql_query)(MYSQL_TYPE *mysql, const char *q)
{
  return MYSQL_FUNC(mysql_real_query)(mysql, q, strlen(q));
}

static void clear_connection_(ObSQLMySQL *ob_mysql)
{
  OB_ASSERT(NULL != ob_mysql);

  if (ob_mysql->rconn_  != ob_mysql->wconn_)
  {
    if (NULL != ob_mysql->rconn_)
    {
      if (OB_SQL_SUCCESS != release_conn(ob_mysql->rconn_))
      {
        TBSYS_LOG(ERROR, "release_conn fail, ob_mysql=%p, rconn=%p, MS=%s, node=%p, "
            "real mysql=%p, pool=%p, is_conn_pool_valid=%s",
            ob_mysql, ob_mysql->rconn_, get_server_str(&ob_mysql->rconn_->server_),
            ob_mysql->rconn_->node_, ob_mysql->rconn_->mysql_,
            ob_mysql->rconn_->pool_, ob_mysql->rconn_->is_conn_pool_valid_ ? "true" : "false");
      }
      ob_mysql->rconn_ = NULL;
    }

    if (NULL != ob_mysql->wconn_)
    {
      if (OB_SQL_SUCCESS != release_conn(ob_mysql->wconn_))
      {
        TBSYS_LOG(ERROR, "release_conn fail, ob_mysql=%p, wconn=%p, MS=%s, node=%p, "
            "real mysql=%p, pool=%p, is_conn_pool_valid=%s",
            ob_mysql, ob_mysql->wconn_, get_server_str(&ob_mysql->wconn_->server_),
            ob_mysql->wconn_->node_, ob_mysql->wconn_->mysql_,
            ob_mysql->wconn_->pool_, ob_mysql->wconn_->is_conn_pool_valid_ ? "true" : "false");
      }
      ob_mysql->wconn_ = NULL;
    }

    ob_mysql->conn_ = NULL;
  }
  else
  {
    if (NULL != ob_mysql->rconn_)
    {
      if (OB_SQL_SUCCESS != release_conn(ob_mysql->rconn_))
      {
        TBSYS_LOG(ERROR, "release_conn fail, ob_mysql=%p, rconn=%p, MS=%s, node=%p, "
            "real mysql=%p, pool=%p, is_conn_pool_valid=%s",
            ob_mysql, ob_mysql->rconn_, get_server_str(&ob_mysql->rconn_->server_),
            ob_mysql->rconn_->node_, ob_mysql->rconn_->mysql_,
            ob_mysql->rconn_->pool_, ob_mysql->rconn_->is_conn_pool_valid_ ? "true" : "false");
      }
    }

    ob_mysql->conn_ = NULL;
    ob_mysql->rconn_ = NULL;
    ob_mysql->wconn_ = NULL;
  }
}

static ObClusterInfo *get_next_slave_cluster_(ObClusterInfo *base_cluster, int32_t &index)
{
  OB_ASSERT(NULL != base_cluster && g_inited);

  ObClusterInfo *ret = NULL;

  TBSYS_LOG(DEBUG, "GET_SLAVE_CLUSTER: BASE={id=%u,addr=%s,type=%d}, index=%d",
      base_cluster->cluster_id_, get_server_str(&base_cluster->cluster_addr_), base_cluster->cluster_type_, index);

  if (0 > index)
  {
    index = 0;
  }
  else
  {
    index++;
  }

  for (int32_t i = index; i < g_group_ds.size_; ++i)
  {
    ObClusterInfo *cluster = g_group_ds.clusters_[i];

    if (MASTER_CLUSTER != cluster->cluster_type_ && cluster->cluster_id_ != base_cluster->cluster_id_)
    {
      index = i;
      ret = cluster;
      break;
    }
  }

  if (NULL == ret)
  {
    index = -1;

    TBSYS_LOG(DEBUG, "GET_SLAVE_CLUSTER: ret=NULL, index=%d", index);
  }
  else
  {
    TBSYS_LOG(DEBUG, "GET_SLAVE_CLUSTER: ret={id=%u,addr=%s,type=%d}, index=%d",
        ret->cluster_id_, get_server_str(&ret->cluster_addr_), ret->cluster_type_, index);
  }

  return ret;
}

static ObSQLConn *choose_conn_(ObSQLMySQL *ob_mysql, const char *q, unsigned long length)
{
  ObSQLConn *ret = NULL;
  ObClusterInfo *cluster = NULL;

  if (0 == pthread_rwlock_rdlock(&g_config_rwlock))
  {
    //根据流量分配和请求类型，选择集群
    if (NULL == (cluster = select_cluster(ob_mysql)))
    {
      TBSYS_LOG(ERROR, "select_cluster fail, no valid cluster avaliable");
    }
    //每次请求都需要根据sql做一致性hash来在特定的集群里面选择新的ms
    else if (NULL == (ret = acquire_conn(cluster, q, length, ob_mysql)) && 1 < g_group_ds.size_)
    {
      ObClusterInfo *base_cluster = cluster;

      TBSYS_LOG(INFO, "CHOOSE_CLUSTER: BASE={id=%u,addr=%s,type=%d}, SIZE=%d",
          base_cluster->cluster_id_, get_server_str(&base_cluster->cluster_addr_),
          base_cluster->cluster_type_, g_group_ds.size_);

      if (2 == g_group_ds.size_)
      {
        cluster = g_group_ds.clusters_[0] == cluster ? g_group_ds.clusters_[1] : g_group_ds.clusters_[0];
        if (NULL != cluster)
        {
          TBSYS_LOG(INFO, "CHOOSE_CLUSTER: NEXT={id=%u,addr=%s,type=%d}",
              cluster->cluster_id_, get_server_str(&cluster->cluster_addr_),cluster->cluster_type_);

          ret = acquire_conn(cluster, q, length, ob_mysql);
        }
      }
      // Cluster size > 2
      else
      {
        int32_t index = -1;

        // Get slave cluster first
        while (NULL != (cluster = get_next_slave_cluster_(base_cluster, index)))
        {
          TBSYS_LOG(INFO, "CHOOSE_CLUSTER: SLAVE={id=%u,addr=%s,type=%d}, index=%d",
              cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->cluster_type_, index);

          if (NULL != (ret = acquire_conn(cluster, q, length, ob_mysql)))
          {
            break;
          }
        }

        if (NULL == ret && MASTER_CLUSTER != base_cluster->cluster_type_)
        {
          cluster = get_master_cluster(&g_group_ds);
          if (NULL != cluster)
          {
            TBSYS_LOG(INFO, "CHOOSE_CLUSTER: MASTER={id=%u,addr=%s,type=%d}",
                cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->cluster_type_);
            ret = acquire_conn(cluster, q, length, ob_mysql);
          }
          else
          {
            TBSYS_LOG(WARN, "CHOOSE_CLUSTER: no master cluster available, cluster_size=%d", g_group_ds.size_);
          }
        }
      }
    }

    if (NULL == ret)
    {
      TBSYS_LOG(WARN, "fail to choose connection from all clusters, cluster size=%d", g_config_using->cluster_size_);
    }

    pthread_rwlock_unlock(&g_config_rwlock);
  }
  else
  {
    TBSYS_LOG(ERROR, "fail to choose connection: pthread_rwlock_rdlock failed on g_config_rwlock");
  }

  return ret;
}

/*get an real MYSQL handle from obsql according to query&& consistence property*/
static MYSQL* select_connection(ObSQLMySQL *ob_mysql, const char *q, unsigned long length, ObSQLType *stype)
{
  OB_ASSERT(NULL != ob_mysql);

  MYSQL *real_mysql = NULL;

  TBSYS_LOG(DEBUG, "select_connection: ob_mysql=%p, conn=%p, wconn=%p, rconn=%p, MS=%s",
      ob_mysql, ob_mysql->conn_, ob_mysql->wconn_, ob_mysql->rconn_,
      NULL == ob_mysql->conn_ ? "" : get_server_str(&ob_mysql->conn_->server_));

  if (! (ob_mysql->has_stmt_))
  {
    //pase sql type
    ObSQLType sql_type = get_sql_type(q, length);
    if (OB_SQL_BEGIN_TRANSACTION == sql_type)
    {
      MYSQL_FUNC(mysql_set_in_transaction)(ob_mysql);
      MYSQL_FUNC(mysql_set_consistence)(ob_mysql);
      TBSYS_LOG(INFO, "set consistence %p", ob_mysql);
    }
    else if (OB_SQL_CONSISTENCE_REQUEST == sql_type)
    {
      //never reach here now
      MYSQL_FUNC(mysql_set_consistence)(ob_mysql);
      TBSYS_LOG(INFO, "set consistence %p", ob_mysql);
    }
    else if (OB_SQL_READ != sql_type)
    {
      MYSQL_FUNC(mysql_set_consistence)(ob_mysql);
      TBSYS_LOG(INFO, "set consistence %p", ob_mysql);
    }

    if (NULL != stype)
    {
      *stype = sql_type;
    }
  }

  //选择主集群
  if (is_in_transaction(ob_mysql) || is_consistence(ob_mysql))
  {
    TBSYS_LOG(DEBUG, "query to master cluster");
    if (is_retry(ob_mysql) || NULL == (ob_mysql->wconn_))
    {
      clear_connection_(ob_mysql);

      ObSQLConn *conn = choose_conn_(ob_mysql, q, length);
      if (NULL != conn)
      {
        ob_mysql->wconn_ = conn;
        ob_mysql->conn_ = conn;
        real_mysql = ob_mysql->conn_->mysql_;
      }
    }
    else
    {
      ob_mysql->conn_ = ob_mysql->wconn_;
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "ordinary query");

    clear_connection_(ob_mysql);

    ObSQLConn *conn = choose_conn_(ob_mysql, q, length);
    if (NULL != conn)
    {
      ob_mysql->rconn_ = conn;
      ob_mysql->conn_ = conn;
      real_mysql = ob_mysql->conn_->mysql_;
    }
  }

  if (NULL != (ob_mysql->conn_))
  {
    real_mysql = ob_mysql->conn_->mysql_;
  }

  TBSYS_LOG(DEBUG, "after select_connection: real_mysql=%p, ob_mysql=%p, conn=%p, wconn=%p, rconn=%p, MS=%s",
      real_mysql, ob_mysql, ob_mysql->conn_, ob_mysql->wconn_, ob_mysql->rconn_,
      NULL == ob_mysql->conn_ ? "" : get_server_str(&ob_mysql->conn_->server_));

  return real_mysql;
}

int STDCALL MYSQL_FUNC(mysql_send_query)(MYSQL_TYPE *mysql, const char *q, unsigned long length)
{
  TBSYS_LOG(DEBUG, "mysql_send_query call mysql_real_query");
  return MYSQL_FUNC(mysql_real_query)(mysql, q, length);
}

//1. select a connect
//2. fault tolerant 不做容错 由应用来处理
int STDCALL MYSQL_FUNC(mysql_real_query)(MYSQL_TYPE *mysql, const char *q, unsigned long length)
{
  int ret = 0;
  ObSQLType stype = OB_SQL_UNKNOWN;
  MYSQL* real_mysql = NULL;
  TBSYS_LOG(DEBUG, "mysql_real_query: mysql=%p, query is %s", mysql, q);
  if (NULL != mysql && OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
  {
    real_mysql = select_connection((ObSQLMySQL *)mysql, q, length, &stype);
    if (NULL != real_mysql)
    {
      ret = CALL_REAL(mysql_real_query, real_mysql, q, length);
      //retry another mergeserver if necessary
      if (0 != ret && need_retry((ObSQLMySQL*)mysql))
      {
        MYSQL_FUNC(mysql_set_retry)((ObSQLMySQL*)mysql);
        TBSYS_LOG(DEBUG, "mysql_real_query fail, ret=%d, retry. mysql=%p, query=%s", ret, mysql, q);
        real_mysql = select_connection((ObSQLMySQL *)mysql, q, length, &stype);
        if (NULL != real_mysql)
        {
          ret = CALL_REAL(mysql_real_query, real_mysql, q, length);
        }
        else
        {
          TBSYS_LOG(WARN, "can not find a valid connection on retry");
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "can not find a valid connection");
      ret = OB_SQL_ERROR;
    }

    if (OB_SQL_SUCCESS == ret &&
        OB_SQL_END_TRANSACTION == stype)
    {
      MYSQL_FUNC(mysql_unset_in_transaction)(mysql);
      MYSQL_FUNC(mysql_unset_consistence)(mysql);
      TBSYS_LOG(INFO, "unset consistence %p", mysql);
    }
    if (OB_SQL_READ != stype)
    {
      MYSQL_FUNC(mysql_unset_consistence)(mysql);
      TBSYS_LOG(INFO, "unset consistence %p", mysql);
    }
    MYSQL_FUNC(mysql_unset_retry)((ObSQLMySQL *)mysql);
  }
  else
  {
    ret = CALL_REAL(mysql_real_query, (MYSQL *)mysql, q, length);
  }
  return ret;
}

MYSQL_RES * STDCALL MYSQL_FUNC(mysql_store_result)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_store_result);
}

MYSQL_RES * STDCALL MYSQL_FUNC(mysql_use_result)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_use_result);
}

void STDCALL MYSQL_FUNC(mysql_get_character_set_info)(MYSQL_TYPE *mysql, MY_CHARSET_INFO *charset)
{
  CALL_MYSQL_REAL_WITH_JUDGE_NO_RETVAL(mysql, mysql_get_character_set_info, charset);
}

int STDCALL MYSQL_FUNC(mysql_shutdown)(MYSQL_TYPE *mysql, enum mysql_enum_shutdown_level shutdown_level)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_shutdown, shutdown_level);
}

int STDCALL MYSQL_FUNC(mysql_dump_debug_info)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_dump_debug_info);
}

int STDCALL MYSQL_FUNC(mysql_refresh)(MYSQL_TYPE *mysql, unsigned int refresh_options)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_refresh, refresh_options);
}

int STDCALL MYSQL_FUNC(mysql_kill)(MYSQL_TYPE *mysql,unsigned long pid)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_kill, pid);
}

int STDCALL MYSQL_FUNC(mysql_set_server_option)(MYSQL_TYPE *mysql, enum enum_mysql_set_option option)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_set_server_option, option);
}

int STDCALL MYSQL_FUNC(mysql_ping)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_ping);
}

const char * STDCALL MYSQL_FUNC(mysql_stat)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_stat);
}

const char * STDCALL MYSQL_FUNC(mysql_get_server_info)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_get_server_info);
}

const char * STDCALL MYSQL_FUNC(mysql_get_client_info)(void)
{
  return OB_CLIENT_INFO;
}

unsigned long STDCALL MYSQL_FUNC(mysql_get_client_version)(void)
{
  return OB_CLIENT_VERSION;
}

const char * STDCALL MYSQL_FUNC(mysql_get_host_info)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_get_host_info);
}

unsigned long STDCALL MYSQL_FUNC(mysql_get_server_version)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_get_server_version);
}

unsigned int STDCALL MYSQL_FUNC(mysql_get_proto_info)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_get_proto_info);
}

MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_dbs)(MYSQL_TYPE *mysql,const char *wild)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_list_dbs, wild);
}

MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_tables)(MYSQL_TYPE *mysql,const char *wild)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_list_tables, wild);
}

MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_processes)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_list_processes);
}

int STDCALL MYSQL_FUNC(mysql_options)(MYSQL_TYPE *mysql,enum mysql_option option, const char *arg)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_options, option, arg);
}

void STDCALL MYSQL_FUNC(mysql_free_result)(MYSQL_RES *result)
{
  CALL_REAL(mysql_free_result, result);
}

void STDCALL MYSQL_FUNC(mysql_data_seek)(MYSQL_RES *result, my_ulonglong offset)
{
  CALL_REAL(mysql_data_seek, result, offset);
}

MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_row_seek)(MYSQL_RES *result, MYSQL_ROW_OFFSET offset)
{
  return CALL_REAL(mysql_row_seek, result, offset);
}

MYSQL_FIELD_OFFSET STDCALL MYSQL_FUNC(mysql_field_seek)(MYSQL_RES *result, MYSQL_FIELD_OFFSET offset)
{
  return CALL_REAL(mysql_field_seek, result, offset);
}

MYSQL_ROW STDCALL MYSQL_FUNC(mysql_fetch_row)(MYSQL_RES *result)
{
  return CALL_REAL(mysql_fetch_row, result);
}

unsigned long * STDCALL MYSQL_FUNC(mysql_fetch_lengths)(MYSQL_RES *result)
{
  return CALL_REAL(mysql_fetch_lengths, result);
}

MYSQL_FIELD * STDCALL MYSQL_FUNC(mysql_fetch_field)(MYSQL_RES *result)
{
  return CALL_REAL(mysql_fetch_field, result);
}

MYSQL_RES * STDCALL MYSQL_FUNC(mysql_list_fields)(MYSQL_TYPE *mysql, const char *table, const char *wild)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_list_fields, table, wild);
}

unsigned long STDCALL MYSQL_FUNC(mysql_escape_string)(char *to,const char *from, unsigned long from_length)
{
  return CALL_REAL(mysql_escape_string, to, from, from_length);
}

unsigned long STDCALL MYSQL_FUNC(mysql_hex_string)(char *to,const char *from, unsigned long from_length)
{
  return CALL_REAL(mysql_hex_string, to, from, from_length);
}

unsigned long STDCALL MYSQL_FUNC(mysql_real_escape_string)(MYSQL_TYPE *mysql, char *to,const char *from, unsigned long length)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_real_escape_string, to, from, length);
}

void STDCALL MYSQL_FUNC(mysql_debug)(const char *debug)
{
  CALL_REAL(mysql_debug, debug);
}

void STDCALL MYSQL_FUNC(myodbc_remove_escape)(MYSQL_TYPE *mysql,char *name)
{
  CALL_MYSQL_REAL_WITH_JUDGE_NO_RETVAL(mysql, myodbc_remove_escape, name);
}

unsigned int STDCALL MYSQL_FUNC(mysql_thread_safe)(void)
{
  return CALL_REAL(mysql_thread_safe);
}

my_bool STDCALL MYSQL_FUNC(mysql_embedded)(void)
{
  return CALL_REAL(mysql_embedded);
}

my_bool STDCALL MYSQL_FUNC(mysql_read_query_result)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_read_query_result);
}

MYSQL_STMT * STDCALL MYSQL_FUNC(mysql_stmt_init)(MYSQL_TYPE *mysql)
{
  MYSQL_STMT *ret = NULL;

  if (NULL != mysql && OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
  {
    ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
    ob_mysql->has_stmt_ = 1;

    if (NULL != ob_mysql->conn_)
    {
      ret = CALL_REAL(mysql_stmt_init, ob_mysql->conn_->mysql_);
    }
    else
    {
      TBSYS_LOG(ERROR, "mysql_stmt_init fail, no valid connection, ob_mysql=%p, conn=%p, wconn=%p, rconn=%p",
          ob_mysql, ob_mysql->conn_, ob_mysql->wconn_, ob_mysql->rconn_);
    }
  }
  else
  {
    ret = CALL_REAL(mysql_stmt_init, (MYSQL *)mysql);
  }

  return ret;
}

int STDCALL MYSQL_FUNC(mysql_stmt_prepare)(MYSQL_STMT *stmt, const char *query, unsigned long length)
{
  if (NULL != stmt && NULL != query)
  {
    TBSYS_LOG(DEBUG, "stmt prepare mysql is %p query is %s", stmt->mysql, query);
  }

  return CALL_STMT_REAL(stmt, mysql_stmt_prepare, query, length);
}

int STDCALL MYSQL_FUNC(mysql_stmt_execute)(MYSQL_STMT *stmt)
{
  if (NULL != stmt)
  {
    TBSYS_LOG(DEBUG, "stmt execute mysql is %p", stmt->mysql);
  }

  return CALL_STMT_REAL(stmt, mysql_stmt_execute);
}

int STDCALL MYSQL_FUNC(mysql_stmt_fetch)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_fetch);
}

int STDCALL MYSQL_FUNC(mysql_stmt_fetch_column)(MYSQL_STMT *stmt, MYSQL_BIND *bind_arg, unsigned int column, unsigned long offset)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_fetch_column, bind_arg, column, offset);
}

int STDCALL MYSQL_FUNC(mysql_stmt_store_result)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_store_result);
}

unsigned long STDCALL MYSQL_FUNC(mysql_stmt_param_count)(MYSQL_STMT * stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_param_count);
}

my_bool STDCALL MYSQL_FUNC(mysql_stmt_attr_set)(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, const void *attr)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_attr_set, attr_type, attr);
}

my_bool STDCALL MYSQL_FUNC(mysql_stmt_attr_get)(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, void *attr)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_attr_get, attr_type, attr);
}

my_bool STDCALL MYSQL_FUNC(mysql_stmt_bind_param)(MYSQL_STMT * stmt, MYSQL_BIND * bnd)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_bind_param, bnd);
}

my_bool STDCALL MYSQL_FUNC(mysql_stmt_bind_result)(MYSQL_STMT * stmt, MYSQL_BIND * bnd)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_bind_result, bnd);
}

// FIXME: As mysql_stmt_close() will clear error information of stmt->mysql, we
// need the error information afterwards in mysql_close(). So here we first store
// the error information before call real mysql_stmt_close() and then set the error
// information back to real MYSQL handle.
my_bool STDCALL MYSQL_FUNC(mysql_stmt_close)(MYSQL_STMT * stmt)
{
  MYSQL *real_mysql = NULL;
  int errcode = 0;

  if (NULL != stmt)
  {
    TBSYS_LOG(DEBUG, "before stmt close: mysql=%p, errno=%d, sqlstate=%s, error=%s",
        stmt->mysql, MYSQL_FUNC(mysql_stmt_errno)(stmt),
        MYSQL_FUNC(mysql_stmt_sqlstate)(stmt), MYSQL_FUNC(mysql_stmt_error)(stmt));

    // NOTE: Store the error information
    real_mysql = stmt->mysql;
    errcode = MYSQL_FUNC(mysql_stmt_errno)(stmt);
  }

  // Call real mysql_stmt_close()
  my_bool ret = CALL_STMT_REAL(stmt, mysql_stmt_close);

  if (NULL != real_mysql && 0 != errcode)
  {
    if (0 != errcode)
    {
      // Set mysql error
      real_mysql->net.last_errno = errcode;
    }

    TBSYS_LOG(DEBUG, "after stmt close: mysql=%p, errno=%d, sqlstate=%s, error=%s",
        real_mysql, CALL_REAL(mysql_errno, real_mysql),
        CALL_REAL(mysql_sqlstate, real_mysql), CALL_REAL(mysql_error, real_mysql));
  }

  return ret;
}

my_bool STDCALL MYSQL_FUNC(mysql_stmt_reset)(MYSQL_STMT * stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_reset);
}

my_bool STDCALL MYSQL_FUNC(mysql_stmt_free_result)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_free_result);
}

my_bool STDCALL MYSQL_FUNC(mysql_stmt_send_long_data)(MYSQL_STMT *stmt, unsigned int param_number, const char *data, unsigned long length)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_send_long_data, param_number, data, length);
}

MYSQL_RES *STDCALL MYSQL_FUNC(mysql_stmt_result_metadata)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_result_metadata);
}

MYSQL_RES *STDCALL MYSQL_FUNC(mysql_stmt_param_metadata)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_param_metadata);
}

unsigned int STDCALL MYSQL_FUNC(mysql_stmt_errno)(MYSQL_STMT * stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_errno);
}

const char *STDCALL MYSQL_FUNC(mysql_stmt_error)(MYSQL_STMT * stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_error);
}

const char *STDCALL MYSQL_FUNC(mysql_stmt_sqlstate)(MYSQL_STMT * stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_sqlstate);
}

MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_stmt_row_seek)(MYSQL_STMT *stmt, MYSQL_ROW_OFFSET offset)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_row_seek, offset);
}

MYSQL_ROW_OFFSET STDCALL MYSQL_FUNC(mysql_stmt_row_tell)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_row_tell);
}

void STDCALL MYSQL_FUNC(mysql_stmt_data_seek)(MYSQL_STMT *stmt, my_ulonglong offset)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_data_seek, offset);
}

my_ulonglong STDCALL MYSQL_FUNC(mysql_stmt_num_rows)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_num_rows);
}

my_ulonglong STDCALL MYSQL_FUNC(mysql_stmt_affected_rows)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_affected_rows);
}

my_ulonglong STDCALL MYSQL_FUNC(mysql_stmt_insert_id)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_insert_id);
}

unsigned int STDCALL MYSQL_FUNC(mysql_stmt_field_count)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_field_count);
}

my_bool STDCALL MYSQL_FUNC(mysql_commit)(MYSQL_TYPE * mysql)
{
  return (my_bool)MYSQL_FUNC(mysql_real_query)(mysql, "commit", 6);
}

my_bool STDCALL MYSQL_FUNC(mysql_rollback)(MYSQL_TYPE * mysql)
{
  return (my_bool)MYSQL_FUNC(mysql_real_query)(mysql, "rollback", 8);
}

my_bool STDCALL MYSQL_FUNC(mysql_autocommit)(MYSQL_TYPE * mysql, my_bool auto_mode)
{
  return (my_bool)MYSQL_FUNC(mysql_real_query)(mysql, auto_mode ?
                          "set autocommit=1":"set autocommit=0",
                          16);
}

my_bool STDCALL MYSQL_FUNC(mysql_more_results)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 0, mysql_more_results);
}

int STDCALL MYSQL_FUNC(mysql_next_result)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, 2006, mysql_next_result);
}

//give back to pool
void STDCALL MYSQL_FUNC(mysql_close)(MYSQL_TYPE *mysql)
{
  if (NULL != mysql && OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
  {
    ObSQLMySQL *ob_mysql = (ObSQLMySQL *)mysql;

    TBSYS_LOG(DEBUG, "mysql close: mysql=%p, mysql->conn_=%p, real mysql=%p, errno=%d, error=%s",
        ob_mysql, NULL == ob_mysql ? NULL : ob_mysql->conn_,
        NULL == ob_mysql ? NULL : (NULL == ob_mysql->conn_ ? NULL : ob_mysql->conn_->mysql_),
        MYSQL_FUNC(mysql_errno)(mysql), MYSQL_FUNC(mysql_error)(mysql));

    clear_connection_(ob_mysql);

    if (1 == ob_mysql->alloc_)
    {
      (void)memset(ob_mysql, 0, sizeof(MYSQL_TYPE));
      ob_free(ob_mysql);
      ob_mysql = NULL;
      mysql = NULL;
    }
  }
  else
  {
    (*g_func_set.real_mysql_close)((MYSQL *)mysql);
  }
}

const char * MYSQL_FUNC(mysql_get_ssl_cipher)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_get_ssl_cipher);
}

void MYSQL_FUNC(mysql_set_local_infile_default)(MYSQL_TYPE *mysql)
{
  CALL_MYSQL_REAL_WITH_JUDGE_NO_RETVAL(mysql, mysql_set_local_infile_default);
}

void MYSQL_FUNC(mysql_set_local_infile_handler)(MYSQL_TYPE *mysql, int (*local_infile_init)(void **, const char *, void *), int (*local_infile_read)(void *, char *, unsigned int), void (*local_infile_end)(void *), int (*local_infile_error)(void *, char*, unsigned int), void *userdata)
{
  CALL_MYSQL_REAL_WITH_JUDGE_NO_RETVAL(mysql, mysql_set_local_infile_handler, local_infile_init, local_infile_read, local_infile_end, local_infile_error, userdata);
}

int STDCALL MYSQL_FUNC(mysql_stmt_next_result)(MYSQL_STMT *stmt)
{
  return CALL_STMT_REAL(stmt, mysql_stmt_next_result);
}

struct st_mysql_client_plugin * MYSQL_FUNC(mysql_client_find_plugin)(MYSQL_TYPE *mysql, const char *name, int type)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_client_find_plugin, name, type);
}

struct st_mysql_client_plugin * MYSQL_FUNC(mysql_client_register_plugin)(MYSQL_TYPE *mysql, struct st_mysql_client_plugin *plugin)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_client_register_plugin, plugin);
}

struct st_mysql_client_plugin * MYSQL_FUNC(mysql_load_plugin)(MYSQL_TYPE *mysql, const char *name, int type, int argc, ...)
{
  struct st_mysql_client_plugin *p;
  va_list args;
  va_start(args, argc);
  p = MYSQL_FUNC(mysql_load_plugin_v)(mysql, name, type, argc, args);
  va_end(args);
  return p;
}

struct st_mysql_client_plugin * MYSQL_FUNC(mysql_load_plugin_v)(MYSQL_TYPE *mysql, const char *name, int type, int argc, va_list args)
{
  CALL_MYSQL_REAL_WITH_JUDGE(mysql, NULL, mysql_load_plugin_v, name, type, argc, args);
}
int MYSQL_FUNC(mysql_plugin_options)(struct st_mysql_client_plugin *plugin, const char *option, const void *value)
{
  return CALL_REAL(mysql_plugin_options, plugin, option, value);
}
