#include "ob_sql_util.h"
#include <stdio.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>
#include "tblog.h"
#include "ob_sql_global.h"

void dump_config(ObSQLGlobalConfig *config, const char *format_str)
{
  if (NULL != config)
  {
    OB_SQL_LOG(INFO, "============ DUMP %s CONFIG BEGIN ==============", format_str);
    OB_SQL_LOG(INFO, "cluster size is %d", config->cluster_size_);
    int cindex = 0;
    int sindex = 0;
    ObSQLClusterConfig *cconfig;
    ObServerInfo server;
    for (cindex = 0; cindex < config->cluster_size_; ++cindex)
    {
      cconfig = config->clusters_+cindex;
      OB_SQL_LOG(INFO, "cluster[%u]=[%s], TYPE=%d FLOW=%d, MS NUM=%d",
          cconfig->cluster_id_, get_server_str(&cconfig->server_), cconfig->cluster_type_,
          cconfig->flow_weight_, cconfig->server_num_);
      for (sindex = 0; sindex < cconfig->server_num_; ++sindex)
      {
        server = cconfig->merge_server_[sindex];
        OB_SQL_LOG(INFO, "MS[%d][%d]=%s", cconfig->cluster_id_, sindex, get_server_str(&server));
      }
#ifdef ENABLE_SELECT_MS_TABLE
      ObSQLSelectMsTable *table = cconfig->table_;
      if (NULL != table)
      {
        for (int tindex = 0; tindex < table->slot_num_; ++tindex)
        {
          OB_SQL_LOG(TRACE, "ds is %p, hashval is %u", (table->items_+tindex)->server_, (table->items_+tindex)->hashvalue_);
        }
      }
#endif
    }
    OB_SQL_LOG(INFO, "============ DUMP %s CONFIG END ===============", format_str);
  }
}

void dump_table()
{
  if (NULL != g_table)
  {
    OB_SQL_LOG(DEBUG, "============ DUMP SELECT TABLE BEGIN ===============");
    for (int idx = 0; idx < OB_SQL_SLOT_NUM; idx++)
    {
      ObClusterInfo *cluster = g_table->clusters_[idx];

      TBSYS_LOG(DEBUG, "g_table[%d]=cluster(id=%d,addr=%s)",
          idx, NULL == cluster ? -1 : cluster->cluster_id_,
          NULL == cluster ? "NULL" : get_server_str(&cluster->cluster_addr_));
    }
    OB_SQL_LOG(DEBUG, "============ DUMP SELECT TABLE END ===============");
  }
}

int get_server_ip(ObServerInfo *server, char *buffer, int32_t size)
{
  int ret = OB_SQL_ERROR;
  if (NULL != buffer && size > 0)
  {
    // ip.v4_ is network byte order
    snprintf(buffer, size, "%d.%d.%d.%d%c",
             (server->ip_ & 0xFF),
             (server->ip_ >> 8) & 0xFF,
             (server->ip_ >> 16) & 0xFF,
             (server->ip_ >> 24) & 0xFF,
             '\0');
    ret = OB_SQL_SUCCESS;
  }
  return ret;
}

uint32_t trans_ip_to_int(const char* ip)
{
  if (NULL == ip) return 0;
  uint32_t x = inet_addr(ip);
  if (x == INADDR_NONE)
  {
    struct hostent *hp = NULL;
    if ((hp = gethostbyname(ip)) == NULL)
    {
      return 0;
    }
    x = ((struct in_addr *)hp->h_addr)->s_addr;
  }
  return x;
}

const char* get_server_str(ObServerInfo *server)
{
  static __thread char buff[OB_SQL_BUFFER_NUM][OB_SQL_IP_BUFFER_SIZE];
  static int64_t i = 0;
  i++;
  memset(buff[i % OB_SQL_BUFFER_NUM], 0, OB_SQL_IP_BUFFER_SIZE);
  trans_int_to_ip(server, buff[i % OB_SQL_BUFFER_NUM], OB_SQL_IP_BUFFER_SIZE);
  return buff[ i % OB_SQL_BUFFER_NUM];
}

void trans_int_to_ip(ObServerInfo *server, char *buffer, int32_t size)
{
  if (NULL != buffer && size > 0)
  {
    if (server->port_ > 0) {
      snprintf(buffer, size, "%d.%d.%d.%d:%d",
               (server->ip_ & 0xFF),
               (server->ip_ >> 8) & 0xFF,
               (server->ip_ >> 16) & 0xFF,
               (server->ip_ >> 24) & 0xFF,
               server->port_);
    }
    else
    {
      snprintf(buffer, size, "%d.%d.%d.%d",
               (server->ip_ & 0xFF),
               (server->ip_ >> 8) & 0xFF,
               (server->ip_ >> 16) & 0xFF,
               (server->ip_ >> 24) & 0xFF);
    }
  }
}

const char* get_ip(ObServerInfo *server)
{
  static __thread char buff[OB_SQL_BUFFER_NUM][OB_SQL_IP_BUFFER_SIZE];
  static int64_t i = 0;
  i++;
  memset(buff[i % OB_SQL_BUFFER_NUM], 0, OB_SQL_IP_BUFFER_SIZE);
  snprintf(buff[i % OB_SQL_BUFFER_NUM], OB_SQL_IP_BUFFER_SIZE,
           "%d.%d.%d.%d",
           (server->ip_ & 0xFF),
           (server->ip_ >> 8) & 0xFF,
           (server->ip_ >> 16) & 0xFF,
           (server->ip_ >> 24) & 0xFF);

  return buff[ i % OB_SQL_BUFFER_NUM];
}

int set_mysql_options(MYSQL *mysql)
{
  int ret = OB_SQL_SUCCESS;
  int64_t connect_timeout = OB_SQL_OPT_CONNECT_TIMEOUT;
  int64_t read_timeout = OB_SQL_OPT_READ_TIMEOUT;
  int64_t write_timeout = OB_SQL_OPT_WRITE_TIMEOUT;

  TBSYS_LOG(DEBUG, "set_mysql_options: CONNECT_TIMEOUT=%ld  READ_TIMEOUT=%ld  WRITE_TIMEOUT=%ld",
      connect_timeout, read_timeout, write_timeout);

  if (NULL == mysql || NULL == g_func_set.real_mysql_options)
  {
    TBSYS_LOG(ERROR, "invalid argument: mysql=%p, g_func_set.real_mysql_options=%p",
        mysql, g_func_set.real_mysql_options);
    ret = OB_SQL_INVALID_ARGUMENT;
  }
  else if (0 != (ret = g_func_set.real_mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout)))
  {
    TBSYS_LOG(WARN, "mysql_options(MYSQL_OPT_CONNECT_TIMEOUT) fail, ret=%d, mysql=%p, timeout=%ld",
        ret, mysql, connect_timeout);
  }
  else if (0 != (ret = g_func_set.real_mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &read_timeout)))
  {
    TBSYS_LOG(WARN, "mysql_options(MYSQL_OPT_READ_TIMEOUT) fail, ret=%d, mysql=%p, timeout=%ld",
        ret, mysql, read_timeout);
  }
  else if (0 != (ret = g_func_set.real_mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &write_timeout)))
  {
    TBSYS_LOG(WARN, "mysql_options(MYSQL_OPT_WRITE_TIMEOUT) fail, ret=%d, mysql=%p, timeout=%ld",
        ret, mysql, write_timeout);
  }

  return ret;
}
