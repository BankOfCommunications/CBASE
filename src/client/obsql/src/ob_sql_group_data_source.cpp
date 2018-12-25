#include "ob_sql_group_data_source.h"
#include "ob_sql_list.h"
#include "ob_sql_global.h"
#include "ob_sql_util.h"
#include "ob_sql_data_source.h"
#include "ob_sql_data_source_utility.h"
#include "ob_sql_conn_acquire.h"
#include "ob_sql_list.h"
#include "stddef.h"
#include "tblog.h"
#include "common/ob_malloc.h"
#include "ob_sql_cluster_config.h"      // delete_cluster_from_config

using namespace oceanbase::common;

/* 从集群池pool中移除一个集群spool */
/// Delete cluster from ObGroupDataSource.
/// @param gds target ObGroupDataSource
/// @param cindex target cluster index, means gds->clusters_[cindex]
static void delete_cluster_(ObGroupDataSource *gds, const int32_t cindex)
{
  if (NULL != gds && 0 <= cindex && cindex < gds->size_ && NULL != gds->clusters_[cindex])
  {
    ObClusterInfo *cluster = gds->clusters_[cindex];

    TBSYS_LOG(INFO, "delete cluster: ID=%d, LMS=%s, MS NUM=%d, cluster number=%d",
        cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_, gds->size_);

    // Recycle all data source
    for (int32_t index = 0; index < cluster->size_; ++index)
    {
      ObDataSource *ds = cluster->dslist_[index];
      if (NULL != ds)
      {
        // Delete un-used MS from black list
        g_ms_black_list.del(ds->server_.ip_);

        recycle_data_source(ds);
        cluster->dslist_[index] = NULL;
      }
      else
      {
        TBSYS_LOG(WARN, "cluster %p has NULL ObDataSource at index=%d: cluster=[id=%d,addr=%s], MS NUM=%d",
            cluster, index, cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_);
      }
    }

    // Clear cluster memory
    (void)memset(cluster, 0, sizeof(ObClusterInfo));

    // Free cluster memory
    ob_free(cluster);
    cluster = NULL;
    gds->clusters_[cindex] = NULL;

    // Update clusters
    for (int32_t index = cindex + 1; index < gds->size_; ++index)
    {
      gds->clusters_[index - 1] = gds->clusters_[index];
    }
    gds->clusters_[gds->size_ - 1] = NULL;

    gds->size_--;
  }
}

static void delete_ms_from_config_(ObSQLClusterConfig &config, const int32_t index)
{
  if (1 <= config.server_num_ && 0 <= index && index < config.server_num_)
  {
    TBSYS_LOG(INFO, "delete MS %s from ObSQLClusterConfig, MS NUM=%d",
        get_server_str(&config.merge_server_[index]), config.server_num_);

    for (int i = index + 1; i < config.server_num_; i++)
    {
      config.merge_server_[i - 1] = config.merge_server_[i];
    }

    config.server_num_--;

    TBSYS_LOG(DEBUG, "after delete MS %s from ObSQLClusterConfig, MS NUM=%d",
        get_server_str(&config.merge_server_[index]), config.server_num_);
  }
}

/* 初始化cluster info */
static int add_cluster_(ObSQLClusterConfig *cluster_conf, ObGroupDataSource *gds)
{
  OB_ASSERT(NULL != cluster_conf && NULL != gds);

  int ret = OB_SQL_SUCCESS;

  if (OB_SQL_MAX_CLUSTER_NUM <= gds->size_)
  {
    TBSYS_LOG(ERROR, "can not add new cluster, current cluster number=%d, max cluster number=%d",
        gds->size_, OB_SQL_MAX_CLUSTER_NUM);
    ret = OB_SQL_ERROR;
  }
  else
  {
    TBSYS_LOG(INFO, "add cluster: ID=%d, LMS=%s, MS NUM=%d, TYPE=%d, FLOW=%d, RDST=%d",
        cluster_conf->cluster_id_, get_server_str(&cluster_conf->server_),
        cluster_conf->server_num_, cluster_conf->cluster_type_, cluster_conf->flow_weight_,
        cluster_conf->read_strategy_);

    ObClusterInfo * cluster = static_cast<ObClusterInfo *>(ob_malloc(sizeof(ObClusterInfo), ObModIds::LIB_OBSQL));
    if (NULL == cluster)
    {
      TBSYS_LOG(ERROR, "alloc memory for ObClusterInfo fail");
      ret = OB_SQL_ERROR;
    }
    else
    {
      (void)memset(cluster, 0, sizeof(ObClusterInfo));

      cluster->size_ = 0;
      cluster->cluster_addr_ = cluster_conf->server_;
      cluster->cluster_id_ = cluster_conf->cluster_id_;
      cluster->cluster_type_ = cluster_conf->cluster_type_;
      cluster->read_strategy_ = cluster_conf->read_strategy_;
      cluster->flow_weight_ = cluster_conf->flow_weight_;

      // Initialize data sources
      for (int index = 0; index < cluster_conf->server_num_; ++index)
      {
        if (OB_SQL_SUCCESS != add_data_source(g_config_using->max_conn_size_, cluster_conf->merge_server_[index], cluster))
        {
          TBSYS_LOG(ERROR, "init ObDataSource %s for cluster %d failed",
              get_server_str(cluster_conf->merge_server_ + index), cluster_conf->cluster_id_);

          delete_ms_from_config_(*cluster_conf, index);
          index--;
        }
      }

      if (0 < cluster->size_)
      {
        gds->clusters_[gds->size_++] = cluster;

        ret = OB_SQL_SUCCESS;
        TBSYS_LOG(INFO, "add cluster success: ID=%d, LMS=%s, MS NUM=%d, cluster number=%d",
            cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_, gds->size_);
      }
      else
      {
        // NOTE: No available MS data source. Need not free data source of cluster
        // Free cluster directly
        (void)memset(cluster, 0, sizeof(ObClusterInfo));
        ob_free(cluster);
        cluster = NULL;
        ret = OB_SQL_ERROR;
      }
    }

    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "add cluster fail, ID=%d, LMS=%s",
          cluster_conf->cluster_id_, get_server_str(&cluster_conf->server_));
    }
  }

  return ret;
}

/// Update specific cluster information and MS connection pool based on configuration
/// @param[in/out] config: cluster configuration, it may be changed as some MS may be deleted when not connected
/// @param[in/out] cluster: specific cluster, its MS list will be refreshed
/// @retval OB_SQL_SUCCESS when cluster information is updated successfully and has valid connections in MS pool
/// @retval ! OB_SQL_SUCCESS when there are no valid connections in MS pool or other error occur
static int update_cluster_(ObSQLClusterConfig &config, ObClusterInfo *cluster)
{
  OB_ASSERT(config.cluster_id_ == cluster->cluster_id_);

  int ret = OB_SQL_SUCCESS;
  int32_t index = 0;
  int32_t sindex = 0;
  int32_t exist = 0;
  ObDataSource *sds = NULL;
  ObServerInfo server;

  TBSYS_LOG(INFO, "update_cluster: ID=%u, LMS=%s, MS NUM=(old=%d,new=%d), FLOW={old=%d,new=%d}, RDST={old=%d,new=%d}",
      config.cluster_id_, get_server_str(&config.server_), config.server_num_, cluster->size_,
      cluster->flow_weight_, config.flow_weight_, cluster->read_strategy_, config.read_strategy_);

  // Update information
  cluster->flow_weight_  = config.flow_weight_;    // NOTE: flow_weight must be updated
  cluster->read_strategy_ = config.read_strategy_;
  cluster->cluster_addr_ = config.server_;
  cluster->cluster_type_ = config.cluster_type_;

  for (sindex = 0; sindex < cluster->size_; ++sindex)
  {
    sds = cluster->dslist_[sindex];
    if (NULL == sds)
    {
      TBSYS_LOG(WARN, "cluster %p has NULL ObDataSource at index=%d: cluster=[id=%d,addr=%s], MS NUM=%d, delete it",
          cluster, sindex, cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_);

      // Delete invalid ds
      for (int i = sindex + 1; i < cluster->size_; i++)
      {
        cluster->dslist_[i - 1] = cluster->dslist_[i];
      }

      cluster->size_--;
      sindex--;
      continue;
    }

    exist = 0;
    for (index = 0; index < config.server_num_; ++index)
    {
      server = config.merge_server_[index];
      if (sds->server_.ip_ == server.ip_ && sds->server_.port_ == server.port_)
      {
        TBSYS_LOG(DEBUG, "update_cluster: MS[%d]=%s, config.server_num_=%d, cluster->size_=%d",
            sindex, get_server_str(&sds->server_), config.server_num_, cluster->size_);
        exist = 1;
        break;
      }
    }

    if (0 == exist)
    {
      delete_data_source(cluster, sindex);
      sindex--;
    }
    else
    {
      if (OB_SQL_SUCCESS != update_data_source(*sds))
      {
        TBSYS_LOG(WARN, "update data source fail, MS=%s, cluster={id=%d,addr=%s}, delete MS from cluster and config",
            get_server_str(&sds->server_), cluster->cluster_id_, get_server_str(&cluster->cluster_addr_));

        delete_data_source(cluster, sindex);
        delete_ms_from_config_(config, index);
        sindex--;
      }
    }
  }

  for (index = 0; index < config.server_num_; ++index)
  {
    exist = 0;
    server = config.merge_server_[index];
    for(sindex = 0; sindex < cluster->size_; ++sindex)
    {
      sds = cluster->dslist_[sindex];
      if (sds->server_.ip_ == server.ip_ && sds->server_.port_ == server.port_)
      {
        exist = 1;
        break;
      }
    }
    if (0 == exist)
    {
      if (OB_SQL_SUCCESS != add_data_source(g_config_using->max_conn_size_, server, cluster))
      {
        TBSYS_LOG(INFO, "add_data_source fail, MS=%s, cluster={id=%d,addr=%s}, delete it from configuration",
            get_server_str(&server), cluster->cluster_id_, get_server_str(&cluster->cluster_addr_));

        delete_ms_from_config_(config, index);
        index--;
      }
    }
  }

  // Check MS number
  if (0 >= config.server_num_)
  {
    TBSYS_LOG(INFO, "cluster {id=%d,addr=%s} has no valid MS after updating",
        config.cluster_id_, get_server_str(&config.server_));

    ret = OB_SQL_ERROR;
  }
  else if (config.server_num_ != cluster->size_)
  {
    TBSYS_LOG(ERROR, "cluster {id=%d,addr=%s} ObSQLClusterConfig::server_num_(%d) != ObClusterInfo::size_(%d)",
        config.cluster_id_, get_server_str(&config.server_), config.server_num_, cluster->size_);

    ret = OB_SQL_ERROR;
  }
  else
  {
    ret = OB_SQL_SUCCESS;

    TBSYS_LOG(INFO, "update cluster {id=%d,addr=%s} success. MS NUM=%d",
        config.cluster_id_, get_server_str(&config.server_), config.server_num_);
  }

  return ret;
}

/* 根据config的配置来更新全局连接池 */
int update_group_ds(ObSQLGlobalConfig *config, ObGroupDataSource *gds)
{
  int ret = OB_SQL_SUCCESS;
  int32_t cindex = 0;
  int32_t index = 0;
  int32_t exist = 0;
  ObClusterInfo * scluster = NULL;
  for (index = 0; index < gds->size_; ++index)
  {
    scluster = gds->clusters_[index];
    if (NULL == scluster)
    {
      TBSYS_LOG(WARN, "gds %p has NULL cluster at index=%d, cluster number=%d, delete it", gds, index, gds->size_);

      // Delete NULL cluster
      for (int i = index + 1; i < gds->size_; i++)
      {
        gds->clusters_[i - 1] = gds->clusters_[i];
      }

      gds->size_--;
      index--;
      continue;
    }

    exist = 0;
    for (cindex = 0; cindex < config->cluster_size_; ++cindex)
    {
      if (scluster->cluster_id_ == config->clusters_[cindex].cluster_id_)
      {
        exist = 1;
        break;
      }
    }

    if (0 == exist)
    {
      delete_cluster_(gds, index);
      index--;
    }
    else
    {
      if (OB_SQL_SUCCESS != update_cluster_(config->clusters_[cindex], scluster))
      {
        TBSYS_LOG(WARN, "delete fail-updated cluster(id=%d,addr=%s) from group data source and configuration",
            scluster->cluster_id_, get_server_str(&scluster->cluster_addr_));

        delete_cluster_(gds, index);
        delete_cluster_from_config(*config, cindex);

        index--;
      }
    }
  }

  for (cindex = 0; cindex < config->cluster_size_; ++cindex)
  {
    exist = 0;
    for (index = 0; index < gds->size_; ++index)
    {
      ObClusterInfo *scluster = gds->clusters_[index];
      if (scluster->cluster_id_ == config->clusters_[cindex].cluster_id_)
      {
        exist = 1;
        break;
      }
    }

    if (0 == exist)
    {
      // Add new cluster
      if (OB_SQL_SUCCESS != add_cluster_(config->clusters_ + cindex, gds))
      {
        TBSYS_LOG(WARN, "delete fail-add cluster (id=%d,addr=%s) from configuration",
            config->clusters_[cindex].cluster_id_, get_server_str(&config->clusters_[cindex].server_));

        delete_cluster_from_config(*config, cindex);
        cindex--;
      }
    }
  }

  if (config->cluster_size_ != gds->size_)
  {
    TBSYS_LOG(ERROR, "after update group data source: config cluster size (%d) != group data source cluster size (%d)",
        config->cluster_size_, gds->size_);

    ret = OB_SQL_ERROR;
  }
  else
  {
    // TODO: Consider about other error situations
    ret = OB_SQL_SUCCESS;
  }

  return ret;
}

void destroy_group_ds(ObGroupDataSource *gds)
{
  if (NULL != gds)
  {
    TBSYS_LOG(INFO, "destroy group datasource: %p", gds);

    for (int32_t cindex = 0; cindex < gds->size_; ++cindex)
    {
      ObClusterInfo *cluster = gds->clusters_[cindex];

      if (NULL != cluster)
      {
        TBSYS_LOG(INFO, "destroy cluster %p: ID=%d, LMS=%s, MS NUM=%d, cluster number=%d",
            cluster, cluster->cluster_id_, get_server_str(&cluster->cluster_addr_), cluster->size_, gds->size_);

        for (int32_t index = 0; index < cluster->size_; ++index)
        {
          ObDataSource *ds = cluster->dslist_[index];
          if (NULL != ds)
          {
            // Delete un-used MS from black list
            g_ms_black_list.del(ds->server_.ip_);

            destroy_data_source(ds);
            cluster->dslist_[index] = NULL;
          }
        }

        // Clear cluster memory
        (void)memset(cluster, 0, sizeof(ObClusterInfo));

        // Free cluster memory
        ob_free(cluster);
        cluster = NULL;
        gds->clusters_[cindex] = NULL;
      }
    }

    (void)memset(gds, 0, sizeof(ObGroupDataSource));
  }
}

ObClusterInfo* get_master_cluster(ObGroupDataSource *gds)
{
  ObClusterInfo *cluster = NULL;
  for (int32_t index = 0; index < gds->size_; ++index)
  {
    if (MASTER_CLUSTER == gds->clusters_[index]->cluster_type_)
    {
      cluster = gds->clusters_[index];
      break;
    }
  }
  return cluster;
}
