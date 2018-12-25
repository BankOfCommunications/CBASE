#include "ob_sql_mysql_adapter.h"
#include "ob_sql_cluster_select.h"
#include <algorithm>
#include <stdlib.h>
#include "ob_sql_atomic.h"
#include "ob_sql_global.h"
#include "ob_sql_util.h"
#include "common/ob_atomic.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;

//SelectTable global_select_table;
//更新配置中的flow_weight
static int update_flow_weight(ObGroupDataSource *gds)
{
  int ret = OB_SQL_SUCCESS;
  int slot = 0;
  int used = 0;
  int index = 0;
  int16_t weight = 0;
  if (NULL == gds)
  {
    TBSYS_LOG(ERROR, "invalid argument gds is %p", gds);
    ret = OB_SQL_ERROR;
  }
  else
  {
    slot = 0;
    for (index = 0; index < gds->size_; ++index)
    {
      slot += gds->clusters_[index]->flow_weight_;
    }
    TBSYS_LOG(INFO, "update flow weight: flow weight sum is %d", slot);
    for (index = 0; index < gds->size_; ++index)
    {
      if (index < gds->size_ - 1)
      {
        if (0 == slot)
        {
          weight = static_cast<int16_t>(OB_SQL_SLOT_NUM/(gds->size_));
        }
        else
        {
          weight = gds->clusters_[index]->flow_weight_;
          weight = static_cast<int16_t>((weight * OB_SQL_SLOT_NUM) /slot);
        }
        used += weight;
      }
      else
      {
        weight = static_cast<int16_t>(OB_SQL_SLOT_NUM - used);
      }

      gds->clusters_[index]->flow_weight_ = weight;

      TBSYS_LOG(INFO, "cluster(id=%u,addr=%s) weight is %d",
          gds->clusters_[index]->cluster_id_,
          get_server_str(&gds->clusters_[index]->cluster_addr_), weight);
    }
  }
  return ret;
}

/* 根据全局配置来初始化集群选择对照表 加写锁 */
int update_select_table(ObGroupDataSource *gds)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == gds)
  {
    TBSYS_LOG(ERROR, "invalid argument gds is %p", gds);
    ret = OB_SQL_ERROR;
  }
  else
  {
    ret = update_flow_weight(gds);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update_flow_weigth failed");
    }
    else
    {
      if (NULL == g_table)
      {
        g_table = (ObSQLSelectTable *)ob_malloc(sizeof(ObSQLSelectTable), ObModIds::LIB_OBSQL);
        if (NULL == g_table)
        {
          TBSYS_LOG(ERROR, "alloc mem for ObSQLSelectTable failed");
          ret = OB_SQL_ERROR;
        }
        else
        {
          g_table->master_count_ = 0;
          TBSYS_LOG(DEBUG, "g_table->master_count_ is %u", g_table->master_count_);
        }
      }

      if (OB_SQL_SUCCESS == ret)
      {
        // Reset cluster select table data
        g_table->reset_data();

        ObClusterInfo *master_cluster = get_master_cluster(gds);
        g_table->master_cluster_ = master_cluster;

        TBSYS_LOG(INFO, "update cluster select table: get master cluster=%p, id=%d, addr=%s",
            master_cluster,
            NULL == master_cluster ? -1 : master_cluster->cluster_id_,
            NULL == master_cluster ? "NULL" : get_server_str(&master_cluster->cluster_addr_));

        // 按照流量分配，将cluster顺序地放在select table中，之后再随机shuffle
        int slot_num = sizeof(g_table->clusters_) / sizeof(g_table->clusters_[0]);
        for (int cindex = 0, sindex = 0; cindex < gds->size_ && sindex < slot_num; ++cindex)
        {
          int flow_weight = gds->clusters_[cindex]->flow_weight_;

          for (int csindex = 0; csindex < flow_weight && sindex < slot_num; ++csindex, ++sindex)
          {
            g_table->clusters_[sindex] = gds->clusters_[cindex];
          }
        }

        //shuffle global_table->data
        ObClusterInfo **start = g_table->clusters_;
        ObClusterInfo **end = g_table->clusters_ + slot_num;
        std::random_shuffle(start, end);
      }
    }
  }
  return ret;
}

void destroy_select_table()
{
  TBSYS_LOG(INFO, "destroy select table %p", g_table);

  if (NULL != g_table)
  {
    ob_free(g_table);
    g_table = NULL;
  }
}

ObClusterInfo* select_cluster(ObSQLMySQL* mysql)
{
  ObClusterInfo *cluster = NULL;

  if (NULL == g_table)
  {
    TBSYS_LOG(ERROR, "cluster select table is NULL, not inited");
  }
  else if (NULL == mysql)
  {
    TBSYS_LOG(ERROR, "invalid argument: mysql=%p", mysql);
  }
  else if (0 > g_group_ds.size_)
  {
    TBSYS_LOG(WARN, "no valid cluster, cluster size=%d", g_group_ds.size_);
  }
  else if (is_consistence(mysql))
  {
    //一致性请求
    TBSYS_LOG(DEBUG, "consistence g_table->master_count_ is %u", g_table->master_count_);
    cluster = g_table->master_cluster_;
    TBSYS_LOG(DEBUG, "consistence cluster=%p, id = %u flow weight is %d ",
        cluster,
        NULL == cluster ? -1 : cluster->cluster_id_,
        NULL == cluster ? -1 : cluster->flow_weight_);

    atomic_inc(&g_table->master_count_);
  }
  else if (1 == g_group_ds.size_)
  {
    // 如果当前仅有一个集群，直接从g_group_ds中获取集群信息
    cluster = g_group_ds.clusters_[0];
  }
  else // g_group_ds.cluster_size_ > 1
  {
    while (1)
    {
      int64_t cluster_index = atomic_inc(&g_table->cluster_index_) % OB_SQL_SLOT_NUM;

      cluster = g_table->clusters_[cluster_index];

      TBSYS_LOG(DEBUG, "select g_table->master_count_ is %u, index=%ld", g_table->master_count_, cluster_index);

      if (NULL == cluster)
      {
        TBSYS_LOG(ERROR, "cluster in select table (g_table) is NULL, index=%ld", cluster_index);
        break;
      }
      else if (MASTER_CLUSTER == cluster->cluster_type_)
      {
        if (atomic_dec_positive(&g_table->master_count_) < 0)
        {
          break;
        }
      }
      else
      {
        break;
      }
    }

    if (NULL == cluster)
    {
      // 如果从g_table中获取失败，则直接从g_group_ds中获取有效的集群信息
      if (0 < g_group_ds.size_ && (NULL != (cluster = g_group_ds.clusters_[0])))
      {
        TBSYS_LOG(WARN, "select cluster from g_table (%p) fail, use first cluster %p in g_group_ds %p instead, size=%d",
            g_table, cluster, &g_group_ds, g_group_ds.size_);
      }
    }
  }

  if (NULL != cluster)
  {
    TBSYS_LOG(DEBUG, "select cluster: ID=%u FLOW=%d, CSIZE=%d, LMS=[%s], TYPE=%d, RDST=%d",
        cluster->cluster_id_, cluster->flow_weight_, cluster->size_,
        get_server_str(&cluster->cluster_addr_), cluster->cluster_type_, cluster->read_strategy_);
  }
  else
  {
    TBSYS_LOG(WARN, "select cluster fail, cluster size=%d", g_group_ds.size_);
  }

  return cluster;
}
