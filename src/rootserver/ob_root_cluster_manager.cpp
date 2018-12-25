#include "ob_root_cluster_manager.h"
#include <tbsys.h>

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootClusterManager::ObRootClusterManager():
  //mod bingo [Paxos Cluster.Balance] 20161021
  master_cluster_id_(1),
  //mod:e
  schema_service_(NULL)
{
}

ObRootClusterManager::~ObRootClusterManager()
{
}

void ObRootClusterManager::init_cluster_tablet_replicas_num(const bool* is_cluster_alive)
{
  OB_ASSERT(NULL != is_cluster_alive);
  int32_t cluster_count = 0;
  for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
  {
    if(is_cluster_alive[cluster_id])
    {
      replicas_num_for_each_cluster_[cluster_id] = OB_SAFE_COPY_COUNT;
      cluster_count++;
    }
    else
    {
      //unused cluster
      replicas_num_for_each_cluster_[cluster_id] = 0;
    }
  }
  if(cluster_count <= 2)
  {
    //use default value
  }
  else
  {
    int32_t each_cluster_replicas_num = OB_MAX_COPY_COUNT / cluster_count;
    if(each_cluster_replicas_num == 0)
    {
      each_cluster_replicas_num = 1; //replicas_num >= 1
    }
    for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
    {
      if(is_cluster_alive[cluster_id])
      {
        replicas_num_for_each_cluster_[cluster_id] = each_cluster_replicas_num;
      }
    }
    int32_t tmp_total = each_cluster_replicas_num * cluster_count;
    if(tmp_total < OB_MAX_COPY_COUNT)
    {
      OB_ASSERT(master_cluster_id_ > 0);
      //add to master cluster
      replicas_num_for_each_cluster_[master_cluster_id_] = OB_MAX_COPY_COUNT - tmp_total + each_cluster_replicas_num;
    }
  }
}

int ObRootClusterManager::set_replicas_num(const int32_t *replicas_num)
{
  int ret = OB_SUCCESS;
  if(NULL == replicas_num)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Invalid arguments");
  }
  else
  {
    int32_t total = 0;
    for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
    {
      total += replicas_num[cluster_id];
    }
    if(total == 0 || total > OB_MAX_COPY_COUNT)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "total replica num is large than OB_MAX_COPY_COUNT, total=%d", total);
    }
    else
    {
      for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
      {
        replicas_num_for_each_cluster_[cluster_id] = replicas_num[cluster_id];
      }
    }
  }
  return ret;
}


int32_t ObRootClusterManager::get_cluster_tablet_replicas_num(const int32_t cluster_id) const
{
  if(cluster_id > 0 && cluster_id <= OB_MAX_CLUSTER_ID)
  {
    return replicas_num_for_each_cluster_[cluster_id];
  }
  return -1;
}

int32_t ObRootClusterManager::get_total_tablet_replicas_num() const
{
  int32_t total_num = 0;
  for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
  {
    if(replicas_num_for_each_cluster_[cluster_id] > 0)
    {
      total_num += replicas_num_for_each_cluster_[cluster_id];
    }
  }
  return total_num;
}

//replicas_num[OB_CLUSTER_ARRAY_LEN]
int ObRootClusterManager::get_cluster_tablet_replicas_num(int32_t *replicas_num,
                                                        const int32_t total_replicas_num)
{
  int ret = OB_SUCCESS;
  int32_t total_num = get_total_tablet_replicas_num();
  if(NULL == replicas_num || total_replicas_num < 0 || total_replicas_num > OB_MAX_CLUSTER_COUNT)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "Invalid arguments for calculate tablet replicas num, total_replicas_num=%d, ret=%d",
              total_replicas_num, ret);
  }
  else if(total_replicas_num == total_num) // no calculate
  {
    for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
    {
      if(replicas_num_for_each_cluster_[cluster_id] > 0)
      {
        replicas_num[cluster_id] = replicas_num_for_each_cluster_[cluster_id];
      }
      else
      {
        replicas_num[cluster_id] = 0;
      }
    }
  }
  else // re-calculate
  {
    int calculate_total = 0;
    int32_t candidate = 0;
    for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
    {
      int32_t default_num = get_cluster_tablet_replicas_num(cluster_id);
      if(default_num > 0) // cluster is in use
      {
        int32_t tmp_num = static_cast<int32_t>(((float) total_replicas_num) * (((float)default_num) / ((float) total_num)));
        if(tmp_num == 0)
        {
          tmp_num = 1; // at least one copy
        }
        replicas_num[cluster_id] = tmp_num;
        calculate_total += tmp_num;
        candidate = replicas_num[candidate] > replicas_num[cluster_id] ? candidate:cluster_id;
      }
      //add bingo [Paxos Replica.Balance] 20170712:b
      else if(default_num == 0)
      {
        replicas_num[cluster_id] = default_num;
      }
      //add:e
    }
    if(calculate_total < total_replicas_num)
    {
      OB_ASSERT(master_cluster_id_ > 0);
      if(replicas_num[master_cluster_id_] != 0) //add bingo [Paxos Replica.Balance] 20170712:b:e
      {
        replicas_num[master_cluster_id_] += (total_replicas_num - calculate_total); // add to master cluster
      }
      //add bingo [Paxos Replica.Balance] 20170712:b
      else
      {
        replicas_num[candidate] += (total_replicas_num - calculate_total);
      }
      //add:e
    }
  }
  return ret;
}

int ObRootClusterManager::get_cluster_tablet_replicas_num(int32_t *replicas_num) const
{
  int ret = OB_SUCCESS;
  if(NULL == replicas_num)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "Invalid arguments for get tablet replicas num, ret=%d", ret);
  }
  else
  {
    for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
    {
      replicas_num[cluster_id] = replicas_num_for_each_cluster_[cluster_id];
    }
  }
  return ret;
}

int ObRootClusterManager::get_master_cluster_tablet_replicas_num(int32_t &master_replicas_num, const int32_t total_replicas_num)
{
  int ret = OB_SUCCESS;
  master_replicas_num = 0;
  int32_t replicas_num[OB_CLUSTER_ARRAY_LEN];
  memset(replicas_num, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
  if(OB_SUCCESS != (ret = get_cluster_tablet_replicas_num(replicas_num, total_replicas_num)))
  {
    TBSYS_LOG(WARN, "fail to get cluster tablet replias num, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(master_cluster_id_ > 0);
    master_replicas_num = replicas_num[master_cluster_id_];
  }
  return ret;
}

//each cluster has one replica at least
int32_t ObRootClusterManager::get_min_replicas_num() const
{
  int32_t count = 0;
  for(int32_t cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
  {
    if(get_cluster_tablet_replicas_num(cluster_id) > 0)
    {
      count++;
    }
  }
  return count;
}

int ObRootClusterManager::is_valid(const int32_t cluster_id, const int32_t replicas_num)
{
  int ret = OB_SUCCESS;
  if(cluster_id < 1 || cluster_id > OB_MAX_CLUSTER_ID || replicas_num < 0)
  {
    TBSYS_LOG(ERROR, "invalid cluster_id or replica_num, cluster_id=%d, replica_num=%d", cluster_id, replicas_num);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    int32_t total_num = replicas_num;
    for(int32_t i = 1; i < OB_CLUSTER_ARRAY_LEN; i++)
    {
      if(i != cluster_id && replicas_num_for_each_cluster_[i] > 0)
      {
        total_num += replicas_num_for_each_cluster_[i];
      }
    }
    if(total_num > OB_MAX_COPY_COUNT)
    {
      TBSYS_LOG(ERROR, "total replica num is larger than OB_MAX_COPY_COUNT, total_num=%d", total_num);
      ret = OB_REPLICA_NUM_OVERFLOW;
    }
  }
  return ret;
}

int32_t ObRootClusterManager::get_master_cluster_id() const
{
  return master_cluster_id_;
}

int ObRootClusterManager::set_master_cluster_id(const int32_t master_cluster_id)
{
  int ret = OB_SUCCESS;
  if(master_cluster_id < 1 || master_cluster_id > OB_MAX_CLUSTER_ID)
  {
    ret = OB_CLUSTER_ID_ERROR;
    TBSYS_LOG(WARN, "fail to set master cluster id, master_cluster_id=%d, ret=%d", master_cluster_id, ret);
  }
  else
  {
    //add pangtianze [Paxos Cluster.Balance] 20161019
      TBSYS_LOG(INFO, "set master cluster id, old=%d, new=%d", master_cluster_id_, master_cluster_id);
    //add:e
    master_cluster_id_ = master_cluster_id;
  }
  return ret;
}

//add pangtianze [Paxos Cluster.Balance] 20170620:b
int ObRootClusterManager::split_config_str(const char* str, int32_t* values)
{
    int rc = OB_SUCCESS;
    int64_t count = 0;
    char temp_buf[OB_MAX_VARCHAR_LENGTH];
    char* ptr = NULL;

    if (NULL == str)
    {
      rc = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "config string is NULL");
    }
    //add pangtianze [Paxos] 20170717:b
    else if (NULL == values)
    {
        rc = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "values array ptr is NULL");
    }
    //add:e
    else if (static_cast<int64_t>(strlen(str)) >= OB_MAX_VARCHAR_LENGTH)
    {
      rc = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "config str length >= OB_MAX_VARCHAR_LENGTH: length=[%d], ret=%d", static_cast<int>(strlen(str)), rc);
    }
    else
    {
      memcpy(temp_buf, str, strlen(str) + 1);
    }

    if (OB_SUCCESS == rc)
    {
      char* save_ptr = NULL;
      ptr = strtok_r(temp_buf, ";", &save_ptr);

      while (NULL != ptr)
      {
        if (count < OB_MAX_CLUSTER_COUNT)
        {
            values[count] = atoi(ptr);
            ptr = strtok_r(NULL, ";", &save_ptr);
            count++;
        }
        else
        {
            break;
        }
      }
    }
    return rc;
}

int ObRootClusterManager::parse_config_value(const char* cluster_replica_num_param, int32_t* replicas_num)
{
    int ret = OB_SUCCESS;
    //add pangtianze [Paxos] 20170717:b
    if (NULL == cluster_replica_num_param)
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "cluster_replica_num_param array ptr is null");
    }
    else if (NULL == replicas_num)
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "replicas_num array ptr is null");
    }
    else
    {
    //add:e
      int32_t values[OB_MAX_CLUSTER_COUNT] = {0};
      if (OB_SUCCESS != (ret = split_config_str(cluster_replica_num_param, values)))
      {
          TBSYS_LOG(ERROR, "parse cluster replica number config error, param=%s", cluster_replica_num_param);
      }
      else
      {
          replicas_num[0] = 0;
          for(int32_t cluster_id = 1;  cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            replicas_num[cluster_id] = values[cluster_id - 1];
          }
      }
    }
    return ret;
}
int ObRootClusterManager::check_total_replica_num(int32_t *replica_num)
{
    int ret = OB_SUCCESS;
    int32_t total_num = 0;
    //add pangtianze [Paxos] 20170717:b
    if (NULL == replica_num)
    {
       ret = OB_INVALID_ARGUMENT;
       TBSYS_LOG(ERROR, "replicas_num array ptr is null");
    }
    else
    {
    //add:e
       for (int cluster_id = 1; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
       {
           if (replica_num[cluster_id] >= 0 && replica_num[cluster_id] <= OB_MAX_COPY_COUNT)
           {
               total_num += replica_num[cluster_id];
           }
           else
           {
               ret = OB_ERROR;
               TBSYS_LOG(ERROR, "eplica num is invalird, cluster_id=%d, replica_num=%d",
                         cluster_id, replica_num[cluster_id]);
               break;
           }
       }
       if (OB_SUCCESS != ret || total_num < 1 || total_num > OB_MAX_COPY_COUNT)
       {
           ret = OB_REPLICA_NUM_OVERFLOW;
           TBSYS_LOG(ERROR, "total replica num is invalid, total_num=%d, ret=%d", total_num, ret);
       }
    }
    return ret;
}
//add:e
