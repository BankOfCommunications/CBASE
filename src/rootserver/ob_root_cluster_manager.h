/**
 * @author lbzhong 20160714
 * cluster manager
 */
#ifndef OB_ROOT_CLUSTER_MANAGER_H
#define OB_ROOT_CLUSTER_MANAGER_H

#include "common/ob_define.h"
#include "common/ob_schema_service.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootClusterManager
    {
    public:
      ObRootClusterManager();
      virtual ~ObRootClusterManager();
      void set_schema_service(common::ObSchemaService *&schema_service) { schema_service_ = schema_service; }

      void init_cluster_tablet_replicas_num(const bool* is_cluster_alive);

      int set_replicas_num(const int32_t *replicas_num);
      int32_t get_cluster_tablet_replicas_num(const int32_t cluster_id) const;
      int32_t get_total_tablet_replicas_num() const;
      int get_cluster_tablet_replicas_num(int32_t *replicas_num, const int32_t total_replicas_num);
      int get_cluster_tablet_replicas_num(int32_t *replicas_num) const;
      int get_master_cluster_tablet_replicas_num(int32_t &master_replicas_num, const int32_t total_replicas_num);
      int32_t get_min_replicas_num() const;

      int32_t get_master_cluster_id() const;
      int set_master_cluster_id(const int32_t master_cluster_id);
      /**
       * if total_num > OB_MAX_COPY_COUNT
       * @brief is_valid
       * @param cluster_id
       * @param replicas_num
       * @return
       */
      int is_valid(const int32_t cluster_id, const int32_t replicas_num);
      //add pangtianze [Paxos Cluster.Balance]
      int split_config_str(const char* str, int32_t* values);
      int parse_config_value(const char* str, int32_t* replicas_num);
      int check_total_replica_num(int32_t *replica_num);
      //add:e
    private:
      int32_t replicas_num_for_each_cluster_[common::OB_CLUSTER_ARRAY_LEN];
      int32_t master_cluster_id_;
      common::ObSchemaService *schema_service_;
    };
  }
}

#endif // OB_ROOT_CLUSTER_MANAGER_H
