/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_manager.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_MANAGER_H
#define _OB_UPS_MANAGER_H 1
#include "common/ob_server.h"
#include "common/ob_ups_info.h"
#include "common/ob_ups_info.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_async_task_queue.h"
#include <tbsys.h>

// forward declarations for unit test classes
class ObUpsManagerTest_test_basic_Test;
class ObUpsManagerTest_test_register_lease_Test;
class ObUpsManagerTest_test_register_lease2_Test;
class ObUpsManagerTest_test_offline_Test;
class ObUpsManagerTest_test_read_percent_Test;
class ObUpsManagerTest_test_read_percent2_Test;
namespace oceanbase
{
  namespace rootserver
  {
    enum ObUpsStatus
    {
      UPS_STAT_OFFLINE = 0,     ///< the ups is offline
      UPS_STAT_NOTSYNC = 1,     ///< slave & not sync
      UPS_STAT_SYNC = 2,        ///< slave & sync
      UPS_STAT_MASTER = 3,      ///< the master
    };
    struct ObUps
    {
      common::ObServer addr_;
      int32_t inner_port_;
      ObUpsStatus stat_;
      common::ObiRole obi_role_;
      int64_t log_seq_num_;
      int64_t lease_;
      int32_t ms_read_percentage_;
      int32_t cs_read_percentage_;
      //add pangtianze [Paxos ups_replication] 20160926:b
      int64_t log_term_;
      int64_t quorum_scale_;
      //add:e
      bool did_renew_received_;
      //add pangtianze [Paxos ups_election] 20161107:b
      bool is_ups_follower_;///< the ups is follower, can not be selected as master
      //add:e
      ObUps()
        :inner_port_(0), stat_(UPS_STAT_OFFLINE),
         log_seq_num_(0), lease_(0),
         ms_read_percentage_(0), cs_read_percentage_(0),
         //add pangtianze [Paxos ups_replication] 20160926:b
         log_term_(OB_INVALID_TERM),quorum_scale_(0),
         //add:e
         did_renew_received_(false),
         //add pangtianze [Paxos ups_election] 20161107:b
         is_ups_follower_(false)
         //add:e
      {
      }
      void reset();
      void convert_to(common::ObUpsInfo &ups_info) const;
    };
    class ObRootWorker;
    class ObRootAsyncTaskQueue;
    class ObUpsManager
    {
      public:
        ObUpsManager(ObRootRpcStub &rpc_stub, ObRootWorker *worker, const int64_t &revoke_rpc_timeout_us,
                     //mod pangtianze 20170210:b
                     //int64_t lease_duration, int64_t lease_reserved_us, int64_t waiting_ups_register_duration,
                     const int64_t &lease_duration,
                     const int64_t &lease_reserved_us,
                     const int64_t &waiting_ups_register_duration,
                     //mod:e
                     const common::ObiRole &obi_role, const volatile int64_t& schema_version,
                     const volatile int64_t& config_version
                     //add pangtianze [Paxos rs_election] 20150820:b
                     , const int64_t &max_deployed_ups_count
                     //add:e
                     );
        virtual ~ObUpsManager();


        //add pangtianze [Paxos rs_election] 20170302:b
        void get_online_ups_array(ObServer *array, int32_t& count);
        //add:e
        //add pangtianze [Paxos ups_election] 20161010:b
        inline bool get_is_select_new_master() const
        {
           return is_select_new_master_;
        }
        inline void set_is_select_new_master(const bool is_select)
        {
          __sync_bool_compare_and_swap(&is_select_new_master_, !is_select, is_select);
        }
        //add:e
        int get_ups_master(ObUps &ups_master) const;
        void set_async_queue(ObRootAsyncTaskQueue * queue);
        void reset_ups_read_percent();
        void print(char* buf, const int64_t buf_len, int64_t &pos) const;
        //add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
        void print_in_cluster(char* buf, const int64_t buf_len, int64_t &pos, int32_t cluster_id) const;
        //add:e
        int get_ups_list(common::ObUpsList &ups_list
                         //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                         , const int32_t cluster_id = 0
                         //add:e
                         ) const;
        int register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease, const char *server_version);
        //mod chujiajia [Paxos rs_election] 20151119:b
        //int renew_lease(const common::ObServer &addr, ObUpsStatus stat, const ObiRole &obi_role)
        int renew_lease(const common::ObServer &addr, ObUpsStatus stat, const ObiRole &obi_role, const int64_t &quorum_scale);
        //mod:e
        int slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr);
        int set_ups_master(const common::ObServer &master, bool did_force);
        int set_ups_config(const common::ObServer &addr, int32_t ms_read_percentage, int32_t cs_read_percentage);
        int set_ups_config(int32_t read_master_master_ups_percentage
                           //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                           //, int32_t read_slave_master_ups_percentage
                           //del:e
                           //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                           ,const int32_t is_strong_consistent = 0
                           //add:e
                           );
        void get_master_ups_config(int32_t &master_master_ups_read_percent
                                   //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                   //, int32_t &slave_master_ups_read_precent
                                   //del:e
                                   ) const;
        int send_obi_role();

        int grant_lease(bool did_force = false);     // for heartbeat thread
        int grant_eternal_lease();     // for heartbeat thread
        int check_lease();    // for check thread
        int check_ups_master_exist(); // for check thread
        //add peiouya [Get_masterups_and_timestamp] 20141017:b
        int64_t get_ups_set_time(const common::ObServer &addr);
        //add 20141017:e
        //add pangtianze [Paxos ups_replication] 20150603:b
        bool check_most_ups_registered();
        //add:e
        //add pangtianze [Paxos ups_replication] 20161010:b
        inline int64_t get_max_deployed_ups_count() const
        {
            return max_deployed_ups_count_;
        }
        //add:e
        //add chujiajia [Paxos rs_election] 20151222:b
        void ups_array_reset();
        void if_all_quorum_changed(char *buf, const int64_t buf_len, int64_t& pos, const int64_t& local_quorum_scale) const;
        int32_t get_ups_count_lock() const;
        int32_t get_active_ups_count_lock() const;
        //add:e
        //add jintianyang [paxos test] 20160530:b
        void print_ups_leader(char *buf, const int64_t buf_len,int64_t& pos);
        //add:e
        //add lbzhong [Paxos Cluster.Balance] 20160706:b
        void is_cluster_alive_with_ups(bool* is_cluster_alive) const;
        bool is_cluster_alive_with_ups(const int32_t cluster_id) const;        
        //add:e
        //add huangjianwei [Paxos rs_switch] 20160728:b
        friend class ObRootInnerTableTask;
        //add:e
      private:
        // change the ups status and then log this change
        void change_ups_stat(const int32_t index, const ObUpsStatus new_stat);
        int find_ups_index(const common::ObServer &addr) const;
        bool did_ups_exist(const common::ObServer &addr) const;
        bool is_ups_master(const common::ObServer &addr) const;
        bool has_master() const;
        bool is_master_lease_valid() const;
        bool need_grant(int64_t now, const ObUps &ups) const;
        int send_granting_msg(const common::ObServer &addr, common::ObMsgUpsHeartbeat &msg);
        //mod pangtianze [Paxos ups_replication] 20170125:b
        //int select_new_ups_master();
        /**
         * @brief the parameter 'exp_ups_idx' means the ups that user expect
         * @param exp_ups_index
         * @return
         */
        int select_new_ups_master(const int exp_ups_idx = OB_INVALID_INDEX);
        //mod:e
        int select_ups_master_with_highest_lsn();
        //del pangtianze [Paxos ups_replication] 20150605:b
        //void update_ups_lsn();
        //bool is_ups_with_highest_lsn(int ups_idx);
        //del:e
        //add pangtianze [Paxos ups_replication] 20150722:b
        int update_ups_lsn();
        bool is_ups_with_highest_lsn_and_term(int ups_idx);
        /**
         * @brief the parameter 'exp_ups_idx' means the ups that user expect,
         *        but still based on log and term
         *        Notify: [if only ups follower has the max term and log, there will be no master forever!! TODO discuss]
         * @param exp_ups_index
         * @return
         */
        int select_ups_master_with_highest_lsn_and_term(const int exp_ups_idx = OB_INVALID_INDEX);
        //add:e
        int revoke_master_lease(int64_t &waiting_lease_us);
        int send_revoking_msg(const common::ObServer &addr, int64_t lease, const common::ObServer& master);
        const char* ups_stat_to_cstr(ObUpsStatus stat) const;
        //add pangtianze [Paxos ups_election] 20161107:b
        const char* ups_follower_to_cstr(const ObUps &ups) const;
        //add:e
        void check_all_ups_offline();
        bool is_idx_valid(int ups_idx) const;
        // push update inner table task into queue
        int refresh_inner_table(const ObTaskType type, const ObUps & ups, const char *server_version);
        // disallow copy
        ObUpsManager(const ObUpsManager &other);
        ObUpsManager& operator=(const ObUpsManager &other);
        //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        int32_t get_active_ups_count(const int32_t cluster_id) const;
        int32_t get_ups_count(const int32_t cluster_id) const;
        void set_ups_read_percent(const int32_t master_read_percent,
                                  const int32_t slave_read_percent, const int32_t cluster_id = 0);
        //add:e
        //for unit test
        int32_t get_ups_count() const;
        int32_t get_active_ups_count() const;
        friend class ::ObUpsManagerTest_test_basic_Test;
        friend class ::ObUpsManagerTest_test_register_lease_Test;
        friend class ::ObUpsManagerTest_test_register_lease2_Test;
        friend class ::ObUpsManagerTest_test_offline_Test;
        friend class ::ObUpsManagerTest_test_read_percent_Test;
        friend class ::ObUpsManagerTest_test_read_percent2_Test;
      private:
        //mod pangtianze [Paxos] 20161020:b
        //static const int32_t MAX_UPS_COUNT = 17;
        static const int32_t MAX_UPS_COUNT = OB_MAX_UPS_COUNT;
        //mod:e
        static const int64_t MAX_CLOCK_SKEW_US = 200000LL; // 200ms
        // data members
        ObRootAsyncTaskQueue * queue_;
        ObRootRpcStub &rpc_stub_;
        ObRootWorker *worker_;
        const common::ObiRole &obi_role_;
        const int64_t &revoke_rpc_timeout_us_;
        mutable tbsys::CThreadMutex ups_array_mutex_;
        ObUps ups_array_[MAX_UPS_COUNT];
        //add peiouya [Get_masterups_and_timestamp] 20141017:b
        int64_t ups_mater_timestamp[MAX_UPS_COUNT];
        //add 20141017:e
        //add pangtianze [Paxos ups_replication] 20150603:b
        const int64_t& max_deployed_ups_count_;
        //add:e
        //mod pangtianze 20170210:b
        /*int64_t lease_duration_us_;
        int64_t lease_reserved_us_;//8500ms*/
        const int64_t &lease_duration_us_;
        const int64_t &lease_reserved_us_;
        //mod:e
        int32_t ups_master_idx_;
        //mod pangtianze 20170210:b
        //int64_t waiting_ups_register_duration_;
        const int64_t &waiting_ups_register_duration_;
        //mod:e
        int64_t waiting_ups_finish_time_;
        const volatile int64_t& schema_version_;
        const volatile int64_t& config_version_;

        int32_t master_master_ups_read_percentage_;
        //add pangtianze [Paxos Cluster.Flow.UPS] 20170119:b
        int32_t is_strong_consistency_;
        //add:e
        //add pangtianze [Paxos rs_election] 2016010:b
        bool is_select_new_master_;
        //add:e
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        //int32_t slave_master_ups_read_percentage_;
        //del:e
        bool is_flow_control_by_ip_;
    };
 } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_UPS_MANAGER_H */

