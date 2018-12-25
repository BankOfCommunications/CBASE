/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150609
*
*  - manage the election node, which represents a logic role in rs eleciton
*
*/
#ifndef OB_ROOT_ELECTION_NODE_MGR_H
#define OB_ROOT_ELECTION_NODE_MGR_H

#include "common/ob_server.h"
#include "common/ob_rs_rs_message.h"
#include "common/ob_version.h"
#include "rootserver/ob_root_rpc_stub.h"
#include <tbsys.h>
#include "Mutex.h"
#include "common/ob_define.h"
#include "rootserver/ob_root_async_task_queue.h"
#include "common/ob_config_manager.h"

namespace oceanbase
{
  namespace rootserver
  {   
    class ObRootWorker;
    class ObRootServer2;
    enum ObRsElectStat
    {
      INIT = 0,
      RS_ELECTION_DOING,
      RS_ELECTION_DONE,
    };
    ///store rs info for election
    struct ObRootElectionNode
    {
      enum ObRsNodeRole
      {
        OB_INIT = -1,
        OB_FOLLOWER = 0,
        OB_CANDIDATE = 1,
        OB_LEADER = 2,
      };
      common::ObServer my_addr_; // rs addr (ip, port)
      common::ObServer votefor_; // the rs addr that I voted for
      ObRsNodeRole rs_election_role_;
      bool is_alive_; // rs is alive or not
      bool has_leader_; //leader exist or not
      int64_t lease_;
      int64_t current_rs_term_; //election term
      int64_t hb_resp_timestamp_; //for leader, value is -1; for others, values is timestamp
      int32_t vote_result_; // -1 unknow, 0 vote for, 1 vote against
      int32_t bd_result_; // -1 unknow, 0 receive broadcast, refuse broadcast
      int32_t paxos_num_;
      int32_t priority_; // priority for rs election
      //add pangtianze [Paxos rs_election] 20161010:b
      int32_t quorum_scale_;
      //add:e
      ObRootElectionNode()
        :rs_election_role_(OB_FOLLOWER), is_alive_(false),has_leader_(false),
         lease_(0), current_rs_term_(0), hb_resp_timestamp_(0), vote_result_(-1),
         bd_result_(-1), paxos_num_(0),priority_(1)
         //add pangtianze [Paxos rs_election] 20161010:b
         ,quorum_scale_(0)
         //add:e
      {
      }
      /// @brief 获取Role
      inline ObRsNodeRole get_role() const {return rs_election_role_;}

      /// @brief 修改Role
      inline void set_role(const ObRsNodeRole role)
      {
        TBSYS_LOG(INFO, "before set_rs_node_role=%s", get_rs_role_str());
        atomic_exchange(reinterpret_cast<uint32_t*>(&rs_election_role_), role);
        TBSYS_LOG(INFO, "after set_rs_node_role=%s", get_rs_role_str());
      }
      inline const char* get_rs_role_str() const
      {
        switch (rs_election_role_)
        {
          case OB_INIT:       return "INIT";
          case OB_FOLLOWER:        return "FOLLOWER";
          case OB_CANDIDATE:   return "CANDIDATE";
          case OB_LEADER:   return "LEADER";
          default:           return "UNKNOWN";
        }
      }
      //add pangtianze [Paxos rs_election] 20170712:b
      inline void reset()
      {
          my_addr_.reset();
          votefor_.reset();
          rs_election_role_ = OB_FOLLOWER;
          is_alive_ = false;
          has_leader_ = false;
          lease_ = 0;
          current_rs_term_ = 0;
          hb_resp_timestamp_ = 0;
          vote_result_ = -1;
          bd_result_ = -1;
          paxos_num_ = 0;
          priority_ = 1;
          quorum_scale_ = 0;
      }
      inline void print()
      {
        TBSYS_LOG(INFO, "addr[%s], votefor[%s], role[%s], alive[%d], has_leader[%d], lease[%ld], "
                        "term[%ld], hb_time[%ld], paxos_num[%d], quorum_scale[%d], priority[%d]"
                  , my_addr_.to_cstring(), votefor_.to_cstring(), get_rs_role_str()
                  , is_alive_, has_leader_, lease_, current_rs_term_, hb_resp_timestamp_
                  , paxos_num_, quorum_scale_, priority_);
      }
      //add:e
    };
    ///manage rs election node
    class ObRootElectionNodeMgr
    {
      public:
        ObRootElectionNodeMgr(ObRootServer2 *root_server,
                              ObRoleMgr *role_mgr,
                              ObRootRpcStub &rpc_stub,
                              const int64_t &revoke_rpc_timeout_us,
                              const int64_t &no_hb_response_duration_us,
                              const int64_t &lease_duration_us,
                              const int64_t &leader_lease_duration_us,
                              const int64_t &rs_paxos_number,
                              const volatile int64_t &config_version);
        ~ObRootElectionNodeMgr();
      private:
        //disallow copy
        ObRootElectionNodeMgr(const ObRootElectionNodeMgr &other);
        ObRootElectionNodeMgr& operator=(const ObRootElectionNodeMgr &other);
      private:
        struct ObRsVoteResult
        {
          int64_t term_; //current term when cal results
          bool result_; //false means vote failed, true means vote successed
          ObRsVoteResult()
            :term_(0), result_(false)
          {}
        };
        struct ObRsBroadcastResult
        {
          int64_t term_; //current term when cal results
          bool result_; //false means broadcast failed, true means broadcast successed
          ObRsBroadcastResult()
            :term_(0), result_(false)
          {}
        };
      public:
        /**
         * @brief grant lease, invoked by heartbeat thread to rs
         * @param is_force
         * @return ret
         */
        int grant_lease();
        int handle_rs_heartbeat(const ObMsgRsHeartbeat &hb);
        int handle_rs_heartbeat_resp(const common::ObMsgRsHeartbeatResp &hb_resp, const int64_t time);
        /**
         * @brief refresh_inner_table, update __all_server
         * @param type
         * @param rs
         * @param server_version
         * @return
         */
        int refresh_inner_table(const ObTaskType type, const common::ObServer &rs, const int32_t status, const char *server_version);

        /**
         * @brief set_rs_priority, do not include my node
         * @param addr
         * @param prioriry
         */
        int set_rs_priority(const common::ObServer &addr, const int32_t prioriry);
        inline int32_t get_my_priority()
        {
            return my_node_.priority_;
        }
        int slave_init_first_meta(const ObTabletMetaTableRow &first_meta_row);
        //add pangtianze [Paxos rs_election] 20150731:b
        int update_rs_array(const common::ObServer *rs_array, const int32_t server_count);
        //add:e
        //add chujiajia [Paxos rs_election] 20151210:b
        int32_t add_rs_register_info(const common::ObServer &rs);
        //add:e
        void set_async_queue(ObRootAsyncTaskQueue * queue);
        inline const ObRootElectionNode::ObRsNodeRole get_my_role() const
        {
          return my_node_.get_role();
        }        
        inline void set_local_node_addr(const ObServer &rs)
        {
          ///only used in RootServer init, no need lock
          my_node_.my_addr_ = rs;
        }
        inline ObRsElectStat get_election_state() const
        {
          return election_state_;
        }
        /**
         * @brief get_election_phase
         *        return 1, in vote phase; 2, in broadcast phase
         * @return
         */
        inline int32_t get_election_phase()
        {
            //only for external call, has lock
            tbsys::CThreadGuard guard(&mutex_);
            int32_t ret = 0;
            if (!vote_request_doing_ && broadcast_doing_)
            {
                //broadcast
                ret = 2;
            }
            else if (vote_request_doing_ && !broadcast_doing_)
            {
                //vote
                ret = 1;
            }
            return ret;
        }
        /**
         * @brief set_election_phase
         * @param vote_request_doing
         * @param broadcast_doing
         */
        inline void set_election_phase_with_lock(const bool vote_request_doing, bool const broadcast_doing)
        {
            tbsys::CThreadGuard guard(&mutex_);
            vote_request_doing_ = vote_request_doing;
            broadcast_doing_ = broadcast_doing;
        }

        inline void set_my_term(const int64_t term)
        {
          atomic_exchange((uint64_t*)&my_node_.current_rs_term_, (uint64_t)term);
        }
        inline const int64_t get_my_term() const
        {
          return my_node_.current_rs_term_;
        }
        //add pangtianze [Paxos rs_election] 20160918:b
        inline void raise_my_term()
        {
          int32_t new_priority = 2 * my_node_.priority_; //使得term差别更加明显
          atomic_add((uint64_t*)&my_node_.current_rs_term_, (uint64_t)new_priority);
        }
        //add pangtianze [Paxos rs_election] 20160918:b
        inline void set_can_select_master_ups(const bool value)
        {
          __sync_bool_compare_and_swap(&can_select_master_ups_, !value, value);
        }
        inline const bool get_can_select_master_ups() const
        {
          return can_select_master_ups_;
        }
        //add:e
        //add pangtianze [Paxos bugfix:old leader still in __all_server after it is down] 20161108:b
        inline void  set_old_rs_master(const common::ObServer &old_rs_master)
        {
            old_rs_master_ = old_rs_master;
        }
        inline int32_t get_old_rs_master(ObServer& master)
        {
            master = old_rs_master_;
            return find_rs_index(master);
        }
        //add:e

        /**
         * @brief leader_broadcast
         * @param is_first_start, if true, will grant max lease
         *                        if false, will grant normal lease
         * @return
         */
        int leader_broadcast(bool force = false);
        int request_votes();

        /**
         * @brief excute whole election from begin to end
         * @return
         */
        int check_lease();
        bool check_most_rs_alive();

        int handle_leader_broadcast_resp(const ObMsgRsLeaderBroadcastResp &lb_resp);
        int handle_vote_request_resp(const ObMsgRsRequestVoteResp &rv_resp);

        /**
         * @brief get_all_alive_servers
         * @param servers, user must make sure servers[] is empty
         * @param server_count
         */
        void get_all_alive_servers(common::ObServer *servers, int32_t &server_count);
        //add pangtianze [Paxos] 20170705:b
        /**
         * @brief get all rs no matter it's alive or not
         * @param servers
         * @param server_count
         */
        void get_all_servers(common::ObServer *servers, int32_t &server_count);
        //add:e
        int32_t get_rs_slave_count();

        int handle_leader_broadcast(const common::ObServer &addr, const int64_t lease,
                                    const int64_t rs_term, const bool force);
        int handle_vote_request(const common::ObServer &addr, const int64_t lease,
                                    const int64_t rs_term);
        void set_vote_request_resp(ObMsgRsRequestVoteResp &rv_resp, const bool granted, const bool need_regist);
        void set_leader_broadcast_resp(ObMsgRsLeaderBroadcastResp &lb_resp, const bool granted);
        int get_leader(common::ObServer &leader);


        //add pangtianze [Paxos rs_election] 20150731:b
        /**
         * @brief print_leader, for rs admin
         * @param buf
         * @param buf_len
         * @param pos
         */
        void print_leader(char *buf, const int64_t buf_len, int64_t& pos);
        void if_all_paxos_changed(char *buf, const int64_t buf_len, int64_t& pos, const int32_t local_paxos_num) const;
        //add:e
        //add pangtianze [Paxos rs_election] 20161010:b
        bool check_all_rs_alive();
        void if_all_quorum_in_rs_changed(char *buf, const int64_t buf_len, int64_t& pos, const int32_t local_quorum_scale) const;
        inline void set_my_quorum_scale(const int32_t quorum)
        {
           atomic_exchange((uint32_t*)&my_node_.quorum_scale_, quorum);
        }
        inline void set_my_paxos_num(const int32_t paxos_num)
        {
           atomic_exchange((uint32_t*)&my_node_.paxos_num_, paxos_num);
        }
        //add:e

        //add pangtianze [Paxos rs_election] 20161010:b
        int handle_change_config_request(const common::ObServer &addr,const int64_t rs_term, const int32_t new_paxos_num);
        //add:e
        inline int64_t get_begin_time()
        {
          return begin_time_;
        }
        //add:e
        //add lbzhong [Paxos Cluster.Balance] 20160708:b
        void is_cluster_alive_with_rs(bool *is_cluster_alive) const;
        bool is_cluster_alive_with_rs(const int32_t cluster_id) const;
        int get_one_slave_rs(ObServer &slaver_rs, const int32_t cluster_id) const;
        int get_one_rs(ObServer &rs, const int32_t cluster_id) const;
        //add:e
        void set_as_leader(const common::ObServer &addr);
        void set_as_follower();
        //add pangtianze [Paxos rs_election] 20170707:b
        void remove_invalid_info();
        int rs_update_lease(const int64_t new_lease);
        //add:e
      private:
        /**
         * @brief send heartbeat message, including lease info, term, and source rs
         * @param addr: targer rootserver
         * @param msg
         * @return ret
         */
        void reset_hb_timestamps();
        //add chujiajia [Paxos rs_election] 20151105:b
        void reset_election_info();
        //add:e
        //int rs_update_lease(const int64_t new_lease);
        /**
         * @brief find the index in other_nodes_arrary for such rs
         * @param addr
         * @return
         */
        int32_t find_rs_index(const common::ObServer &addr);
        //add pangtianze [Paxos rs_election] 20170710:b
        /**
         * @brief find_rs_info_index, including invalid rs info
         * @param addr
         * @return
         */
        int32_t find_rs_info_index(const common::ObServer &addr);
        //add:e
        int32_t add_rs(const common::ObServer &rs);
        /**
         * @brief set_as_candidate
         * @param has_leader, false means in vote phase, true means in broadcast phase
         */
        void set_as_candidate(const bool has_leader);
        //add pangtianze [Paxos rs_election] 20150813:b
        int start_leader_broadcast(bool force = false);
        int start_request_votes();
        //add:e
        int excute_election_vote(const int64_t now);
        int excute_election_broadcast(const int64_t now);
        bool success_vote();
        bool success_broadcast();
        /**
         * @brief get random time, rs will wait for that time
         *        before start an election.
         *        randon = rand()%(b-a) + a
         * @return random time
         */
        int64_t get_random_time();
        int32_t get_rs_paxos_number() const;
        /**
         * @brief need_leader_change_role, without lock
         * @param now
         * @return
         */
        bool need_leader_change_role(const int64_t now);
        void sort_hb_timestamp_asc();
        int64_t get_middle_timestamp(const int alive_rs_count);
        void reset_hb_timestamp_array();
        void start_election();
        void end_election();
        void get_vote_result(int64_t &term, bool &result);
        void get_broadcast_result(int64_t &term, bool &result);
        void set_vote_result(const int64_t term, const bool result);
        void set_broadcast_result(int64_t const term, const bool result);
        //add pangtianze [Paxos bugfix:old leader still in __all_server after it is down] 20161108:b
        int32_t get_rs_cluster_id(const ObServer &server);
        //add:e
        //add pangtianze [Paxos rs_electoin] 20170628:b
        inline void print_lease_info(const bool is_leader);
        //ad:e
        inline void set_local_node_role(const ObRootElectionNode::ObRsNodeRole &role)
        {
          my_node_.set_role(role);
        }
        inline void reset_vote_for()
        {
          common::ObSpinLockGuard guard(vote_for_lock_);
          my_node_.votefor_.reset();
        }
        inline void set_vote_for(const ObServer &vote_for)
        {
          common::ObSpinLockGuard guard(vote_for_lock_);
          my_node_.votefor_ = vote_for;
        }
        inline const ObServer get_vote_for()
        {
          common::ObSpinLockGuard guard(vote_for_lock_);
          return my_node_.votefor_;
        }
        inline void set_has_leader(const bool has_leader)
        {
          atomic_exchange(reinterpret_cast<uint32_t*>(&my_node_.has_leader_), has_leader);
        }
        inline void set_election_state(const ObRsElectStat state)
        {
            TBSYS_LOG(INFO, "before set_election_stat=%s role=%s", get_election_stat_str(), my_node_.get_rs_role_str());
            atomic_exchange(reinterpret_cast<uint32_t*>(&election_state_), state);
            TBSYS_LOG(INFO, "after set_election_stat=%s role=%s", get_election_stat_str(), my_node_.get_rs_role_str());
        }
        inline const char* get_election_stat_str() const
        {
          switch (election_state_)
          {
            case INIT:       return "INIT";
            case RS_ELECTION_DOING:        return "RS_ELECTION_DOING";
            case RS_ELECTION_DONE:   return "RS_ELECTION_DONE";
            default:           return "UNKNOWN";
          }
        }
        inline const bool get_has_leader() const
        {
          return my_node_.has_leader_;
        }
        inline void set_node_alive_stat(ObRootElectionNode &node, const bool is_alive)
        {
          __sync_bool_compare_and_swap(&node.is_alive_, !is_alive, is_alive);
        }
        inline void set_my_node_info(const int64_t lease, const bool has_leader, const common::ObServer addr,
                         const int64_t rs_term)
        {
          int ret = OB_SUCCESS;
          if (OB_SUCCESS != (ret = rs_update_lease(lease)))
          {
              TBSYS_LOG(WARN, "update lease failed, ret=%d, sender=%s", ret, addr.to_cstring());
          }
          set_has_leader(has_leader);
          set_vote_for(addr);
          set_my_term(rs_term);
        }
      private:
         static const int64_t MAX_CLOCK_SKEW_US = 200000LL; // 200ms
         // data members
         //add pangtianze [Paxos bugfix:old leader still in __all_server after it is down] 20161108:b
         common::ObServer old_rs_master_;
         //add:e
         ObRootAsyncTaskQueue *queue_;
         ObRootServer2 *root_server_;
         ObRoleMgr *role_mgr_;
         ObRootRpcStub &rpc_stub_;
         mutable tbsys::CThreadMutex nodes_mutex_; //lock for rs nodes
         mutable tbsys::CThreadMutex mutex_;
         ObRootElectionNode other_nodes_[OB_MAX_RS_COUNT - 1]; //store other rs info
         ObRootElectionNode my_node_; //store local rootserver info
         ObRsElectStat election_state_;
         const int64_t &revoke_rpc_timeout_us_; // 3s
         const int64_t &no_hb_response_duration_us_;
         const int64_t &lease_duration_us_;
         const int64_t &leader_lease_duration_us_;
         int64_t election_timeout_us_; //
         int64_t election_vote_lease_us_;
         int64_t election_broadcast_lease_us_;
         int32_t vote_count_; // agreeable votes count
         int32_t broadcast_count_; //confirmed broadcast count
         int64_t rs_hb_timestamp_array_[OB_MAX_RS_COUNT - 1]; // only for sort, only include ts of rs in other node array
         int64_t begin_time_; //election begin time
         int64_t end_time_; //election end time
         const int64_t &rs_paxos_number_;
         const volatile int64_t& config_version_;
         common::ObSpinLock vote_for_lock_;
         bool vote_request_doing_;
         bool broadcast_doing_;
         bool has_reset_hb_timestamps_;
         //add pangtianze [Paxos rs_election] 20160918:b
         bool can_select_master_ups_;
         //add:e
         int64_t normal_election_duration_us_;
         ObRsVoteResult vote_result_;
         ObRsBroadcastResult broadcast_result_;
         common::ObSpinLock vote_result_lock_;
         common::ObSpinLock broadcast_result_lock_;
     };
  }//end namespace rootserver
}//end namespace oceanbase

#endif // OB_ROOT_ELECTION_NODE_MGR_H
