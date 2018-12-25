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
#include "common/ob_define.h"
#include "ob_root_election_node_mgr.h"
#include "common/ob_rs_rs_message.h"
#include "ob_root_server2.h"
#include "ob_root_worker.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;


ObRootElectionNodeMgr::ObRootElectionNodeMgr(ObRootServer2 *root_server,
                                             ObRoleMgr *role_mgr,
                                             ObRootRpcStub &rpc_stub,
                                             const int64_t &revoke_rpc_timeout_us,
                                             const int64_t &no_hb_response_duration_us,
                                             const int64_t &lease_duration_us,
                                             const int64_t &leader_lease_duration_us,
                                             const int64_t &rs_paxos_number,
                                             const volatile int64_t &config_version)
  :queue_(NULL), root_server_(root_server),role_mgr_(role_mgr),rpc_stub_(rpc_stub),
   election_state_(RS_ELECTION_DONE), revoke_rpc_timeout_us_(revoke_rpc_timeout_us),
   no_hb_response_duration_us_(no_hb_response_duration_us),
   lease_duration_us_(lease_duration_us),
   leader_lease_duration_us_(leader_lease_duration_us),
   election_timeout_us_(200000), //200ms
   election_vote_lease_us_(0),election_broadcast_lease_us_(0),
   vote_count_(0), broadcast_count_(0), begin_time_(0),end_time_(0),
   rs_paxos_number_(rs_paxos_number),
   config_version_(config_version),vote_request_doing_(true),broadcast_doing_(true),
   has_reset_hb_timestamps_(false)
   //add pangtianze [Paxos rs_election] 20160918:b
   ,can_select_master_ups_(true)
   //add:e
{
  normal_election_duration_us_ = 30 * 1000000; //30s
  reset_hb_timestamp_array();
}
ObRootElectionNodeMgr::~ObRootElectionNodeMgr()
{
}
void ObRootElectionNodeMgr::set_async_queue(ObRootAsyncTaskQueue * queue)
{
  queue_ = queue;
}

void ObRootElectionNodeMgr::set_as_leader(const ObServer &addr)
{

   root_server_->set_rt_master(addr);
   set_local_node_role(ObRootElectionNode::OB_LEADER);
   set_vote_for(addr);
   set_has_leader(true);
}
void ObRootElectionNodeMgr::set_as_follower()
{
    set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
    reset_vote_for();
    set_has_leader(false);
}
void ObRootElectionNodeMgr::set_as_candidate(const bool has_leader)
{
  set_local_node_role(ObRootElectionNode::OB_CANDIDATE);
  //add huangjianwei [Paxos rs_election] 20160328:b
  int64_t now = tbsys::CTimeUtil::getTime();
  election_vote_lease_us_ = now + get_random_time();
  //add:e
  set_has_leader(has_leader);
}

int ObRootElectionNodeMgr::grant_lease()
{
  int ret = OB_SUCCESS;  
  int64_t now = tbsys::CTimeUtil::getTime();
  OB_ASSERT(NULL != root_server_); //add pangtianze [Paxos] 20170717

  //get rs list
  ObServer servers[OB_MAX_RS_COUNT];
  int32_t server_count = 0;
  get_all_alive_servers(servers, server_count);

  tbsys::CThreadGuard guard(&nodes_mutex_);
  my_node_.paxos_num_ = get_rs_paxos_number();
  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; ++i)
  {    
    if (other_nodes_[i].is_alive_)
    {
        other_nodes_[i].lease_ = now + lease_duration_us_;
        //construct msg for leader to grant lease to other rs
        ObMsgRsHeartbeat msg;
        msg.addr_ = my_node_.my_addr_;
        msg.lease_ = other_nodes_[i].lease_;
        msg.current_term_ = get_my_term();
        msg.config_version_ = config_version_;
        //add pangtianze [Paxos rs_election] 20161020:b
        msg.paxos_num_ = get_rs_paxos_number();
        msg.quorum_scale_ = (int32_t)root_server_->get_ups_manager()->get_max_deployed_ups_count();
        //add:e
        //add wangdonghui [paxos daily merge] 20170510 :b
        msg.last_frozen_time_ = root_server_->get_last_frozen_time();
        //add :e
        //send lease msg
        int ret2 = rpc_stub_.grant_lease_to_rs(other_nodes_[i].my_addr_, msg, servers, server_count);
        TBSYS_LOG(DEBUG, "send lease to rs, rs=%s rs_term=%ld lease=%ld", to_cstring(other_nodes_[i].my_addr_), msg.current_term_, msg.lease_);
        if (OB_SUCCESS != ret2)
        {
          TBSYS_LOG(WARN, "grant lease to rs err, err=%d, ups=%s",
                    ret2, other_nodes_[i].my_addr_.to_cstring());
        }
    }
  } //end for
  return ret;
}

int ObRootElectionNodeMgr::handle_rs_heartbeat(const ObMsgRsHeartbeat &hb)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != root_server_); //add pangtianze [Paxos] 20170717
  tbsys::CThreadGuard guard(&nodes_mutex_);
  if (hb.current_term_ >= get_my_term())
  {
    if (OB_UNLIKELY(ObRootElectionNode::OB_LEADER == get_my_role()))
    {
      //rarely enter here
      ret = OB_LEADER_EXISTED;
      set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
      TBSYS_LOG(ERROR, "leader received heartbeat with higher term, change to follower, rs sender=%s, sender term=%ld, local_term=%ld, err=%d",
                to_cstring(hb.addr_), hb.current_term_, get_my_term(), ret);
    }
    else if (ObRootElectionNode::OB_CANDIDATE == get_my_role())
    {
      set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
      set_my_node_info(hb.lease_, true, hb.addr_, hb.current_term_);
      //add chujiajia[paxos]20151028:b
      end_election();
      char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
      get_vote_for().ip_to_string(ip_tmp, sizeof(ip_tmp));
      root_server_->get_config().master_root_server_ip.set_value(ip_tmp);
      root_server_->get_config().master_root_server_port = get_vote_for().get_port();
      if (OB_SUCCESS != (ret = root_server_->get_config_mgr()->dump2file()))
      {
        TBSYS_LOG(WARN, "Dump to file failed! ret=%d.", ret);
      }
      //add:e
      root_server_->set_rt_master(hb.addr_);
    }
    else if (ObRootElectionNode::OB_FOLLOWER == get_my_role())
    {
      if (!has_reset_hb_timestamps_)
      {
        reset_hb_timestamps();
      }
      //mod chujiajia [Paxos rs_election] 20151111:b
      //if (get_has_leader() == false || (get_vote_for() == hb.addr_ && get_has_leader() == true))
      if (!get_has_leader() || (get_vote_for() == hb.addr_ && get_has_leader()))
      //mod:e
      {
        //add chujiajia [Paxos rs_election] 20151028:b
        if(!get_has_leader())
        {
          set_my_node_info(hb.lease_, true, hb.addr_, hb.current_term_);
          end_election();
          char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
          get_vote_for().ip_to_string(ip_tmp, sizeof(ip_tmp));
          root_server_->get_config().master_root_server_ip.set_value(ip_tmp);
          root_server_->get_config().master_root_server_port = get_vote_for().get_port();
          if (OB_SUCCESS != (ret = root_server_->get_config_mgr()->dump2file()))
          {
            TBSYS_LOG(WARN, "Dump to file failed! ret=%d.", ret);
          }
        }
        //add:e
        else
        {
          TBSYS_LOG(DEBUG, "follower recevie heartbeat from leader[%s]", hb.addr_.to_cstring());
          set_my_node_info(hb.lease_, true, hb.addr_, hb.current_term_);
          root_server_->set_rt_master(hb.addr_);
          //add wangdonghui [paxos daily merge] 20170510 :b
          root_server_->set_last_frozen_time(hb.last_frozen_time_);
          //add :e
        }
        //add pangtianze [rs_election] 20161020:b
        //del bingo [Paxos rs management] 20170217:b
        /*if (1 == hb.paxos_num_)
        {
           TBSYS_LOG(INFO, "rootserver paxos number is set as 1, so slave should stop");
           role_mgr_->set_state(ObRoleMgr::STOP);
        }
        else
        {*/
           set_my_paxos_num(hb.paxos_num_);
           root_server_->set_paxos_num(hb.paxos_num_);
           set_my_quorum_scale(hb.quorum_scale_);
           root_server_->set_quorum_scale(hb.quorum_scale_);
        //}
        //del:e
        //add:e
      }
      else // get_vote_for() != hb.addr_ && get_has_leader() == true
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "another leader occured, another leader=%s, my leader=%s",
                  to_cstring(hb.addr_), to_cstring(get_vote_for()));
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "unknow rootserver election role");
    }    
    if (ObRoleMgr::SLAVE == role_mgr_->get_role())
    {
      root_server_->get_config_mgr()->got_version(hb.config_version_);
    }
  }
  else
  {
    //mod pangtianze [Paxos rs_election] 20150914:b
    //TBSYS_LOG(WARN, "receive invalid msg!");
     if (OB_LIKELY(ObRootElectionNode::OB_LEADER != get_my_role()))
     {
       set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
       if (OB_SUCCESS != (ret = rs_update_lease(hb.lease_)))
       {
           TBSYS_LOG(WARN, "update lease failed, ret=%d, sender=%s", ret, hb.addr_.to_cstring());
       }
       TBSYS_LOG(WARN, "receive heartbeat with lower term, term=%ld, sender=%s", hb.current_term_, hb.addr_.to_cstring());
     }
     else
     {
       //rarely enter here
       ret = OB_LEADER_EXISTED;
       TBSYS_LOG(ERROR, "leader received heartbeat with lower term, ignore it, rs sender=%s, sender term=%ld, local_term=%ld, err=%d",
                   to_cstring(hb.addr_), hb.current_term_, get_my_term(), ret);
     }
    //mod:e
  }
  //add pangtianze [Paxos bugfix:old leader still in __all_server after it is down] 20161108:b
  int32_t idx = OB_INVALID_INDEX;
  if (old_rs_master_ != hb.addr_)
  {
     if (OB_INVALID_INDEX != (idx = find_rs_index(hb.addr_)))
     {
        old_rs_master_ = other_nodes_[idx].my_addr_;
        //can not cluster_id in hb msg
        old_rs_master_.cluster_id_ = get_rs_cluster_id(other_nodes_[idx].my_addr_);
     }
  }
  //add:e
  return ret;
}
void ObRootElectionNodeMgr::reset_hb_timestamps()
{
  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; ++i)
  {
    //del pangtianze [Paxos] 20170615:b
    //if (other_nodes_[i].is_alive_)
    //{
    //del
      other_nodes_[i].hb_resp_timestamp_ = 0;
    //}
  }
}

int ObRootElectionNodeMgr::rs_update_lease(const int64_t new_lease) //rs 更新租约
{
  int ret = OB_SUCCESS;
  if (new_lease >= my_node_.lease_)
  {    
    my_node_.lease_ = new_lease;
  }
  else
  {
    ret = OB_INVALID_VALUE;
    //mod pangtianze [Paxos rs_election] 20170628:b
    //TBSYS_LOG(WARN, "received lease is earlier than self lease, don't need update self lease");
    TBSYS_LOG(WARN, "received lease[%ld] is earlier than self lease[%ld], don't need update self lease"
              , new_lease, my_node_.lease_);
    //mod:e
  }
  return ret;
}
int32_t ObRootElectionNodeMgr::find_rs_index(const common::ObServer &addr)
{
   int32_t index = OB_INVALID_INDEX;

   for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
   {
     if ((other_nodes_[i].my_addr_ == addr) && other_nodes_[i].is_alive_)
     {
         index = i;
         break;
     }
   }
   return index;
}

//add pangtianze [Paxos rs_election] 20170710:b
int32_t ObRootElectionNodeMgr::find_rs_info_index(const common::ObServer &addr)
{
    int32_t index = OB_INVALID_INDEX;
    for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
    {
      if (other_nodes_[i].my_addr_ == addr)
      {
          index = i;
          break;
      }
    }
    return index;
}
//add:e

int ObRootElectionNodeMgr::set_rs_priority(const common::ObServer &addr, const int32_t prioriry)
{
   int ret = OB_SUCCESS;
   tbsys::CThreadGuard guard(&nodes_mutex_);
   if (addr == my_node_.my_addr_)
   {
     my_node_.priority_ = prioriry;
     TBSYS_LOG(INFO, "set new priority successed, rs=%s, priority=%d", addr.to_cstring(), prioriry);

   }
   else
   {
     int idx = find_rs_index(addr);
     if (OB_INVALID_INDEX != idx)
     {
        other_nodes_[idx].priority_ = prioriry;
        TBSYS_LOG(INFO, "set new priority successed, rs=%s, priority=%d", addr.to_cstring(), prioriry);
     }
     else
     {
       ret = OB_ERROR;
       TBSYS_LOG(WARN, "do not find target rootserver in leader while setting prioritys, rs=%s", addr.to_cstring());
     }
   }
   return ret;
}

int ObRootElectionNodeMgr::slave_init_first_meta(const ObTabletMetaTableRow &first_meta_row)
{
  int ret = OB_SUCCESS;
  int32_t rs_count = 1;
  tbsys::CThreadGuard guard(&nodes_mutex_);
  for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].is_alive_)
    {
      rs_count++;
    }
  }
  if (rs_count == get_rs_paxos_number())
  {
    for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
    {
      if (other_nodes_[i].is_alive_)
      {
        ret = rpc_stub_.rs_init_first_meta(other_nodes_[i].my_addr_, first_meta_row, revoke_rpc_timeout_us_);
        if (OB_INIT_TWICE == ret)
        {
          ret = OB_SUCCESS;
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to init first meta in slave rootserver, err=%d, rootserver=%s",
                    ret, to_cstring(other_nodes_[i].my_addr_));
          break;
        }
      }
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "real rootserver number not equal with the num needed, real num=%d, need num=%d",
              rs_count, get_rs_paxos_number());
  }
  return ret;
}

//add pangtianze [Paxos rs_election] 20150731:b
int ObRootElectionNodeMgr::update_rs_array(const common::ObServer *rs_array, const int32_t server_count)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&nodes_mutex_);
  //can delete rs from local rs array
  for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].is_alive_)
    {
      bool is_in_received_array = false;
      for (int32_t j = 0; j < server_count; j++)
      {
        if (other_nodes_[i].my_addr_ == rs_array[j])
        {
          is_in_received_array = true;
          break;
        }
      }
      if (!is_in_received_array)
      {
        set_node_alive_stat(other_nodes_[i], false);
        TBSYS_LOG(INFO, "the rootserver has been not alive, rs=%s", other_nodes_[i].my_addr_.to_cstring());
      }
    }
  }
  //can add rs
  for (int32_t k = 0; k < server_count; k++)
  {
    add_rs(rs_array[k]);
  }
  return ret;
}
//add:e
int32_t ObRootElectionNodeMgr::add_rs(const common::ObServer &rs)
{
  int ret = OB_SUCCESS;
  int32_t idx = OB_INVALID_INDEX;
  int32_t count = 0;
//  tbsys::CThreadGuard guard(&nodes_mutex_);
  if (my_node_.my_addr_ == rs)
  {
     // do nothing
  }
  else if(OB_INVALID_INDEX == (idx = find_rs_info_index(rs))) //mod pangtianze [Paxos rs_election] 20170710:b
  {
    for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
    {
      if (!other_nodes_[i].my_addr_.is_valid())
      {
        other_nodes_[i].my_addr_ = rs;
        set_node_alive_stat(other_nodes_[i], true);
        TBSYS_LOG(INFO, "add new rs info into rs_array, rs=%s", rs.to_cstring());
        count++;
        break;
      }
      if (count == OB_MAX_RS_COUNT)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "all rs count has been up to MAX[%d], can't add new rs", OB_MAX_RS_COUNT);
      }
    }
  }
  //add pangtianze [Paxos rs_election] 20170710:b
  else if (idx >= 0 && !other_nodes_[idx].is_alive_)
  {
      int32_t old_cluster_id = other_nodes_[idx].my_addr_.cluster_id_;
      other_nodes_[idx].reset();
      other_nodes_[idx].my_addr_ = rs;
      set_node_alive_stat(other_nodes_[idx], true);
      TBSYS_LOG(INFO, "add new rs info into rs_array again, rs=%s", rs.to_cstring());
      //add bingo [Paxos RS restart] 20170306:b
      if(old_cluster_id!= rs.cluster_id_)  //mod pangtianze [Paxos rs_election] 20170713
      {
        set_node_alive_stat(other_nodes_[idx], false);
        other_nodes_[idx].my_addr_.cluster_id_ = rs.cluster_id_;
        TBSYS_LOG(INFO, "the rootserver info with old cluster_id has been deleted, rs=%s", other_nodes_[idx].my_addr_.to_cstring());
      }
      //add:e
  }
  else if (idx >= 0 && other_nodes_[idx].is_alive_)  //register rs still alive
  {
      int32_t old_cluster_id = other_nodes_[idx].my_addr_.cluster_id_;
      //add bingo [Paxos RS restart] 20170306:b
      if(other_nodes_[idx].my_addr_.cluster_id_ != rs.cluster_id_)
      {
     //   set_node_alive_stat(other_nodes_[idx], false);
        other_nodes_[idx].my_addr_.cluster_id_ = rs.cluster_id_;
        TBSYS_LOG(INFO, "rs cluster id has changed, rs=%s, cluster_id[%d->%d]",
                  other_nodes_[idx].my_addr_.to_cstring(), old_cluster_id, rs.cluster_id_);
      }
      //add:e
  }
  //add:e
  return ret;
}

//add chujiajia [Paxos rs_election] 20151210:b
int32_t ObRootElectionNodeMgr::add_rs_register_info(const common::ObServer &rs)
{
  int ret = OB_SUCCESS;
  int32_t idx = OB_INVALID_INDEX;
  int32_t count = 0;
  tbsys::CThreadGuard guard(&nodes_mutex_);
  TBSYS_LOG(INFO, "will add reigster rs[%s]", rs.to_cstring());
  if (my_node_.my_addr_ == rs)
  {
     ret = OB_ERROR;
     TBSYS_LOG(ERROR, "self regist to self, weird erro! rs=%s", rs.to_cstring());
  }
  else if(OB_INVALID_INDEX == (idx = find_rs_info_index(rs)))
  {
    for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
    {
      if (!other_nodes_[i].my_addr_.is_valid())
      {
        other_nodes_[i].my_addr_ = rs;
        other_nodes_[i].hb_resp_timestamp_ = tbsys::CTimeUtil::getTime();
        set_node_alive_stat(other_nodes_[i], true);
        TBSYS_LOG(INFO, "add new rs info into rs_array, rs=%s", rs.to_cstring());
        count++;
        break;
      }
      if (count == OB_MAX_RS_COUNT)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "all rs count has been up to MAX[%d], can't add new rs", OB_MAX_RS_COUNT);
      }
    }
  }
  //add pangtianze [Paxos rs_election] 20170710:b
  else if (idx >= 0 && !other_nodes_[idx].is_alive_)
  {
      other_nodes_[idx].reset();
      other_nodes_[idx].my_addr_ = rs;
      other_nodes_[idx].hb_resp_timestamp_ = tbsys::CTimeUtil::getTime();
      set_node_alive_stat(other_nodes_[idx], true);
      TBSYS_LOG(INFO, "add new rs info into rs_array again, rs=%s", rs.to_cstring());
  }
  else if (idx >= 0 && other_nodes_[idx].is_alive_)  //register rs still alive
  {
      int32_t old_cluster_id = other_nodes_[idx].my_addr_.cluster_id_;
      //add bingo [Paxos RS restart] 20170306:b
      if(other_nodes_[idx].my_addr_.cluster_id_ != rs.cluster_id_)
      {
        refresh_inner_table(SERVER_OFFLINE, other_nodes_[idx].my_addr_, 0, "hb server version null"); //also other_nodes_[idx].my_addr_
        other_nodes_[idx].my_addr_.cluster_id_ = rs.cluster_id_;
        TBSYS_LOG(INFO, "rs cluster id has changed, rs=%s, cluster_id[%d->%d]",
                  other_nodes_[idx].my_addr_.to_cstring(), old_cluster_id, rs.cluster_id_);
      }
      //add:e
      else
      {
         TBSYS_LOG(INFO, "rs[%s] is still alive, ignore reigst", rs.to_cstring());
      }
  }
  //add:e

  return ret;
}
//add:e

int ObRootElectionNodeMgr::handle_leader_broadcast(const common::ObServer &addr, const int64_t lease,
                            const int64_t rs_term, const bool force)
{
    int ret = OB_SUCCESS;
    OB_ASSERT(NULL != root_server_); //add pangtianze [Paxos] 20170717
    tbsys::CThreadGuard guard(&nodes_mutex_);
    if (OB_UNLIKELY(force && rs_term >= get_my_term()))
    {
      //receive force leader broadcast and higher term
      set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
      set_my_node_info(lease, true, addr, rs_term);

      //add chujiajia[paxos]20151012:b
      end_election();
      char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
      get_vote_for().ip_to_string(ip_tmp, sizeof(ip_tmp));
      root_server_->get_config().master_root_server_ip.set_value(ip_tmp);
      root_server_->get_config().master_root_server_port = get_vote_for().get_port();
      if (OB_SUCCESS != (ret = root_server_->get_config_mgr()->dump2file()))
      {
        TBSYS_LOG(WARN, "Dump to file failed! ret=%d", ret);
      }
      //add:e

      TBSYS_LOG(INFO, "follow a new leader, rs leader=%s, rs term=%ld", addr.to_cstring(), rs_term);
    }
    else if(rs_term >= get_my_term())
    {
      //receive no-force leader broadcast and higher term
      if (OB_UNLIKELY(ObRootElectionNode::OB_LEADER == get_my_role()))
      {
        //never should not enter here never
        ret = OB_LEADER_EXISTED;
        role_mgr_->set_state(ObRoleMgr::ERROR);
        TBSYS_LOG(ERROR, "leader received leader broadcast, rs sender=%s, err=%d", to_cstring(addr), ret);
      }
      else if (ObRootElectionNode::OB_CANDIDATE == get_my_role())
      {
        if (!get_has_leader())
        {
          //candidate maybe in vote phase, grant sender broadcast
          set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
          set_my_node_info(lease, true, addr, rs_term);
          //add chujiajia [Paxos rs_election] 20151028:b
          end_election();
          //add:e
          //add chujiajia[Paxos rs_election]20151012:b
          char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
          get_vote_for().ip_to_string(ip_tmp,sizeof(ip_tmp));
          root_server_->get_config().master_root_server_ip.set_value(ip_tmp);
          root_server_->get_config().master_root_server_port = get_vote_for().get_port();
          if (OB_SUCCESS != (ret = root_server_->get_config_mgr()->dump2file()))
          {
            TBSYS_LOG(WARN, "Dump to file failed! ret=%d", ret);
          }
          //add:e
          TBSYS_LOG(INFO, "follow a new leader, rs leader=%s, rs term=%ld", addr.to_cstring(), rs_term);
        }
        else
        {
          //candidate maybe in broadcast phase, refuse other leader boradcast package
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "candidate maybe in broadcast phase, refuse other leader boradcast package! err = %d", ret);
        }
      }
      else if (ObRootElectionNode::OB_FOLLOWER == get_my_role())
      {
        if (!get_has_leader()
            || (get_has_leader() && get_vote_for() == addr))
        {
          set_my_node_info(lease, true, addr, rs_term);
          //add chujiajia [Paxos rs_election] 20151028:b
          end_election();
          //add:e
          //add chujiajia[Paxos rs_election]20151012:b
          char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
          get_vote_for().ip_to_string(ip_tmp,sizeof(ip_tmp));
          root_server_->get_config().master_root_server_ip.set_value(ip_tmp);
          root_server_->get_config().master_root_server_port = get_vote_for().get_port();
          if (OB_SUCCESS != (ret = root_server_->get_config_mgr()->dump2file()))
          {
            TBSYS_LOG(WARN, "Dump to file failed! ret=%d", ret);
          }
          //add:e
          TBSYS_LOG(INFO, "follow a new leader, rs leader=%s, rs term=%ld", addr.to_cstring(), rs_term);

//          //add bingo [Paxos rs ms] 20170517:b //del 20170627
//          std::vector<ObServer> ms_list;
//          while(ms_list.empty())
//          {
//            //usleep(100* 1000);
//            root_server_->get_config_mgr()->get_ms_list(ms_list);
//          }
//          //update ms provider ,then schema service can scan inner table
//          root_server_->get_schema_service_ms_provider()->reset(ms_list);
//          //add:e
        }
        else
        {
          //is followering a leader
          ret = OB_LEADER_EXISTED;
          TBSYS_LOG(WARN, "leader is exist, ret = %d", ret);
        }
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "unknow rs election role, err = %d", ret);
      }
    }
    else
    {
       //lower term, refuse sender broadcast
      ret = OB_INVALID_TERM;
      TBSYS_LOG(WARN, "receive invalid msg!");
    }
  return ret;
}
int ObRootElectionNodeMgr::handle_vote_request(const common::ObServer &addr, const int64_t lease,
                            const int64_t rs_term)
{
    int ret = OB_SUCCESS;
    tbsys::CThreadGuard guard(&nodes_mutex_);
    if (ObRootElectionNode::OB_LEADER == get_my_role())
    {
      ret = OB_LEADER_EXISTED;
      TBSYS_LOG(WARN, "leader received vote request, rs sender=%s, err=%d", to_cstring(addr), ret);
      if (-1 == find_rs_index(addr))
      {
          ret = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(WARN, "rs has been removed from rs list, need re-regist, rs=%s, err=%d", addr.to_cstring(), ret);
      }
    }
    else if (ObRootElectionNode::OB_CANDIDATE == get_my_role())
    {
     if (!get_has_leader() && get_my_term() < rs_term)
      {
        set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
        set_my_node_info(lease, false, addr, rs_term);
        TBSYS_LOG(INFO, "grand vote to a candidate, rs candidate=%s, rs term=%ld", addr.to_cstring(), rs_term);
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "candidate maybe in broadcast phase, refuse other candidate vote request package!");
      }
    }
    else if (ObRootElectionNode::OB_FOLLOWER == get_my_role())
    {
     if (get_has_leader())
      {
        ret = OB_LEADER_EXISTED;
        TBSYS_LOG(WARN, "leader is exist, ret = %d", ret);
      }
      else if (!get_has_leader() && (get_my_term() < rs_term || !get_vote_for().is_valid()))
      {
        //grant sender vote
        set_my_node_info(lease, false, addr, rs_term);
        TBSYS_LOG(INFO, "grand vote to a candidate, rs candidate=%s, rs term=%ld", addr.to_cstring(), rs_term);
      }
      else
      {
        ret = OB_INVALID_TERM;
        TBSYS_LOG(WARN, "vote msg is invalid, ret = %d", ret);
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "unknow rs election role, ret = %d", ret);
    }
  return ret;
}

int ObRootElectionNodeMgr::leader_broadcast(bool force)
{
  int ret = OB_SUCCESS;
  int64_t now = tbsys::CTimeUtil::getTime();
  if (force)
  {
    //change stat to candidate
    set_election_phase_with_lock(false, true);
    raise_my_term();
    set_as_candidate(true);
    start_election();
  }
  // send broadcast msg to other rs
  {
    tbsys::CThreadGuard guard(&nodes_mutex_);
    ObMsgRsLeaderBroadcast msg;
     msg.addr_ = my_node_.my_addr_;
     msg.term_ = get_my_term();
     msg.lease_ = now + lease_duration_us_;
     for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
     {
       if (other_nodes_[i].is_alive_)
       {
         ObRootElectionNode &node = other_nodes_[i];
         // send broadcast message
         if (OB_SUCCESS != (ret = rpc_stub_.send_leader_broadcast(node.my_addr_, msg, force)))
          {
              TBSYS_LOG(WARN, "failed to send leader broadcast msg, err=%d, targer rootserver=%s",
                  ret, node.my_addr_.to_cstring());
          }
        }
     }
  }
  return ret;
}
int ObRootElectionNodeMgr::request_votes()
{
  int ret = OB_SUCCESS;
  int64_t now = tbsys::CTimeUtil::getTime();
  tbsys::CThreadGuard guard(&nodes_mutex_);
  // set local node info before request vote
  raise_my_term();
  TBSYS_LOG(INFO, "new rs term=%ld", get_my_term());
  vote_count_ = 1;
  my_node_.vote_result_ = 0;
  // send vote request to other rs
  ObMsgRsRequestVote msg;
  msg.addr_ = my_node_.my_addr_;
  msg.lease_ = now + lease_duration_us_;
  msg.term_ = get_my_term();
  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].is_alive_)
    {
      ObRootElectionNode &node = other_nodes_[i];
      // send votes
      if (OB_SUCCESS != (ret = rpc_stub_.send_vote_request(node.my_addr_, msg)))
      {
        TBSYS_LOG(WARN, "failed to send vote request msg, err=%d, target rootserver=%s",
                  ret, node.my_addr_.to_cstring());
      }
    }
  }
  return ret;
}
int ObRootElectionNodeMgr::handle_leader_broadcast_resp(const ObMsgRsLeaderBroadcastResp &lb_resp)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&nodes_mutex_);

    int idx = find_rs_index(lb_resp.addr_);
    if (OB_INVALID_INDEX != idx)
    {
      other_nodes_[idx].bd_result_ = lb_resp.is_granted_?0:1;
      other_nodes_[idx].current_rs_term_ = lb_resp.term_;
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "rs not exist, rs=%s", lb_resp.addr_.to_cstring());
    }
    if (OB_SUCCESS == ret)
    {
      broadcast_count_ = 1;
      for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
      {
        if (other_nodes_[i].is_alive_
            && other_nodes_[i].current_rs_term_ == get_my_term()
            && other_nodes_[i].bd_result_ == 0)
        {
          ++broadcast_count_;
        }
      }
      if (broadcast_count_ > (get_rs_paxos_number() / 2))
      {
        //leader do broadcast success
        ret = OB_SUCCESS;
        set_broadcast_result(get_my_term(), true);
        TBSYS_LOG(INFO, "candidate has get most broadcast confirm from all rootservers, confirm count=%d, paxos number=%d",
                  broadcast_count_, get_rs_paxos_number());
      }
      else
      {
        // do not get most leader broadcast grant result
        set_broadcast_result(get_my_term(), false);
      }
    }
  return ret;
}

int ObRootElectionNodeMgr::handle_vote_request_resp(const ObMsgRsRequestVoteResp &rv_resp)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != root_server_); //add pangtianze [Paxos] 20170717
  tbsys::CThreadGuard guard(&nodes_mutex_);
    int idx = find_rs_index(rv_resp.addr_);
    if (!rv_resp.is_granted_ && rv_resp.need_regist_) //this branch is for slave rs re-regist to master, after net partition recover
    {
      TBSYS_LOG(INFO, "candidate finds leader, leader rs_term=%ld, local rs_term=%ld, switch to follower", rv_resp.term_, get_my_term());
      set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
      set_my_term((rv_resp.term_));
      common::ObFetchParam fetch_param;
      int64_t new_paxos_num = 0;
      int64_t new_quorum_scale = 0;
      int err = rpc_stub_.slave_register(rv_resp.addr_, my_node_.my_addr_, fetch_param, new_paxos_num,
                                                new_quorum_scale , root_server_->get_config().slave_register_timeout);
      if (OB_SUCCESS == err)
      {
          ///set new param
          root_server_->set_paxos_num(new_paxos_num);
          root_server_->set_quorum_scale(new_quorum_scale);
      }
    }
    else if (OB_INVALID_INDEX != idx && get_my_term() >= rv_resp.term_)
    {
      other_nodes_[idx].vote_result_ = rv_resp.is_granted_?0:1;
      other_nodes_[idx].current_rs_term_ = rv_resp.term_;
    }
    else if (!rv_resp.is_granted_ && get_my_term() < rv_resp.term_)
    {
      //find higher term
      ret = OB_ERROR;
      set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
      set_my_term(rv_resp.term_);
      TBSYS_LOG(INFO, "candidate finds higher rs_term=%ld, switch to follower", rv_resp.term_);
    }
    if (OB_SUCCESS == ret)
    {
      vote_count_ = 1;
      for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
      {
        if (other_nodes_[i].is_alive_
            && other_nodes_[i].current_rs_term_ == get_my_term()
            && other_nodes_[i].vote_result_ == 0)
        {
          ++vote_count_;
        }
      }
      if (vote_count_ > (get_rs_paxos_number() / 2))
      {
        // vote request phase success
        ret = OB_SUCCESS;
        set_vote_result(get_my_term(), true);
        TBSYS_LOG(INFO, "candidate has get most votes from all rootservers, vote count=%d, paxos number=%ld",
                  vote_count_, rs_paxos_number_);
      }
      else
      {
        // do not get most vote request grant result
        set_vote_result(get_my_term(), false);
      }
    }
  return ret;

}

//add pangtianze [Paxos rs_election] 20150813:b
int ObRootElectionNodeMgr::start_leader_broadcast(bool force)
{
  int ret = OB_SUCCESS;
  if (root_server_ == NULL)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "root_server_ is null");
  }
  else if (root_server_->get_worker() == NULL)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "worker_ is null");
  }
  else
  {
    ret = root_server_->get_worker()->submit_leader_broadcast_task(force);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "failed to submit leader broadcast task, is_force=%d, err=%d", force, ret);
    }
  }
  return ret;
}
int ObRootElectionNodeMgr::start_request_votes()
{
  int ret = OB_SUCCESS;
  if (root_server_ == NULL)
  {
    //add chujiajia [Paxos rs_election] 20151111:b
    ret = OB_NOT_INIT;
    //add:e
    TBSYS_LOG(ERROR, "root_server_ is null");
  }
  else if (root_server_->get_worker() == NULL)
  {
    //add chujiajia [Paxos rs_election] 20151111:b
    ret = OB_NOT_INIT;
    //add:e
    TBSYS_LOG(ERROR, "worker_ is null");
  }
  else
  {
    ret = root_server_->get_worker()->submit_vote_request_task();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "failed to submit vote request task, err=%d", ret);
    }
  }
  return ret;
}
//add:e

void ObRootElectionNodeMgr::set_vote_request_resp(ObMsgRsRequestVoteResp &rv_resp, const bool granted, const bool need_regist)
{
  rv_resp.is_granted_ = granted;
  rv_resp.need_regist_ = need_regist;
  rv_resp.addr_ = my_node_.my_addr_;
  rv_resp.term_ = get_my_term();
}
void ObRootElectionNodeMgr::set_leader_broadcast_resp(ObMsgRsLeaderBroadcastResp &lb_resp, const bool granted)
{
  lb_resp.is_granted_ = granted;
  lb_resp.addr_ = my_node_.my_addr_;
  lb_resp.term_ = get_my_term();
}

int ObRootElectionNodeMgr::get_leader(common::ObServer &leader)
{
  int ret = OB_SUCCESS;
  if (get_has_leader() && get_vote_for().is_valid()
          && election_state_ == RS_ELECTION_DONE)
  {
    leader = get_vote_for();
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "no leader exist, rs term=%ld, election state=%s", get_my_term(), get_election_stat_str());
  }
  return ret;
}          

int32_t ObRootElectionNodeMgr::get_rs_paxos_number() const
{
  return (int32_t)rs_paxos_number_;
}
bool ObRootElectionNodeMgr::need_leader_change_role(const int64_t now)
{
  bool ret = false;
  OB_ASSERT(NULL != root_server_); //add pangtianze [Paxos] 20170717
  //if all hb_resp_timestamp_ == 0, means this is a new leader, return false
  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
  {
    if (/*other_nodes_[i].is_alive_ && del pangtianze*/other_nodes_[i].hb_resp_timestamp_ != 0)
    {
      ret = true;
      break;
    }
  }

  //rs_paxos_number == 1 means leader do not need care hb resp
  if (ret && get_rs_paxos_number() > 1)
  {
    int32_t rs_count = (int32_t)get_rs_paxos_number() - 1; //count of array object
    sort_hb_timestamp_asc();
    //compare middle timestamp and now
    int64_t ts = get_middle_timestamp(rs_count);
    if (ts < 0)
    {
      ret = false;
      TBSYS_LOG(WARN, "invalid rs heartbeat timestamp, hb=%ld", ts);
    }
    else if (ts + no_hb_response_duration_us_ > now)
    {
      ret = false;
      //add chujiajia [Paxos rs_election] 20151203:b
      for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1 ; i++)
      {
        if(other_nodes_[i].is_alive_ && (other_nodes_[i].hb_resp_timestamp_ == 0 || (other_nodes_[i].hb_resp_timestamp_ + lease_duration_us_ < now)))
        {
          //find slave rootserver offline
          other_nodes_[i].is_alive_ = false;
          root_server_->commit_task(SERVER_OFFLINE, OB_ROOTSERVER, other_nodes_[i].my_addr_, 0, "hb server version null");
          TBSYS_LOG(INFO, "rootserver[%s] -> offline",other_nodes_[i].my_addr_.to_cstring());
        }
      }
      //add:e
    }
    else
    {
      ret = true;
      TBSYS_LOG(WARN, "leader has failed to keep contact with most rs");
    }
  }
  else if (ret && get_rs_paxos_number() == 1)
  {
      ret = false;
      //add chujiajia [Paxos rs_election] 20151203:b
      for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1 ; i++)
      {
        if(other_nodes_[i].is_alive_ && (other_nodes_[i].hb_resp_timestamp_ == 0 || (other_nodes_[i].hb_resp_timestamp_ + lease_duration_us_ < now)))
        {
          //find slave rootserver offline
          other_nodes_[i].is_alive_ = false;
          root_server_->commit_task(SERVER_OFFLINE, OB_ROOTSERVER, other_nodes_[i].my_addr_, 0, "hb server version null");
          TBSYS_LOG(INFO, "rootserver[%s] -> offline",other_nodes_[i].my_addr_.to_cstring());
        }
      }
      //add:e
  }
  else if (ret && get_rs_paxos_number() < 1)
  {
    ret = false;
    TBSYS_LOG(WARN, "unexpected paxos number: [%d]" ,get_rs_paxos_number());
  }
  else
  {
    ret = false;
  }
  return ret;

}
//notice, rs count in other_nodes must < deployed count
void ObRootElectionNodeMgr::sort_hb_timestamp_asc()
{
  int alive_rs_count = 0;
  reset_hb_timestamp_array();
  // 1. put all alive rs heartbeat timestamp into array
  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].is_alive_)
    {
      rs_hb_timestamp_array_[alive_rs_count]
          = other_nodes_[i].hb_resp_timestamp_;
      alive_rs_count++;
    }
  }
  // 2. bubble sort asc
  int64_t temp = 0;
  int32_t k = (int32_t)get_rs_paxos_number() - 2;// k = count_for_sort - 1
  while(k > 0)
  {
    int pos = 0;
    for (int32_t j = 0; j < k; j++)
    {
      if (rs_hb_timestamp_array_[j] > rs_hb_timestamp_array_[j + 1])
      {
        pos = j;
        temp = rs_hb_timestamp_array_[j];
        rs_hb_timestamp_array_[j] = rs_hb_timestamp_array_[j + 1];
        rs_hb_timestamp_array_[j + 1] = temp;
      }
    }
    k = pos;
  }
}
int64_t ObRootElectionNodeMgr::get_middle_timestamp(const int alive_rs_count)
{
  int64_t ts = -1;
  int idx = 0;
  if (alive_rs_count > 0)
  {
    idx = alive_rs_count / 2; // alive_rs_count/2+1-1
    ts = rs_hb_timestamp_array_[idx];
  }
  else
  {
    TBSYS_LOG(WARN, "no other rs alive");
  }
  return ts;
}

void ObRootElectionNodeMgr::reset_hb_timestamp_array()
{
  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
  {
    rs_hb_timestamp_array_[i] = 0;
  }
}

//add pangtianze [Paxos rs_election] 20150731:b
void ObRootElectionNodeMgr::print_leader(char *buf, const int64_t buf_len, int64_t &pos)
{
    //add pangtianze [Paxos] 20170717:b
    if (NULL == buf)
    {
        TBSYS_LOG(ERROR, "buf is null");
    }
    //add:e
    else if (election_state_ == RS_ELECTION_DOING)
    {
      databuff_printf(buf, buf_len, pos, "rs is doing election, please wait and do again later");
    }
    else
    {
      common::ObServer leader;
      get_leader(leader);
      databuff_printf(buf, buf_len, pos, "rs election done, leader rootserver[%s]", leader.to_cstring());
    }
}
void ObRootElectionNodeMgr::if_all_paxos_changed(char *buf, const int64_t buf_len, int64_t& pos, const int32_t local_paxos_num) const
{
  //add chujiajia [Paxos rs_election] 20151230:b
  int32_t index = OB_INVALID_INDEX;
  //add:e
  //add pangtianze [Paxos] 20170717:b
  if (NULL == buf)
  {
      TBSYS_LOG(ERROR, "buf is null");
  }
  //add:e
  else if (ObRootElectionNode::OB_LEADER != get_my_role())
  {
    databuff_printf(buf, buf_len, pos, "target rootserver is not leader, please send command to leader, my paxos_num = %d", get_rs_paxos_number());
  }
  else
  {
    tbsys::CThreadGuard guard(&nodes_mutex_);
    bool all_changed = true;
    for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
    {
      if (other_nodes_[i].is_alive_ && other_nodes_[i].paxos_num_ != local_paxos_num)
      {
        all_changed = false;
        //add chujiajia [Paxos rs_election] 20151230:b
        index = i;
        //add:e
        TBSYS_LOG(WARN, "paxos num not be changed, rs[%s], paxos_num[%d]",
                  other_nodes_[i].my_addr_.to_cstring(), other_nodes_[i].paxos_num_);
        break;
      }
    }
    //add pangtianze [Paxos rs_election] 20161010:b
    if (get_rs_paxos_number() != local_paxos_num)
    {
       all_changed = false;
       TBSYS_LOG(WARN, "paxos num not be changed, rs[%s], paxos_num[%d]",
                 my_node_.my_addr_.to_cstring(), my_node_.paxos_num_);
    }
    //add:e
    if (all_changed)
    {
      databuff_printf(buf, buf_len, pos, "paxos num in all rs are same, current_paxos_num=%d", get_rs_paxos_number());
    }
    else
    {
      databuff_printf(buf, buf_len, pos, "paxos num in some rootservers, such as [%s], are different from the leader, leader paxos num=%d, Please do change_command again!",
                      other_nodes_[index].my_addr_.to_cstring(), get_rs_paxos_number());
    }
  }
}
//add:e
//add pangtianze [Paxos rs_election] 20161010:b
bool ObRootElectionNodeMgr::check_all_rs_alive()
{
  bool ret = false;
  int32_t count = 1; // obviously, self rs is alive
  tbsys::CThreadGuard guard(&nodes_mutex_);

  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].is_alive_)
    {
       count ++;
    }
  }
  if (count == get_rs_paxos_number())
  {
    ret = true;
  }
  return ret;
}
void ObRootElectionNodeMgr::if_all_quorum_in_rs_changed(char *buf, const int64_t buf_len, int64_t& pos, const int32_t local_quorum_scale) const
{
    //add chujiajia [Paxos rs_election] 20151230:b
    int32_t index = OB_INVALID_INDEX;
    //add:e
    //add pangtianze [Paxos] 20170717:b
    if (NULL == buf)
    {
        TBSYS_LOG(ERROR, "buf is null");
    }
    //add:e
    else
    {
      tbsys::CThreadGuard guard(&nodes_mutex_);
      bool all_changed = true;
      for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
      {
        if (other_nodes_[i].is_alive_ && other_nodes_[i].quorum_scale_ != local_quorum_scale)
        {
          all_changed = false;
          //add chujiajia [Paxos rs_election] 20151230:b
          index = i;
          //add:e
          TBSYS_LOG(WARN, "quorum scale not be changed, rs[%s], quorum_scale[%d]",
                    other_nodes_[index].my_addr_.to_cstring(), other_nodes_[index].quorum_scale_);
          break;
        }
      }
      if (all_changed)
      {
        databuff_printf(buf, buf_len, pos, "quorum scale in all rs are same, current_quorum_scale=%ld\n", root_server_->get_ups_manager()->get_max_deployed_ups_count());
      }
      else
      {
        databuff_printf(buf, buf_len, pos,
                        "quorum scale in some rootservers, such as [%s], are different from the leader, leader quorum scale=%ld, Please do change_command again!\n",
                        other_nodes_[index].my_addr_.to_cstring(), root_server_->get_ups_manager()->get_max_deployed_ups_count());
      }
    }
}
//add:e
void ObRootElectionNodeMgr::start_election()
{
  set_election_state(RS_ELECTION_DOING);
  //add pangtianze [Paxos rs_election] 20160911:b
  int64_t now = tbsys::CTimeUtil::getTime();
  atomic_exchange((uint64_t*)&begin_time_, (uint64_t)now);
  TBSYS_LOG(INFO, "start rootserver election, rs term=%ld, start time=%ld", get_my_term(), begin_time_);
  //add:e
}
void ObRootElectionNodeMgr::end_election()
{
  set_election_state(RS_ELECTION_DONE);
  end_time_ = tbsys::CTimeUtil::getTime();
  ObServer leader;
  if (OB_SUCCESS == get_leader(leader))
  {
    TBSYS_LOG(INFO, "end rootserver election, rs term=%ld, leader rootserver=%s, start time=%ld, end time=%ld, duration time=[%ld]ms",
              get_my_term(), leader.to_cstring(), begin_time_, end_time_, (end_time_ - begin_time_)/1000);
  }
  //add chujiajia [Paxos rs_election] 20151111:b
  else
  {
    TBSYS_LOG(WARN, "end rootserver election, but can not get leader info!");
  }
  //add:e
}
void ObRootElectionNodeMgr::get_vote_result(int64_t &term, bool &result)
{
  common::ObSpinLockGuard guard(vote_result_lock_);
  term = vote_result_.term_;
  result = vote_result_.result_;
}
void ObRootElectionNodeMgr::get_broadcast_result(int64_t &term, bool &result)
{
  common::ObSpinLockGuard guard(broadcast_result_lock_);
  term = vote_result_.term_;
  result = vote_result_.result_;
}
void ObRootElectionNodeMgr::set_vote_result(const int64_t term, const bool result)
{
  common::ObSpinLockGuard guard(vote_result_lock_);
  vote_result_.term_ = term;
  vote_result_.result_ = result;
}
void ObRootElectionNodeMgr::set_broadcast_result(int64_t const term, const bool result)
{
  common::ObSpinLockGuard guard(broadcast_result_lock_);
  vote_result_.term_ = term;
  vote_result_.result_ =result;
}

//add chujiajia [Paxos rs_election] 20151105:b
void ObRootElectionNodeMgr::reset_election_info()
{
  int64_t now = tbsys::CTimeUtil::getTime();
  atomic_exchange((uint64_t*)&begin_time_, (uint64_t)now);
  set_election_phase_with_lock(false, false);
  set_vote_result(0, false);
  set_broadcast_result(0, false);
  for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
  {
    other_nodes_[i].vote_result_ = -1;
    other_nodes_[i].bd_result_ = -1;
    other_nodes_[i].current_rs_term_ = 0;
  }
}
//add:e
bool ObRootElectionNodeMgr::check_most_rs_alive()
{
  bool ret = false;
  int32_t count = 1; // obviously, self rs is alive
  tbsys::CThreadGuard guard(&nodes_mutex_);

  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].is_alive_)
    {
       count ++;
    }
  }
  if (count > get_rs_paxos_number()/2)
  {
    ret = true;
  }
  return ret;
}
int ObRootElectionNodeMgr::excute_election_vote(const int64_t now)
{
  int ret = OB_SUCCESS;
  broadcast_doing_ = false;
  vote_request_doing_ = true;

  //election lease expired
  if (election_vote_lease_us_ < now)
  {
     election_vote_lease_us_ = now + election_timeout_us_ + get_random_time();
     ret = start_request_votes();
     if (OB_SUCCESS != ret)
     {
       TBSYS_LOG(ERROR, "start vote request failed, err=%d", ret);
     }
  }
  return ret;
}
int ObRootElectionNodeMgr::excute_election_broadcast(const int64_t now)
{
  int ret = true;
  vote_request_doing_ = false;
  broadcast_doing_ = true;
  //election lease expired
  if (election_broadcast_lease_us_ < now)
  {
     election_broadcast_lease_us_ = now + election_timeout_us_;
     ret = start_leader_broadcast();
     if (OB_SUCCESS != ret)
     {
       TBSYS_LOG(ERROR, "start leader broadcast failed, err=%d", ret);
     }
  }
  return ret;
}

bool ObRootElectionNodeMgr::success_vote()
{
  bool is_success = false;
  int64_t term = 0;
  bool vote_result = false;
  get_vote_result(term, vote_result);
  if (vote_result && term == get_my_term())
  {
    TBSYS_LOG(INFO, "request vote success");
    is_success = true;
  }
  return is_success;
}

bool ObRootElectionNodeMgr::success_broadcast()
{
  bool is_success = false;
  int64_t term = 0;
  bool broadcast_result = false;
  get_broadcast_result(term, broadcast_result);
  if (broadcast_result && term == get_my_term())
  {
    TBSYS_LOG(INFO, "leader broadcast success");
    is_success = true;
  }
  return is_success;
}

int ObRootElectionNodeMgr::check_lease()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != root_server_); //add pangtianze [Paxos] 20170717
  OB_ASSERT(NULL != role_mgr_); //add pangtianze [Paxos] 20170717
  tbsys::CThreadGuard guard(&nodes_mutex_);
  int64_t now = tbsys::CTimeUtil::getTime();
  // [rs is leader]
  // check most rs exist or not, if not, leader need to change role, switch to follower;
  // else, do nothing
  if (ObRootElectionNode::OB_LEADER == get_my_role())
  {
     //change hb response to decide change role or not
     if (!need_leader_change_role(now))
     {
       //add pangtianze [Paxos rs_election] 20160918:b
       set_can_select_master_ups(true);
       //add:e
       my_node_.lease_ = now + leader_lease_duration_us_; // update self lease
     }
     else
     {
       //add pangtianze [Paxos rs_election] 20160918:b
       //if leader need change role, it can not select master ups
       set_can_select_master_ups(false);
       //add:e
       if (now > my_node_.lease_)
       {
         //lease expired, switch to follower
         set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
         reset_vote_for();
         set_has_leader(false);
         //reset_all_other_nodes(); //no need to reset
         TBSYS_LOG(INFO, "leader begin to switch to follower");
         //add pangtianze [Paxos rs_election] 20170628
         print_lease_info(true);
         //add:e
         if (OB_SUCCESS != (ret = root_server_->switch_master_to_slave()))
         {
           role_mgr_->set_state(ObRoleMgr::STOP);
           TBSYS_LOG(ERROR, "rootserver switch to slave failed, will stop");
         }
         else
         {
           TBSYS_LOG(INFO, "rootserver switch to slave success");
         }
       }
     }
  }
  // [rs is candidate]
  // excute election again;
  else if (ObRootElectionNode::OB_CANDIDATE == get_my_role())
  {
    if (now > begin_time_ + normal_election_duration_us_)
    {
       TBSYS_LOG(WARN, "election duration is too long, begin time=%ld, now=%ld, will switch to follower",
                 begin_time_, now);
       //add pangtianze [Paxos rs_election] 20161107:b
       set_local_node_role(ObRootElectionNode::OB_FOLLOWER);
       reset_vote_for();
       set_has_leader(false);
       set_election_phase_with_lock(false, false);
       //add:e
    }
    if (get_rs_paxos_number() > 1)
    {
      tbsys::CThreadGuard guard(&mutex_);
      if (vote_request_doing_ && !broadcast_doing_)
      {
        if (success_vote())
        {
          TBSYS_LOG(INFO, "candidate request vote successed, will begin leader broadcast.");

          ret = excute_election_broadcast(now);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "leader broadcast error, err=%d", ret);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "excute election failed, then will start a new election");
          ret = excute_election_vote(now);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "vote request error, err=%d", ret);
          }
        }
      }
      if (!vote_request_doing_ && broadcast_doing_)
      {
        if (success_broadcast())
        {
          broadcast_doing_ = false;
          TBSYS_LOG(INFO, "candidate broadcast successed, will change to leader.");
          set_as_leader(my_node_.my_addr_);
          role_mgr_->set_state(ObRoleMgr::SWITCHING);//switch slave to master
          end_election();
          TBSYS_LOG(INFO, "elecion success!");
        }
        else
        {
          broadcast_doing_ = false;
          TBSYS_LOG(WARN, "excute election failed, then will start a new election");
          ret = excute_election_vote(now);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "vote request error, err=%d", ret);
          }
        }
      }
    }
  }
  // [rs is follower]
  // check lease, if expired, switch to candidate, and then excute_election;
  // else, sleep some time
  else if (ObRootElectionNode::OB_FOLLOWER == get_my_role())
  {
    //follower must be slave, otherwise, change to slave from master
    if (ObRoleMgr::MASTER == role_mgr_->get_role())
    {
      TBSYS_LOG(INFO, "leader begin to switch to follower");
      if (OB_SUCCESS != (ret = root_server_->switch_master_to_slave()))
      {
        role_mgr_->set_state(ObRoleMgr::STOP);
        TBSYS_LOG(ERROR, "rootserver switch to slave failed, will stop, err = %d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "rootserver switch to slave success");
      }
    }
    else
    {
      if (get_rs_paxos_number() > 1)
      {
        if (now > my_node_.lease_ + MAX_CLOCK_SKEW_US) //lease expired
        {
          //add pangtianze [Paxos rs_election] 20170628
          print_lease_info(false);
          //add:e
          //add chujiajia [Paxos rs_election] 20151028:b
          reset_election_info();
          //add:e
          start_election();
          set_election_phase_with_lock(true, false);
          set_as_candidate(false);
        }
      }
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "unknow rs election role in rs check thread");
  }
  return ret;
}
//add pangtianze [Paxos bugfix:old leader still in __all_server after it is down] 20161108:b
int32_t ObRootElectionNodeMgr::get_rs_cluster_id(const ObServer &server)
{
  int32_t cluster_id = 0;
  for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].my_addr_ == server)
    {
        cluster_id = other_nodes_[i].my_addr_.cluster_id_;
        break;
    }
  }
  return cluster_id;
}
//add:e
//add pangtianze [Paxos rs_electoin] 20170628:b
void ObRootElectionNodeMgr::print_lease_info(const bool is_leader)
{
    if (is_leader)
    {
        TBSYS_LOG(INFO, "leader lease=%ld", my_node_.lease_);
        for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
        {
          if (other_nodes_[i].my_addr_.is_valid() && other_nodes_[i].is_alive_)
          {
              TBSYS_LOG(INFO, "follower=%s, lease=%ld", other_nodes_[i].my_addr_.to_cstring(), other_nodes_[i].lease_);
          }
          else if (other_nodes_[i].my_addr_.is_valid())
          {
              TBSYS_LOG(INFO, "follower=%s, lease=%ld, not alive", other_nodes_[i].my_addr_.to_cstring(), other_nodes_[i].lease_);
          }
        }
    }
    else
    {
       TBSYS_LOG(INFO, "my lease=%ld", my_node_.lease_);
    }
}

//ad:e
int64_t ObRootElectionNodeMgr::get_random_time()
{
  int64_t now = tbsys::CTimeUtil::getTime();
  int64_t random_time = 0;
  srand((unsigned int)now);
  random_time = rand()%100000 + 100000; //100ms~200ms
  TBSYS_LOG(INFO, "election random %ldms", random_time/1000);
  return random_time;
}

void ObRootElectionNodeMgr::get_all_alive_servers(ObServer *servers, int32_t &server_count)
{
  server_count = 0;
  //add pangtianze [Paxos] 20170717
  if (NULL == servers)
  {
      TBSYS_LOG(ERROR, "servers array ptr is null");
  }
  //add:e
  else
  {
      tbsys::CThreadGuard guard(&nodes_mutex_);
      for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
      {
        if (other_nodes_[i].is_alive_ && other_nodes_[i].my_addr_.is_valid())
        {
          servers[server_count] = other_nodes_[i].my_addr_;
          server_count++;
        }
      }
      if (my_node_.my_addr_.is_valid())
      {
        servers[server_count] = my_node_.my_addr_;
        server_count++;
      }
  }
}
//add pangtianze [Paxos] 20170705:b
void ObRootElectionNodeMgr::get_all_servers(common::ObServer *servers, int32_t &server_count)
{
    server_count = 0;
    //add pangtianze [Paxos] 20170717
    if (NULL == servers)
    {
        TBSYS_LOG(ERROR, "servers array ptr is null");
    }
    //add:e
    else
    {
        tbsys::CThreadGuard guard(&nodes_mutex_);
        for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
        {
          if (other_nodes_[i].my_addr_.is_valid())
          {
            servers[server_count] = other_nodes_[i].my_addr_;
            server_count++;
          }
        }
        if (my_node_.my_addr_.is_valid())
        {
          servers[server_count] = my_node_.my_addr_;
          server_count++;
        }
    }
}
//add:e
int32_t ObRootElectionNodeMgr::get_rs_slave_count()
{
  int32_t server_count = 0;
  tbsys::CThreadGuard guard(&nodes_mutex_);
  for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].is_alive_ && other_nodes_[i].my_addr_.is_valid())
    {
      server_count++;
    }
  }
  return server_count;
}
int ObRootElectionNodeMgr::refresh_inner_table(const ObTaskType type, const common::ObServer &rs, const int32_t status, const char *server_version)
{
  int ret = OB_SUCCESS;
  if (queue_ != NULL)
  {
    ObRootAsyncTaskQueue::ObSeqTask task;
    task.type_ = type;
    task.role_ = OB_ROOTSERVER;
    task.server_ = rs;
    task.inner_port_ = 0;
    // set as slave
    task.server_status_ = status;
    // set as master
    int64_t server_version_length = strlen(server_version);
    if (server_version_length < OB_SERVER_VERSION_LENGTH)
    {
      strncpy(task.server_version_, server_version, server_version_length + 1);
    }
    else
    {
      strncpy(task.server_version_, server_version, OB_SERVER_VERSION_LENGTH - 1);
      task.server_version_[OB_SERVER_VERSION_LENGTH - 1] = '\0';
    }
    ret = queue_->push(task);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "push update server task failed:server[%s], type[%d], ret[%d]",
          task.server_.to_cstring(), task.type_, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "push update server task succ:server[%s]", task.server_.to_cstring());
    }
  }
  return ret;
}


int ObRootElectionNodeMgr::handle_rs_heartbeat_resp(const common::ObMsgRsHeartbeatResp &hb_resp, const int64_t time)
{
  int ret = OB_SUCCESS;
  int32_t idx = OB_INVALID_INDEX;
  tbsys::CThreadGuard guard(&nodes_mutex_);
  if (get_my_term() < hb_resp.current_term_)
  {
    //if term from follower high than leader, update leader term but do not change role
    set_my_term(hb_resp.current_term_);
    TBSYS_LOG(WARN, "find higher term from heartbeat response, set leader term as %ld", get_my_term());
  }
  if (OB_INVALID_INDEX == (idx = find_rs_index(hb_resp.addr_)))
  {
    TBSYS_LOG(WARN, "rs not exist, addr=%s", hb_resp.addr_.to_cstring());
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
      //set info according hb response
     set_node_alive_stat(other_nodes_[idx], true);
     other_nodes_[idx].hb_resp_timestamp_ = time;
     atomic_exchange((uint32_t*)&other_nodes_[idx].paxos_num_, hb_resp.paxos_num_);
     TBSYS_LOG(DEBUG, "update rs heartbeat response time and paxos num, rs addr=%s, timestamp=%ld, paxos num=%d", hb_resp.addr_.to_cstring(), time, hb_resp.paxos_num_);
     atomic_exchange((uint32_t*)&other_nodes_[idx].quorum_scale_, hb_resp.quorum_scale_);
     TBSYS_LOG(DEBUG, "update rs heartbeat response time and quorum scale, rs addr=%s, timestamp=%ld, quorum scale=%d", hb_resp.addr_.to_cstring(), time, hb_resp.quorum_scale_);
  }
  return ret;
}

//add pangtianze [Paxos rs_election] 20161010:b
int ObRootElectionNodeMgr::handle_change_config_request(const common::ObServer &addr, const int64_t rs_term, const int32_t new_paxos_num)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != root_server_); //add pangtianze [Paxos] 20170717
  if (ObRootElectionNode::OB_LEADER == get_my_role())
  {
    ret = OB_LEADER_EXISTED;
    TBSYS_LOG(WARN, "leader received change config request, rs sender=%s, err=%d", to_cstring(addr), ret);
  }
  else if (ObRootElectionNode::OB_CANDIDATE == get_my_role()
           || ObRootElectionNode::OB_FOLLOWER == get_my_role())
  {
    tbsys::CThreadGuard guard(&nodes_mutex_);
    if(rs_term >= get_my_term())
    {
      set_my_term(rs_term);
      //add bingo [Paxos rs management] 20170217:b
      if(1 == new_paxos_num)
      {
        TBSYS_LOG(INFO, "rootserver paxos number is set as 1, so slave should stop");
        role_mgr_->set_state(ObRoleMgr::STOP);
      }
      else
      {
      //add:m
        my_node_.paxos_num_ = new_paxos_num;
        root_server_->set_paxos_num(new_paxos_num);
        TBSYS_LOG(INFO, "Rootserver save new paxos num, new_paxos_num = %d, rs_paxos_number = %d", new_paxos_num, get_rs_paxos_number());
      }
      //add:e
    }
    else
    {
      ret = OB_INVALID_TERM;
      TBSYS_LOG(WARN, "change_config_request is msg invalid!, err = %d", ret);
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "unknow rs election role");
  }
  //add pangtianze [Paxos rs_election] 20170712:b
  {
     TBSYS_LOG(INFO, "slave remove invalid rootserver info");
     root_server_->get_rs_node_mgr()->remove_invalid_info();
  }
  //add:e
  return ret;
}
//add:e

//add lbzhong [Paxos Cluster.Balance] 20160708:b
void ObRootElectionNodeMgr::is_cluster_alive_with_rs(bool *is_cluster_alive) const
{
  //add pangtianze [Paxos] 20170717:b
  if (NULL == is_cluster_alive)
  {
      TBSYS_LOG(ERROR, "is_cluster_aliave array ptr is null");
  }
  //add:e
  else
  {
      tbsys::CThreadGuard guard(&nodes_mutex_);
      for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
      {
        if (other_nodes_[i].my_addr_.is_valid() && other_nodes_[i].is_alive_
            && !is_cluster_alive[other_nodes_[i].my_addr_.cluster_id_])
        {
          is_cluster_alive[other_nodes_[i].my_addr_.cluster_id_] = true;
        }
      }
      if (my_node_.my_addr_.is_valid() && !is_cluster_alive[my_node_.my_addr_.cluster_id_])
      {
        is_cluster_alive[my_node_.my_addr_.cluster_id_] = true;
      }
  }
}

bool ObRootElectionNodeMgr::is_cluster_alive_with_rs(const int32_t cluster_id) const
{
  tbsys::CThreadGuard guard(&nodes_mutex_);
  if (my_node_.my_addr_.is_valid() && my_node_.my_addr_.cluster_id_ == cluster_id)
  {
    return true;
  }
  for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].my_addr_.is_valid() && other_nodes_[i].is_alive_
        && other_nodes_[i].my_addr_.cluster_id_ == cluster_id)
    {
      return true;
    }
  }
  return false;
}

int ObRootElectionNodeMgr::get_one_slave_rs(ObServer &slaver_rs, const int32_t cluster_id) const
{ //mod bingo [Paxos Cluster.Balance bug fix result code] 20160923
  int ret = OB_ERROR;
  tbsys::CThreadGuard guard(&nodes_mutex_);
  for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
  {
    if (other_nodes_[i].my_addr_.is_valid() && other_nodes_[i].is_alive_
        && cluster_id == other_nodes_[i].my_addr_.cluster_id_)
    {
      slaver_rs = other_nodes_[i].my_addr_;
      ret=OB_SUCCESS;
      //mod:e
      break;
    }
  }
  return ret;
}

int ObRootElectionNodeMgr::get_one_rs(ObServer &rs, const int32_t cluster_id) const
{ //mod bingo [Paxos Cluster.Balance bug fix result code] 20160923
  int ret = OB_ERROR;
  tbsys::CThreadGuard guard(&nodes_mutex_);
  if(my_node_.my_addr_.is_valid() && cluster_id == my_node_.my_addr_.cluster_id_)
  {
    rs = my_node_.my_addr_;
    ret = OB_SUCCESS;
  }
  else
  {
    for (int32_t i = 0; i< OB_MAX_RS_COUNT - 1; i++)
    {
      if (other_nodes_[i].my_addr_.is_valid() && other_nodes_[i].is_alive_
          && cluster_id == other_nodes_[i].my_addr_.cluster_id_)
      {
        rs = other_nodes_[i].my_addr_;
        ret = OB_SUCCESS;
        //mod:e
        break;
      }
    }
  }
  return ret;
}
//add:e

//add pangtianze [Paxos rs_election] 20170707:b
void ObRootElectionNodeMgr::remove_invalid_info()
{
  tbsys::CThreadGuard guard(&nodes_mutex_);

  for (int32_t i = 0; i < OB_MAX_RS_COUNT - 1; i++)
  {
    ObServer rs = other_nodes_[i].my_addr_;
    if (other_nodes_[i].my_addr_.is_valid() && !other_nodes_[i].is_alive_)
    {
        other_nodes_[i].my_addr_.reset();
        TBSYS_LOG(INFO, "the rootserver info has been deleted, rs=%s", rs.to_cstring());
    }
  }
}
//add:e
