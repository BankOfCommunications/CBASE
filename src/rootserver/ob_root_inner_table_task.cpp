/**
  * (C) 2007-2010 Taobao Inc.
  *
  * This program is free software; you can redistribute it and/or modify
  * it under the terms of the GNU General Public License version 2 as
  * published by the Free Software Foundation.
  *
  * Version: $Id$
  *
  * Authors:
  *   zhidong <xielun.szd@taobao.com>
  *     - some work details if you want
  */

#include "ob_root_inner_table_task.h"
#include "ob_root_async_task_queue.h"
#include "ob_root_sql_proxy.h"
#include "sql/ob_sql.h"
//add lbzhong [Paxos Cluster.Balance] 201607014:b
#include "common/ob_inner_table_operator.h"
//add:e

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

ObRootInnerTableTask::ObRootInnerTableTask():cluster_id_(-1), timer_(NULL), queue_(NULL), proxy_(NULL)
{
}

ObRootInnerTableTask::~ObRootInnerTableTask()
{
}

int ObRootInnerTableTask::init(const int cluster_id, ObRootSQLProxy & proxy, ObTimer & timer,
    ObRootAsyncTaskQueue & queue
   //add lbzhong [Paxos Cluster.Balance] 201607014:b
   , ObRootServer2 *root_server
   //add:e
   )
{
  int ret = OB_SUCCESS;
  if (cluster_id < 0)
  {
    TBSYS_LOG(WARN, "check init param failed:cluster_id[%d]", cluster_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    cluster_id_ = cluster_id;
    timer_ = &timer;
    queue_ = &queue;
    proxy_ = &proxy;
    //add lbzhong [Paxos Cluster.Balance] 201607014:b
    root_server_ = root_server;
    //add:e
  }
  return ret;
}

int ObRootInnerTableTask::modify_all_server_table(const ObRootAsyncTaskQueue::ObSeqTask & task)
{
  int ret = OB_SUCCESS;
  // write server info to internal table
  char buf[OB_MAX_SQL_LENGTH] = "";
  int32_t server_port = task.server_.get_port();
  char ip_buf[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (false == task.server_.ip_to_string(ip_buf, sizeof(ip_buf)))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "convert server ip to string failed:ret[%d]", ret);
  }
  else
  {
    switch (task.type_)
    {
      case SERVER_ONLINE:
      {
        //add wangdonghui [ups state exception in __all_server] 20170511 :b
        if(task.server_status_ == 1 && task.role_ == OB_UPDATESERVER)
        {
            const char * sql_temp = "DELETE /*+UD_MULTI_BATCH*/ FROM __all_server where svr_type = \'%s\' and svr_role = %d;";
            snprintf(buf, sizeof (buf), sql_temp, print_role(task.role_), task.server_status_);
            ObString sql;
            sql.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
            ret = proxy_->query(true, RETRY_TIMES, TIMEOUT, sql);
            if (OB_SUCCESS == ret)
            {
              TBSYS_LOG(INFO, "process inner task succ:task_id[%lu], timestamp[%ld], sql[%s]",
                  task.get_task_id(), task.get_task_timestamp(), buf);
            }
        }
        //add :e
        if(OB_SUCCESS == ret)
        {
            const char * sql_temp = "REPLACE INTO __all_server(cluster_id, svr_type,"
              " svr_ip, svr_port, inner_port, svr_role, svr_version)"
              " VALUES(%d,\'%s\',\'%s\',%u,%u,%d,\'%s\');";
            //mod chujiajia [Paxos rs_election] 20151210:b
            //snprintf(buf, sizeof (buf), sql_temp, cluster_id_, print_role(task.role_),
            snprintf(buf, sizeof (buf), sql_temp, task.server_.cluster_id_, print_role(task.role_),
            //mod:e
                ip_buf, server_port, task.inner_port_, task.server_status_, task.server_version_);
        }
        break;
      }
      case SERVER_OFFLINE:
      {
        const char * sql_temp = "DELETE FROM __all_server WHERE svr_type=\'%s\' AND"
          " svr_ip=\'%s\' AND svr_port=%d AND cluster_id=%d;";
        snprintf(buf, sizeof (buf), sql_temp, print_role(task.role_),
        //mod chujiajia [Paxos rs_election] 20151210:b
        //    ip_buf, server_port, cluster_id_);
                 ip_buf, server_port, task.server_.cluster_id_);
        //mod:e
        break;
      }
      case ROLE_CHANGE:
      {
        //add wangdonghui [ups state exception in __all_server] 20170511 :b
        if(task.server_status_ == 1 && task.role_ == OB_UPDATESERVER)
        {
            const char * sql_temp = "DELETE /*+UD_MULTI_BATCH*/ FROM __all_server where svr_type = \'%s\' and svr_role = %d;";
            snprintf(buf, sizeof (buf), sql_temp, print_role(task.role_), task.server_status_);
            ObString sql;
            sql.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
            ret = proxy_->query(true, RETRY_TIMES, TIMEOUT, sql);
            if (OB_SUCCESS == ret)
            {
              TBSYS_LOG(INFO, "process inner task succ:task_id[%lu], timestamp[%ld], sql[%s]",
                  task.get_task_id(), task.get_task_timestamp(), buf);
            }
        }
        //add :e
        if(OB_SUCCESS == ret)
        {
            const char * sql_temp = "REPLACE INTO __all_server(cluster_id, svr_type,"
              " svr_ip, svr_port, inner_port, svr_role) VALUES(%d,\'%s\',\'%s\',%u,%u,%d);";
            //mod chujiajia [Paxos rs_election] 20151210:b
            //snprintf(buf, sizeof (buf), sql_temp, cluster_id_, print_role(task.role_),
            snprintf(buf, sizeof (buf), sql_temp, task.server_.cluster_id_, print_role(task.role_),
            //mod:e
                     ip_buf, server_port, task.inner_port_, task.server_status_);
        }
        break;
      }
      //add wangdonghui [ups state exception in __all_server] 20170522 :b
      //delete stale ups info from __all_server table 1 days(86400000000) before
      case RS_ROLE_CHANGE:
      {
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        cur_time -= 21600000000;//1/4days
        const char * sql_temp = "DELETE /*+UD_MULTI_BATCH*/ FROM __all_server where gm_modify < cast(%ld as modifytime)"
                                "and svr_type='updateserver';";
        snprintf(buf, sizeof (buf), sql_temp, cur_time);
        break;
      }
      //add :e
      default:
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check input param failed:task_type[%d]", task.type_);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObString sql;
    sql.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
    ret = proxy_->query(true, RETRY_TIMES, TIMEOUT, sql);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "process inner task succ:task_id[%lu], timestamp[%ld], sql[%s]",
          task.get_task_id(), task.get_task_timestamp(), buf);
    }
  }
  return ret;
}

int ObRootInnerTableTask::modify_all_cluster_table(const ObRootAsyncTaskQueue::ObSeqTask & task)
{
  int ret = OB_SUCCESS;
  // write cluster info to internal table
  char buf[OB_MAX_SQL_LENGTH] = "";

  if (task.type_ == LMS_ONLINE)
  {
    char ip_buf[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (false == task.server_.ip_to_string(ip_buf, sizeof(ip_buf)))
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "convert server ip to string failed:ret[%d]", ret);
    }
    else
    {

      const char * sql_temp = "REPLACE INTO %s"
        "(cluster_id, cluster_vip, cluster_port)"
        "VALUES(%d, \'%s\',%u);";
      //add lbzhong [Paxos Cluster.Balance] 20160707:b
      if(task.server_.cluster_id_ != cluster_id_)
      {
        snprintf(buf, sizeof (buf), sql_temp, OB_ALL_CLUSTER, task.server_.cluster_id_, ip_buf,
               task.inner_port_);
      }
      else
      {
      //add:e
        snprintf(buf, sizeof (buf), sql_temp, OB_ALL_CLUSTER, cluster_id_, ip_buf,
               task.inner_port_);
      } //add lbzhong [Paxos Cluster.Balance] 20160707:b:e
    }
  }
  else if (task.type_ == OBI_ROLE_CHANGE)
  {
    if (1 == task.cluster_role_)
    {
      ret = clean_other_masters();
    }
    if (OB_SUCCESS == ret)
    {
      const char * sql_temp = "REPLACE INTO %s"
        "(cluster_id, cluster_role)"
        "VALUES(%d, %d);";
      snprintf(buf, sizeof (buf), sql_temp, OB_ALL_CLUSTER, cluster_id_, task.cluster_role_);
    }
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check input param failed:task_type[%d]", task.type_);
  }
  if (OB_SUCCESS == ret)
  {
    ObString sql;
    sql.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
    ret = proxy_->query(true, RETRY_TIMES, TIMEOUT, sql);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "process inner task succ:task_id[%lu], timestamp[%ld], sql[%s]",
          task.get_task_id(), task.get_task_timestamp(), buf);
    }
  }
  return ret;
}

int ObRootInnerTableTask::clean_other_masters(void)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, OB_MAX_CLUSTER_COUNT> cluster_ids;
  ObServer ms;
  bool query_master = true;
  if (OB_SUCCESS != (ret = proxy_->ms_provider_.get_ms(ms, query_master)))
  {
    TBSYS_LOG(WARN, "get mergeserver address failed, ret %d", ret);
  }
  else if (OB_SUCCESS != (ret = proxy_->rpc_stub_.fetch_master_cluster_id_list(
          ms, cluster_ids, TIMEOUT)))
  {
    TBSYS_LOG(WARN, "fetch master cluster id list failed, ret %d, ms %s", ret, to_cstring(ms));
  }
  else
  {
    char sql[OB_MAX_SQL_LENGTH];
    for (int64_t i = 0; OB_SUCCESS == ret && i < cluster_ids.count(); i++)
    {
      if (cluster_ids.at(i) == cluster_id_)
      {
        continue;
      }
      int n = snprintf(sql, sizeof(sql),
          "REPLACE INTO %s (cluster_id, cluster_role) VALUES (%ld, %d);",
          OB_ALL_CLUSTER, cluster_ids.at(i), 2);
      if (n < 0 || n >= static_cast<int>(sizeof(sql)))
      {
        TBSYS_LOG(WARN, "sql buffer not enough, n %d, errno %d", n, n < 0 ? errno : 0);
        ret = OB_BUF_NOT_ENOUGH;
      }
      else if (OB_SUCCESS != (ret = proxy_->query(
              query_master, RETRY_TIMES, TIMEOUT, ObString::make_string(sql))))
      {
        TBSYS_LOG(WARN, "execute sql [%s] failed, ret %d", sql, ret);
      }
    }
  }
  return ret;
}

void ObRootInnerTableTask::runTimerTask(void)
{
  if (check_inner_stat() != true)
  {
    TBSYS_LOG(WARN, "check inner stat failed");
  }
  else
  {
    int ret = OB_SUCCESS;
    int64_t cur_time = tbsys::CTimeUtil::getTime();
    int64_t end_time = cur_time + MAX_TIMEOUT;
    while ((OB_SUCCESS == ret) && (end_time > cur_time))
    {
      ret = process_head_task();
      if ((ret != OB_SUCCESS) && (ret != OB_ENTRY_NOT_EXIST))
      {
        TBSYS_LOG(WARN, "process head inner task failed:ret[%d]", ret);
      }
      else
      {
        cur_time = tbsys::CTimeUtil::getTime();
      }
    }
  }
}

int ObRootInnerTableTask::process_head_task(void)
{
  // process the head task
  ObRootAsyncTaskQueue::ObSeqTask task;
  int ret = queue_->head(task);
  //add pangtianze [Paxos]  20170708:b
  if (OB_SUCCESS == ret && !root_server_->is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "slave can not process inner task, ignore it");
  }
  //add:e
  if (OB_SUCCESS == ret)
  {
    switch (task.type_)
    {
    case ROLE_CHANGE:
    case SERVER_ONLINE:
    case SERVER_OFFLINE:
      {
        ret = modify_all_server_table(task);
        //add lbzhong [Paxos Cluster.Balance] 201607014:b
        if(OB_SUCCESS == ret && (SERVER_ONLINE == task.type_ || SERVER_OFFLINE == task.type_))
        {
          ret = check_cluster_alive(task);
        }
        //add:e
        break;
      }
    case OBI_ROLE_CHANGE:
    case LMS_ONLINE:
      {
        ret = modify_all_cluster_table(task);
        break;
      }
    //add lbzhong [Paxos Cluster.Balance] 201607014:b
    case LMS_OFFLINE:
      {
        ret = check_cluster_alive(task);
        break;
      }
    //add:e
    //add wangdonghui [ups state exception in __all_server] 20170522 :b
    case RS_ROLE_CHANGE:
      {
        ret = modify_all_server_table(task);
        break;
      }
    default:
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "not supported task right now");
        break;
      }
    }
  }
  // pop the succ task
  if (OB_SUCCESS == ret || (OB_NOT_MASTER == ret && root_server_->is_slave())) //mod pangtianze [Paxos]  20170708:b
  {
    ObRootAsyncTaskQueue::ObSeqTask pop_task;
    ret = queue_->pop(pop_task);
    if ((ret != OB_SUCCESS) || (pop_task != task))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "pop the succ task failed:ret[%d]", ret);
      pop_task.print_info();
      task.print_info();
    }
  }
  return ret;
}

//add lbzhong [Paxos Cluster.Balance] 201607014:b
int ObRootInnerTableTask::check_cluster_alive(const ObRootAsyncTaskQueue::ObSeqTask &task)
{
  int ret = OB_SUCCESS;
  const int32_t cluster_id = task.server_.cluster_id_;
  //add bingo [Paxos __all_cluster] 20170406:b
  int32_t master_cluster_id = root_server_->get_master_cluster_id();
  //add:e
  bool need_execute_sql = false;
  char buf[OB_MAX_SQL_LENGTH] = {0};
  ObString sql(sizeof (buf), 0, buf);
  if(SERVER_ONLINE == task.type_) //insert
  {
    bool is_exist = false;
    int64_t rs_port = 0;
    ObServer ms;
    if (OB_SUCCESS != (ret = proxy_->ms_provider_.get_ms(ms, true)))
    {
      TBSYS_LOG(WARN, "get mergeserver address failed, ret %d", ret);
    }
    //check the cluster info in __all_cluster or not
    if(OB_SUCCESS != (ret = proxy_->rpc_stub_.is_cluster_exist(is_exist, rs_port, ms, TIMEOUT, cluster_id)))
    {
      TBSYS_LOG(WARN, "check cluster exist failed, ret %d", ret);
    }
    else if(!is_exist)
    {
      //del bingo [Paxos Cluster.Balance] 20161019:b [Paxos __all_cluster] 20170406:b 
      //int32_t master_cluster_id = root_server_->get_master_cluster_id();
      //ObiRole role;
      //del:e
      ObServer rs;
      root_server_->get_rs_node_mgr()->get_one_rs(rs, cluster_id);
      int64_t INIT_CLUSTER_FLOW_PERCENT = 0;
      if (master_cluster_id == cluster_id)
      {
        INIT_CLUSTER_FLOW_PERCENT = 100;
        //del bingo [Paxos Cluster.Balance] 20161019:b
        //role.set_role(ObiRole::MASTER);
        //del:e
      }
      else
      {
        INIT_CLUSTER_FLOW_PERCENT = 0;
        //del bingo [Paxos Cluster.Balance] 20161019:b
        //role.set_role(ObiRole::SLAVE);
        //del:e
      }
      //mod bingo [Paxos Cluster.Balance] 20161019:b
      /*if(OB_SUCCESS != (ret = ObInnerTableOperator::update_all_cluster(sql, cluster_id, rs, role, INIT_CLUSTER_FLOW_PERCENT)))*/
      if(OB_SUCCESS != (ret = ObInnerTableOperator::update_all_cluster(sql, cluster_id, rs, master_cluster_id, INIT_CLUSTER_FLOW_PERCENT)))
      //mod:e
      {
        TBSYS_LOG(WARN, "update all_cluster sql failed! ret: [%d], cluster_id=%d", ret, cluster_id);
      }
      need_execute_sql = true;
    }
    else if(OB_ROOTSERVER == task.role_ && 0 == rs_port) // update rootserver port
    {
      const char * format = "REPLACE INTO %s (cluster_id, rootserver_port) VALUES (%d,%d);";
      int size = snprintf(sql.ptr(), sql.size(), format, OB_ALL_CLUSTER, cluster_id, task.server_.get_port());
      sql.assign_ptr(sql.ptr(), size);
      need_execute_sql = true;
    }
  }
  else if(SERVER_OFFLINE == task.type_ || LMS_OFFLINE == task.type_) //delete
  {
    bool is_alive = root_server_->is_cluster_alive(cluster_id);
    if(!is_alive)
    {
      const char * format = "DELETE FROM %s WHERE cluster_id=%d;";
      int size = snprintf(sql.ptr(), sql.size(), format, OB_ALL_CLUSTER, cluster_id);
      sql.assign_ptr(sql.ptr(), size);
      need_execute_sql = true;
      //add bingo [Paxos __all_cluster] 20170406:b  20170417:b 20170713:b
      if(OB_SUCCESS != (ret = process_offline_cluster(cluster_id)))
      {
        TBSYS_LOG(WARN,"process offline cluster faild! ret %d", ret);
      }
      //add:e
    }
    //add bingo [Paxos datasource __all_cluster] 20170407:b
    else if((!root_server_->get_server_manager().is_cluster_alive_with_ms(cluster_id)) ||
            (!root_server_->get_server_manager().is_cluster_alive_with_cs(cluster_id)) ) //all ms offline or all cs offline
    {
      //add bingo [Paxos __all_cluster] 20170713:b
      if(OB_SUCCESS != (ret = process_cluster_while_all_mscs_offline(cluster_id)))
      {
        TBSYS_LOG(WARN,"process cluster while all mscs offline faild! ret %d", ret);
      }
      //add:e
    }
    //add:e
  }

  if(OB_SUCCESS == ret && need_execute_sql)
  {
    if (OB_SUCCESS == (ret = proxy_->query(true, RETRY_TIMES, TIMEOUT, sql)))
    {
      TBSYS_LOG(INFO, "process inner task succ:sql[%s]", buf);
    }
  }
  return ret;
}


//add bingo [Paxos __all_cluster] 20170713:b
int ObRootInnerTableTask::process_offline_cluster(int32_t cluster_id)
{
  int ret = OB_SUCCESS;
  int32_t master_cluster_id = root_server_->get_master_cluster_id();
  if(master_cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    TBSYS_LOG(WARN,"invalid master cluster id %d", master_cluster_id);
  }
  if(cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    TBSYS_LOG(WARN,"invalid cluster id %d", cluster_id);
  }

  if(OB_SUCCESS == ret)
  {
    int32_t cluster_flows[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_flows, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
	//add bingo [Paxos datasource __all_cluster] 20170714:b
    int64_t cluster_ports[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_ports, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int64_t));
	//add:e

    ObServer ms;
    bool query_master = false;
    if (OB_SUCCESS != (ret = proxy_->get_ms_provider().get_ms(ms,query_master)))
    {
      TBSYS_LOG(WARN, "get mergeserver address failed, ret %d", ret);
    }
    else if (OB_SUCCESS != (ret = proxy_->get_rpc_stub().fetch_cluster_flow_list(ms, cluster_flows, root_server_->get_config().network_timeout)))
    {
      TBSYS_LOG(WARN, "fetch master cluster flow list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add bingo [Paxos datasource __all_cluster] 20170714:b
    else if (OB_SUCCESS != (ret = proxy_->get_rpc_stub().fetch_cluster_port_list(ms, cluster_ports, root_server_->get_config().network_timeout)))
    {
      TBSYS_LOG(WARN, "fetch cluster port list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add:e
    else if(0 != cluster_flows[cluster_id] || master_cluster_id == cluster_id)
    {
      int32_t new_flow = cluster_flows[cluster_id];
      int32_t m_cid = master_cluster_id;
      if(master_cluster_id == cluster_id)
      {
	    //add bingo [Paxos datasource __all_cluster] 20170714:b
        if(cluster_ports[root_server_->get_self().cluster_id_] == 0)
        {//add:e
          for(int32_t cid = 1; cid < OB_CLUSTER_ARRAY_LEN; cid++)
          {
            //add bingo [Paxos datasource __all_cluster] 20170714:b
            if(cid != cluster_id && cluster_ports[cid] != 0)
            {//add:e
              m_cid = cid;
              break;
            }
          }
        }

        if(m_cid == master_cluster_id)
        {
          m_cid = root_server_->get_self().cluster_id_;
        }
        root_server_->set_master_cluster_id(m_cid);
        new_flow += cluster_flows[m_cid];
      }
      else
      {
        new_flow += cluster_flows[master_cluster_id];
      }

      char buf_tmp[OB_MAX_SQL_LENGTH] = "";
      const char * sql_temp = "REPLACE INTO %s"
        "(cluster_id, cluster_role, cluster_flow_percent)"
        "VALUES(%d, %d, %d);";
      snprintf(buf_tmp, sizeof (buf_tmp), sql_temp, OB_ALL_CLUSTER, m_cid, 1, new_flow);
      ObString sql_tmp;
      sql_tmp.assign_ptr(buf_tmp, static_cast<ObString::obstr_size_t>(strlen(buf_tmp)));
      ret = proxy_->query(true, root_server_->get_config().retry_times, root_server_->get_config().network_timeout, sql_tmp);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "update all_cluster sql success! ret: [%d], cluster_id=%d", ret, root_server_->get_master_cluster_id());
      }
      else
      {
        TBSYS_LOG(WARN, "update all_cluster sql failed! ret: [%d], cluster_id=%d", ret, root_server_->get_master_cluster_id());
      }
    }
  }

  return ret;
}

int ObRootInnerTableTask::process_cluster_while_all_mscs_offline(int32_t cluster_id)
{
  int ret = OB_SUCCESS;
  int32_t master_cluster_id = root_server_->get_master_cluster_id();
  if(master_cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    TBSYS_LOG(WARN,"invalid master cluster id %d", master_cluster_id);
  }
  if(cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    TBSYS_LOG(WARN,"invalid cluster id %d", cluster_id);
  }

  if(OB_SUCCESS == ret)
  {
    //get cluster flow
    int32_t cluster_flows[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_flows, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
    //add bingo [Paxos datasource __all_cluster] 20170714:b
    int64_t cluster_ports[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_ports, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int64_t));
    //add:e

    ObServer ms;
    bool query_master = false;
    if (OB_SUCCESS != (ret = proxy_->get_ms_provider().get_ms(ms,query_master)))
    {
      TBSYS_LOG(WARN, "get mergeserver address failed, ret %d", ret);
    }
    else if (OB_SUCCESS != (ret = proxy_->get_rpc_stub().fetch_cluster_flow_list(ms, cluster_flows, root_server_->get_config().network_timeout)))
    {
      TBSYS_LOG(WARN, "fetch master cluster flow list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add bingo [Paxos datasource __all_cluster] 20170714:b
    else if (OB_SUCCESS != (ret = proxy_->get_rpc_stub().fetch_cluster_port_list(ms, cluster_ports, root_server_->get_config().network_timeout)))
    {
      TBSYS_LOG(WARN, "fetch cluster port list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add:e
    else if(0 != cluster_flows[cluster_id] || master_cluster_id == cluster_id)
    {
      //get cluster with ms and cs
      bool is_cluster_alive_with_ms_and_cs[OB_CLUSTER_ARRAY_LEN];
      root_server_->get_alive_cluster_with_ms_and_cs(is_cluster_alive_with_ms_and_cs);

      //check whether needs to redistribute cluster flow
      int32_t max_flow_cluster_id = 0;
      for(int32_t cid = 1; cid < OB_CLUSTER_ARRAY_LEN; cid++)
      {
        if(cid != cluster_id)
        {
          //add bingo [Paxos datasource __all_cluster] 20170714:b
          if(cluster_ports[cid] != 0)
          {//add:e
            max_flow_cluster_id = cluster_flows[max_flow_cluster_id]  > cluster_flows[cid] ? max_flow_cluster_id : cid;
          }
        }
      }
      int32_t target_cluster_id = 0;
      int32_t target_cluster_flow = 100;
      if(max_flow_cluster_id == 0 || !is_cluster_alive_with_ms_and_cs[max_flow_cluster_id])
      {
        for(int32_t cid = 1; cid < OB_CLUSTER_ARRAY_LEN; cid++)
        {
          if(cid != cluster_id)
          {
            //add bingo [Paxos datasource __all_cluster] 20170714:b
            if(cluster_ports[cid] != 0  && is_cluster_alive_with_ms_and_cs[cid])
            {//add:e
              target_cluster_id = cid;
              break;
            }
          }
        }
        if(target_cluster_id == 0)
        {
          TBSYS_LOG(WARN,"No valid cluster to switch master cluster");
          ret = OB_SUCCESS;
        }
      }
      else
      {
        target_cluster_id = max_flow_cluster_id;
        target_cluster_flow = cluster_flows[max_flow_cluster_id] + cluster_flows[cluster_id];
      }

      //redistribute cluster flow
      if( 0 != target_cluster_id )
      {
        int32_t c_role = 2;
        if(master_cluster_id == cluster_id)
        {
          root_server_->set_master_cluster_id(target_cluster_id);
          c_role = 1;
        }
        else if(master_cluster_id == target_cluster_id)
        {
          c_role = 1;
        }
        //add cluster flow to target cluster
        char buf_tmp[OB_MAX_SQL_LENGTH] = "";
        const char * sql_temp = "REPLACE INTO %s"
          "(cluster_id, cluster_role, cluster_flow_percent)"
          "VALUES(%d, %d, %d);";
        snprintf(buf_tmp, sizeof (buf_tmp), sql_temp, OB_ALL_CLUSTER, target_cluster_id, c_role, target_cluster_flow);
        ObString sql_tmp;
        sql_tmp.assign_ptr(buf_tmp, static_cast<ObString::obstr_size_t>(strlen(buf_tmp)));
        ret = proxy_->query(true, root_server_->get_config().retry_times, root_server_->get_config().network_timeout, sql_tmp);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "add cluster flow to target cluster success! ret:[%d], cluster_id=%d, cluster_flow_percent=%d", ret, target_cluster_id, target_cluster_flow);
        }
        else
        {
          TBSYS_LOG(WARN, "add cluster flow to target cluster failed! ret:[%d], cluster_id=%d, cluster_flow_percent=%d", ret, target_cluster_id, target_cluster_flow);
        }

        //zero the offline(ms/cs) cluster flow
        char buf_tmp1[OB_MAX_SQL_LENGTH] = "";
        const char * sql_temp1 = "REPLACE INTO %s"
          "(cluster_id, cluster_role, cluster_flow_percent)"
          "VALUES(%d, %d, %d);";
        snprintf(buf_tmp1, sizeof (buf_tmp1), sql_temp1, OB_ALL_CLUSTER, cluster_id, 2, 0);
        ObString sql_tmp1;
        sql_tmp1.assign_ptr(buf_tmp1, static_cast<ObString::obstr_size_t>(strlen(buf_tmp1)));
        ret = proxy_->query(true, root_server_->get_config().retry_times, root_server_->get_config().network_timeout, sql_tmp1);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "zero cluster flow of cluster[%d] success! ret:[%d]", cluster_id, ret);
        }
        else
        {
          TBSYS_LOG(WARN, "zero cluster flow of cluster[%d] failed! ret:[%d]", cluster_id, ret);
        }
      }
    }
  }
  return ret;
}
//add:e

