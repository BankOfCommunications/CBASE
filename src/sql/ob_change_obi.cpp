/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_change_obi.cpp
 *
 * Authors:
 *   Wu Di <lide.wd@alipay.com>
 *
 */

#include <string.h>
#include "ob_change_obi.h"
#include "ob_sql.h"

using namespace oceanbase::sql;

ObChangeObi::ObChangeObi()
  :context_(NULL), result_set_out_(NULL), master_cluster_index_(-1),
  cluster_count_(0), target_cluster_index_(-1), check_ups_log_interval_(1),
  force_(false), change_obi_timeout_us_(0),target_server_port_(0),
  got_rs_(false), multi_master_cluster_(false), master_cluster_exists_(false)
{
  role_.set_role(ObiRole::INIT);
  memset(target_server_ip_, 0, sizeof(target_server_ip_));
  memset(all_rs_ups_, 0, sizeof(all_rs_ups_));
}

ObChangeObi::~ObChangeObi()
{
}
int ObChangeObi::get_next_row(const common::ObRow *&row)
{
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}
void ObChangeObi::set_context(ObSqlContext *context)
{
  context_ = context;
  result_set_out_ = context_->session_info_->get_current_result_set();
}
int ObChangeObi::open()
{
  int ret = OB_SUCCESS;
  context_->disable_privilege_check_ = true;
  ObString ob_query_timeout = ObString::make_string(OB_QUERY_TIMEOUT_PARAM);
  if (OB_SUCCESS != (ret = context_->session_info_->update_system_variable(ob_query_timeout, change_obi_timeout_value_)))
  {
    TBSYS_LOG(WARN, "temporarily set ob query timeout failed, ret=%d", ret);
  }
  else if (NULL == (context_->schema_manager_ = context_->merger_schema_mgr_->get_user_schema(0)))
  {
    ret = OB_SCHEMA_ERROR;
    TBSYS_LOG(WARN, "get schema error, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = get_rs()))
  {
    TBSYS_LOG(WARN, "get rs failed, ret=%d", ret);
  }
  // rootserver都获取不到，直接失败，不管是否force
  if (OB_SUCCESS == ret)
  {
    if (!got_rs_)
    {
      ret = OB_CLUSTER_INFO_NOT_EXIST;
      TBSYS_LOG(ERROR, "get cluster failed, ret=%d", ret);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "get cluster failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    get_master_ups();
    // SET_MASTER
    if (role_.get_role() == ObiRole::MASTER)
    {
      if (OB_SUCCESS != (ret = check_addr()))
      {
        TBSYS_LOG(ERROR, "check addr failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = change_cluster_to_master(target_server_)))
      {
        TBSYS_LOG(WARN, "change cluster to master cluster failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = check_if_cluster_is_master(target_server_)))
      {
        TBSYS_LOG(WARN, "cluster is not yet master, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = broadcast_master_cluster()))
      {
        TBSYS_LOG(WARN, "set new master cluster addr to all cluster failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = post_check()))
      {
        TBSYS_LOG(WARN, "post_check failed, ret=%d", ret);
      }
      if(OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "set master success!");
      }
      else
      {
        TBSYS_LOG(ERROR, "set master failed!");
      }
    }
    // SET_SLAVE
    else if (role_.get_role() == ObiRole::SLAVE)
    {
      if (OB_SUCCESS != (ret = check_addr()))
      {
        TBSYS_LOG(ERROR, "check_addr failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = change_cluster_to_slave(target_cluster_index_)))
      {
        TBSYS_LOG(WARN, "change cluster to slave failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = check_if_cluster_is_slave(target_cluster_index_)))
      {
        TBSYS_LOG(WARN, "cluster is not yet slave, ret=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "set slave success!");
      }
      else
      {
        TBSYS_LOG(ERROR, "set slave failed!");
      }
    }
    else // 主备切换
    {
      // 虽然所有集群地址都获得到了，但是每个函数失败时还是需要处理是不是强制切换的情况！！！
      // 如果有多个主集群，则主备切换直接失败！！
      if (multi_master_cluster_)
      {
        ret = OB_MULTIPLE_MASTER_CLUSTERS_EXIST;
        TBSYS_LOG(ERROR, "mutiple master clusters exist, ret=%d, use ALTER SYSTEM SET_SLAVE SLAVE='' to change master to slave", ret);
      }
      else if (OB_SUCCESS != (ret = check_new_master_addr()))
      {
        TBSYS_LOG(WARN, "new master addr[%s:%d] invalid, ret=%d", target_server_ip_, target_server_port_, ret);
      }
      else if (OB_SUCCESS != (ret = check_ups_log()))
      {
        TBSYS_LOG(WARN, "master ups and slave ups not sync,ret=%d", ret);
      }
      else if (master_cluster_exists_)
      {
        if (OB_SUCCESS != (ret = change_cluster_to_slave(master_cluster_index_)))
        {
          TBSYS_LOG(WARN, "change master to slave failed, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = check_if_cluster_is_slave(master_cluster_index_)))
        {
          TBSYS_LOG(WARN, "old master is not yet slave, ret=%d", ret);
        }
      }
      
      if (OB_SUCCESS != ret)
      {}
      else if (OB_SUCCESS != (ret = waiting_old_slave_ups_sync()))
      {
        TBSYS_LOG(ERROR, "waiting old slave ups sync failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = change_cluster_to_master(target_server_)))
      {
        TBSYS_LOG(WARN, "change slave cluster to master cluster failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = check_if_cluster_is_master(target_server_)))
      {
        TBSYS_LOG(WARN, "old slave is not yet master, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = broadcast_master_cluster()))
      {
        TBSYS_LOG(WARN, "set new master rs addr to all cluster failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = post_check()))
      {
        TBSYS_LOG(WARN, "post_check failed, ret=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "change obi role success!");
      }
      else
      {
        TBSYS_LOG(ERROR, "change obi role failed, ret=%d", ret);
      }
    }
  }
  context_->disable_privilege_check_ = false;
  return ret;
}
int ObChangeObi::close()
{
  int ret = OB_SUCCESS;
  ObString ob_query_timeout = ObString::make_string(OB_QUERY_TIMEOUT_PARAM);
  if (OB_SUCCESS != (ret = context_->session_info_->update_system_variable(ob_query_timeout, old_ob_query_timeout_)))
  {
    TBSYS_LOG(WARN, "restore ob query timeout failed, ret=%d", ret);
  }
  return ret;
}
void ObChangeObi::get_master_ups()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0;i < cluster_count_; ++i)
  {
    if (OB_SUCCESS != (ret = context_->rs_rpc_proxy_->fetch_master_ups(all_rs_ups_[i].master_rs_, all_rs_ups_[i].master_ups_)))
    {
      TBSYS_LOG(WARN, "get master ups from rootserver[%s] failed, ret=%d", to_cstring(all_rs_ups_[i].master_rs_), ret);
    }
    else
    {
      all_rs_ups_[i].got_master_ups_ = true;
    }
  }
}
int ObChangeObi::get_rs()
{
  int ret = OB_SUCCESS;
  ObString get_cluster;
  if (force_)
  {
    get_cluster = ObString::make_string("select /*+read_consistency(weak) */ cluster_id, cluster_role, cluster_vip, rootserver_port from __all_cluster");
  }
  else
  {
    get_cluster = ObString::make_string("select cluster_id, cluster_role, cluster_vip, rootserver_port from __all_cluster");
  }
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if (OB_SUCCESS != (ret = result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(get_cluster, result, *context_)))
  {
    TBSYS_LOG(WARN, "direct execute sql=%.*s failed, ret=%d", get_cluster.length(), get_cluster.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(result.is_with_rows() == true);
    const ObRow *row = NULL;
    const ObObj *pcell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    while ((OB_SUCCESS == ret) && (OB_SUCCESS == (ret = result.get_next_row(row))))
    {
      int64_t cluster_id = -1;
      int64_t cluster_role = -1;
      ObString cluster_vip;
      int64_t rootserver_port = -1;

      if (OB_SUCCESS != (ret = row->raw_get_cell(0, pcell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
      }
      else if (pcell->get_type() != ObIntType)
      {
        TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
      }
      else if (OB_SUCCESS != (ret = pcell->get_int(cluster_id)))
      {
        TBSYS_LOG(WARN, "pcell->get_int failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row->raw_get_cell(1, pcell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
      }
      else if (pcell->get_type() != ObIntType)
      {
        TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
      }
      else if (OB_SUCCESS != (ret = pcell->get_int(cluster_role)))
      {
        TBSYS_LOG(WARN, "pcell->get_int failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row->raw_get_cell(2, pcell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
      }
      else if (pcell->get_type() != ObVarcharType)
      {
        TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
      }
      else if (OB_SUCCESS != (ret = pcell->get_varchar(cluster_vip)))
      {
        TBSYS_LOG(WARN, "pcell->get_varchar failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row->raw_get_cell(3, pcell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
      }
      else if (pcell->get_type() != ObIntType)
      {
        TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
      }
      else if (OB_SUCCESS != (ret = pcell->get_int(rootserver_port)))
      {
        TBSYS_LOG(WARN, "pcell->get_int failed, ret=%d", ret);
      }
      if (OB_SUCCESS != ret)
      {
        break;
      }
      else
      {
        got_rs_ = true;
        char ip_str[MAX_IP_ADDR_LENGTH];
        memset(ip_str, 0 , sizeof(ip_str));
        memcpy(ip_str, cluster_vip.ptr(), cluster_vip.length());
        ObServer server(ObServer::IPV4, ip_str, static_cast<int32_t>(rootserver_port));
        if (cluster_id >= 0)
        {
          int64_t index = cluster_count_;
          cluster_count_++;
          OB_ASSERT(index >= 0 && index < MAX_CLUSTER_COUNT);
          if (cluster_id == 0)
          {
            TBSYS_LOG(WARN, "cluster id == 0, it should be greater than 0");
          }
          all_rs_ups_[index].cluster_id_ = cluster_id;
          // master cluster
          if (cluster_role == 1)
          {
            master_cluster_exists_ = true;
            if (master_cluster_index_ != -1)
            {
              multi_master_cluster_ = true;
            }
            //目前，主集群的index
            master_cluster_index_ = index;
          }
          all_rs_ups_[index].master_rs_ = server;
          if (strlen(ip_str) != strlen(target_server_ip_))
          {
            continue;
          }
          if (!strncmp(ip_str, target_server_ip_, strlen(ip_str)))
          {
            target_cluster_index_ = index;
            if (static_cast<int32_t>(rootserver_port) != target_server_port_)
            {
              ret = OB_INVALID_ARGUMENT;
              TBSYS_LOG(WARN, "input cluster addr[%s:%d] is not a valid cluster,ret=%d", target_server_ip_, target_server_port_, ret);
            }
          }
        }
        else
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "cluster_role = %ld, not expected", cluster_role);
        }
      }
    }// while
    if (ret == OB_ITER_END)
    {
      ret = OB_SUCCESS;
    }
    int tmp_err = result.close();
    if (OB_SUCCESS != tmp_err)
    {
      TBSYS_LOG(WARN, "failed to close result set, ret=%d", ret);
    }
    result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);

  if (OB_SUCCESS == ret && !master_cluster_exists_)
  {
    int64_t master_index = -1;
    ret = get_master_rs_by_rpc(master_index);
    if (OB_SUCCESS != ret)
    {
      if (ret == OB_MULTIPLE_MASTER_CLUSTERS_EXIST)
      {
        multi_master_cluster_ = true;
        ret = OB_SUCCESS;
      }
    }
    else if (master_index >= 0)
    {
      master_cluster_exists_ = true;
      master_cluster_index_ = master_index;
    }
  }

  return ret;
}

int ObChangeObi::get_master_rs_by_rpc(int64_t &master_index)
{
  int ret = 0;
  master_index = -1;
  for (int64_t i = 0; OB_SUCCESS == ret && i < cluster_count_; i++)
  {
    ObiRole role;
    int ret2 = context_->rs_rpc_proxy_->get_obi_role(TIMEOUT, all_rs_ups_[i].master_rs_, role);
    if (OB_SUCCESS != ret2)
    {
      TBSYS_LOG(WARN, "get obi role from [%s] failed, ret %d",
          to_cstring(all_rs_ups_[i].master_rs_), ret);
    }
    else
    {
      TBSYS_LOG(INFO, "rootserver %s role %s", to_cstring(all_rs_ups_[i].master_rs_),
          role.get_role_str());
      if (role.get_role() == ObiRole::MASTER)
      {
        if (master_index < 0)
        {
          master_index = i;
        }
        else
        {
          TBSYS_LOG(WARN, "multi master cluster %s and %s",
              to_cstring(all_rs_ups_[master_index].master_rs_), to_cstring(all_rs_ups_[i].master_rs_));
          ret = OB_MULTIPLE_MASTER_CLUSTERS_EXIST;
        }
      }
    }
  }
  return ret;
}

int ObChangeObi::check_new_master_addr()
{
  int ret = OB_SUCCESS;
  if (master_cluster_index_ >=  cluster_count_ || master_cluster_index_ < -1)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "master_cluster_index_=%ld cluster_count_=%ld", master_cluster_index_, cluster_count_);
  }
  else
  {
    if (force_)
    {
      // 强切，只需要检查指定集群是否存在
      bool found = false;
      for (int64_t i = 0;i < cluster_count_; ++i)
      {
        if (all_rs_ups_[i].master_rs_.get_ipv4() == ObServer::convert_ipv4_addr(target_server_ip_)
            && all_rs_ups_[i].master_rs_.get_port() == target_server_port_)
        {
          found = true;
          break;
        }
      }
      if (found)
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_IP_PORT_IS_NOT_CLUSTER;
        TBSYS_LOG(ERROR, "input new master addr[%s:%d] is not a valid cluster, ret=%d", target_server_ip_, target_server_port_, ret);
      }
    }
    else
    {
      //不存在主集群
      if (master_cluster_index_ == -1)
      {
        ret = OB_MASTER_CLUSTER_NOT_EXIST;
        TBSYS_LOG(ERROR, "master cluster not exist, force is false, normal change obi role failed, ret=%d", ret);
      }
      else
      {
        //普通切，并且存在主集群
        if (all_rs_ups_[master_cluster_index_].master_rs_.get_ipv4() == ObServer::convert_ipv4_addr(target_server_ip_)
            && all_rs_ups_[master_cluster_index_].master_rs_.get_port() == target_server_port_)
        {
          ret = OB_CLUSTER_ALREADY_MASTER;
          TBSYS_LOG(WARN, "new_master_ip[%s] is already master cluster, ret=%d", target_server_ip_, ret);
        }
        else
        {
          bool found = false;
          for (int64_t i = 0;i < cluster_count_; ++i)
          {
            if (all_rs_ups_[i].master_rs_.get_ipv4() == ObServer::convert_ipv4_addr(target_server_ip_)
                && all_rs_ups_[i].master_rs_.get_port() == target_server_port_)
            {
              found = true;
              break;
            }
          }
          if (!found)
          {
            ret = OB_IP_PORT_IS_NOT_SLAVE_CLUSTER;
            TBSYS_LOG(WARN, "new_master_ip[%s:%d] is not a slave cluster, ret=%d", target_server_ip_, target_server_port_, ret);
          }
        }
      }
    }
  }
  return ret;
}
int ObChangeObi::check_ups_log()
{
  int ret = OB_SUCCESS;
  if (force_)
  {
    // do nothing;
  }
  else
  {
    if (master_cluster_index_ < 0 || master_cluster_index_ >= cluster_count_)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "master_cluster_index_=%ld cluster_count_=%ld ret=%d", master_cluster_index_, cluster_count_, ret);
    }
    else if (!all_rs_ups_[master_cluster_index_].got_master_ups_ || !all_rs_ups_[target_cluster_index_].got_master_ups_)
    {
      ret = OB_GET_CLUSTER_MASTER_UPS_FAILED;
      TBSYS_LOG(ERROR, "get master ups failed, get old master cluster master ups[%s], get target cluster master ups[%s], ret=%d",
          all_rs_ups_[master_cluster_index_].got_master_ups_ == true ? "success" : "failure",
          all_rs_ups_[target_cluster_index_].got_master_ups_ == true ? "success" : "failure",
          ret);
    }
    else
    {
      int64_t timeout = TIMEOUT;
      int64_t master_ups_log_seq = -1;
      int64_t slave_ups_log_seq = -1;
      for (int64_t i = 0;i < RETRY_TIME; ++i)
      {
        if (OB_SUCCESS != (ret = context_->merger_rpc_proxy_->get_ups_log_seq(all_rs_ups_[master_cluster_index_].master_ups_,
                timeout, master_ups_log_seq)))
        {
          TBSYS_LOG(WARN, "get master ups log seq from ups[%s] failed, ret=%d", to_cstring(all_rs_ups_[master_cluster_index_].master_ups_), ret);
        }
        else
        {
          // waiting for slave ups sync
          usleep(WAIT_SLAVE_UPS_SYNC_TIME_US);
          if (OB_SUCCESS != (ret = context_->merger_rpc_proxy_->get_ups_log_seq(all_rs_ups_[target_cluster_index_].master_ups_,
                  timeout, slave_ups_log_seq)))
          {
            TBSYS_LOG(WARN, "get slave ups log seq from ups[%s] failed, ret=%d", to_cstring(all_rs_ups_[target_cluster_index_].master_ups_), ret);
          }
          else if (slave_ups_log_seq < master_ups_log_seq)
          {
            ret = OB_LOG_NOT_SYNC;
            TBSYS_LOG(WARN, "master ups and slave ups no sync, slave_ups_log_seq=%ld, master_ups_log_seq=%ld ret=%d",
                slave_ups_log_seq, master_ups_log_seq, ret);
          }
          else
          {
            TBSYS_LOG(INFO, "master ups and slave ups sync");
            break;
          }
        }
      }
    }
  }
  return ret;
}
int ObChangeObi::change_cluster_to_slave(int64_t cluster_index)
{
  int ret = OB_SUCCESS;
  int64_t timeout = TIMEOUT;
  if (cluster_index < 0 || cluster_index >= cluster_count_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "cluster_index=%ld cluster_count_=%ld ret=%d", cluster_index, cluster_count_, ret);
  }
  else if (OB_SUCCESS != (ret = set_slave_in_inner_table(cluster_index)))
  {
    TBSYS_LOG(WARN, "set cluster to slave in __all_cluster table failed, ret %d", ret);
  }
  else
  {
    ObiRole obi_role;
    obi_role.set_role(ObiRole::SLAVE);
    if (OB_SUCCESS != (ret = context_->rs_rpc_proxy_->set_obi_role(all_rs_ups_[cluster_index].master_rs_, timeout, obi_role)))
    {
      if (force_)
      {
        TBSYS_LOG(WARN, "set obi role[slave] to cluster[%s] failed, but force is true, continue", to_cstring(all_rs_ups_[cluster_index].master_rs_));
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "set obi role[slave] to cluster[%s] failed, ret=%d", to_cstring(all_rs_ups_[cluster_index].master_rs_), ret);
      }
    }
    else
    {
      TBSYS_LOG(INFO, "change cluster[%s] to slave finished, wait to check", to_cstring(all_rs_ups_[cluster_index].master_rs_));
    }
  }
  return ret;
}

int ObChangeObi::set_slave_in_inner_table(int64_t cluster_index)
{
  int ret = OB_SUCCESS;
  if (cluster_index < 0 || cluster_index >= cluster_count_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "cluster_index=%ld cluster_count_=%ld ret=%d", cluster_index, cluster_count_, ret);
  }
  else
  {
    char sql[OB_MAX_SQL_LENGTH];
    int n = snprintf(sql, sizeof(sql), "update %s set cluster_role = 2 where cluster_id = %ld and cluster_role != 2",
        OB_ALL_CLUSTER, all_rs_ups_[cluster_index].cluster_id_);
    if (n < 0 || n >= static_cast<int>(sizeof(sql)))
    {
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "snprintf failed, n %d, errno %d", n, n < 0 ? errno : 0);
    }
    else
    {
      ObString sql_str = ObString::make_string(sql);
      ObResultSet rs;
      context_->session_info_->set_current_result_set(&rs);
      if (OB_SUCCESS != (ret = rs.init()))
      {
        TBSYS_LOG(WARN, "result set init failed, ret %d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(sql_str, rs, *context_)))
      {
        TBSYS_LOG(WARN, "direct execute sql %s failed, ret %d", sql, ret);
      }
      else if (OB_SUCCESS != (ret = rs.open()))
      {
        TBSYS_LOG(WARN, "open result failed, ret %d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "execute %s success, affect row %ld", sql, rs.get_affected_rows());
      }
      int ret2 = rs.close();
      if (OB_SUCCESS != ret2)
      {
        TBSYS_LOG(WARN, "close result failed, ret %d", ret2);
      }
      rs.reset();
      context_->session_info_->set_current_result_set(result_set_out_);
      if (OB_SUCCESS != ret && force_)
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(INFO, "ignore set cluster %ld to slave error %d for force operation",
            all_rs_ups_[cluster_index].cluster_id_, ret);
      }
    }
  }

  return ret;
}

int ObChangeObi::check_if_cluster_is_slave(int64_t cluster_index)
{
  int ret = OB_SUCCESS;
  int64_t timeout = TIMEOUT;
  ObiRole obi_role;
  if (OB_SUCCESS != (ret = context_->rs_rpc_proxy_->get_obi_role(timeout, all_rs_ups_[cluster_index].master_rs_, obi_role)))
  {
    TBSYS_LOG(WARN, "get obi role from cluster[%s] failed, ret=%d", to_cstring(all_rs_ups_[cluster_index].master_rs_), ret);
  }
  else
  {
    if (obi_role.get_role() != ObiRole::SLAVE)
    {
      ret = OB_CLUSTER_IS_NOT_SLAVE;
      TBSYS_LOG(WARN, "cluster[%s] is not yet slave, role is[%s]", to_cstring(all_rs_ups_[cluster_index].master_rs_), obi_role.get_role() == ObiRole::MASTER ? "master" : "init");
    }
    else
    {
      TBSYS_LOG(INFO, "cluster[%s] is slave now", to_cstring(all_rs_ups_[cluster_index].master_rs_));
    }
  }
  if (OB_SUCCESS != ret)
  {
    if (force_)
    {
      TBSYS_LOG(WARN, "cluster[%s] is not yet slave, role is[%s], but force is true, continue", to_cstring(all_rs_ups_[cluster_index].master_rs_), obi_role.get_role() == ObiRole::MASTER ? "master" : "init");
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
int ObChangeObi::waiting_old_slave_ups_sync()
{
  int ret = OB_SUCCESS;
  if (force_)
  {
    // do nothing
    TBSYS_LOG(INFO, "force change obi role, DO NOT CHECK UPS LOG");
  }
  else if (!all_rs_ups_[master_cluster_index_].got_master_ups_ || !all_rs_ups_[target_cluster_index_].got_master_ups_)
  {
    ret = OB_GET_CLUSTER_MASTER_UPS_FAILED;
    TBSYS_LOG(ERROR, "get master ups failed, get old master cluster master ups[%s], get target cluster master ups[%s], ret=%d",
        all_rs_ups_[master_cluster_index_].got_master_ups_ == true ? "success" : "failure",
        all_rs_ups_[target_cluster_index_].got_master_ups_ == true ? "success" : "failure",
        ret);
  }
  else
  {
    int64_t st = tbsys::CTimeUtil::getTime();
    int64_t ed = tbsys::CTimeUtil::getTime() + check_ups_log_interval_ * 1000 * 1000;
    int64_t timeout = TIMEOUT;
    int64_t master_ups_log_seq = -1;
    int64_t slave_ups_log_seq = -1;
    if (OB_SUCCESS != (ret = context_->merger_rpc_proxy_->get_ups_log_seq(all_rs_ups_[master_cluster_index_].master_ups_,
            timeout, master_ups_log_seq)))
    {
      TBSYS_LOG(WARN, "get master ups log seq from ups[%s] failed, ret=%d", to_cstring(all_rs_ups_[master_cluster_index_].master_ups_), ret);
    }
    else
    {
      st = tbsys::CTimeUtil::getTime();
      while(st <= ed)
      {
        if (OB_SUCCESS != (ret = context_->merger_rpc_proxy_->get_ups_log_seq(all_rs_ups_[target_cluster_index_].master_ups_,
                timeout, slave_ups_log_seq)))
        {
          TBSYS_LOG(WARN, "get slave ups log seq from ups[%s] failed, ret=%d", to_cstring(all_rs_ups_[target_cluster_index_].master_ups_), ret);
        }
        else if (master_ups_log_seq != slave_ups_log_seq)
        {
          ret = OB_LOG_NOT_SYNC;
          TBSYS_LOG(WARN, "master ups and slave ups not sync, ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "master ups and slave ups sync");
          break;
        }
        usleep(RETRY_INTERVAL_US);
        st = tbsys::CTimeUtil::getTime();
      }
      if (st > ed)
      {
        ret = OB_RESPONSE_TIME_OUT;
        TBSYS_LOG(ERROR, "master master ups and slave master ups not sync, ret=%d", ret);
      }
    }
  }
  return ret;
}
int ObChangeObi::change_cluster_to_master(const ObServer &target_server)
{
  int ret = OB_SUCCESS;
  int64_t timeout = TIMEOUT;
  ObiRole obi_role;
  obi_role.set_role(ObiRole::MASTER);
  if (OB_SUCCESS != (ret = context_->rs_rpc_proxy_->set_obi_role(target_server, timeout, obi_role)))
  {
    TBSYS_LOG(WARN, "set obi role [master] to cluster[%s] failed, ret=%d", to_cstring(target_server), ret);
  }
  else
  {
    TBSYS_LOG(INFO, "change cluster[%s] to master finished, wait to check", to_cstring(target_server));
  }
  return ret;
}
int ObChangeObi::check_if_cluster_is_master(const ObServer &target_server)
{
  int ret = OB_SUCCESS;
  int64_t timeout = TIMEOUT;
  ObiRole obi_role;
  if (OB_SUCCESS != (ret = context_->rs_rpc_proxy_->get_obi_role(timeout, target_server, obi_role)))
  {
    TBSYS_LOG(WARN, "get obi role from cluster[%s] failed, ret=%d", to_cstring(target_server), ret);
  }
  else
  {
    if (obi_role.get_role() != ObiRole::MASTER)
    {
      ret = OB_CLUSTER_IS_NOT_MASTER;
      TBSYS_LOG(WARN, "cluster[%s] is not yet master, role is[%s]", to_cstring(target_server), obi_role.get_role() == ObiRole::SLAVE ? "slave" : "init");
    }
    else
    {
      TBSYS_LOG(INFO, "cluster[%s] is master now", to_cstring(target_server));
    }
  }
  return ret;
}
int ObChangeObi::set_master_rs_vip_port_to_all_cluster()
{
  int ret = OB_SUCCESS;
  int64_t timeout = TIMEOUT;
  //给自己发
  if (OB_SUCCESS != (ret = context_->rs_rpc_proxy_->set_master_rs_vip_port_to_cluster(target_server_, timeout, target_server_ip_, target_server_.get_port())))
  {
    // WARN log for mysql warnings.
    TBSYS_LOG(WARN, "set new master cluster vip[%s] and port[%d] to self cluster[%s] failed, ret=%d", target_server_ip_, target_server_.get_port(), to_cstring(target_server_), ret);
    TBSYS_LOG(ERROR, "set new master cluster vip[%s] and port[%d] to self cluster[%s] failed, ret=%d", target_server_ip_, target_server_.get_port(), to_cstring(target_server_), ret);
  }
  else
  {
    TBSYS_LOG(INFO, "set new master cluster addr[%s:%d] to server[%s] success", target_server_ip_, target_server_.get_port(), to_cstring(target_server_));
  }
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0;i < cluster_count_; ++i)
    {
      if (i == target_cluster_index_)
      {
        continue;
      }
      //其它所有备集群发
      if (OB_SUCCESS != (ret = context_->rs_rpc_proxy_->set_master_rs_vip_port_to_cluster(all_rs_ups_[i].master_rs_, timeout, target_server_ip_, target_server_.get_port())))
      {
        TBSYS_LOG(WARN, "set new master cluster vip[%s] and port[%d] to cluster[%s] failed, ret=%d", target_server_ip_, target_server_.get_port(), to_cstring(all_rs_ups_[i].master_rs_), ret);
      }
      else
      {
        TBSYS_LOG(INFO, "set new master cluster addr[%s:%d] to server[%s] success", target_server_ip_, target_server_.get_port(), to_cstring(all_rs_ups_[i].master_rs_));
      }
    }
    if (OB_SUCCESS != ret)
    {
      //如果使用的是强切，那么通知其他cluster新的主集群地址失败时，仍然认为成功
      if (force_)
      {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObChangeObi::broadcast_master_cluster()
{
  int ret = OB_SUCCESS;
  int retry_times = 1;
  do {
    if (OB_SUCCESS != (ret = set_master_rs_vip_port_to_all_cluster()))
    {
      TBSYS_LOG(WARN, "set_master_rs_vip_port_to_all_cluster failed, ret %d, master cluster rs [%s:%d",
          ret, target_server_ip_, target_server_.get_port());
    }
  } while (OB_SUCCESS != ret && retry_times-- > 0);

  if (OB_SUCCESS != ret)
  {
    // WARN log for mysql warnings.
    TBSYS_LOG(WARN, "broadcast master cluster rs vip:port [%s:%d] to all clusters failed, ret %d. "
        "You can try to recover this by re-execute the SQL with force flag or "
        "execute the follow command on all master rootserver: "
        " rs_admin -r rs_ip -p rs_port set_config -o master_root_server_ip='%s',master_root_server_port='%d'",
        target_server_ip_, target_server_.get_port(), ret, target_server_ip_, target_server_.get_port());
    TBSYS_LOG(ERROR, "broadcast master cluster rs vip:port [%s:%d] to all clusters failed, ret %d. "
        "You can try to recover this by re-execute the SQL with force flag or "
        "execute the follow command on all master rootserver: "
        " rs_admin -r rs_ip -p rs_port set_config -o master_root_server_ip='%s',master_root_server_port='%d'",
        target_server_ip_, target_server_.get_port(), ret, target_server_ip_, target_server_.get_port());
    ret = OB_BROADCAST_MASTER_CLUSTER_ADDR_FAIL;
  }

  return ret;
}

int ObChangeObi::ms_renew_master_master_ups()
{
  int ret = OB_SUCCESS;
  ObServer new_master_master_ups;
  if (OB_SUCCESS != (ret = context_->merger_rpc_proxy_->get_master_ups(true, new_master_master_ups)))
  {
    TBSYS_LOG(WARN, "renew new master master ups failed, ret=%d", ret);
  }
  else 
  {
    TBSYS_LOG(INFO, "ms renew new master master ups[%s] success", to_cstring(new_master_master_ups));
  }
  return ret;
}
int ObChangeObi::update_all_sys_config()
{
  int ret = OB_SUCCESS;
  char str[200];
  memset(str, 0 , sizeof(str));
  int cnt = snprintf(str, sizeof(str), "alter system set master_root_server_ip='%s' server_type=rootserver, master_root_server_port=%d server_type=rootserver", target_server_ip_, target_server_.get_port());
  ObString alter_sql;
  alter_sql.assign_ptr(str, cnt);
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if (OB_SUCCESS != (ret = result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(alter_sql, result, *context_)))
  {
    TBSYS_LOG(WARN, "direct execute sql=%.*s failed, ret=%d", alter_sql.length(), alter_sql.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = result.open()))
  {
    TBSYS_LOG(WARN, "open result failed, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(result.is_with_rows() == false);
    TBSYS_LOG(INFO, "execute %.*s success", alter_sql.length(), alter_sql.ptr());
    int tmp_err = result.close();
    if (OB_SUCCESS != tmp_err)
    {
      TBSYS_LOG(WARN, "failed to close result set, ret=%d", ret);
    }
    result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}
int ObChangeObi::waiting_for_all_sys_config_stat_updated()
{
  // 这里其实包含两个时间，
  // 1. chunkserver每隔5s会强制更新主主UPS地址。
  // 2. 每隔server必须等到心跳(5s)到来发现config版本变了才会去更新__all_sys_config_stat这张表的信息
  // 这里为了保险，设置成10s
  //sleep(10);
  return OB_SUCCESS;
}
int ObChangeObi::check_master_rs_vip_port()
{
  int ret = OB_SUCCESS;
  ObString sql = ObString::make_string("select name, value from __all_sys_config_stat where name='master_root_server_ip' or name='master_root_server_port'");
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if (OB_SUCCESS != (ret = result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(sql, result, *context_)))
  {
    TBSYS_LOG(WARN, "direct execute sql=%.*s failed, ret=%d", sql.length(), sql.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(result.is_with_rows() == true);
    const ObRow *row = NULL;
    const ObObj *pcell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    ObString name;
    ObString value;
    while ((OB_SUCCESS == ret) && (OB_SUCCESS == (ret = result.get_next_row(row))))
    {
      if (OB_SUCCESS != (ret = row->raw_get_cell(0, pcell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
      }
      else if (pcell->get_type() != ObVarcharType)
      {
        TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
      }
      else if (OB_SUCCESS != (ret = pcell->get_varchar(name)))
      {
        TBSYS_LOG(WARN, "get varchar from ObObj failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row->raw_get_cell(1, pcell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
      }
      else if (pcell->get_type() != ObVarcharType)
      {
        TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
      }
      else if (OB_SUCCESS != (ret = pcell->get_varchar(value)))
      {
        TBSYS_LOG(WARN, "get varchar from ObObj failed, ret=%d", ret);
      }
      else
      {
        if (name == "master_root_server_ip")
        {
          if (value != target_server_ip_)
          {
            ret = OB_CONFIG_NOT_SYNC;
            TBSYS_LOG(ERROR, "config item:name[%.*s] in __all_sys_config_stat not sync, it should be %s, but is %.*s ret=%d",
                name.length(), name.ptr(), target_server_ip_, value.length(), value.ptr(), ret);
            break;
          }
        }
        else if (name == "master_root_server_port")
        {
          char port_str[10];
          memset(port_str, 0 , sizeof(port_str));
          int cnt = snprintf(port_str, sizeof(port_str), "%d", target_server_.get_port());
          if (cnt == value.length() && !strncmp(port_str, value.ptr(), value.length()))
          {
            // nothing
          }
          else
          {
            ret = OB_CONFIG_NOT_SYNC;
            TBSYS_LOG(ERROR, "config item:name[%.*s] in __all_sys_config_stat not sync, ret=%d",
                name.length(), name.ptr(), ret);
            break;
          }
        }
        else
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "column value not expected, name=%.*s ret=%d", name.length(), name.ptr(), ret);
          break;
        }
      }
    }
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}
void ObChangeObi::reset()
{
  context_ = NULL;
  result_set_out_ = NULL;
  master_cluster_index_ = -1;
  target_cluster_index_ = -1;
  force_ = false;
  target_server_port_ = 0;
  memset(target_server_ip_, 0, sizeof(target_server_ip_));
  memset(all_rs_ups_, 0, sizeof(all_rs_ups_));
  target_server_.reset();
  got_rs_ = false;
  multi_master_cluster_ = false;
  master_cluster_exists_ = false;
}
void ObChangeObi::reuse()
{
  reset();
}
int ObChangeObi::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  UNUSED(row_desc);
  return OB_NOT_SUPPORTED;
}
int64_t ObChangeObi::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObChangeObi(new_master_ip=%s)\n", target_server_ip_);
  return pos;
}
void ObChangeObi::set_old_ob_query_timeout(const ObObj &old_ob_query_timeout)
{
  old_ob_query_timeout_ = old_ob_query_timeout;
}
int ObChangeObi::set_change_obi_timeout(const ObObj &change_obi_timeout_value)
{
  int ret = OB_SUCCESS;
  change_obi_timeout_value_ = change_obi_timeout_value;
  if (OB_SUCCESS != (ret = change_obi_timeout_value_.get_int(change_obi_timeout_us_)))
  {
    TBSYS_LOG(WARN, "get int failed, ret=%d", ret);
  }
  return ret;
}
void ObChangeObi::set_check_ups_log_interval(const int interval)
{
  check_ups_log_interval_ = interval;
}
void ObChangeObi::set_force(const bool force)
{
  force_ = force;
}
bool ObChangeObi::is_force()
{
  return force_;
}
int ObChangeObi::execute_sql(const ObString &sql)
{
  int ret = OB_SUCCESS;
  ObResultSet result;
  context_->session_info_->set_current_result_set(&result);
  if (OB_SUCCESS != (ret = result.init()))
  {
    TBSYS_LOG(WARN, "result init failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(sql, result, *context_)))
  {
    TBSYS_LOG(WARN, "direct execute %.*s failed, ret=%d", sql.length(), sql.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(result.is_with_rows() == false);
    int err = result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set, err=%d", err);
    }
    result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObChangeObi::set_target_server_addr(const ObString &target_server)
{
  int ret = OB_SUCCESS;
  if (target_server.length() < MAX_IP_PORT_LENGTH)
  {
    char buf[MAX_IP_PORT_LENGTH];
    memset(buf, 0 , sizeof(buf));
    memcpy(buf, target_server.ptr(), target_server.length());
    char *index = strrchr(buf, ':');
    if (NULL == index)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "input new master addr[%s] invalid, ret=%d", buf, ret);
    }
    else
    {
      memcpy(target_server_ip_, target_server.ptr(), (index - buf));
      target_server_port_ = atoi(static_cast<const char*>(target_server.ptr() + (index - buf + 1)));
      if (target_server_port_ <= 1024)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "input new master addr[%s] invalid, ret=%d", buf, ret);
      }
      else
      {
        bool tmp = target_server_.set_ipv4_addr(target_server_ip_, target_server_port_);
        if (!tmp)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "set_ipv4_addr failed,ret=%d", ret);
        }
      }
    }
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "length of %.*s invalid, ret=%d", target_server.length(), target_server.ptr(), ret);
  }
  return ret;
}
void ObChangeObi::set_target_role(const ObiRole role)
{
  role_ = role;
}
int ObChangeObi::check_addr()
{
  int ret = OB_SUCCESS;
  // 检查自己是不是存在
  bool found = false;
  for (int64_t i = 0;i < cluster_count_; ++i)
  {
    if (all_rs_ups_[i].master_rs_.get_ipv4() == ObServer::convert_ipv4_addr(target_server_ip_)
        && all_rs_ups_[i].master_rs_.get_port() == target_server_port_)
    {
      found = true;
      break;
    }
  }
  if (found)
  {
    ret = OB_SUCCESS;
  }
  else
  {
    ret = OB_IP_PORT_IS_NOT_CLUSTER;
    TBSYS_LOG(ERROR, "input cluster[%s:%d] is not a valid cluster, ret=%d", target_server_ip_, target_server_port_, ret);
  }
  return ret;
}
int ObChangeObi::post_check()
{
  int ret = OB_SUCCESS;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  int64_t end_time = start_time + change_obi_timeout_us_;
  while (start_time <= end_time)
  {
    // maybe new master master ups hasn't been elected yet, rare event
    if (OB_SUCCESS != (ret = ms_renew_master_master_ups()))
    {
      TBSYS_LOG(WARN, "ms renew master master ups failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = update_all_sys_config()))
    {
      TBSYS_LOG(WARN, "update __all_sys_config failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = waiting_for_all_sys_config_stat_updated()))
    {
    }
    else
    {
      //主备切换才需要检查
      if (!force_ && role_.get_role() == ObiRole::INIT)
      {
        if (OB_SUCCESS != (ret = check_master_rs_vip_port()))
        {
          TBSYS_LOG(WARN, "check_master_rs_vip_port failed, ret=%d", ret);
        }
        else
        {
          break;
        }
      }
      else
      {
        // don't check new master rs and vip port
        break;
      }
    }
    sleep(2);
    start_time = tbsys::CTimeUtil::getTime();
  }// while
  return ret;
}
