
#include "ob_restart_server.h"

using namespace oceanbase;
using namespace rootserver;

ObRestartServer::ObRestartServer()
  :config_(NULL),
  server_manager_(NULL),
  root_table_(NULL),
  rpc_stub_(NULL),
  log_worker_(NULL),
  root_table_build_mutex_(NULL),
  server_manager_rwlock_(NULL),
  root_table_rwlock_(NULL)
{
}

bool ObRestartServer::check_inner_stat()
{
  return NULL != config_ &&
  NULL != server_manager_ &&
  NULL != root_table_ &&
  NULL != rpc_stub_ &&
  NULL != log_worker_ &&
  NULL != root_table_build_mutex_ &&
  NULL != server_manager_rwlock_ &&
  NULL != root_table_rwlock_;
}

int ObRestartServer::restart_servers()
{
  int ret = common::OB_SUCCESS;

  ObChunkServerManager::iterator it;
  common::ObServer chunk_server;

  if (!check_inner_stat())
  {
    ret = common::OB_ERROR;
    TBSYS_LOG(WARN, "ObRestartServer check inner stat fail");
  }

  bool has_wait_restart = false;

  if(common::OB_SUCCESS == ret)
  {
    for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
    {
      if(it->wait_restart_)
      {
        has_wait_restart = true;
        break;
      }
    }
  }

  if(common::OB_SUCCESS == ret && has_wait_restart)
  {
    find_can_restart_server((int32_t)config_->tablet_replicas_num);
    for(it = server_manager_->begin(); it != server_manager_->end(); ++it)
    {
      if(it->wait_restart_ && it->can_restart_)
      {
        chunk_server = it->server_;
        chunk_server.set_port(it->port_cs_);
        ret = rpc_stub_->shutdown_cs(chunk_server, true, config_->network_timeout);
        if(common::OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "restart cs fail:ret[%d], chunkserver[%s]", ret, chunk_server.to_cstring());
        }
        else
        {
          shutdown_cs(it);
          it->status_ = ObServerStatus::STATUS_DEAD;
        }
        it->wait_restart_ = false;
        break;
      }
    }
  }

  return ret;
}

void ObRestartServer::shutdown_cs(ObServerStatus* it)
{
  int64_t now = tbsys::CTimeUtil::getTime();
  TBSYS_LOG(INFO,"shutdown cs, now:%ld, lease_duration:%s", now, config_->cs_lease_duration_time.str());
  server_manager_->set_server_down(it);
  log_worker_->server_is_down(it->server_, now);

  tbsys::CThreadGuard mutex_guard(root_table_build_mutex_); //this for only one thread modify root_table
  tbsys::CWLockGuard guard(*root_table_rwlock_);
  if (root_table_ != NULL)
  {
    root_table_->server_off_line(static_cast<int32_t>(it - server_manager_->begin()), now);
  }
  else
  {
    TBSYS_LOG(ERROR, "root_table_for_query_ = NULL, server_index=%ld", it - server_manager_->begin());
  }
}

void ObRestartServer::find_can_restart_server(int32_t expected_replicas_num)
{
  ObRootTable2::const_iterator it;
  int32_t replicas_num = 0;
  ObChunkServerManager::iterator server_status_it;
  ObServerStatus* server_status = NULL;
  int32_t server_info_index = common::OB_INVALID_INDEX;


  for (server_status_it = server_manager_->begin(); server_status_it != server_manager_->end(); ++server_status_it)
  {
    server_status_it->can_restart_ = true;
  }

  tbsys::CRLockGuard guard(*root_table_rwlock_);
  for (it = root_table_->begin(); it != root_table_->sorted_end(); ++it)
  {
    replicas_num = 0;
    for (int i = 0; i < expected_replicas_num; ++i)
    {
      server_info_index = it->server_info_indexes_[i];
      if (common::OB_INVALID_INDEX != server_info_index)
      {
        server_status = server_manager_->get_server_status(server_info_index);
        if(NULL != server_status)
        {
          if(ObServerStatus::STATUS_SERVING == server_status->status_)
          {
            replicas_num++;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "get server status fail:server index[%d]", server_info_index);
        }
      }
    }

    if (replicas_num < expected_replicas_num)
    {
      for(int i = 0; i < expected_replicas_num; ++i)
      {
        server_info_index = it->server_info_indexes_[i];
        if(common::OB_INVALID_INDEX != server_info_index)
        {
          server_status = server_manager_->get_server_status(server_info_index);
          if(NULL != server_status)
          {
            server_status->can_restart_ = false;
          }
          else
          {
            TBSYS_LOG(WARN, "get server status fail:server index[%d]", server_info_index);
          }
        }
      }
    }

    bool has_can_restart = false;
    for (server_status_it = server_manager_->begin(); server_status_it != server_manager_->end(); ++server_status_it)
    {
      if(server_status_it->wait_restart_ && server_status_it->can_restart_)
      {
        has_can_restart = true;
        break;
      }
    }
    if(!has_can_restart)
    {
      break;
    }
  }
}



