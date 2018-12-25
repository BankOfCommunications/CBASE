/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_ms_provider.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_ms_provider.h"
#include "common/ob_scan_param.h"
//add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
#include "ob_root_server2.h"
//add:e
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootMsProvider::ObRootMsProvider(ObChunkServerManager &server_manager):server_manager_(server_manager)
{
  init_ = false;
  config_ = NULL;
  rpc_stub_ = NULL;
  //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
  root_server_ = NULL;
  //add:e
}

ObRootMsProvider::~ObRootMsProvider()
{
}

void ObRootMsProvider::init(ObRootServerConfig & config, ObRootRpcStub &rpc_stub
                            //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                            , ObRootServer2 &root_server
                            //add:e
                            )
{
  init_ = true;
  config_ = &config;
  rpc_stub_ = &rpc_stub;
  //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
  root_server_ = &root_server;
  //add:e
}

int ObRootMsProvider::get_ms(ObServer &server, const bool query_master_cluster)
{
  int ret= OB_ERROR;
  if (init_)
  {
    if (query_master_cluster)
    {
      ObServer master_rs;
      ObServer master_master_rs;
      config_->get_root_server(master_rs);
      config_->get_master_root_server(master_master_rs);
      if (master_rs == master_master_rs)
      {
        ret = get_ms(server);
      }
      else
      {
        ret = get_ms(master_master_rs, server);
      }
    }
    else
    {
      ret = get_ms(server);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get ms. ret=%d", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(TRACE, "get ms addr, ms_addr=%s", to_cstring(server));
    }
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "ms provide need init first");
  }
  return ret;
}

int ObRootMsProvider::get_ms(const ObServer & master_rs, ObServer & server)
{
  ObArray<ObServer> ms_array;
  int ret = rpc_stub_->fetch_ms_list(master_rs, ms_array, config_->network_timeout);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get ms list, ret=%d", ret);
  }
  else
  {
    static __thread int32_t last_index = -1;
    int32_t server_size = (int32_t)ms_array.count();
    if (server_size > 0)
    {
      ret = OB_SUCCESS;
      last_index = (last_index + 1) % server_size;
      server = ms_array.at(last_index);
      TBSYS_LOG(TRACE, "get alive ms succ:index[%d], server[%s]", last_index, server.to_cstring());
    }
    else
    {
      ret = OB_MS_NOT_EXIST;
      TBSYS_LOG(ERROR, "fail to get master instance ms. ms_list.count=0");
    }
  }
  return ret;
}

int ObRootMsProvider::get_ms(ObServer & server)
{
  int ret = OB_SUCCESS;
  if (init_)
  {
    static __thread int32_t last_index = -1;
    int32_t server_size = (int32_t)server_manager_.size();
    if (server_size > 0)
    {
      last_index = (last_index + 1) % server_size;
      //add bingo [Paxos Cluster.Flow.Ms] 20161020:b
      ObUps master_ups;
      int32_t master_ups_cluster_id = root_server_->get_master_cluster_id();
      if(OB_SUCCESS != (ret = root_server_->get_ups_manager()->get_ups_master(master_ups)))
      {
        TBSYS_LOG(WARN, "there is no master ups at this moment, ret[%d], then use master_cluster_id", ret);
      }
      else
      {
        master_ups_cluster_id = master_ups.addr_.cluster_id_;
      }
      //add:e
      ret = server_manager_.get_next_alive_ms(last_index, server
                                              //mod bingo [Paxos Cluster.Flow.Ms] 20161020:b
                                              //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                                              //, root_server_->get_master_cluster_id()
                                              , master_ups_cluster_id
                                              //add:e
                                              //mod:e
                                              );
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get next alive ms failed:index[%d], ret[%d]", last_index, ret);
      }
      else
      {
        TBSYS_LOG(TRACE, "get alive ms succ:index[%d], server[%s]", last_index, server.to_cstring());
      }
    }
    else
    {
     //mod pangtianze [Paxos] 20161024:b
     //ret = OB_MS_NOT_EXIST;
     //TBSYS_LOG(WARN, "no merge server find in server manager");
     ObServer master_master_rs;
	 config_->get_master_root_server(master_master_rs);
     ret = get_ms(master_master_rs, server);
     if (OB_SUCCESS != ret)
     {
         ret = OB_MS_NOT_EXIST;
         TBSYS_LOG(WARN, "no merge server find in server manager");
     }
     //mod:e
    }
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "ms provide need init first");
  }

  return ret;
}

