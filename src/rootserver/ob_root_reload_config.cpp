/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 17:05:22 fufeng.syd>
 * Version: $Id$
 * Filename: ob_root_reload_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "ob_root_reload_config.h"
#include "ob_root_server2.h"

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootReloadConfig::ObRootReloadConfig(const ObRootServerConfig &config)
  : config_(config), root_server_(NULL)
{

}

ObRootReloadConfig::~ObRootReloadConfig()
{

}

int ObRootReloadConfig::operator ()()
{
  int ret = OB_SUCCESS;

  if (NULL == root_server_)
  {
    TBSYS_LOG(WARN, "NULL root server.");
    ret = OB_NOT_INIT;
  }
  else
  {
    const ObRootServerConfig& config = root_server_->get_config();
    //config.print();

    if (OB_SUCCESS == ret && NULL != root_server_->ups_manager_)
    {
      root_server_->ups_manager_->set_ups_config((int32_t)config.read_master_master_ups_percent
                                                 //add pangtianze [Paxos Cluster.Flow.UPS] 20170119:b
                                                 ,(int32_t)config.is_strong_consistency_read
                                                 //add:e
                                                 //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                                 //,(int32_t)config.read_slave_master_ups_percent
                                                 //del:e
                                                 );
    }

    TBSYS_LOG(INFO, "after reload config, ret=%d", ret);
  }
  return ret;
}

void ObRootReloadConfig::set_root_server(ObRootServer2 &root_server)
{
  root_server_ = &root_server;
}
