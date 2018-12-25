/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 12:43:01 fufeng.syd>
 * Version: $Id$
 * Filename: ob_root_server_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#include "ob_root_server_config.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

int ObRootServerConfig::get_root_server(ObServer &server) const
{
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(root_server_ip, (int32_t)port))
  {
    TBSYS_LOG(WARN, "Bad rootserver address! ip: [%s], port: [%s]",
              root_server_ip.str(), port.str());
    ret = OB_BAD_ADDRESS;
  }
  //add lbzhong [Paxos Cluster.Balance] 201607011:b
  server.cluster_id_ = (int32_t)cluster_id;
  //add:e
  return ret;
}

int ObRootServerConfig::get_master_root_server(ObServer &server) const
{
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(master_root_server_ip, (int32_t)master_root_server_port))
  {
    TBSYS_LOG(WARN, "Bad rootserver address! ip: [%s], port: [%s]",
              master_root_server_ip.str(), master_root_server_port.str());
    ret = OB_BAD_ADDRESS;
  }
  return ret;
}
