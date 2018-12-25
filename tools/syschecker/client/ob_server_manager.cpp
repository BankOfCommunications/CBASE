/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_manager.cpp for manage servers. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <stdlib.h>
#include <tblog.h>
#include "common/ob_malloc.h"
#include "ob_server_manager.h"

namespace oceanbase 
{ 
  namespace client 
  {
    using namespace common;

    ObServerManager::ObServerManager() 
    {
    }

    ObServerManager::~ObServerManager()
    {
    }

    const ObServer& ObServerManager::get_root_server() const
    {
      return root_server_;
    }

    int ObServerManager::set_root_server(const ObServer& root_server)
    {
      int ret = OB_SUCCESS;

      if (root_server.get_ipv4() == 0 || root_server.get_port() == 0)
      {
        TBSYS_LOG(WARN, "invalid root server, ip=%d, port=%d",
                  root_server.get_ipv4(), root_server.get_port());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        root_server_ = root_server;
      }

      return ret;
    }

    const ObServer& ObServerManager::get_update_server() const
    {
      return update_server_;
    }

    int ObServerManager::set_update_server(const ObServer& update_server)
    {
      int ret = OB_SUCCESS;

      if (update_server.get_ipv4() == 0 || update_server.get_port() == 0)
      {
        TBSYS_LOG(WARN, "invalid update server, ip=%d, port=%d",
                  update_server.get_ipv4(), update_server.get_port());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        update_server_ = update_server;
      }

      return ret;
    }

    const ObServer& ObServerManager::get_random_merge_server() const
    {
      return merge_servers_.at(random() % merge_servers_.count());
    }

    int ObServerManager::add_merge_server(const ObServer& merge_server)
    {
      int ret = OB_SUCCESS;

      if (merge_server.get_ipv4() == 0 || merge_server.get_port() == 0)
      {
        TBSYS_LOG(WARN, "invalid merge server, ip=%d, port=%d",
                  merge_server.get_ipv4(), merge_server.get_port());
        ret = OB_ERROR;
      }
      else
      {
        merge_servers_.push_back(merge_server);
      }

      return ret;
    }


    const ObServer& ObServerManager::get_random_chunk_server() const
    {
      return chunk_servers_.at(random() % chunk_servers_.count());
    }

    int ObServerManager::add_chunk_server(const ObServer& chunk_server)
    {
      int ret = OB_SUCCESS;

      if (chunk_server.get_ipv4() == 0 || chunk_server.get_port() == 0)
      {
        TBSYS_LOG(WARN, "invalid merge server, ip=%d, port=%d",
                  chunk_server.get_ipv4(), chunk_server.get_port());
        ret = OB_ERROR;
      }
      else
      {
        chunk_servers_.push_back(chunk_server);
      }

      return ret;
    }

  } // end namespace client
} // end namespace oceanbase
