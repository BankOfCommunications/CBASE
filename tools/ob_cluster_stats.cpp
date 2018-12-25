/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ob_cluster_stats.cpp is for what ...
 *
 *  Version: $Id: ob_cluster_stats.cpp 2010年12月22日 15时13分21秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include "rootserver/ob_chunk_server_manager.h"
#include "ob_cluster_stats.h"
#include "client_rpc.h"
#include "cs_admin.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

namespace
{
  const char* SERVER_SECTION = "app";
  const int64_t UPS_MASTER_START_INDEX = 3;
  const int64_t UPS_MASTER_END_INDEX = 10;
};

namespace oceanbase
{
  namespace tools
  {

    int32_t ObClusterStats::refresh()
    {
      int ret = 0;
      ObRole server_type = store_.diff.get_server_type();
      if (server_type == OB_ROOTSERVER)
      {
        ret = ObServerStats::refresh();
      }
      else if (server_type == OB_UPDATESERVER)
      {
        ObServer update_server;
        ret = rpc_stub_.get_update_server(update_server);
        ObClientRpcStub update_stub;
        if (OB_SUCCESS == ret)
        {
          ret = update_stub.initialize(update_server,
              &GFactory::get_instance().get_base_client().get_client_mgr());
        }
        if (OB_SUCCESS == ret)
        {
          ret = update_stub.fetch_stats(store_.current);
        }
      }
      else if (server_type == OB_CHUNKSERVER
          || server_type == OB_MERGESERVER)
      {
        ObChunkServerManager obsm;
        ret = rpc_stub_.rs_dump_cs_info(obsm);
        if (OB_SUCCESS == ret)
        {
          ObChunkServerManager::const_iterator it = obsm.begin();
          ObServer node;
          char ipstr[OB_MAX_SERVER_ADDR_SIZE];
          int alive_server_count = 0;
          while (it != obsm.end())
          {
            node = it->server_;
            if (server_type == OB_CHUNKSERVER)
              node.set_port(it->port_cs_);
            else
              node.set_port(it->port_ms_);

            node.to_string(ipstr, OB_MAX_SERVER_ADDR_SIZE);
            /*
            fprintf(stderr, "dump server ip:%s, server status:%ld, serving:%d\n",
                ipstr, it->status_, ObServerStatus::STATUS_SERVING);
                */

            if (it->status_ != ObServerStatus::STATUS_DEAD)
            {
              ObClientRpcStub node_stub;
              ObStatManager node_stats;
              node_stats.set_server_type(server_type);
              ret = node_stub.initialize( node, &GFactory::get_instance().get_base_client().get_client_mgr());
              if (OB_SUCCESS != ret) { ++it; continue ;}
              ret = node_stub.fetch_stats(node_stats);
              //printf("ret=%d, node_stats:%lu\n", ret, node_stats.get_server_type());
              if (OB_SUCCESS != ret) { ++it; continue ;}
              if (alive_server_count == 0)
                store_.current = node_stats;
              else
                store_.current.add(node_stats);
              ++alive_server_count;
            }
            ++it;
          }

          if (0 == alive_server_count)
          {
            fprintf(stderr, "no any alive servers, cannot collect data.\n");
          }

        }

      }
      return ret;
    }

    int load_string(tbsys::CConfig& config, char* dest, const int32_t size,
        const char* section, const char* name, bool not_null)
    {
      int ret = OB_SUCCESS;
      if (NULL == dest || 0 >= size || NULL == section || NULL == name)
      {
        ret = OB_ERROR;
      }

      const char* value = NULL;
      if (OB_SUCCESS == ret)
      {
        value = config.getString(section, name);
        if (not_null && (NULL == value || 0 >= strlen(value)))
        {
          TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && NULL != value)
      {
        if ((int32_t)strlen(value) >= size)
        {
          TBSYS_LOG(ERROR, "%s.%s too long, length (%d) > %d",
              section, name, (int32_t)strlen(value), size);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          memset(dest, 0, size);
          strncpy(dest, value, strlen(value));
        }
      }

      return ret;
    }

    ObAppStats::ObAppStats(ObClientRpcStub &stub, const ObRole server_type, const char* config_file_name)
      : ObServerStats(stub, server_type)
    {
      root_server_array_.init(MAX_SERVER_NUM, root_servers_, 0);
      tbsys::CConfig config;
      if(config.load(config_file_name))
      {
        fprintf(stderr, "load file %s error\n", config_file_name);
      }
      else
      {
        master_index_ = -1;
        int count = config.getInt(SERVER_SECTION, "cluster_count", 0);
        if (count <= 0)
        {
          fprintf(stderr, "cluster_count %d cannot < 0\n", count);
        }
        else
        {
          const int MAX_LEN = 64;
          char ipname[MAX_LEN];
          char portname[MAX_LEN];
          char ip[MAX_LEN];
          char mastername[MAX_LEN];
          int32_t port = 0;
          int is_master = 0;
          ObServer server;
          for (int i = 0; i < count ; ++i)
          {
            snprintf(ipname, MAX_LEN, "rs%d_ip", i);
            snprintf(portname, MAX_LEN, "rs%d_port", i);
            snprintf(mastername, MAX_LEN, "rs%d_is_master", i);
            if (OB_SUCCESS != load_string(config, ip, MAX_LEN, SERVER_SECTION, ipname, true))
            {
              fprintf(stderr, "%s is null\n", ipname);
              break;
            }
            port = config.getInt(SERVER_SECTION, portname, 0);
            if (port <= 0)
            {
              fprintf(stderr, "%s is 0\n", portname);
              break;
            }
            is_master = config.getInt(SERVER_SECTION, mastername, 0);
            if (is_master > 0) master_index_ = i;
            server.set_ipv4_addr(ip, port);
            root_server_array_.push_back(server);
          }
        }
      }
    }

    int ObAppStats::sum_cs_cluster_stats(const ObServer& rs, const ObRole server_type)
    {
      ObClientRpcStub rs_stub;
      ObChunkServerManager obsm;
      int ret = rs_stub.initialize(rs, &GFactory::get_instance().get_base_client().get_client_mgr());
      if (OB_SUCCESS == ret)
      {
        ret = rs_stub.rs_dump_cs_info(obsm);
      }

      ObServer server_list[MAX_SERVER_NUM];
      ObArrayHelper<ObServer> server_array(MAX_SERVER_NUM, server_list, 0);
      if (OB_SUCCESS == ret)
      {
        ObChunkServerManager::const_iterator it = obsm.begin();
        ObServer node;
        while (it != obsm.end())
        {
          node = it->server_;
          if (server_type == OB_CHUNKSERVER)
            node.set_port(it->port_cs_);
          else
            node.set_port(it->port_ms_);

          /*
          node.to_string(ipstr, 64);
             fprintf(stderr, "dump server ip:%s, server status:%ld, serving:%d\n",
             ipstr, it->status_, ObServerStatus::STATUS_SERVING);
             */

          if (it->status_ != ObServerStatus::STATUS_DEAD)
          {
            server_array.push_back(node);
          }
          ++it;
        }

        ret = sum_cluster_stats(server_type, server_array);

      }

      return ret;
    }

    int ObAppStats::sum_ups_cluster_stats(const ObServer& rs, const ObRole server_type)
    {
      ObClientRpcStub rs_stub;
      ObUpsList upslist;
      ObServer server_list[MAX_SERVER_NUM];
      ObArrayHelper<ObServer> server_array(MAX_SERVER_NUM, server_list, 0);

      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = rs_stub.initialize(rs,
              &GFactory::get_instance().get_base_client().get_client_mgr())))
      {
        fprintf(stderr, "initialize server stub error\n");
      }
      else if (OB_SUCCESS != (ret = rs_stub.fetch_update_server_list(upslist)))
      {
        fprintf(stderr, "fetch_update_server_list error\n");
      }
      else
      {
        for (int i = 0; i < upslist.ups_count_; i++)
        {
          ObUpsInfo upsinfo = upslist.ups_array_[i];
          server_array.push_back(upsinfo.addr_);
        }
      }

      ret = sum_cluster_stats(server_type, server_array);

      return ret;
    }

    int ObAppStats::sum_cluster_stats(const ObRole server_type, const common::ObArrayHelper<common::ObServer>& server_array)
    {
      ObStatManager node_stats;
      node_stats.set_server_type(server_type);
      char addr_string[MAX_SERVER_NUM];
      int ret = OB_SUCCESS;

      for (int i = 0; i < server_array.get_array_index(); i++)
      {
        ObServer addr = *server_array.at(i);
        addr.to_string(addr_string, MAX_SERVER_NUM);
        ObClientRpcStub remote_stub;
        if (OB_SUCCESS != (ret = remote_stub.initialize(addr,
                &GFactory::get_instance().get_base_client().get_client_mgr())))
        {
          fprintf(stderr, "initialize server stub error\n");
          break;
        }
        else if (OB_SUCCESS != (ret = remote_stub.fetch_stats(node_stats)))
        {
          fprintf(stderr, "fetch server (%s) stats error\n", addr_string);
          break;
        }
        if (store_.current.begin(mod_) == store_.current.end(mod_))
          store_.current = node_stats;
        else
          store_.current.add(node_stats);
      }

      return ret;
    }

    int32_t ObAppStats::refresh()
    {
      int ret = 0;
      ObRole server_type = store_.diff.get_server_type();

      store_.current.reset();
      store_.current.set_server_type(server_type);


      if (server_type == OB_ROOTSERVER)
      {
        ret = sum_cluster_stats(server_type, root_server_array_);
      }
      else if (server_type == OB_UPDATESERVER)
      {
        ObStatManager node_stats;
        node_stats.set_server_type(server_type);

        for (int i = 0; i < root_server_array_.get_array_index(); ++i)
        {
          if (i == master_index_)
          {
            ObClientRpcStub rs_stub;
            ObClientRpcStub ups_stub;
            ObServer update_server;

            if (OB_SUCCESS != (ret = rs_stub.initialize(*root_server_array_.at(i),
                &GFactory::get_instance().get_base_client().get_client_mgr())))
            {
              fprintf(stderr, "initialize rs stub failed\n");
              break;
            }
            else if (OB_SUCCESS != (ret = rs_stub.get_update_server(update_server)))
            {
              fprintf(stderr, "get update server addr from rs failed\n");
              break;
            }
            else if (OB_SUCCESS != (ret = ups_stub.initialize(update_server,
                    &GFactory::get_instance().get_base_client().get_client_mgr())))
            {
              fprintf(stderr, "initialize ups stub failed\n");
              break;
            }
            else if (OB_SUCCESS != (ret = ups_stub.fetch_stats(node_stats)))
            {
              fprintf(stderr, "fetch ups stats failed\n");
              break;
            }
          }

          ret = sum_ups_cluster_stats(*root_server_array_.at(i), server_type);
          if (OB_SUCCESS != ret) break;
        }

        // set apply stat. value
        ObStatManager::const_iterator it = store_.current.begin(mod_);
        ObStat *stat_item = NULL;;
        while (it != store_.current.end(mod_))
        {
          for (int index = UPS_MASTER_START_INDEX; index <= UPS_MASTER_END_INDEX; ++index)
          {
            node_stats.get_stat(it->get_mod_id(), it->get_table_id(), stat_item);
            if (NULL != stat_item)
              const_cast<ObStat*>(it)->set_value(index, stat_item->get_value(index));
          }
          ++it;
        }
      }
      else if (server_type == OB_CHUNKSERVER
          || server_type == OB_MERGESERVER)
      {
        for (int i = 0; i < root_server_array_.get_array_index(); ++i)
        {
          ret = sum_cs_cluster_stats(*root_server_array_.at(i), server_type);
          if (OB_SUCCESS != ret) break;
        }
      }

      return ret;
    }

  }
}
