/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_client.cpp for define oceanbase client API. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "ob_client.h"

namespace oceanbase 
{ 
  namespace client 
  {
    using namespace common;

    ObClient::ObClient(ObServerManager& servers_mgr) 
    : servers_mgr_(servers_mgr), timeout_(DEFAULT_TIME_OUT)
    {

    }

    ObClient::~ObClient()
    {

    }

    int ObClient::init(const int64_t timeout)
    {
      int ret = OB_SUCCESS;

      if (timeout <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, timeout=%ld", timeout);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ObServer dummy_server;
        ret = ObBaseClient::initialize(dummy_server);
        if (OB_SUCCESS == ret)
        {
          timeout_ = timeout;
          ret = rpc_stub_.init(&thread_buffer_, &get_client_mgr());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to init server rpc stub");
          }
        }
      }

      /*
      if (OB_SUCCESS == ret)
      {
        ret = start();
      }
      */

      return ret;
    }

    int ObClient::fetch_schema(const int64_t timestap, 
                               ObSchemaManagerV2& schema_mgr)
    {
      return (rpc_stub_.fetch_schema(timeout_, servers_mgr_.get_root_server(), timestap,
                                     false, schema_mgr));
    }

    int ObClient::fetch_update_server(ObServer& update_server)
    {
      return (rpc_stub_.fetch_update_server(timeout_, servers_mgr_.get_root_server(), 
                                            update_server));
    }

    int ObClient::ms_scan(const ObScanParam& scan_param,
                          ObScanner& scanner)
    {
      return (rpc_stub_.scan(timeout_, servers_mgr_.get_random_merge_server(),
                             scan_param, scanner));
    }

    int ObClient::ms_get(const ObGetParam& get_param,
                         ObScanner& scanner)
    {
      return (rpc_stub_.get(timeout_, servers_mgr_.get_random_merge_server(),
                            get_param, scanner ));
    }

    int ObClient::ups_apply(const ObMutator& mutator)
    {
      const ObServer& update_server = servers_mgr_.get_update_server();
      int ret = OB_SUCCESS;

      ret = rpc_stub_.ups_apply(timeout_, update_server, mutator);
      if (OB_RESPONSE_TIME_OUT == ret)
      {
        // update ups location
        ObServer tmp_server;
        int tmp_ret = rpc_stub_.fetch_update_server(timeout_, servers_mgr_.get_root_server(),
            tmp_server);
        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(ERROR, "failed to fetch update server addr, ret=%d", tmp_ret);
          ret = OB_ERROR;
        }
        else
        {
          servers_mgr_.set_update_server(tmp_server);
          TBSYS_LOG(INFO, "update ups location, ip[%d.%d.%d.%d] port[%d]",
              (tmp_server.get_ipv4() >> 24) & 0xff, (tmp_server.get_ipv4() >> 16) & 0xff,
              (tmp_server.get_ipv4() >> 8) & 0xff, tmp_server.get_ipv4() & 0xff,
              tmp_server.get_port());
        }
      }

      return ret;
    }

    int ObClient::ups_scan(const ObScanParam& scan_param,
                           ObScanner& scanner)
    {
      return (rpc_stub_.scan(timeout_, servers_mgr_.get_update_server(),
                             scan_param, scanner ));
    }

    int ObClient::ups_get(const ObGetParam& get_param,
                          ObScanner& scanner)
    {
      return (rpc_stub_.get(timeout_, servers_mgr_.get_update_server(),
                            get_param, scanner ));
    }

    int ObClient::cs_sql_scan(const sql::ObSqlScanParam& scan_param, common::ObNewScanner& scanner)
    {
      return (rpc_stub_.cs_sql_scan(timeout_, servers_mgr_.get_random_chunk_server(), scan_param, scanner));
    }

    int ObClient::cs_sql_get(const sql::ObSqlGetParam& get_param, common::ObNewScanner& scanner)
    {
      return (rpc_stub_.cs_sql_get(timeout_, servers_mgr_.get_random_chunk_server(), get_param, scanner));
    }

    int ObClient::set_ups_by_rs()
    {
      ObServer ups;
      int ret = rpc_stub_.fetch_update_server(timeout_, servers_mgr_.get_root_server(), ups);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "fetch_update_server ret=%d", ret);
      }
      else
      {
        servers_mgr_.set_update_server(ups);
      }
      return ret;
    }

    int ObClient::set_cs_by_rs()
    {
      servers_mgr_.get_chunk_servers().clear();
      return rpc_stub_.rs_get_data_servers(timeout_, servers_mgr_.get_root_server(), 
          OB_GET_CS_LIST, servers_mgr_.get_chunk_servers());
    }

    int ObClient::set_ms_by_rs()
    {
      servers_mgr_.get_merge_servers().clear();
      return rpc_stub_.rs_get_data_servers(timeout_, servers_mgr_.get_root_server(), 
          OB_GET_MS_LIST, servers_mgr_.get_merge_servers());
    }

    int ObClient::get_last_frozen_version(int64_t &version)
    {
      return rpc_stub_.get_last_frozen_memtable_version(timeout_, servers_mgr_.get_update_server(), version);
    }

    } // end namespace client
  } // end namespace oceanbase
