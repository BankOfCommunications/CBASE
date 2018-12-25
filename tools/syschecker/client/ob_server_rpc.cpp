/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_rpc.cpp for define API of merge server, 
 * chunk server, update server, root server rpc. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_server_rpc.h"
#include "common/ob_result.h"

namespace oceanbase 
{ 
  namespace client 
  {
    using namespace common;
    using namespace common::serialization;

    ObServerRpc::ObServerRpc()
    {

    }
    
    ObServerRpc::~ObServerRpc()
    {
    }
    
    int ObServerRpc::cs_sql_scan(const int64_t timeout, const common::ObServer & server,
        const sql::ObSqlScanParam & scan_param, common::ObNewScanner& scanner)
    {
      return send_1_return_1(server, timeout, OB_SQL_SCAN_REQUEST, DEFAULT_VERSION, scan_param, scanner);
    }

    int ObServerRpc::cs_sql_get(const int64_t timeout, const common::ObServer & server,
        const sql::ObSqlGetParam& get_param, common::ObNewScanner& scanner)
    {
      return send_1_return_1(server, timeout, OB_SQL_GET_REQUEST, DEFAULT_VERSION, get_param, scanner);
    }

    int ObServerRpc::rs_get_data_servers(const int64_t timeout, const common::ObServer & root_server,
        const int32_t pcode, common::ObArray<common::ObServer> &data_servers)
    {
      int ret = OB_SUCCESS; 
      int64_t pos = 0; 
      int32_t num = 0;
      int64_t reserve = 0;
      ObDataBuffer data_buffer; 
      ObResultCode rc;
      ObServer server;

      if (OB_SUCCESS != (ret = send_param_0(data_buffer, root_server, timeout, pcode, DEFAULT_VERSION)))
      {
        TBSYS_LOG(ERROR, "send_param_0 error, root_server=%s, timeout=%ld, pcode=%d", 
            to_cstring(root_server), timeout, pcode);
      }
      else if (OB_SUCCESS != (ret = deserialize_result_1(data_buffer, pos, rc, num)))
      {
        TBSYS_LOG(ERROR, "deserialize_result error, root_server=%s, timeout=%ld, pcode=%d, pos=%ld", 
            to_cstring(root_server), timeout, pcode, pos);
      }
      else
      {
        for (int32_t i = 0; i < num && OB_SUCCESS == ret; ++i)
        {
          if (OB_SUCCESS != (ret = server.deserialize(data_buffer.get_data(), data_buffer.get_position(), pos)))
          {
            TBSYS_LOG(ERROR, "deserialize server error, ret=%d, root_server=%s, timeout=%ld, pcode=%d, pos=%ld", 
                ret, to_cstring(root_server), timeout, pcode, pos);
          }
          else if (OB_SUCCESS != (ret = data_servers.push_back(server)))
          {
            TBSYS_LOG(ERROR, "push back, ret=%d, server=%s, timeout=%ld, pcode=%d, pos=%ld", 
                ret, to_cstring(server), timeout, pcode, pos);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi64( 
                  data_buffer.get_data(), data_buffer.get_position(), pos, &reserve)))
          {
            TBSYS_LOG(ERROR, "deserialize reserve error, ret= %d, root_server=%s, timeout=%ld, pcode=%d, pos=%ld", 
                ret, to_cstring(root_server), timeout, pcode, pos);
          }
        }
      }
      return ret;
    }


  } // end namespace client
} // end namespace oceanbase
