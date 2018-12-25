/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_rpc.h for define API of merge server, 
 * chunk server, update server, root server rpc. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CLIENT_OB_SERVER_RPC_H_
#define OCEANBASE_CLIENT_OB_SERVER_RPC_H_

#include "common/ob_general_rpc_stub.h"
#include "common/ob_new_scanner.h"
#include "sql/ob_sql_scan_param.h"
#include "sql/ob_sql_get_param.h"
#include "rootserver/ob_chunk_server_manager.h"

namespace oceanbase 
{
  namespace client 
  {
    class ObServerRpc : public common::ObGeneralRpcStub
    {
      public:

        ObServerRpc();
        ~ObServerRpc();

      public:
        int cs_sql_scan(const int64_t timeout, const common::ObServer & root_server,
            const sql::ObSqlScanParam & scan_param, common::ObNewScanner& scanner);
        int cs_sql_get(const int64_t timeout, const common::ObServer & root_server,
            const sql::ObSqlGetParam& get_param, common::ObNewScanner& scanner);
        int rs_get_data_servers(const int64_t timeout, const common::ObServer & root_server,
            const int32_t pcode, common::ObArray<common::ObServer> &chunk_servers);
    };
  } // namespace oceanbase::client
} // namespace Oceanbase

#endif // OCEANBASE_CLIENT_OB_SERVER_RPC_H_
