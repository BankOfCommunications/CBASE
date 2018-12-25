/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_client.h for define oceanbase client API. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CLIENT_OB_CLIENT_H_
#define OCEANBASE_CLIENT_OB_CLIENT_H_

#include "common/ob_base_client.h"
#include "common/ob_general_rpc_stub.h"
#include "common/thread_buffer.h"
#include "common/ob_array.h"
#include "ob_server_manager.h"
#include "ob_server_rpc.h"

namespace oceanbase 
{
  namespace client 
  {
    class ObClient : public common::ObBaseClient
    {
    public:
      ObClient(ObServerManager& servers_mgr);
      virtual ~ObClient();

      virtual int init(const int64_t timeout);

      // root server interface
      int fetch_schema(const int64_t timestap, 
                       common::ObSchemaManagerV2& schema_mgr);
      int fetch_update_server(common::ObServer& update_server);
  
      // merge server interface
      int ms_scan(const common::ObScanParam& scan_param,
                  common::ObScanner& scanner);
      int ms_get(const common::ObGetParam& get_param,
                 common::ObScanner& scanner);

      // update server interface
      int ups_apply(const common::ObMutator &mutator);
      int ups_scan(const common::ObScanParam& scan_param,
                   common::ObScanner& scanner);
      int ups_get(const common::ObGetParam& get_param,
                  common::ObScanner& scanner);

      // chunk server interface
      int cs_sql_scan(const sql::ObSqlScanParam& scan_param,
                  common::ObNewScanner& scanner);
      int cs_sql_get(const sql::ObSqlGetParam& get_param,
                  common::ObNewScanner& scanner);
      int get_last_frozen_version(int64_t &version);

    public:
      int set_ups_by_rs();
      int set_cs_by_rs();
      int set_ms_by_rs();

    private:
      static const int64_t DEFAULT_TIME_OUT = 2000000; //2s

    private:
      DISALLOW_COPY_AND_ASSIGN(ObClient);

      ObServerRpc rpc_stub_;
      common::ThreadSpecificBuffer thread_buffer_;
      ObServerManager& servers_mgr_;
      int64_t timeout_;
    };
  } // namespace oceanbase::client
} // namespace Oceanbase

#endif //OCEANBASE_CLIENT_OB_CLIENT_H_
