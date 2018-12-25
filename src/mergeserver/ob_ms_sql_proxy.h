/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2012-11-26 15:19:53 ryan>
 * Version: $Id$
 * Filename: ob_ms_sql_proxy.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_MS_SQL_PROXY_H_
#define _OB_MS_SQL_PROXY_H_

#include "common/ob_string.h"
#include "sql/ob_sql_result_set.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_result_set.h"

using namespace oceanbase::common;
using oceanbase::sql::ObSQLResultSet;

namespace oceanbase
{
  namespace mergeserver
  {
    /* predeclearation */
    class ObMergeServerService;
    /*
     * @class ObMsSQLProxy
     * @brief sql execution proxy
     */
    class ObMsSQLProxy
    {
      public:
        ObMsSQLProxy();
        virtual ~ObMsSQLProxy() {};

       int execute(const ObString &cmd,
                    ObSQLResultSet &rs,
                    sql::ObSqlContext &context,
                    int64_t schema_version);
        void set_env(ObMergerRpcProxy  *rpc_proxy,
                     ObMergerRootRpcProxy *root_rpc,
                     ObMergerAsyncRpcStub   *async_rpc,
                     common::ObMergerSchemaManager *schema_mgr,
                     common::ObTabletLocationCacheProxy *cache_proxy,
                     const ObMergeServerService *ms_service);
        // @note context & session should be allocated on the stack
        int init_sql_env(ObSqlContext &context, int64_t &schema_version,
                         ObSQLResultSet &result, ObSQLSessionInfo &session);
        int cleanup_sql_env(ObSqlContext &context, ObSQLResultSet &rs);
      private:
        const ObMergeServerService *ms_service_;
        // environment virable
        mergeserver::ObMergerRpcProxy  *rpc_proxy_;
        mergeserver::ObMergerRootRpcProxy *root_rpc_;
        mergeserver::ObMergerAsyncRpcStub   *async_rpc_;
        common::ObMergerSchemaManager *schema_mgr_;
        ObTabletLocationCacheProxy *cache_proxy_;
        common::DefaultBlockAllocator block_allocator_;
    };
  }
}


#endif /* _OB_MS_SQL_PROXY_H_ */
