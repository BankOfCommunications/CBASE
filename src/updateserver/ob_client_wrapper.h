////===================================================================
 //
 // ob_ups_cache.h / updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-02-24 by Rongxuan (rongxuan.lc@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#ifndef OCEANBASE_UPDATESERVER_CLIENTWRAPPER_H_
#define OCEANBASE_UPDATESERVER_CLIENTWRAPPER_H_

#include "mergeserver/ob_ms_get_cell_stream.h"
#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "mergeserver/ob_merge_join_agent_imp.h"
#include "ob_ups_rpc_proxy.h"
#include "ob_ups_cache.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;
    using namespace mergeserver;
    class ObClientWrapper
    {
      public:
        ObClientWrapper(const int64_t rpc_retry_times,
            const int64_t rpc_timeout,
            const ObServer & root_server,
            const ObServer & merge_server,
            ObUpsTableMgr & table_mgr,
            ObUpsCache& ups_cache);

        ~ObClientWrapper();

        int init(ObMergerRpcStub * rpc_stub,
            ObMergerSchemaManager * schema,
            ObMergerLocationCacheProxy * cache);

        // 获取一批cell的值
        //通过ObMergeJoinAgent的get_cell得到所有结果值
        //get_param的中必须把end_version_设为ups活跃表版本号-1
        int get(const ObGetParam & get_param, const ObSchemaManagerV2 & schema_mgr);

        int get_cell(oceanbase::common::ObCellInfo * *cell);
        int next_cell();
        void clear();

        inline ObUpsMergerRpcProxy& get_proxy()
        {
          return rpc_proxy_;
        }

      private:
        static const int64_t MAX_MEMORY_SIZE = 2 * 1024L * 1024L;

      private:
        bool init_;
        ObMSGetCellStream * ups_stream_;
        ObMSGetCellStream * ups_join_stream_;
        ObUpsMergerRpcProxy rpc_proxy_;
        ObGetMergeJoinAgentImp ups_rpc_agent_;
    };
  } //end of namespace updateserver
}//end of namespace oceanbase

#endif //OCEANBASE_UPDATASERVER_CLIENT_WRAPPER
