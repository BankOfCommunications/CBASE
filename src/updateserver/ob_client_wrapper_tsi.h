/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_client_wrapper_tsi.h,v 0.1 2011/08/11 18:53:15 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_CLIENT_WRAPPER_TSI_H__
#define __OCEANBASE_CHUNKSERVER_OB_CLIENT_WRAPPER_TSI_H__

#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "ob_client_wrapper.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;
    using namespace oceanbase::mergeserver;

    class ObClientWrapperTSI
    {
      public:
        ObClientWrapperTSI()
        {
          client_wrapper_ = NULL;
          is_init_ = false;
        }

        ~ObClientWrapperTSI()
        {
          if (NULL != client_wrapper_)
          {
            delete client_wrapper_;
            client_wrapper_ = NULL;
          }
        }

      public:
        int init(const int64_t rpc_retry_times, const int64_t rpc_timeout, const ObServer& root_server,
            const ObServer& merge_server, ObUpsTableMgr& table_mgr,
            ObUpsCache& ups_cache, ObMergerRpcStub* rpc_stub, ObMergerSchemaManager* schema,
            ObMergerLocationCacheProxy* tablet_cache)
        {
          int ret = OB_SUCCESS;

          if (!is_init_)
          {
            client_wrapper_ = new (std::nothrow) ObClientWrapper(rpc_retry_times, rpc_timeout, root_server,
                merge_server, table_mgr, ups_cache);
            if (NULL == client_wrapper_)
            {
              TBSYS_LOG(ERROR, "no enough memory");
              ret = OB_MEM_OVERFLOW;
            }
            else
            {
              ret = client_wrapper_->init(rpc_stub, schema, tablet_cache);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to init client wrapper, ret=%d", ret);
                if (NULL != client_wrapper_)
                {
                  delete client_wrapper_;
                  client_wrapper_ = NULL;
                }
              }
            }

            if (OB_SUCCESS == ret)
            {
              is_init_ = true;
            }
          }

          return ret;
        }

        ObClientWrapper* get_client_wrapper() const
        {
          return client_wrapper_;
        }

      private:
        ObClientWrapper* client_wrapper_;
        bool is_init_;
    };
  }
}

#endif //__OB_CLIENT_WRAPPER_TSI_H__

