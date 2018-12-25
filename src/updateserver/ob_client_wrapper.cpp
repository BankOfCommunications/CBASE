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

#include "ob_client_wrapper.h"
#include "common/ob_define.h"
#include "mergeserver/ob_ms_get_cell_stream.h"
#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;
    using namespace mergeserver;
    ObClientWrapper::ObClientWrapper(const int64_t rpc_retry_times,
        const int64_t rpc_timeout,
        const ObServer & root_server,
        const ObServer & merge_server,
        ObUpsTableMgr& table_mgr,
        ObUpsCache& ups_cache)
        :rpc_proxy_(rpc_retry_times,rpc_timeout, root_server, merge_server, table_mgr, ups_cache)
    {
      ups_rpc_agent_.init(rpc_proxy_);
      init_ = false;
    }

    ObClientWrapper::~ObClientWrapper()
    {
      if(NULL != ups_stream_)
      {
        delete ups_stream_;
      }
      if(NULL != ups_join_stream_)
      {
        delete ups_join_stream_;
      }
    }

   int ObClientWrapper::init(ObMergerRpcStub * rpc_stub,
       ObMergerSchemaManager * schema,
       ObMergerLocationCacheProxy* cache)
    {
      int ret = OB_SUCCESS;
      if(init_)
      {
        TBSYS_LOG(WARN, "have already inited");
        ret = OB_ERROR;
      }
      else
      {
        if(NULL == rpc_stub || NULL == schema || NULL == cache)
        {
          TBSYS_LOG(WARN, "input param error, rpc=%p, schema=%p, cache=%p",
              rpc_stub, schema, cache);
          ret = OB_ERROR;
        }
        else
        {
          if((OB_SUCCESS == rpc_proxy_.init(rpc_stub, schema, cache)))
          {
            ups_stream_ = new(std::nothrow) ObMSGetCellStream(&rpc_proxy_);
            if(NULL == ups_stream_)
            {
              TBSYS_LOG(WARN,"new ups_stream failed");
              ret = OB_ERROR;
            }
            else
            {
              ups_join_stream_ = new(std::nothrow) ObMSGetCellStream(&rpc_proxy_);
              if(NULL == ups_join_stream_)
              {
                TBSYS_LOG(WARN,"new ups_join_stream failed");
                ret = OB_ERROR;
              }
              else
              {
                init_ = true;
              }
            }
          }
          else
          {
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }


    int ObClientWrapper::get_cell(oceanbase::common::ObCellInfo * *cell)
    {
      return ups_rpc_agent_.get_cell(cell);
    }

    int ObClientWrapper::next_cell()
    {
      return ups_rpc_agent_.next_cell();
    }

    void ObClientWrapper::clear()
    {
      ups_rpc_agent_.clear();
    }

    int ObClientWrapper::get(const ObGetParam& get_param, const ObSchemaManagerV2 & schema_mgr)
    {
      int ret = OB_SUCCESS;
      if(!init_)
      {
        TBSYS_LOG(WARN,"have not inited");
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        const ObCellInfo * cell = get_param[0];
        if((0 == get_param.get_cell_size()) || (NULL == cell))
        {
          TBSYS_LOG(ERROR, "check get param failed:size[%ld], cell[%p]",
              get_param.get_cell_size(), cell);
          ret = OB_ERROR;
        }
        else
        {
          ret = ups_rpc_agent_.set_request_param(-1, get_param, *ups_stream_, *ups_join_stream_, schema_mgr, MAX_MEMORY_SIZE);
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN,"ups rpc agent set request param failed,ret=%d",ret);
          }
        }
      }
      return ret;
    }
  }//end of namespace updateserver
}//end of namespace oceanbase
