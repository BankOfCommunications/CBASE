/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ups_rpc_proxy.cpp,v 0.1 2011/06/30 11:28:56 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ups_rpc_proxy.h"
#include "mergeserver/ob_ms_tablet_location_item.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace updateserver
  {
    ObUpsMergerRpcProxy::ObUpsMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
        const ObServer& root_server, const ObServer& merge_server, ObUpsTableMgr& table_mgr,
        ObUpsCache& ups_cache) : ObMergerRpcProxy(retry_times, timeout, root_server, merge_server)
        , table_mgr_(table_mgr), ups_cache_(ups_cache)
    {
      // does nothing
    }

    ObUpsMergerRpcProxy::~ObUpsMergerRpcProxy()
    {
    }

    int ObUpsMergerRpcProxy::init(ObMergerRpcStub* rpc_stub, common::ObMergerSchemaManager* schema,
        ObMergerLocationCacheProxy* cache)
    {
      return ObMergerRpcProxy::init(rpc_stub, schema, cache);
    }

    int ObUpsMergerRpcProxy::cs_get(const ObGetParam& get_param,
        ObMergerTabletLocation& addr, ObScanner& scanner, ObIterator*& it_out)
    {
      int err = OB_SUCCESS;
      int64_t cell_size = get_param.get_cell_size();

      if (0 == cell_size)
      {
        TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObBufferHandle buffer_handle;
        ObUpsCacheValue value;
        ObCellInfo* cell_info = get_param[0];
        if (NULL == cell_info)
        {
          TBSYS_LOG(WARN, "invalid param, the first cell info is NULL");
          err = OB_ERROR;
        }
        else
        {
          err = ups_cache_.get(cell_info->table_id_, cell_info->row_key_,
              buffer_handle, cell_info->column_id_, value);
          if (OB_ENTRY_NOT_EXIST != err && OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to get from cache, err=%d", err);
          }
          else if (OB_ENTRY_NOT_EXIST == err)
          {
            // get cell data from chunk server
            TBSYS_LOG(DEBUG, "not exist in cache, get from cs");
            err = ObMergerRpcProxy::cs_get(get_param, addr, scanner, it_out);
          }
          else
          {
            ObCellInfo result_cell;
            result_cell.table_id_ = cell_info->table_id_;
            result_cell.row_key_ = cell_info->row_key_;
            result_cell.column_id_ = cell_info->column_id_;

            bool compare_row = (OB_ALL_MAX_COLUMN_ID == cell_info->column_id_);
            if (compare_row)
            {
              if (ObExtendType != value.value.get_type())
              {
                TBSYS_LOG(WARN, "cell value is not extend type, %s", print_obj(value.value));
                err = OB_ERROR;
              }
              else if (ObActionFlag::OP_ROW_EXIST == value.value.get_ext())
              {
                result_cell.value_.set_ext(ObActionFlag::OP_NOP);
              }
              else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == value.value.get_ext())
              {
                result_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
              }
              else
              {
                TBSYS_LOG(WARN, "invalid value, ext_val=%ld", value.value.get_ext());
                err = OB_ERROR;
              }
            }
            else
            {
              result_cell.value_ = value.value;
            }

            err = scanner.add_cell(result_cell);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to add cell, err=%d", err);
            }
            else
            {
              scanner.set_data_version(value.version);
              scanner.set_is_req_fullfilled(1 == get_param.get_cell_size() ? true : false, 1);

              addr.server_.chunkserver_.reset(); // reset cs addr
              addr.server_.tablet_version_ = 0;
              it_out = &scanner;
            }
          }
        }
      }

      return err;
    }

    int ObUpsMergerRpcProxy::ups_get(const ObMergerTabletLocation& addr,
        const ObGetParam& get_param, ObScanner& scanner)
    {
      int err = OB_SUCCESS;

      ObCellInfo * cell = get_param[0];
      if (NULL == cell)
      {
        TBSYS_LOG(ERROR, "check first cell failed:cell[%p]", cell);
        err = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        int64_t timeout = DEFAULT_TIMEOUT;
        err = table_mgr_.get(get_param, scanner, start_time, timeout);
        if (OB_INVALID_START_VERSION == err)
        {
          if (0 == addr.server_.chunkserver_.get_ipv4()
              && 0 == addr.server_.chunkserver_.get_port())
          {
            // the cs data is got from ups cache
            // TODO (rizhao) invalid ups cache item
          }
          else
          {
            ObVersionRange version_range = get_param.get_version_range();
            TBSYS_LOG(WARN, "invalid version, start_version=%c%ld,%ld%c, err=%d",
                      version_range.border_flag_.inclusive_start() ? '[':'(', (int64_t)version_range.start_version_,
                      (int64_t)version_range.end_version_, version_range.border_flag_.inclusive_end() ? ']':')', err);
          }
        }
        else if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to call table_mgr#scan, err=%d", err);
        }
      }

      if (OB_SUCCESS == err && get_param.get_cell_size() > 0 && scanner.is_empty())
      {
        TBSYS_LOG(WARN, "update server error, response result with zero cell");
        err = OB_ERR_UNEXPECTED;
      }

      return err;
    }
  }
}
