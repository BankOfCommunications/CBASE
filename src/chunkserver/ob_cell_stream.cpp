/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_cell_stream.cpp for define rpc interface between chunk 
 * server and update server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_cell_stream.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObCellStream::ObCellStream(ObMergerRpcProxy * rpc, const ObServerType server_type,
        const int64_t time_out)
    {
      reset_inner_stat();
      rpc_proxy_ = rpc;
      server_type_ = server_type;
      time_out_ = time_out;
      timeout_time_ = 0;
    }
    
    ObCellStream::~ObCellStream()
    {

    }
    
    int ObCellStream::scan(const ObScanParam &scan_param)
    {
      UNUSED(scan_param);
      TBSYS_LOG(ERROR, "%s", "not implement");
      return OB_ERROR;
    }
    
    void ObCellStream::reset()
    {
      reset_inner_stat();
    }
    
    int ObCellStream::get(const ObReadParam &read_param, ObCellArrayHelper & get_cells)
    {
      UNUSED(get_cells);
      UNUSED(read_param);
      TBSYS_LOG(ERROR, "%s", "not implement");
      return OB_ERROR;
    }
    
    int ObCellStream::check_scanner_result(const common::ObScanner & result)
    {
      bool is_fullfill = false;
      int64_t item_count = 0;
      int ret = result.get_is_req_fullfilled(is_fullfill, item_count);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get request is fullfilled failed:ret[%d]", ret);
      }
      else
      {
        if ((false == is_fullfill) && result.is_empty())
        {
          TBSYS_LOG(ERROR, "check scanner result empty but not fullfill:fullfill[%d], "
                           "item_count[%ld]",
              is_fullfill, item_count);
          ret = OB_ERROR;
        }
      }
      return ret;
    }
    
    int ObCellStream::get_cell(ObCellInfo** cell)
    {
      int ret = OB_SUCCESS;
      if ((NULL == cell) && !check_inner_stat())
      {
        TBSYS_LOG(ERROR, "check input cell or inner stat failed:cell[%p]", cell);
        ret = OB_ERROR;
      }
      else
      {
        ret = cur_result_.get_cell(cell);
        if (OB_SUCCESS == ret)
        {
          cur_cell_ = *cell;
        }
        else
        {
          TBSYS_LOG(ERROR, "iterator get cell failed:ret[%d]", ret);
        }
      }
      return ret;
    }
    
    int ObCellStream::get_cell(ObCellInfo** cell, bool * is_row_changed)
    {
      int ret = OB_SUCCESS;
      ret = cur_result_.get_cell(cell, is_row_changed);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get cell failed:ret[%d]", ret);
      }
      else
      {
        cur_cell_ = *cell;
      }
      return ret;
    }
    ////add wenghaixing [secondary index static_index_build.cs_scan]20150326
    int ObCellStream::rpc_scan_row_data(const ObScanParam &param, const ObServer &chunkserver)
    {
      int ret = OB_SUCCESS;
      int64_t timeout =
        timeout_time_ > 0 ? (timeout_time_ - tbsys::CTimeUtil::getTime()) : time_out_;
      ret = rpc_proxy_->cs_cs_scan(param, chunkserver, cur_result_, server_type_, timeout);
      TBSYS_LOG(DEBUG,"test:whx cs_cs_scan = %s,server = %s", to_cstring(*param.get_fake_range()),to_cstring(chunkserver));
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
      }
      else
      {
        if (first_rpc_)
        {
          FILL_TRACE_LOG("rpc ups scan, timeout=%ld, size=%ld, "
                         "cell_num=%ld, row_num=%ld, ret=%d",
            timeout, cur_result_.get_size(), cur_result_.get_cell_num(),
            cur_result_.get_row_num(), ret);
        }
        ret = check_scanner_result(cur_result_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "check scanner result failed:ret[%d]", ret);
        }
        else
        {
          first_rpc_ = false;
        }
      }
      return ret;
    }
    /// add e
    int ObCellStream::rpc_scan_row_data(const ObScanParam & param)
    {
      int ret = OB_SUCCESS;
      int64_t timeout = 
        timeout_time_ > 0 ? (timeout_time_ - tbsys::CTimeUtil::getTime()) : time_out_;
      ret = rpc_proxy_->ups_scan(param, cur_result_, server_type_, timeout);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
      }
      else
      {
        if (first_rpc_)
        {
          FILL_TRACE_LOG("rpc ups scan, timeout=%ld, size=%ld, "
                         "cell_num=%ld, row_num=%ld, ret=%d", 
            timeout, cur_result_.get_size(), cur_result_.get_cell_num(), 
            cur_result_.get_row_num(), ret);
        }
        ret = check_scanner_result(cur_result_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "check scanner result failed:ret[%d]", ret);
        }
        else
        {
          first_rpc_ = false;
        }
      }
      return ret;
    }
    
    int ObCellStream::rpc_get_cell_data(const common::ObGetParam & param)
    {
      int ret = OB_SUCCESS;
      int64_t timeout = 
        timeout_time_ > 0 ? (timeout_time_ - tbsys::CTimeUtil::getTime()) : time_out_;
      ret = rpc_proxy_->ups_get(param, cur_result_, server_type_, timeout);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get data through rpc call failed:ret[%d]", ret);
      }
      else
      {
        if (first_rpc_)
        {
          FILL_TRACE_LOG("rpc ups get, timeout=%ld, size=%ld, "
                         "cell_num=%ld, row_num=%ld, ret=%d", 
            timeout, cur_result_.get_size(), cur_result_.get_cell_num(), 
            cur_result_.get_row_num(), ret);
        }
        ret = check_scanner_result(cur_result_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "check scanner result failed:ret[%d]", ret);
        }
        else
        {
          first_rpc_ = false;
          ret = cur_result_.next_cell();
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "check get ups first cell failed:ret[%d]", ret);
          }
        }
      }
      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
