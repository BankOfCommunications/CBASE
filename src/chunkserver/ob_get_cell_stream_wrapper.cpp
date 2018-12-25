/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_cell_stream_wrapper.cpp is for concealing 
 * ObMergerRpcProxy initialization details from chunkserver 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_get_cell_stream_wrapper.h"
#include "common/ob_define.h"
#include "tbsys.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObGetCellStreamWrapper::ObGetCellStreamWrapper(
        ObMergerRpcProxy& rpc_proxy, const int64_t time_out)
    : get_cell_stream_(&rpc_proxy, CHUNK_SERVER, time_out), 
      scan_cell_stream_(&rpc_proxy, CHUNK_SERVER, time_out)
    //add wenghaixing [secondary index static_index_build.cs_scan]20150324
      ,cs_scan_cell_stream_(&rpc_proxy, CHUNK_SERVER, time_out)
    //add e
    {
    }
    
    ObGetCellStreamWrapper::~ObGetCellStreamWrapper()
    {

    }
    
    ObGetCellStream *ObGetCellStreamWrapper::get_ups_get_cell_stream()
    {
      return &get_cell_stream_;
    }
    
    ObScanCellStream *ObGetCellStreamWrapper::get_ups_scan_cell_stream()
    {
      return &scan_cell_stream_;
    }
    //add wenghaixing [secondary index static_index_build.cs_scan]20150323
    ObCSScanCellStream *ObGetCellStreamWrapper::get_cs_scan_cell_stream()
    {
      return &cs_scan_cell_stream_;
    }
    //add e
  } // end namespace chunkserver
} // end namespace oceanbase
