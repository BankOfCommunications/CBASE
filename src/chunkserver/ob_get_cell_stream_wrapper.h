/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_cell_stream_wrapper.h is for concealing ObMergerRpcProxy
 * initialization details from chunkserver
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_GET_CELL_STREAM_WRAPPER_H_ 
#define OCEANBASE_CHUNKSERVER_GET_CELL_STREAM_WRAPPER_H_

#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/thread_buffer.h"
#include "ob_rpc_proxy.h"
#include "ob_get_cell_stream.h"
#include "ob_scan_cell_stream.h"
//add wenghaixing [secondary index static_index_build.cs_scan]20150324
#include "ob_cs_scan_cell_stream.h"
//add e
namespace oceanbase
{
  namespace chunkserver
  {
    class ObGetCellStreamWrapper
    {
    public:
      /**
       * @param retry_times retry times
       * @param timeout network timeout
       * @param update_server address of update server
       */
      ObGetCellStreamWrapper(ObMergerRpcProxy& rpc_proxy, const int64_t time_out = 0);
      ~ObGetCellStreamWrapper();

      // get cell stream used for join
      ObGetCellStream *get_ups_get_cell_stream();
      // get cell stream used for merge
      ObScanCellStream *get_ups_scan_cell_stream();
      //add wenghaixing [secondary index static_index_build.cs_scan]20150323
      ObCSScanCellStream *get_cs_scan_cell_stream();
      //add e
    private:
      ObGetCellStream get_cell_stream_;
      ObScanCellStream scan_cell_stream_;
      //add wenghaixing [secondary index static_index_build.cs_scan]20150324
      ObCSScanCellStream cs_scan_cell_stream_;
      //add e
    };
  }
}   

#endif //OCEANBASE_CHUNKSERVER_GET_CELL_STREAM_WRAPPER_H_
