/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_cell_stream.h for define rpc interface between chunk 
 * server and update server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_CELL_STREAM_H_
#define OCEANBASE_CHUNKSERVER_CELL_STREAM_H_

#include "common/ob_scanner.h"
#include "common/ob_iterator.h"
#include "common/ob_read_common_data.h"
#include "common/ob_tablet_info.h"
#include "ob_cell_array_helper.h"
#include "ob_rpc_proxy.h"
#include "ob_join_cache.h"
#include "ob_row_cell_vec.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * ObCellStream class is a virtual class as the interface, this
     * is the parent class of ObGetCellStream and ObScanCellStream
     * for two different stream types, it only provide common
     * interfaces. it uses rpc proxy to provide endless stream
     */
    class ObCellStream : public common::ObIterator
    {
    public:
      ObCellStream(ObMergerRpcProxy * rpc, 
        const common::ObServerType server_type = common::MERGE_SERVER,
        const int64_t time_out = 0);
      virtual ~ObCellStream();

    public:
      virtual void reset();

      // get the current cell info, the same as parent interface
      virtual int get_cell(common::ObCellInfo** cell);

      // the same as parent interface
      virtual int get_cell(common::ObCellInfo** cell, bool * is_row_changed);

      // as the parent class, not implement the parent virtual function
      // the same as parent interface
      virtual int get(const common::ObReadParam & read_param,
                      ObCellArrayHelper & get_cells);

      // as the parent class, not implement the parent virtual function
      // the same as parent interface
      virtual int scan(const common::ObScanParam & scan_param); 

      // move to next for get cell
      virtual int next_cell(void) = 0;

      // get data version
      virtual int64_t get_data_version() const = 0;

      // set cache,  cached content is 
      // [tableid, version, rowkey]->(ObRowCellVec of the row)
      virtual void set_cache(ObJoinCache & cache)
      {
        UNUSED(cache);
      }

      // get a row in the cache
      virtual int get_cache_row(const common::ObCellInfo & key, ObRowCellVec *& result)
      {
        UNUSED(key);
        UNUSED(result);
        return common::OB_ENTRY_NOT_EXIST;
      }

      void set_timeout_time(const int64_t timeout_time)
      {
        if (timeout_time > 0)
        {
          timeout_time_ = timeout_time;
        }
        else
        {
          timeout_time_ = 0;
        }
      }

      void set_timeout(const int64_t timeout)
      {
        if (timeout > 0)
        {
          time_out_ = timeout;
        }
        else
        {
          time_out_ = 0;
        }
      }
    
    protected:
      // check init and scan or get
      virtual bool check_inner_stat(void) const;

      // reset inner stat
      virtual void reset_inner_stat(void);

      // one rpc call for scan the row data from server
      int rpc_scan_row_data(const common::ObScanParam & param);

      //add wenghaixing [secondary index static_index_build.cs_scan]20150326
      int rpc_scan_row_data(const ObScanParam &param, const ObServer &chunkserver);
      //add e

      // one rpc call for get cell data from server
      int rpc_get_cell_data(const common::ObGetParam & param);
      
    private:
      // check scanner result valid
      static int check_scanner_result(const common::ObScanner & result);

    protected:
      bool first_rpc_;                // is the first rpc call
      common::ObCellInfo *last_cell_; // last iterator cell
      common::ObCellInfo *cur_cell_;  // current iterator cell
      common::ObScanner cur_result_;  // rpc call returned temp result for iterator
      ObMergerRpcProxy * rpc_proxy_;  // rpc proxy for sys rpc call
      common::ObServerType server_type_;//server type
      int64_t time_out_;  // network timeout get/scan from ups
      int64_t timeout_time_; // the absolute time of timeout
    };

    // check inner stat
    inline bool ObCellStream::check_inner_stat(void) const
    {
      return(NULL != rpc_proxy_);
    }

    // reset innser stat 
    inline void ObCellStream::reset_inner_stat(void)
    {
      first_rpc_ = true;
      cur_result_.reset();  //when scanner deserialize, it will reset, but this also need
      last_cell_ = NULL;
      cur_cell_ = NULL;
    }

    #define MAKE_SCAN_SIZE(ups_scan_size,max_scan_rows) ((ups_scan_size & 0xFFFFFFFF) + (max_scan_rows << 32))
    #define GET_SCAN_SIZE(scan_size) (scan_size & 0xFFFFFFFF)
    #define GET_SCAN_ROWS(scan_size) (scan_size >> 32)
  }
}

#endif //OCEANBASE_CHUNKSERVER_CELL_STREAM_H_
