/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_scan_cell_stream.h for define scan rpc interface between 
 * chunk server and update server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_SCAN_CELL_STREAM_H_
#define OCEANBASE_CHUNKSERVER_SCAN_CELL_STREAM_H_

#include "common/ob_read_common_data.h"
#include "ob_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * this is one of ObCellStream subclasses, it can provide cell
     * stream through scan operation from chunkserver or update
     * server according to scan param. it encapsulates many rpc
     * calls when one server not serving all the required data or
     * the packet is too large.
     */
    class ObScanCellStream : public ObCellStream
    {
    public:
      ObScanCellStream(ObMergerRpcProxy * rpc_proxy,
        const common::ObServerType server_type = common::MERGE_SERVER,
        const int64_t time_out = 0);
      virtual ~ObScanCellStream();

    public: 
      // get next cell
      virtual int next_cell(void);
      // scan init
      virtual int scan(const common::ObScanParam & param);

      /**
       * get the current scan data version, this function must 
       * be called after next_cell() 
       * 
       * @return int64_t return data version
       */
      virtual int64_t get_data_version() const;

    private:

      // check whether finish scan, if finished return server's servering tablet range
      // param  @param current scan param
      int check_finish_scan(const common::ObScanParam & param); 

      // scan for get next cell
      int get_next_cell(void);

      // scan data
      // param @param scan data param
      int scan_row_data();

      // check inner stat
      bool check_inner_stat(void) const; 

      // reset inner stat
      void reset_inner_stat(void);
    
    private:
      DISALLOW_COPY_AND_ASSIGN(ObScanCellStream);

      bool finish_;                             // finish all scan routine status
      common::ObMemBuf range_buffer_;           // for modify param range
      const common::ObScanParam * scan_param_;  // orignal scan param
      common::ObScanParam cur_scan_param_;      // current scan param
    };

    // check inner stat
    inline bool ObScanCellStream::check_inner_stat(void) const
    {
      return(ObCellStream::check_inner_stat() && (NULL != scan_param_));
    }

    // reset inner stat
    inline void ObScanCellStream::reset_inner_stat(void)
    {
      ObCellStream::reset_inner_stat();
      finish_ = false;
      scan_param_ = NULL;
      cur_scan_param_.reset();
    }
  }
}

#endif //OCEANBASE_CHUNKSERVER_SCAN_CELL_STREAM_H_
