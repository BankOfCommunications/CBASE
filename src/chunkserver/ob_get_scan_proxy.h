/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_scan_proxy.h for get scan proxy of merge and join 
 * agent. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_GET_SCAN_PROXY_H_
#define OCEANBASE_CHUNKSERVER_GET_SCAN_PROXY_H_

#include "ob_rpc_proxy.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTablet;
    class ObTabletManager;

    /**
     * proxy of get and scan data from chunk server, merge agent
     * need this instance to get data from chunk server directly,
     * and the data isn't passed by network.
     */
    class ObGetScanProxy : public ObMergerRpcProxy
    {
    public:
      ObGetScanProxy(ObTabletManager& manager);
      virtual ~ObGetScanProxy();

    public:
      /**
       * get static multi-rows, multi-tablets, multi-tables data from 
       * local chunk server. it's just a wrapper of get interface, it 
       * just get data from local chunk server directly, it doesn't 
       * get data through network. 
       * 
       * @param get_param [in] get param
       * @param scanner [out] scanner to store the get result
       * @param it_out [out] stores the internal sstable getter 
       *               pointer
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR
       */
      virtual int cs_get(const common::ObGetParam& get_param, 
                         common::ObScanner& scanner, 
                         common::ObIterator* it_out[],
                         int64_t& it_size); 
      
      /**
       * scan one range static data from local chunk server. it's just
       * a wrapper of scan interface, it just scan data from local 
       * chunk server directly, it doesn't scan data through network. 
       * 
       * @param scan_param [in] scan param
       * @param scanner [out] scanner to store the scan result
       * @param it_out [out] stores the internal sequence sstablet 
       *               scanner pointer
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR, OB_ALLOCATE_MEMORY_FAILED 
       */
      virtual int cs_scan(const common::ObScanParam & scan_param, 
                          common::ObScanner& scanner, 
                          common::ObIterator* it_out[],
                          int64_t& it_size);

      /**
       * get the current scan tablet range,this function must be 
       * called after cs_scan() 
       * 
       * @return const common::ObNewRange& return current scan tablet 
       *         range
       */
      const common::ObNewRange& get_tablet_range() const;

    private:
      int get_compact_scanner(const common::ObGetParam& get_param,
                              common::ObScanner& orig_scanner,
                              common::ObScanner& compact_scanner);

      int get_compact_row(chunkserver::ObTablet& tablet,
                          common::ObRowkey& rowkey,
                          const int64_t commpactsstable_version,
                          const common::ColumnFilter* cf,
                          common::ObScanner& compact_scanner);

      int fill_compact_data(common::ObIterator& iterator,
                            common::ObScanner& scanner);

      int build_get_column_filter(const common::ObGetParam& get_param,
                                  const int32_t offset,const int32_t size,
                                  common::ColumnFilter& cf);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObGetScanProxy);

      ObTabletManager& tablet_manager_; //tablet manager
      common::ObNewRange tablet_range_;    //tablet range, only reutrun by cs_scan
    };
  } /* chunkserver */
} /* oceanbase */

#endif  //OCEANBASE_CHUNKSERVER_GET_SCAN_PROXY_H_
