/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_query_service.cpp for query(get or scan), do merge, join, 
 * group by, order by, limit, topn operation and so on. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_QUERY_SERVICE_H_
#define OCEANBASE_CHUNKSERVER_QUERY_SERVICE_H_

#include "ob_rpc_proxy.h"
#include "ob_query_agent.h"
#include "ob_chunk_server.h"
#include "ob_get_scan_proxy.h"
#include "ob_get_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;

    /**
     * this is a helper class, it uses merge join agent to read data
     * from local chunk server, merge and join the data with update
     * server and returns the merge join result.
     */
    class ObQueryService
    {
    public:
      ObQueryService(ObChunkServer& chunk_server);
      ~ObQueryService();

    public:
      /**
       * get multi-rows static data from chunk server, then do merge 
       * and join with the dynamic data from update server, at the end
       * return the final data. this interface doesn't return the all 
       * the data to get in get_param, it just try its best to do it. 
       * it will stop at the first row which can't handle and returns 
       * the current handled cell index. 
       * 
       * @param get_param [in] get param, only includes id, excludes 
       *                  name, it may include multi-versions,
       *                  multi-tables, mulit-rows and multi-cells.
       * @param scanner [out] the result scanner, it stores the get 
       *                data and the current handled cell index in
       *                get param and data version.
       * @param timeout_time when is time out. 
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR, OB_INVALID_ARGUMENT, OB_SCHEMA_ERROR,
       *         OB_CS_TABLET_NOT_EXIST
       */
      int get(const common::ObGetParam & get_param, common::ObScanner & scanner, 
              const int64_t timeout_time); 
      void end_get();
      
      /**
       * scan one range static data from chunk server, then do merge 
       * and join with the dynamic data from update server, at the end
       * return the final data. maybe the range in scan param across 
       * several tablet, but this interface only try its best possible 
       * to handle the first tablet. 
       * 
       * @param scan_param [in] scan param, it stores the whole range 
       *                   to scan.
       * @param scanner [out] the result scanner, it stores the scan 
       *                data and the first tablet range in scan param
       *                and data version.
       * @param timeout_time when is time out.  
       * 
       * @return int if success, returns OB_SUCCESS, else returns 
       *         OB_ERROR, OB_SCHEMA_ERROR
       */
      int scan(const common::ObScanParam & scan_param, common::ObScanner & scanner,
               const int64_t timeout_time);
      void end_scan();

      int fill_get_data(const common::ObGetParam & get_param, common::ObScanner& scanner);
      int fill_scan_data(common::ObScanner& scanner);

    private:
      static const int64_t MAX_ROW_COLUMN_NUMBER = common::OB_MAX_COLUMN_NUMBER * 4;

      DISALLOW_COPY_AND_ASSIGN(ObQueryService);
      ObChunkServer& chunk_server_;
      ObGetScanProxy get_scan_proxy_;
      ObMergerRpcProxy& rpc_proxy_;
      ObQueryAgent query_agent_;
      ObGetCellStream ups_get_cell_stream_;
      ObScanCellStream ups_scan_cell_stream_;
      const common::ObSchemaManagerV2 *schema_mgr_;
      common::ObInnerCellInfo* row_cells_[MAX_ROW_COLUMN_NUMBER];
      int64_t row_cells_cnt_;
      int64_t got_rows_cnt_;
      int64_t max_scan_rows_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif  //OCEANBASE_CHUNKSERVER_QUERY_SERVICE_H_
