/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_server_merger_proxy.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_chunk_server_merger_proxy.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_cell_array.h"

namespace oceanbase 
{
  using namespace common;

  namespace chunkserver 
  {

    ObChunkServerMergerProxy::ObChunkServerMergerProxy(ObTabletManager& manager)
      : ObMergerRpcProxy(), cs_reader_(manager)
    {}
        
    ObChunkServerMergerProxy::~ObChunkServerMergerProxy() {}


    int ObChunkServerMergerProxy::cs_scan(const common::ObScanParam & scan_param, 
                                          common::ObScanner & scanner, 
                                          common::ObIterator * &it_out)
    {
      int ret = OB_SUCCESS;
      
      ObCellInfo cell_ext;
      it_out = NULL;

      if ((ret = cs_reader_.scan(scan_param)) != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG,"scan chunkserver failed");
      }
      else
      {
        it_out = &cs_reader_;
      }

      if (OB_SUCCESS == ret)
      {
        cell_ext.column_id_ = 2; //meaninglessness,just to suppress the scanner warn log

        cell_ext.table_id_ = scan_param.get_table_id();
        cell_ext.row_key_ = scan_param.get_range()->end_key_;

        scanner.reset();
        scanner.set_range(*scan_param.get_range());
        //data version
        scanner.set_data_version(scan_param.get_version_range().start_version_);
        scanner.set_is_req_fullfilled(true,1);

        if ( (ret = scanner.add_cell(cell_ext)) != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG,"add cell failed(%d)",ret);
        }
      }
      TBSYS_LOG(DEBUG," merger proxy ret [%d]",ret);
      return ret;
    }

    void ObChunkServerMergerProxy::reset()
    {
      cs_reader_.reset();
    }
  } /* chunkserver */
} /* oceanbase */

