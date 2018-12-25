/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_server_merger_proxy.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_MERGER_PROXY_H_
#define OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_MERGER_PROXY_H_

#include "ob_rpc_proxy.h"
#include "ob_merge_reader.h"
#include "common/ob_cell_array.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;

    class ObChunkServerMergerProxy : public ObMergerRpcProxy
    {
      public:
        ObChunkServerMergerProxy(ObTabletManager& manager);
        virtual ~ObChunkServerMergerProxy();

      public:
  
        virtual int cs_scan(const common::ObScanParam & scan_param, 
                            common::ObScanner & scanner, 
                            common::ObIterator * &it_out);

        void reset();

      private:
        ObMergeReader cs_reader_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif
