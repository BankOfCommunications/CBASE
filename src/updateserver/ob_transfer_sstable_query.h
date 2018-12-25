/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_transfer_sstable_query.h for get or scan columns from
 * transfer sstables. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_UPDATESERVER_OB_TRANSFER_SSTABLE_QUERY_H_
#define OCEANBASE_UPDATESERVER_OB_TRANSFER_SSTABLE_QUERY_H_

#include "common/ob_rowkey.h"
#include "common/thread_buffer.h"
#include "common/ob_fileinfo_manager.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_row_cache.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_sstable_mgr.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObTransferSSTableQuery
    {
    public:
      ObTransferSSTableQuery(common::IFileInfoMgr &fileinfo_mgr);
      ~ObTransferSSTableQuery();

      int init(const int64_t block_cache_size, 
               const int64_t block_index_cache_size,
               const int64_t sstable_row_cache_size = 0);

      int enlarge_cache_size(const int64_t block_cache_size, 
                             const int64_t block_index_cache_size,
                             const int64_t sstable_row_cache_size = 0);

      int get(const common::ObGetParam& get_param, sstable::ObSSTableReader& reader, 
              common::ObIterator* iterator);

      int scan(const common::ObScanParam& scan_param, sstable::ObSSTableReader& reader,
               common::ObIterator* iterator);

      int get_sstable_end_key(const sstable::ObSSTableReader& reader,
                              const uint64_t table_id, common::ObRowkey& row_key);

      sstable::ObBlockIndexCache &get_block_index_cache()
      {
        return block_index_cache_;
      };

    private:
      DISALLOW_COPY_AND_ASSIGN(ObTransferSSTableQuery);

      bool inited_;
      sstable::ObBlockCache block_cache_;
      sstable::ObBlockIndexCache block_index_cache_;
      sstable::ObSSTableRowCache* sstable_row_cache_;
    };
  }//end namespace updateserver
}//end namespace oceanbase

#endif  // OCEANBASE_UPDATESERVER_OB_TRANSFER_SSTABLE_QUERY_H_
