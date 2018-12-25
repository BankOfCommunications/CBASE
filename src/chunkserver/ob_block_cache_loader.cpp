/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_block_cache_loader.cpp for load new block cache.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/utility.h"
#include "common/ob_range2.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "ob_tablet.h"
#include "ob_block_cache_loader.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace common::serialization;
    using namespace sstable;

    ObSSTableReader *ObBlockCacheLoader::get_sstable_reader(uint64_t table_id, 
                                                            const ObRowkey& rowkey,
                                                            ObTablet*& tablet)
    {
      int status;
      ObSSTableReader* reader = NULL;
      int32_t size            = 1;
      ObNewRange range;

      range.table_id_ = table_id;
      range.start_key_ = rowkey;
      range.end_key_ = rowkey;
      range.border_flag_.set_inclusive_start(); 
      range.border_flag_.set_inclusive_end();

      // tablet_version_ is newest version of tablets, but 
      // has not upgrade service at this time, so use 
      // acquire_tablet_all_version to find the newest tablet 
      status = tablet_image_->acquire_tablet_all_version(range, 
          ObMultiVersionTabletImage::SCAN_FORWARD, 
          ObMultiVersionTabletImage::FROM_NEWEST_INDEX,
          tablet_version_, tablet);
      if (OB_SUCCESS == status && NULL != tablet)
      {
        /** FIXME: find sstable may return more than one reader */
        status = tablet->find_sstable(rowkey, &reader, size);
        if (OB_SUCCESS != status)
        {
          if (OB_SIZE_OVERFLOW == status)
          {
            TBSYS_LOG(WARN, "find sstable reader by rowkey return more than"
                            "one reader, tablet=%p", tablet);
          }
          reader = NULL;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to acquire tablet, table_id=%lu, row_key: %s",
            table_id, to_cstring(rowkey));
      }

      return reader;
    }

    int ObBlockCacheLoader::load_block_into_cache(ObBlockIndexCache& index_cache, 
                                                  ObBlockCache& block_cache,
                                                  const uint64_t table_id,
                                                  const uint64_t column_group_id,
                                                  const ObRowkey& rowkey,
                                                  ObSSTableReader* sstable_reader)
    {
      int ret                 = OB_SUCCESS;
      ObSSTableReader* reader = NULL;
      ObTablet* tablet        = NULL;
      ObBlockIndexPositionInfo info;
      ObBlockPositionInfo pos_info;
      ObBufferHandle handler;

      if (NULL == sstable_reader)
      {
        reader = get_sstable_reader(table_id, rowkey, tablet);
      }
      else
      {
        reader = sstable_reader;
      }

      if (NULL == reader)
      {
        TBSYS_LOG(INFO, "start rowkey of old block in old tablet image "
                        "is non-existent in new tablet image, rowkey: %s", to_cstring(rowkey));
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        const ObSSTableTrailer& trailer = reader->get_trailer();
        memset(&info, 0, sizeof(info));
        info.sstable_file_id_ = reader->get_sstable_id().sstable_file_id_;
        info.offset_ = trailer.get_block_index_record_offset();
        info.size_   = trailer.get_block_index_record_size();
  
        //load block index into cache
        ret = index_cache.get_single_block_pos_info(info, table_id, column_group_id, rowkey, 
                                                    OB_SEARCH_MODE_GREATER_EQUAL, pos_info);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(DEBUG, "failed to get block position info, table_id=%lu, ret=%d, row_key: %s",
                    table_id, ret, to_cstring(rowkey));
          
        }
      }

      if (OB_SUCCESS == ret)
      {
        //load block into cache
        ret = block_cache.get_block(info.sstable_file_id_, pos_info.offset_, 
                                    pos_info.size_, handler, table_id);
        if (ret != pos_info.size_)
        {
          TBSYS_LOG(WARN, "get block return unexpected block size, expected block "
                          "size=%ld, get block size=%d", 
                    pos_info.size_, ret);
          ret = OB_ERROR;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }

      if (NULL != tablet && OB_SUCCESS != tablet_image_->release_tablet(tablet))
      {
        TBSYS_LOG(WARN, "failed to release tablet, tablet=%p", tablet);
      }

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
