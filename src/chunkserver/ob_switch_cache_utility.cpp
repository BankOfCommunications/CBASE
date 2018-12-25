/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_switch_cache_utility.cpp for switch block cache and block 
 * index cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "tblog.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_block_index_cache.h"
#include "ob_switch_cache_utility.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_main.h"

namespace oceanbase 
{ 
  namespace chunkserver 
  {
    using namespace common;
    using namespace sstable;

    int ObSwitchCacheUtility::destroy_cache(ObBlockCache& block_cache, 
                                            ObBlockIndexCache& index_cache)
    {
      int ret = OB_SUCCESS;

      ret = index_cache.destroy();
      if (OB_SUCCESS == ret)
      {
        ret = block_cache.destroy();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to destroy block cache");
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to destroy index cache");
      }

      return ret;
    }

    int ObSwitchCacheUtility::switch_cache(ObMultiVersionTabletImage& tablet_image, 
                                           const int64_t src_tablet_version, 
                                           const int64_t dst_tablet_version, 
                                           ObBlockCache& src_block_cache,
                                           ObBlockCache& dst_block_cache,
                                           ObBlockIndexCache &dst_index_cache)
    {
      int status                = OB_SUCCESS;
      uint64_t table_id         = OB_INVALID_ID;
      uint64_t column_group_id  = OB_INVALID_ID;
      ObRowkey start_key;

      if (src_tablet_version < 0 || dst_tablet_version < 0)
      {
        TBSYS_LOG(WARN, "invalid param, src_tablet_version=%ld, dst_tablet_version=%ld",
                  src_tablet_version, dst_tablet_version);
        status = OB_ERROR;
      }
      else
      {
        status = cache_reader_.set_tablet_image(&tablet_image);
        if (OB_SUCCESS == status)
        {
          status = cache_loader_.set_tablet_image(&tablet_image);
        }
        if (OB_SUCCESS == status)
        {
          status = cache_reader_.set_tablet_version(src_tablet_version);
        }
        if (OB_SUCCESS == status)
        {
          status = cache_loader_.set_tablet_version(dst_tablet_version);
        }
      }
      
      while (OB_SUCCESS == status)
      {
        status = cache_reader_.get_start_key_of_next_block(src_block_cache, table_id, 
                                                           column_group_id, start_key);
        if (OB_SUCCESS == status)
        {
          status = cache_loader_.load_block_into_cache(dst_index_cache, dst_block_cache, 
                                                       table_id, column_group_id, start_key);
          /**
           * ingore all the error of load new block, because sometime the 
           * start rowkey of block in old tablet image is deleted, so the 
           * rowkey is not-existent in new tablet image. for this case, 
           * load block will fail. 
           */
          if (OB_SUCCESS != status)
          {
            status = OB_SUCCESS;
          }
        }
        else
        {
          if (OB_ITER_END != status)
          {
            TBSYS_LOG(WARN, "failed to get start key of next block");
          }
          break;
        }
      }

      if (OB_ITER_END != status)
      {
        TBSYS_LOG(WARN, "Problem load new block cache");
        status = OB_ERROR;
      }
      else
      {
        status = OB_SUCCESS;
      }

      return status;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
