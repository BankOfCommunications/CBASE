/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_switch_cache_utility.h for switch block cache and block 
 * index cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_OB_SWITCH_CACHE_UTILITY_H_
#define OCEANBASE_CHUNKSERVER_OB_SWITCH_CACHE_UTILITY_H_

#include "ob_block_cache_reader.h"
#include "ob_block_cache_loader.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    class ObSwitchCacheUtility
    {
    public:
      ObSwitchCacheUtility()
      {
      }

      ~ObSwitchCacheUtility()
      {
      }

      /**
       * destroy the old block cache and block index cache
       *  
       * @param block_cache block cache 
       * @param index_cache block index cache
       *  
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int destroy_cache(sstable::ObBlockCache& block_cache, 
                        sstable::ObBlockIndexCache& index_cache);
    
      /**
       * switch block cache and block index, reload block cache from 
       * src block cache to dst block cache 
       * 
       * @param tablet_image source tablet image 
       * @param src_tablet_version tablet version(serving) 
       * @param dst_tablet_version(unserving)
       * @param dst_tablet_image destination tablet image(unserving)
       * @param src_block_cache source block cache(serving)
       * @param dst_block_cache destination block cache(unserving)
       * @param dst_index_cache destination block index 
       *                        cache(unserving)
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int switch_cache(ObMultiVersionTabletImage& tablet_image, 
                       const int64_t src_tablet_version, 
                       const int64_t dst_tablet_version, 
                       sstable::ObBlockCache& src_block_cache, 
                       sstable::ObBlockCache& dst_block_cache, 
                       sstable::ObBlockIndexCache &dst_index_cache);

    private:
      ObBlockCacheReader cache_reader_; //block cache reader, used to read block 
                                        //one by one from old block cache
      ObBlockCacheLoader cache_loader_; //block cache loader, used to load new block 
                                        //from sstable file into new block cache
    };
  } // end namespace chunkserver
} // end namespace oceanbase

#endif //OCEANBASE_CHUNKSERVER_OB_SWITCH_CACHE_UTILITY_H_
