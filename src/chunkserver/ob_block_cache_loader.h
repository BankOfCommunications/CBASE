/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_block_cache_loader.h for load new block cache.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_OB_BLOCK_CACHE_LOADER_H_
#define OCEANBASE_CHUNKSERVER_OB_BLOCK_CACHE_LOADER_H_

#include "common/ob_string.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_block_index_cache.h"
#include "ob_tablet_image.h"

namespace oceanbase 
{
  namespace common
  {
    class ObRowkey;
  }
  namespace chunkserver 
  {
    class ObBlockCacheLoader
    {
    public:
      ObBlockCacheLoader()
      : tablet_image_(NULL), tablet_version_(0)
      {
      }

      ObBlockCacheLoader(ObMultiVersionTabletImage* tablet_image) 
      : tablet_image_(tablet_image), tablet_version_(0)
      {
      }

      ~ObBlockCacheLoader()
      {
      }
    
      /**
       * load block data from sstable file into cache by start key
       *  
       * @param index_cache block index cache which stores block index 
       *                    data
       * @param block_cache block cache which stores block data 
       * @param table_id table id of the row key 
       * @param column_group_id id of column group 
       * @param rowkey row key of the block to load 
       * @param reader the sstable reader which the rowkey belong to 
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int load_block_into_cache(sstable::ObBlockIndexCache& index_cache, 
                                sstable::ObBlockCache& block_cache,
                                const uint64_t table_id,
                                const uint64_t column_group_id,
                                const common::ObRowkey& rowkey,
                                sstable::ObSSTableReader* sstable_reader = NULL);

      /**
       * set tablet image for this block cache reader, the tablet 
       * image is used to find the sstable reader by key.
       * 
       * @param tablet_image tablet image pointer to set
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      inline int set_tablet_image(ObMultiVersionTabletImage* tablet_image)
      {
        int ret = common::OB_SUCCESS;

        if (NULL == tablet_image)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          tablet_image_ = tablet_image;
        }

        return ret;
      }

      /**
       * set tablet version of this block cache reader
       * 
       * @param tablet_version tablet version to set
       * 
       * @return int f success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      inline int set_tablet_version(const int64_t tablet_version)
      {
        int ret = common::OB_SUCCESS;

        if (tablet_version < 0)
        {
          TBSYS_LOG(WARN, "invalide param, tablet_version=%ld", tablet_version);
          ret = common::OB_ERROR;
        }
        else
        {
          tablet_version_ = tablet_version;
        }

        return ret;
      }

      /**
       * get the tablet image pointer of the block cache reader 
       * 
       * @return const ObMultiVersionTabletImage* 
       */
      inline const ObMultiVersionTabletImage* get_tablet_image() const
      {
        return tablet_image_;
      }

    private:
      /**
       * get sstable reader by rowkey
       *  
       * @param table_id table id of the row key 
       * @param rowkey row key to search 
       * @param tablet tablet which the reader belongs to 
       * 
       * @return ObSSTableReader* return the sstable reader which the 
       *         rowkey belong to, if fail, return NULL.
       */
      sstable::ObSSTableReader* get_sstable_reader(uint64_t table_id, 
                                                   const common::ObRowkey& rowkey,
                                                   ObTablet*& tablet);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObBlockCacheLoader);

      ObMultiVersionTabletImage* tablet_image_;   //tablet image, used to find sstable reader
      int64_t tablet_version_;  //tablet version
    };

  } // end namespace chunkserver
} // end namespace oceanbase

#endif //OCEANBASE_CHUNKSERVER_OB_BLOCK_CACHE_LOADER_H_
