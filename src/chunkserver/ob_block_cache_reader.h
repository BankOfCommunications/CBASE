/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_block_cache_reader.h for read cached block.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_OB_BLOCK_CACHE_READER_H_
#define OCEANBASE_CHUNKSERVER_OB_BLOCK_CACHE_READER_H_

#include <tblog.h>
#include "sstable/ob_sstable_block_builder.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_blockcache.h"
#include "ob_tablet_image.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    class ObBlockCacheReader
    {
      static const int64_t DEFAULT_UNCOMPRESSED_BUFSIZE = 128 * 1024; //128k

    public:
      ObBlockCacheReader() 
      : uncompressed_buf_(DEFAULT_UNCOMPRESSED_BUFSIZE),
        uncompressed_data_size_(0), tablet_image_(NULL), tablet_version_(0)
      {
      }

      ObBlockCacheReader(ObMultiVersionTabletImage *tablet_image)
      : uncompressed_buf_(DEFAULT_UNCOMPRESSED_BUFSIZE), 
        uncompressed_data_size_(0), tablet_image_(tablet_image), tablet_version_(0)
      {
      }

      ~ObBlockCacheReader()
      {
      }
    
      /**
       * get the start key of next block
       *  
       * @param block_cache block cache 
       * @param table_id table id of the sstable which the block 
       *                 belongs to
       * @param column_group_id column group id of the sstable which 
       *                 the block belongs to 
       * @param start_key start key of next block 
       * @param reader the sstable reader which the rowkey belong to 
       * 
       * @return int if success,return OB_SUCCESS or OB_ITER_END, else 
       *         return OB_ERROR
       */
      int get_start_key_of_next_block(sstable::ObBlockCache& block_cache, 
                                      uint64_t& table_id,
                                      uint64_t& column_group_id,
                                      common::ObRowkey& start_key,
                                      sstable::ObSSTableReader* sstable_reader = NULL);

      /**
       * set tablet image for this block cache reader, the tablet 
       * image is used to find the sstable reader by sstable id.
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
          TBSYS_LOG(WARN, "invalide param, table_image=%p", tablet_image);
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
       * get sstable reader by sstable id
       * 
       * @param sstable_id sstable id to search
       * 
       * @return ObSSTableReader* return the sstable reader which the 
       *         sstable belong to, if fail, return NULL.
       */
      sstable::ObSSTableReader* get_sstable_reader(const uint64_t sstable_id,
        ObTablet* &tablet);

      /**
       * decompress block data, first get the sstable reader by 
       * sstable id, then get compressor from sstable reader, at the 
       * end decompress the src data 
       * 
       * @param src_buf source buffer to decompress
       * @param src_len source buffer length
       * @param dst_buf destination buffer to store uncompressed data
       * @param dst_len destination buffer length
       * @param data_len the actual uncompressed data length 
       * @param reader the sstable reader which the current block 
       *               belong to 
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int decompress_block(const char* src_buf, 
                           const int64_t src_len, char* dst_buf, 
                           const int64_t dst_len, int64_t& data_len,
                           sstable::ObSSTableReader* sstable_reader = NULL);

      /**
       * parse one record buffer, the record buffer includes record 
       * header, compressed payload data(block) 
       * 
       * @param handle record buffer handle 
       * @param buffer_size record buffer size 
       * @param sstable_reader sstable reader
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int parse_record_buffer(sstable::ObBufferHandle& handle, 
                              const int64_t buffer_size,
                              sstable::ObSSTableReader* sstable_reader);

      /**
       * read the next block data, return the actual block data and 
       * block data len 
       *  
       * @param block_cache block cache 
       * @param table_id table id of the sstable which the block 
       *                 belongs to
       * @param column_group_id column group id of the sstable which 
       *                 the block belongs to
       * @param reader the sstable reader which the current block 
       *               belong to
       * 
       * @return int if success,return OB_SUCCESS or OB_ITER_END, else 
       *         return OB_ERROR
       */
      int read_next_block(sstable::ObBlockCache& block_cache, 
                          uint64_t& table_id,
                          uint64_t& column_group_id,
                          sstable::ObSSTableReader* sstable_reader = NULL);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObBlockCacheReader);

      common::ObMemBuf uncompressed_buf_; //buffer to store uncomopressed data
      int64_t uncompressed_data_size_;    //uncompressed data size                                      
      ObMultiVersionTabletImage* tablet_image_; //tablet image, used to find sstable reader
      int64_t tablet_version_;
    };

  } // end namespace chunkserver
} // end namespace oceanbase

#endif //OCEANBASE_CHUNKSERVER_OB_BLOCK_CACHE_READER_H_
