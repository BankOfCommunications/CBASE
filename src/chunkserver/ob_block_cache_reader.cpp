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
#include <tblog.h>
#include "common/ob_record_header.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_disk_path.h"
#include "ob_block_cache_reader.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace common::serialization;
    using namespace sstable;

    ObSSTableReader* ObBlockCacheReader::get_sstable_reader(const uint64_t sstable_id,
      ObTablet* &tablet)
    {
      ObSSTableReader* reader = NULL;
      ObSSTableId sst_id(sstable_id);
      int ret = OB_SUCCESS;
      tablet = NULL;

      if (NULL != tablet_image_)
      {
        ret = tablet_image_->acquire_tablet(sst_id, tablet_version_, tablet);
        if (OB_SUCCESS == ret && NULL != tablet)
        {
          ret = tablet->find_sstable(sst_id, reader);
        }
      }

      return reader;
    }

    int ObBlockCacheReader::decompress_block(const char* src_buf, 
                                             const int64_t src_len, char* dst_buf, 
                                             const int64_t dst_len, int64_t& data_len,
                                             ObSSTableReader* sstable_reader)
    {
      int ret                   = OB_SUCCESS;
      ObCompressor* compressor  = NULL;

      if (NULL == src_buf || src_len <= 0 || NULL == dst_buf || dst_len <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, src_buf=%p, src_len=%ld, dst_buf=%p,"
                        "dst_len=%ld, data_len=%ld", 
                  src_buf, src_len, dst_buf, dst_len, data_len);
        ret = OB_ERROR;
      }

      if (NULL == sstable_reader)
      {
        TBSYS_LOG(WARN, "sstable reader is NULL");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == (compressor = sstable_reader->get_decompressor()))
        {
          TBSYS_LOG(WARN, "failed to get compressor");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = compressor->decompress(src_buf, src_len, dst_buf,
                                     dst_len, data_len);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to decompress");
        }
      }

      return ret;
    }

    int ObBlockCacheReader::parse_record_buffer(ObBufferHandle& handle, 
                                                const int64_t buffer_size,
                                                ObSSTableReader* sstable_reader) 
    {
      int ret               = OB_SUCCESS;
      const char* data_buf  = NULL;
      int64_t pos           = 0;
      ObRecordHeader header;

      if (NULL == sstable_reader)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "sstable reader is NULL");
      }

      if (OB_SUCCESS == ret)
      {
        memset(&header, 0, sizeof(header));
        data_buf = handle.get_buffer();
        ret = header.deserialize(data_buf, sizeof(header), pos);
        ret = ObRecordHeader::check_record(header, data_buf + sizeof(ObRecordHeader), 
                                           buffer_size - sizeof(ObRecordHeader), 
                                           ObSSTableWriter::DATA_BLOCK_MAGIC);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "invalid block data");
        }
      }

      /**
       *  If the block data is not compressed, we use the memory of
       *  the block cache, we copy the block data from block cache.
       *  but we need ensure when we copy the memory of block cache,
       *  the buffer handle is not out of lifecycle. when the lifecycle
       *  of buffer handle is over, we can't use the block cache. If
       *  the block is compressed, we decompress the block data into
       *  an extra uncompressed buffer.
       */
      if (OB_SUCCESS == ret
          && OB_SUCCESS == (ret = uncompressed_buf_.ensure_space(header.data_length_)))
      {
        if (header.is_compress())
        {
          ret = decompress_block(data_buf + pos, header.data_zlength_, 
                                 uncompressed_buf_.get_buffer(), 
                                 uncompressed_buf_.get_buffer_size(), 
                                 uncompressed_data_size_, sstable_reader);
          if (OB_SUCCESS != ret)
          {
            uncompressed_data_size_ = 0;
            TBSYS_LOG(WARN, "decompress failed, data_zlength=%d data_length=%d,"
                            "uncompressed_buf=%p, uncompressed_buf_size=%ld",
                      header.data_zlength_, header.data_length_, 
                      uncompressed_buf_.get_buffer(),
                      uncompressed_buf_.get_buffer_size());
          }
        }
        else 
        {
          memcpy(uncompressed_buf_.get_buffer(), data_buf + pos, header.data_length_);
          uncompressed_data_size_ = header.data_length_;
        }
      }

      return ret;
    }

    int ObBlockCacheReader::read_next_block(ObBlockCache& block_cache, 
                                            uint64_t& table_id,
                                            uint64_t& column_group_id,
                                            ObSSTableReader* sstable_reader) 
    {
      int ret                       = OB_SUCCESS;
      ObSSTableReader* reader       = NULL;
      const ObSSTableSchema* schema = NULL;
      const ObSSTableSchemaColumnDef* column_def  = NULL;
      ObDataIndexKey dataindex_key;
      static ObBufferHandle handle; //we must ensure only one thread call this function
      ObTablet* tablet = NULL;

      table_id = OB_INVALID_ID;
      column_group_id = OB_INVALID_ID;
      ret = block_cache.get_next_block(dataindex_key, handle);
      if (OB_SUCCESS == ret)
      {
        if (NULL == sstable_reader)
        {
          reader = get_sstable_reader(dataindex_key.sstable_id, tablet);
        }
        else
        {
          reader = sstable_reader;
        }

        if (NULL == reader)
        {
          TBSYS_LOG(INFO, "Problem get sstable reader, sstable id=%lu", 
                    dataindex_key.sstable_id);
          ret = OB_ERROR;
        }
        else if (NULL != (schema = reader->get_schema())
                 && NULL != (column_def = schema->get_column_def(0)))
        {
          table_id = column_def->table_id_;
          column_group_id = schema->get_table_first_column_group_id(table_id);
          ret = parse_record_buffer(handle, dataindex_key.size, reader);
        }
        else
        {
          TBSYS_LOG(INFO, "Problem get sstable scheam, sstable id=%lu, reader=%p, "
                          "schema=%p, column_def=%p", 
                    dataindex_key.sstable_id, reader, schema, column_def);
          ret = OB_ERROR;
        }
      }
      else if (OB_ITER_END == ret)
      {
        //complete traversal
        handle.reset();
        TBSYS_LOG(INFO, "complete traversal block cache");
      }
      else
      {
        TBSYS_LOG(WARN, "load block from obcache failed, ret=%d", ret);
      }

      if (NULL == sstable_reader && NULL != tablet)
      {
        ret = tablet_image_->release_tablet(tablet);
      }

      return ret;
    }

    int ObBlockCacheReader::get_start_key_of_next_block(ObBlockCache& block_cache, 
                                                        uint64_t& table_id,
                                                        uint64_t& column_group_id,
                                                        ObRowkey& start_key,
                                                        ObSSTableReader* sstable_reader)
    {
      int ret                   = OB_SUCCESS;
      const char* start_row     = NULL;
      int64_t pos               = 0;
      int64_t block_header_size = sizeof(ObSSTableBlockHeader);
      int64_t column_count      = 0;

      UNUSED(start_key);
      do
      {
        //traverse until find a valid block or reach the end block
        ret = read_next_block(block_cache, table_id, column_group_id, sstable_reader);
      } while (OB_SUCCESS != ret && OB_ITER_END != ret);

      if (OB_SUCCESS == ret)
      {
        if (uncompressed_data_size_ < block_header_size)
        {
          TBSYS_LOG(WARN, "Block size is too small, block size=%ld", 
                    uncompressed_data_size_);
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          sstable_reader->get_schema()->get_rowkey_column_count(table_id, column_count);
          start_row = uncompressed_buf_.get_buffer() + sizeof(ObSSTableBlockHeader);
          if (column_count == 0)
          {
          }
          else
          {
            start_key.assign(const_cast<ObObj*>(start_key.ptr()), column_count);
            ret = start_key.deserialize_objs(start_row, uncompressed_data_size_ - block_header_size, pos);
          }
        }

      }
      else if (OB_ITER_END == ret)
      {
        //complete traversal, do nothing
      }

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
