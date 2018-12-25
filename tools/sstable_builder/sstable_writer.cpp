/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * sstable_writer.cpp for persistent ssatable.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include <atomic.h>
#include "common/ob_crc64.h"
#include "common/utility.h"
#include "common/serialization.h"
#include "common/ob_row_util.h"
#include "sstable_writer.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace common::serialization;
    using namespace sstable;

    const int64_t SSTableWriter::DEFAULT_KEY_BUF_SIZE       = 64;
    const int64_t SSTableWriter::DEFAULT_COMPRESS_BUF_SIZE  = 128 * 1024;
    const int64_t SSTableWriter::DEFAULT_SERIALIZE_BUF_SIZE = 1024 * 1024;
    const int64_t SSTableWriter::DEFAULT_WRITE_BUFFER_SIZE  = 4 * 1024 * 1024;
    
    const int16_t SSTableWriter::DATA_BLOCK_MAGIC    = 0x4461; //"Da"
    const int16_t SSTableWriter::BLOCK_INDEX_MAGIC   = 0x4249; //"BI"
    const int16_t SSTableWriter::BLOOM_FILTER_MAGIC  = 0x426D; //"Bm"
    const int16_t SSTableWriter::SCHEMA_MAGIC        = 0x5363; //"Sc"
    const int16_t SSTableWriter::RANGE_MAGIC         = 0x5267; //"Rg"
    const int16_t SSTableWriter::TRAILER_MAGIC       = 0x5472; //"Tr"

    SSTableWriter::SSTableWriter()
      : inited_(false), first_row_(true), use_binary_rowkey_(false), add_row_count_(true),  
      table_id_(OB_INVALID_ID), column_group_id_(OB_INVALID_ID), 
      cur_key_buf_(DEFAULT_KEY_BUF_SIZE), bf_key_buf_(DEFAULT_KEY_BUF_SIZE), 
      offset_(0), prev_offset_(0), row_count_(0), prev_column_group_row_count_(0), 
      column_group_row_count_(0), compressor_(NULL), uncompressed_blocksize_(0), 
      compress_buf_(DEFAULT_COMPRESS_BUF_SIZE), serialize_buf_(DEFAULT_SERIALIZE_BUF_SIZE),
      enable_bloom_filter_(false), sstable_checksum_(0),
      write_buf_(NULL), data_size_(0), write_buf_size_(0)
    {
      reset();
    }

    SSTableWriter::~SSTableWriter()
    {
      if (NULL != compressor_)
      {
        destroy_compressor(compressor_);
        compressor_ = NULL;
      }

      if (NULL != write_buf_)
      {
        ob_free(write_buf_);
        write_buf_ = NULL;
      }
    }

    int SSTableWriter::create_sstable(const ObSSTableSchema& schema,
                                      const ObString& compressor_name,
                                      const int64_t table_version,
                                      const int store_type,
                                      const int64_t block_size,
                                      const int64_t element_count)
    {
      int ret = OB_SUCCESS;
      reset();
      
      if (inited_)
      {
        TBSYS_LOG(WARN, "sstable writer is inited, please close sstable first");
        ret = OB_ERROR;
      }
      else if ((store_type < OB_SSTABLE_STORE_DENSE || store_type > OB_SSTABLE_STORE_MIXED
                || block_size <= 0 || element_count < 0))
      {
        TBSYS_LOG(WARN, "Wrong store type or block size or bloom filter element count, "
                  "store_type=%d, block_size=%ld, element_count=%ld",
                  store_type, block_size, element_count);
        ret = OB_ERROR;
      }
      else if (table_version < 0)
      {
        TBSYS_LOG(WARN, "invalid arguments table_version= %ld", table_version);
        ret = OB_ERROR;
      }
      else if (0 >= schema.get_column_count())
      {
        TBSYS_LOG(WARN, "Wrong schema with column def count =%ld", schema.get_column_count());
        ret = OB_ERROR;
      }
      else if (NULL == compressor_name.ptr() || compressor_name.length() <= 0)
      {
        TBSYS_LOG(WARN, "Wrong compressor name, name_ptr=%p, name_len=%d",
                  compressor_name.ptr(), compressor_name.length());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = init_write_buf()))
      {
        TBSYS_LOG(WARN, "init write buffer failed");
      }
      
      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = copy_schema(schema)))
      {
        TBSYS_LOG(WARN, "copy schema failed");
      }

      //initialize trailer
      if (OB_SUCCESS == ret)
      {
        uncompressed_blocksize_ = block_size;
        ret = store_trailer_fields(schema.get_column_def(0)->table_id_,
                                   compressor_name, table_version, store_type, block_size);
      }

      //initialize block builder
      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = block_builder_.init(block_size)))
      {
        TBSYS_LOG(WARN, "Problem allocate memory for block builder");
      }

      //index_builder don't need to initialize

      //initialize bloom filter
      if (OB_SUCCESS == ret)
      {
        if ( 0 == element_count)
        {
          enable_bloom_filter_ = false;
        }
        else
        {
          enable_bloom_filter_ = true;
        }
      }

      if (OB_SUCCESS == ret && enable_bloom_filter_
          && OB_SUCCESS != (ret = bloom_filter_.init(element_count)))
      {
        TBSYS_LOG(WARN, "Problem initialize bloom filter");
      }

      //create compressor
      if (OB_SUCCESS == ret && NULL == compressor_)
      {
        compressor_ = create_compressor(compressor_name.ptr());
        if (NULL == compressor_)
        {
          TBSYS_LOG(WARN, "Problem create compressor");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        data_size_ = 0;
        inited_ = true;
      }

      return ret;
    }

    int SSTableWriter::create_sstable(const ObSSTableSchema& schema,
                                      const ObTrailerParam& trailer_param,
                                      const int64_t element_count)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        TBSYS_LOG(WARN, "sstable writer is inited, please close sstable first");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = trailer_.set_frozen_time(trailer_param.frozen_time_)))
      {
        TBSYS_LOG(WARN, "failed to set frozen time, frozen_time=%ld", 
                  trailer_param.frozen_time_);
      }
      else
      {
        ret = create_sstable(schema, trailer_param.compressor_name_, 
                             trailer_param.table_version_, trailer_param.store_type_,
                             trailer_param.block_size_, element_count);
      }

      return ret;
    }
    
    int SSTableWriter::append_row(const ObSSTableRow& row,
                                  int64_t& approx_space_usage)
    {
      int ret                        = OB_SUCCESS;
      int64_t row_size               = row.get_serialize_size();
      const uint64_t table_id        = row.get_table_id();
      const uint64_t column_group_id = row.get_column_group_id();

      ObRowkey row_key;
      ObString binary_key;
      int64_t rowkey_column_count = 0;
      if (row.is_binary_rowkey())
      {
        rowkey_column_count = row.get_rowkey_info()->get_size();
      }
      else
      {
        schema_.get_rowkey_column_count(table_id, rowkey_column_count);
      }
      row_key.assign(NULL, rowkey_column_count);

      if (!inited_)
      {
        TBSYS_LOG(WARN, "sstable writer isn't inited, please create sstable first");
        ret = OB_ERROR;
      }
      else if (0 >= rowkey_column_count || OB_SUCCESS != (ret = row.get_rowkey(row_key)))
      {
        TBSYS_LOG(WARN, "current row does not contains rowkey column count=%ld, table_id=%lu", 
            rowkey_column_count, table_id);
        ret = OB_ERROR;
      }
      else if (row.get_obj_count() == 0)
      {
        TBSYS_LOG(WARN, "There is no objects in the row to append");
        ret = OB_ERROR;
      }
      else if (table_id == OB_INVALID_ID || table_id == 0)
      {
        TBSYS_LOG(WARN, "invalid row, table_id=%lu", table_id);
        ret = OB_ERROR;
      }
      else if (column_group_id == OB_INVALID_ID)
      {
        TBSYS_LOG(WARN, "invalid row, column_group_id=%lu", column_group_id);
        ret = OB_ERROR;
      }
      else if (NULL == row_key.get_obj_ptr() || row_key.get_obj_cnt() <= 0)
      {
        TBSYS_LOG(WARN, "The row to append with NULL row key, key_ptr=%p, "
                        "key_len=%ld", row_key.get_obj_ptr(), row_key.get_obj_cnt());
        ret = OB_ERROR;
      }
      else if (is_dense_format() && OB_SUCCESS != (ret = row.check_schema(schema_)))
      {
        TBSYS_LOG(WARN, "The objects in the row are inconsistent with the schema");
      }
      else if (!first_row_ && is_invalid_row_key(table_id, column_group_id, row_key))
      {
        if (table_id == table_id_ && column_group_id == column_group_id_ 
            && row_key == cur_key_)
        {
          //don't append the same row
          TBSYS_LOG(DEBUG, "append same row");
          return ret;
        }
        /**
         * if the row is not the first row, need check whether the
         * current row is greater than the previous row order by 
         * (table_id, column_group_id, row_key) 
         */
        TBSYS_LOG(WARN, "Current row is not in order. last_row(table_id=%ld, "
                        "column_group_id=%ld), this_row(table_id=%ld, column_group_id=%ld)",
                  table_id, column_group_id, table_id_, column_group_id_);
        TBSYS_LOG(WARN, "Dump key of last row(%s) in buffer and current row(%s)",
            to_cstring(row_key), to_cstring(cur_key_));
        ret = OB_ERROR;
      }
      else if (!first_row_ && use_binary_rowkey_ && !row.is_binary_rowkey())
      {
        TBSYS_LOG(WARN, "use binary rowkey in first row, but current row(%s) "
            "is not binary rowkey format.", to_cstring(row_key));
        ret = OB_ERROR;
      }
      else if (first_row_)
      {
        //if it is first row, set table_id_ and column_group_id_
        table_id_        = table_id;
        column_group_id_ = column_group_id;
        use_binary_rowkey_ = row.is_binary_rowkey();
      }

      /**
       * if there is not enough space in block builder to store
       * current row, or the block data space usage in block builder
       * is greater than the uncompressed block size, or chage the
       * table id, or change column goupr, write the block into
       * sstable file immediately.
       */
      if (OB_SUCCESS == ret && need_switch_block(table_id, column_group_id, row_size))
      {
        ret = write_current_block();

        /**
         * if column_group have changed, have to insert an block
         * index item to block index builder, which has the start key
         * of column group and zero record length.
         */
        if (OB_SUCCESS == ret 
            && (table_id_ != table_id || column_group_id_ != column_group_id)
            && OB_SUCCESS == (ret = check_row_count()))
        {
          if (table_id_ != table_id)
          {
            prev_column_group_row_count_ = 0;
            add_row_count_ = true;
          }
          else if (column_group_id_ != column_group_id)
          {
            add_row_count_ = false;
          }

          table_id_ = table_id;
          column_group_id_ = column_group_id;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to write current block to disk, "
                          "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        //add row into block builder
        uint64_t row_checksum = 0;
        ret = block_builder_.add_row(row, row_checksum);
        if (OB_SUCCESS == ret)
        {
          ++column_group_row_count_;
          if (add_row_count_)
          {
            ++row_count_; //update row count of sstable
          }
        }
        else
        {
          if (first_row_)
          {
            reset();
          }
          TBSYS_LOG(WARN, "Problem add row into block builder, "
                          "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
        }

        //backup current row key
        if (OB_SUCCESS == ret)
        {
          if (use_binary_rowkey_)
          {
            if ( (OB_SUCCESS == (ret = row.get_binary_rowkey(binary_key)))
                && (OB_SUCCESS == (ret = cur_key_buf_.ensure_space(
                      binary_key.length(), ObModIds::OB_SSTABLE_WRITER))) )
            {
              memcpy(cur_key_buf_.get_buffer(), binary_key.ptr(), binary_key.length());
              cur_binary_key_.assign(cur_key_buf_.get_buffer(), static_cast<int32_t>(binary_key.length()));
            }
          }
          else
          {
            ObMemBufAllocatorWrapper allocator(cur_key_buf_);
            ret = row_key.deep_copy(cur_key_, allocator);
          }
        }

        //if this row is the first row of sstable, store the row key as start key
        if (OB_SUCCESS == ret && first_row_)
        {
          first_row_ = false;
        }

        if (OB_SUCCESS == ret && enable_bloom_filter_)
        {
          ret = update_bloom_filter(column_group_id, table_id, row_key);
        }
      }

      approx_space_usage = offset_ + block_builder_.get_block_size();
      
      return ret;
    }

    int SSTableWriter::append_row(const ObRow& row, int64_t& approx_space_usage)
    {
      int ret                        = OB_SUCCESS;
      int64_t row_size               = ObRowUtil::get_row_serialize_size(row);
      uint64_t table_id              = OB_INVALID_ID;
      uint64_t column_id             = 0;
      const uint64_t column_group_id = 0;

      const ObRowkey *row_key = NULL;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "sstable writer isn't inited, please create sstable first");
        ret = OB_NOT_INIT;
      }
      else if (row.get_column_num() == 0)
      {
        TBSYS_LOG(WARN, "There is no objects in the row[%s] to append", to_cstring(row));
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == row.get_row_desc())
      {
        TBSYS_LOG(WARN, "row desc of row[%s] is NULL", to_cstring(row));
      }
      else if (OB_SUCCESS != (ret = row.get_row_desc()->get_tid_cid(0LL, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "get_tid_cid ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row.get_rowkey(row_key)) || NULL == row_key)
      {
        TBSYS_LOG(WARN, "current row[%s] does not contains rowkey.", to_cstring(row));
        ret = OB_SKIP_INVALID_ROW;
      }
      else if (table_id == OB_INVALID_ID || table_id == 0)
      {
        TBSYS_LOG(WARN, "invalid row, table_id=%lu", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == row_key->get_obj_ptr() || row_key->get_obj_cnt() <= 0)
      {
        TBSYS_LOG(WARN, "The row to append with NULL row key, key_ptr=%p, "
                        "key_len=%ld", row_key->get_obj_ptr(), row_key->get_obj_cnt());
        ret = OB_SKIP_INVALID_ROW;
      }
      /*@TODO check schema
      else if (is_dense_format() && OB_SUCCESS != (ret = row.check_schema(schema_)))
      {
        TBSYS_LOG(WARN, "The objects in the row are inconsistent with the schema");
      }
      */
      else if (!first_row_ && is_invalid_row_key(table_id, column_group_id, *row_key))
      {
        /**
         * if the row is not the first row, need check whether the
         * current row is greater than the previous row order by 
         * (table_id, column_group_id, row_key) 
         */
        TBSYS_LOG(WARN, "Current row is not in order. last_row(table_id=%ld, "
            "column_group_id=%ld), this_row(table_id=%ld, column_group_id=%ld)",
            table_id, column_group_id, table_id_, column_group_id_);
        TBSYS_LOG(WARN, "Dump key of last row(%s) in buffer and current row(%s)",
            to_cstring(cur_key_), to_cstring(*row_key));
        if (table_id == table_id_ && column_group_id == column_group_id_ 
            && *row_key == cur_key_)
        {
          ret = OB_SKIP_INVALID_ROW;
        }
        else
        {
          ret = OB_ERROR;
        }
      }
      else if (first_row_)
      {
        //if it is first row, set table_id_ and column_group_id_
        table_id_        = table_id;
        column_group_id_ = column_group_id;
      }

      /**
       * if there is not enough space in block builder to store
       * current row, or the block data space usage in block builder
       * is greater than the uncompressed block size, or chage the
       * table id, or change column goupr, write the block into
       * sstable file immediately.
       */
      if (OB_SUCCESS == ret && need_switch_block(table_id, column_group_id, row_size))
      {
        ret = write_current_block();

        /**
         * if column_group have changed, have to insert an block
         * index item to block index builder, which has the start key
         * of column group and zero record length.
         */
        if (OB_SUCCESS == ret 
            && (table_id_ != table_id || column_group_id_ != column_group_id)
            && OB_SUCCESS == (ret = check_row_count()))
        {
          if (table_id_ != table_id)
          {
            prev_column_group_row_count_ = 0;
            add_row_count_ = true;
          }
          else if (column_group_id_ != column_group_id)
          {
            add_row_count_ = false;
          }

          table_id_ = table_id;
          column_group_id_ = column_group_id;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to write current block to disk, "
                          "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        //add row into block builder
        ret = block_builder_.add_row(table_id_, column_group_id_, row, row_size);
        if (OB_SUCCESS == ret)
        {
          ++column_group_row_count_;
          if (add_row_count_)
          {
            ++row_count_; //update row count of sstable
          }
        }
        else
        {
          if (first_row_)
          {
            reset();
          }
          TBSYS_LOG(WARN, "Problem add row into block builder, "
                          "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
        }

        //backup current row key
        if (OB_SUCCESS == ret)
        {
          ObMemBufAllocatorWrapper allocator(cur_key_buf_);
          ret = row_key->deep_copy(cur_key_, allocator);
        }

        //if this row is the first row of sstable, store the row key as start key
        if (OB_SUCCESS == ret && first_row_)
        {
          first_row_ = false;
        }

        if (OB_SUCCESS == ret && enable_bloom_filter_)
        {
          ret = update_bloom_filter(column_group_id, table_id, *row_key);
        }
      }

      approx_space_usage = offset_ + block_builder_.get_block_size();
      
      return ret;
    }

    int SSTableWriter::close_sstable(int64_t& trailer_offset,
                                     int64_t& sstable_size)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(INFO, "sstable writer isn't inited, needn't close sstable");
      }

      if (row_count_ > 0)
      {
        //if block builder has remaining row data, write it to buffer 
        if (OB_SUCCESS == ret && block_builder_.get_row_count() > 0)
        {
          ret = write_current_block();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "Problem write the last block, "
                            "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
          }
        }
        
        //write block index
        if (OB_SUCCESS == ret)
        {
          ret = write_block_index();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "Problem write block index, "
                            "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
          }
        }
      }
      else
      {
        //no data in sstable, write nothing, just  write key stream and trailer
        TBSYS_LOG(INFO, "empty sstable, just write key stream and trailer, "
                        "table_id=%lu, row_count=%ld, offset=%ld",
                  table_id_, row_count_, offset_);
      }

      //write bloom filter
      if (OB_SUCCESS == ret && enable_bloom_filter_)
      {
        ret = write_bloom_filter();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem write bloom filter, "
                          "table_id=%lu, row_count=%ld, offset=%ld",
                  table_id_, row_count_, offset_);
        }
      }

      //write schema
      if (OB_SUCCESS == ret)
      {
        ret = write_schema();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem write schema, "
                          "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
        }
      }

      //write range
      if (OB_SUCCESS == ret)
      {
        ret = write_range();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem write range, "
              "table_id=%lu, row_count=%ld, offset=%ld",
              table_id_, row_count_, offset_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        trailer_offset = offset_;
        ret = write_trailer();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem write trailer, "
                          "table_id=%lu, row_count=%ld, offset=%ld",
                    table_id_, row_count_, offset_);
        }
        else
        {
          sstable_size = offset_;
        }
      }

      if (OB_SUCCESS != ret)
      {
        trailer_offset = -1;  //error happens, set -1
        sstable_size = -1;
      }

      reset();  //reset writer in order to reuse it

      return ret;
    }

    int SSTableWriter::set_tablet_range(const ObNewRange& tablet_range)
    {
      return trailer_.copy_range(tablet_range);
    }

    /*
    int SSTableWriter::update_bloom_filter(const uint64_t column_group_id,
                                           const uint64_t table_id,
                                           const ObString& row_key)
    {
      int ret             = OB_SUCCESS;
      UNUSED(column_group_id);
      UNUSED(table_id);
      bloom_filter_.insert(row_key);
      return ret;
    }
    */

    int SSTableWriter::update_bloom_filter(const uint64_t column_group_id,
                                             const uint64_t table_id,
                                             const ObRowkey& row_key)
    {//TODO: return ret
      int ret             = OB_SUCCESS;
      UNUSED(column_group_id);
      UNUSED(table_id);
      bloom_filter_.insert(row_key);
      return ret;
    }
    
    bool SSTableWriter::is_dense_format()
    {
      return (trailer_.get_row_value_store_style() == OB_SSTABLE_STORE_DENSE);
    }

    int SSTableWriter::write_record_header(const int16_t magic,
                                             const char* comp_data, 
                                             const int64_t comp_size, 
                                             const int64_t uncomp_size,
                                             int64_t& wrote_len)
    {
      int ret              = OB_SUCCESS;
      int64_t pos          = 0;
      int64_t header_len   = sizeof(ObRecordHeader);
      int64_t checksum_len = sizeof(uint64_t);
      char header_buf[header_len];
      char checksum_buf[checksum_len];
      ObRecordHeader header;

      if (NULL == comp_data || comp_size <= 0 || uncomp_size <=0)
      {
        TBSYS_LOG(WARN, "invalid param, comp_data=%p, comp_size=%ld, uncomp_size=%ld",
                  comp_data, comp_size, uncomp_size);
        ret = OB_ERROR;
      }
      else
      {
        header.set_magic_num(magic);
        header.header_length_ = static_cast<int16_t>(header_len);
        header.version_ = 0;      //current record header version is 0
        header.reserved_ = 0;

        /**
         * if data_length_ == data_zlength_, it means that the data is
         * not compressed, if data_length_ < data_zlength_, it means
         * that the data compressed. if data_length_ > data_zlength_,
         * it's wrong.
         */
        header.data_length_ = static_cast<int32_t>(uncomp_size);
        header.data_zlength_ = static_cast<int32_t>(comp_size);

        /**
         * if using compression, store the checksum of compressed data,
         * if not using compression, store the checksum of uncompressed
         * data
         */
        header.data_checksum_ = ob_crc64(comp_data, comp_size);

        //caculate the checksum of sstable
        ret = encode_i64(checksum_buf, checksum_len, pos, header.data_checksum_);
        if (OB_SUCCESS == ret)
        {
          sstable_checksum_ = ob_crc64(sstable_checksum_, checksum_buf, checksum_len);
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialize checksum of block_");
        }

        if (OB_SUCCESS == ret)
        {
          //must call this function after setting all the member of reocrd header
          header.set_header_checksum();
          pos = 0;
          ret = header.serialize(header_buf, header_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "Problem serialize block header to sstable");
          }
        }

        if (OB_SUCCESS == ret)
        {
          ret = copy2_write_buf(header_buf, header_len, wrote_len);
        }
      }

      return ret;
    }

    int SSTableWriter::compress_and_write(const char* input,
                                          const int64_t input_len,
                                          const int16_t magic,
                                          int64_t& wrote_len,
                                          const bool need_compress)
    {
      int ret                   = OB_SUCCESS;
      int64_t compress_buf_len  = 0;
      int64_t compressed_size   = 0;
      const char* output        = input;
      int64_t output_len        = input_len;
      int64_t copy_size         = 0;

      wrote_len = 0;
      if (NULL == input || input_len <= 0 || NULL == compressor_)
      {
        TBSYS_LOG(WARN, "Can't write NULL input, input=%p, input_len=%ld, " 
            "compressor_=%p", 
          input, input_len, compressor_);
        ret = OB_ERROR;
      }
      else 
      {
        compress_buf_len  = input_len + compressor_->get_max_overflow_size(input_len);
      }

      //compress if necessary
      if (OB_SUCCESS == ret && need_compress 
          && OB_SUCCESS == (ret = compress_buf_.ensure_space(
            compress_buf_len, ObModIds::OB_SSTABLE_WRITER)))
      {
        ret = compressor_->compress(input, input_len, compress_buf_.get_buffer(),
                                    compress_buf_len, compressed_size);
        if (OB_SUCCESS == ret)
        {
          /**
           * if the compressed size equals with the input length, don't
           * compress data, just output the original data. if
           * compressed_size < input_len, output the compressed data
           */
          if (compressed_size < input_len)
          {
            output = compress_buf_.get_buffer();
            output_len = compressed_size;
          }
          else if (compressed_size > input_len)
          {
            TBSYS_LOG(INFO, "The compressed size is larger than original data size,"
                            "input_len=%ld, compressed_size=%ld",
                      input_len, compressed_size);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to compress, input=%p, "
                          "input_len=%ld, compress_buf=%p, compress_buf_len=%ld",
                    input, input_len, compress_buf_.get_buffer(), 
                    compress_buf_len);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = write_record_header(magic, output, output_len, input_len, copy_size);
      }

      if (OB_SUCCESS == ret)
      {
        wrote_len += copy_size;
        ret = copy2_write_buf(output, output_len, copy_size);
        if (OB_SUCCESS == ret)
        {
          wrote_len += copy_size;
        }
      }

      if (OB_SUCCESS != ret)
      {
        /**
         * write record header success, but write compressed data 
         * failed, we could stop write data into sstable.
         */
        wrote_len = 0;
      }
      
      return ret;
    }

    int SSTableWriter::write_current_block()
    {
      int ret               = OB_SUCCESS;
      int32_t record_size   = 0;
      int64_t wrote_len     = 0;

      //builder block data with expected format
      ret = block_builder_.build_block();
      if (OB_SUCCESS == ret)
      {
        //compress and write block data into buffer
        ret = compress_and_write(block_builder_.block_buf(),
                                 block_builder_.get_block_data_size(),
                                 DATA_BLOCK_MAGIC, wrote_len, true);
      }

      if (OB_SUCCESS == ret)
      {
        offset_ += wrote_len;   //update current sstable file offset
        block_builder_.reset(); //reset block builder, very important

        //add one block index entry into block index builder
        record_size = static_cast<int32_t>(offset_ - prev_offset_);
        prev_offset_ = offset_;
        if (use_binary_rowkey_)
        {
          ret = index_builder_.add_entry(table_id_, column_group_id_, 
              cur_binary_key_, record_size);
        }
        else
        {
          ret = index_builder_.add_entry(table_id_, column_group_id_, 
              cur_key_, record_size);
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem add entry to block index builder, "
                          "table_id=%lu, record_size=%d, row_key: %s",
                    table_id_, record_size, to_cstring(cur_key_));
        }
      }

      return ret;
    }

    int SSTableWriter::write_block_index()
    {
      int ret                      = OB_SUCCESS;
      int64_t wrote_len            = 0;
      int64_t index_buf_size       = 0;
      int64_t index_buf_size_input = index_builder_.get_index_block_size();

      if (index_buf_size_input > MAX_BLOCK_INDEX_SIZE)
      {
        TBSYS_LOG(ERROR, "block index is too large, can't write, "
                         "block_index_size=%ld, max_block_index_size=%ld",
          index_buf_size_input, MAX_BLOCK_INDEX_SIZE);
        ret = OB_ERROR;
      }
      else 
      {
        ret = serialize_buf_.ensure_space(index_buf_size_input, ObModIds::OB_SSTABLE_WRITER);
      }

      if (OB_SUCCESS == ret)
      {
        //build block index with expected format
        ret = index_builder_.build_block_index(use_binary_rowkey_, serialize_buf_.get_buffer(),
                                               index_buf_size_input, index_buf_size);
        if (OB_SUCCESS == ret)
        {
          //set block index offset and size
          ret = trailer_.set_block_index_record_offset(offset_);
          if (OB_SUCCESS == ret)
          {
            ret = trailer_.set_block_index_record_size(index_buf_size
                                                       + sizeof(ObRecordHeader));
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        //compress and write block index, with uncompressed format
        ret = compress_and_write(serialize_buf_.get_buffer(), index_buf_size,
                                 BLOCK_INDEX_MAGIC, wrote_len, false);
        if (OB_SUCCESS == ret)
        {
          offset_ += wrote_len;
        }
      }

      return ret;
    }

    int SSTableWriter::write_bloom_filter()
    {
      int ret                       = OB_SUCCESS;
      int64_t wrote_len             = 0;
      const char* bloom_filter_buf  = NULL;

      //set values related with bloom filter in trailer
      ret = trailer_.set_bloom_filter_record_offset(offset_);
      if (OB_SUCCESS == ret)
      {
        ret = trailer_.set_bloom_filter_record_size(bloom_filter_.get_nbyte() + sizeof(ObRecordHeader));
        if (OB_SUCCESS == ret)
        {
          ret = trailer_.set_bloom_filter_hash_count(bloom_filter_.get_nhash());
        }
      }

      if (OB_SUCCESS == ret)
      {
        //only store the bitmap of bloom filter, with uncompressed format
        bloom_filter_buf = reinterpret_cast<const char*>(bloom_filter_.get_buffer());
        ret = compress_and_write(bloom_filter_buf, bloom_filter_.get_nbyte(),
                                 BLOOM_FILTER_MAGIC, wrote_len, false);
        if (OB_SUCCESS == ret)
        {
          offset_ += wrote_len;
        }
      }

      return ret;
    }

    int SSTableWriter::write_schema()
    {
      int ret           = OB_SUCCESS;
      int64_t wrote_len = 0;
      int schema_size   = static_cast<int32_t>(schema_.get_serialize_size());
      int64_t pos       = 0;
     
      //serialize schema
      ret = serialize_buf_.ensure_space(schema_size, ObModIds::OB_SSTABLE_WRITER);
      if (OB_SUCCESS == ret)
      {
        ret = schema_.serialize(serialize_buf_.get_buffer(), schema_size, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem serialize schema for sstable");
        }
      }

      if (OB_SUCCESS == ret)
      {
        //set values related with schem in trailer
        ret = trailer_.set_schema_record_offset(offset_);
        if (OB_SUCCESS == ret)
        {
          ret = trailer_.set_schema_record_size(schema_size + sizeof(ObRecordHeader));
        }
      }

      if (OB_SUCCESS == ret)
      {
        //compress and write schema, with uncompressed format
        ret = compress_and_write(serialize_buf_.get_buffer(), schema_size, SCHEMA_MAGIC,
                                 wrote_len, false);
        if (OB_SUCCESS == ret)
        {
          offset_ += wrote_len;
        }
      }

      return ret;
    }

    int SSTableWriter::write_range()
    {
      int ret           = OB_SUCCESS;
      int64_t wrote_len = 0;
      int64_t pos = 0;
      const ObNewRange& range = trailer_.get_range();
      int64_t range_size = range.get_serialize_size();

      if (OB_SUCCESS != 
          (ret = serialize_buf_.ensure_space(range_size, ObModIds::OB_SSTABLE_WRITER)))
      {
        TBSYS_LOG(WARN, "failed to ensure space for range, ret=%d", ret);
      }
      else if (OB_SUCCESS !=
          (ret = range.serialize(serialize_buf_.get_buffer(), range_size, pos)))
      {
        TBSYS_LOG(WARN, "Problem serialize range for sstable, ret=%d", ret);
      }
      else if (OB_SUCCESS != 
          (ret = trailer_.set_range_record_offset(offset_)))
      {
        TBSYS_LOG(WARN, "failed to set range record offset");
      }
      else if (OB_SUCCESS != 
          (ret = trailer_.set_range_record_size(range_size + sizeof(ObRecordHeader))))
      {
        TBSYS_LOG(WARN, "failed to set range record size");
      }    
      else if (OB_SUCCESS != 
          (ret = compress_and_write(serialize_buf_.get_buffer(), range_size,
                                    RANGE_MAGIC, wrote_len, false)))
      {
        TBSYS_LOG(WARN, "failed to write range");
      }
      else
      {
        offset_ += wrote_len;
      }

      return ret;
    }

    int SSTableWriter::write_trailer()
    {
      int ret                     = OB_SUCCESS;
      int64_t wrote_len           = 0;
      int64_t pos                 = 0;
      int64_t trailer_size        = 0;
      int64_t trailer_offset_size = 0;
      int64_t trailer_buf_size    = 0;
      ObTrailerOffset trailer_offset;

      //allocate temp buffer to store serialized trailer
      trailer_size = trailer_.get_serialize_size();
      trailer_offset_size = trailer_offset.get_serialize_size();
      trailer_buf_size = trailer_size + trailer_offset_size;

      ret = serialize_buf_.ensure_space(trailer_buf_size, ObModIds::OB_SSTABLE_WRITER);
      if (OB_SUCCESS == ret)
      {
        trailer_.set_sstable_checksum(sstable_checksum_);
        if (OB_SUCCESS == ret)
        {
          ret = trailer_.set_row_count(row_count_);
          if (OB_SUCCESS == ret)
          {
            ret = trailer_.set_block_count(index_builder_.get_block_count());
          }
        }
      }

      //serialize trailer
      if (OB_SUCCESS == ret)
      {
        ret = trailer_.serialize(serialize_buf_.get_buffer(), trailer_buf_size, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem serialize trailer for sstable");
        }
      }

      //serialize trailer offset
      if (OB_SUCCESS == ret)
      {
        trailer_offset.trailer_record_offset_ = offset_;
        ret = trailer_offset.serialize(serialize_buf_.get_buffer(), trailer_buf_size, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Problem serialize trailer offset for sstable");
        }
      }

      if (OB_SUCCESS == ret)
      {
        //compress and write trailer, with uncompressed format
        ret = compress_and_write(serialize_buf_.get_buffer(), trailer_size,
                                 TRAILER_MAGIC, wrote_len, false);
        if (OB_SUCCESS == ret)
        {
          offset_ += wrote_len;
        }
      }

      if (OB_SUCCESS == ret)
      {
        //write trailer record offset to data buffer
        ret = copy2_write_buf(serialize_buf_.get_buffer() + trailer_size, 
                              trailer_offset_size, wrote_len);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Problem write trailer offset to sstable, error=%s",
                    strerror(errno));
        }
        else
        {
          offset_ += trailer_offset_size;
          TBSYS_LOG(INFO, "close sstable, checksum=%lu, table_id=%lu, "
                          "sstable_size=%ld, row_count=%ld, sstable_block_size=%ld, "
                          "end_rowkey=%s",
                    trailer_.get_sstable_checksum(), table_id_,
                    offset_, row_count_, uncompressed_blocksize_, to_cstring(cur_key_));
        }
      }

      return ret;
    }

    int SSTableWriter::copy2_write_buf(const char* data, const int64_t data_size,
                                       int64_t& wrote_len)
    {
      int ret = OB_SUCCESS;
      wrote_len = 0;

      if (NULL == data || data_size < 0)
      {
        TBSYS_LOG(WARN, "invalid param, data=%p, data_size=%ld", 
                  data, data_size);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS == (ret = ensure_write_buf(data_size)))
      {
        memcpy(write_buf_ + data_size_, data, data_size);
        data_size_ += data_size;
        wrote_len += data_size;
      }

      return ret;
    }

    void SSTableWriter::reset()
    {
      inited_ = false;
      first_row_ = true;
      use_binary_rowkey_ = false;
      add_row_count_ = true;
      table_id_ = OB_INVALID_ID;
      column_group_id_ = OB_INVALID_ID;
      cur_key_.assign(NULL, 0);
      offset_ = 0;
      prev_offset_ = 0;
      row_count_ = 0;
      prev_column_group_row_count_ = 0;
      column_group_row_count_ = 0;
      sstable_checksum_ = 0;
      uncompressed_blocksize_ = 0;

      if (NULL != compressor_)
      {
        destroy_compressor(compressor_);
        compressor_ = NULL;
      }
     
      schema_.reset();
      block_builder_.reset();
      index_builder_.reset();
      if (enable_bloom_filter_)
      {
        bloom_filter_.destroy();
        enable_bloom_filter_ = false;
      }
      trailer_.reset();
    }
  
    int SSTableWriter::copy_schema(const ObSSTableSchema& schema)
    {
      int ret         = OB_SUCCESS;
      int64_t column_num  = schema.get_column_count();
      const ObSSTableSchemaColumnDef * column_def = NULL;

      for(int32_t i = 0; i < column_num; ++i)
      {
        column_def = schema.get_column_def(i);
        if (NULL != column_def)
        {
          ret = schema_.add_column_def(*column_def);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "add column def failed column_count=%ld curr_ofset=%d", 
                      column_num, i);
            break;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "got null column from scheam, column_def=%p", column_def);
          ret = OB_ERROR;
          break;
        }
      }

      return ret;
    }

    inline bool SSTableWriter::need_switch_block(const uint64_t table_id, 
                                                 const uint64_t column_group_id, 
                                                 const int64_t row_size)
    {
      return ((block_builder_.get_block_size() > uncompressed_blocksize_
               || table_id_ != table_id || column_group_id_ != column_group_id
               || OB_SUCCESS != block_builder_.ensure_space(row_size)));
    }

    inline bool SSTableWriter::is_invalid_row_key(const uint64_t table_id, 
                                                  const uint64_t column_group_id,
                                                  const common::ObRowkey& row_key)
    {
      return (table_id < table_id_
              ||(table_id == table_id_ && column_group_id < column_group_id_)
              ||(table_id == table_id_ && column_group_id == column_group_id_ 
                 && row_key <= cur_key_));
    }

    int SSTableWriter::init_write_buf()
    {
      int ret = OB_SUCCESS;

      if (NULL == write_buf_)
      {
        write_buf_ = static_cast<char*>(ob_malloc(DEFAULT_WRITE_BUFFER_SIZE, 
          ObModIds::OB_SSTABLE_WRITER));
        if (NULL == write_buf_)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for write buffer");
          ret = OB_ERROR;
        }
        else
        {
          data_size_ = 0;
          write_buf_size_ = DEFAULT_WRITE_BUFFER_SIZE;
        }
      }

      return ret;
    }

    int SSTableWriter::ensure_write_buf(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (write_buf_size_ - data_size_ < size)
      {
        //enlarge write buffer
        int64_t new_buffer_size = write_buf_size_;
        char * new_write_buffer = NULL;
        if(write_buf_size_ * 2 - data_size_ < size)
        {
          new_buffer_size = write_buf_size_ + size;
        }
        else
        {
          new_buffer_size = write_buf_size_ * 2;
        }
        
        new_write_buffer = static_cast<char*>(ob_malloc(new_buffer_size, 
          ObModIds::OB_SSTABLE_WRITER));
        if (NULL == new_write_buffer)
        {
          TBSYS_LOG(ERROR, "alloc memory failed new_write_buffer=%p", new_write_buffer);
          ret = OB_ERROR;
        }
        else
        {
          memcpy(new_write_buffer, write_buf_, data_size_);
          ob_free(write_buf_);
          write_buf_size_ = new_buffer_size;
          write_buf_ = new_write_buffer;
        }
      }

      return ret;
    }

    int SSTableWriter::store_trailer_fields(const uint64_t first_table_id,
                                            const ObString& compressor_name,
                                            const int64_t table_version, 
                                            const int store_type, 
                                            const int64_t block_size)
    {
      int ret = OB_SUCCESS;

      ret = trailer_.set_block_size(block_size);
      if (OB_SUCCESS == ret)
      {
        ret = trailer_.set_table_version(table_version);
      }

      if (OB_SUCCESS == ret)
      {
        //table trailer is 0x200 in version 0.2.0
        ret = trailer_.set_trailer_version(ObSSTableTrailer::SSTABLEV3);
      }

      if (OB_SUCCESS == ret)
      {
        /**
         * FIXME: currently we assume that a physical file only has one
         * sstable, so the first block data offset of sstable is 0. so
         * we can initialize this value here. if one physical file store
         * more than one sstable, we must ensure the assignment is ok.
         */
        ret = trailer_.set_first_block_data_offset(offset_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = trailer_.set_row_value_store_style(static_cast<int16_t>(store_type));
      }

      if (OB_SUCCESS == ret)
      {
        //add the first table id in schema into trailer
        ret = trailer_.set_first_table_id(first_table_id);
      }

      //initialize compressor name
      if (OB_SUCCESS == ret)
      {
        if (compressor_name.length() <= OB_MAX_COMPRESSOR_NAME_LENGTH)
        {
          ret = trailer_.set_compressor_name(compressor_name.ptr());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "Problem set compressor name");
          }
        }
        else
        {
          TBSYS_LOG(WARN, "Compressor name length is too large, "
                          "compressor name length=%d, max_len=%ld",
                    compressor_name.length(), OB_MAX_COMPRESSOR_NAME_LENGTH);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int SSTableWriter::check_row_count()
    {
      int ret = OB_SUCCESS;

      if (is_dense_format())
      {
        if (prev_column_group_row_count_ > 0 
            && column_group_row_count_ != prev_column_group_row_count_)
        {
          TBSYS_LOG(WARN, "current column group row count isn't consistent "
                          "with previou column group for dense format, "
                          "pre_row_count=%ld, cur_row_count=%ld, table_id=%lu, "
                          "colum_group_id=%lu",
                    prev_column_group_row_count_, column_group_row_count_,
                    table_id_, column_group_id_);
          ret = OB_ERROR;
        }
        prev_column_group_row_count_ = column_group_row_count_;
        column_group_row_count_ = 0;  //reset row counter of column group
      }

      return ret;
    }

    const char* SSTableWriter::get_write_buf(int64_t& data_size) const
    {
      data_size = data_size_;
      return write_buf_;
    }

    void SSTableWriter::reset_data_size()
    {
      data_size_ = 0;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
