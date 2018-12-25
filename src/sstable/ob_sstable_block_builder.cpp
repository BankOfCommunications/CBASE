/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_sstable_block_builder.cpp for persistent ssatable block 
 * and store inner block index. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "common/serialization.h"
#include "common/ob_row_util.h"
#include "common/ob_crc64.h"
#include "ob_sstable_block_builder.h"

namespace oceanbase 
{ 
  namespace sstable 
  {
    using namespace common;
    using namespace common::serialization;

    ObSSTableBlockBuilder::ObSSTableBlockBuilder() 
    : row_index_buf_(NULL), row_index_buf_cur_(NULL), row_index_buf_size_(0),
      block_buf_(NULL), block_buf_cur_(NULL), block_buf_size_(0)
    { 
      memset(&block_header_, 0, sizeof(ObSSTableBlockHeader));
    }

    ObSSTableBlockBuilder::~ObSSTableBlockBuilder()
    {
      if ( NULL != block_buf_)
      {
        ob_free(block_buf_);
        block_buf_ = NULL;
      }
    }

    void ObSSTableBlockBuilder::reset() 
    {
      row_index_buf_cur_ = row_index_buf_;
      memset(&block_header_, 0, sizeof(ObSSTableBlockHeader));

      //reserve space to store block header
      block_buf_cur_ = block_buf_ + block_header_.get_serialize_size();
    }

    inline const int64_t ObSSTableBlockBuilder::get_row_index_remain() const
    {
      return (row_index_buf_size_ - (row_index_buf_cur_ - row_index_buf_));
    }

    inline const int64_t ObSSTableBlockBuilder::get_block_data_remain() const
    {
      return (block_buf_size_ - row_index_buf_size_ 
              - (block_buf_cur_ - block_buf_));
    }

    DEFINE_SERIALIZE(ObSSTableBlockHeader)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len)) 
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret 
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, row_index_array_offset_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, row_count_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_group_id_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, table_id_))))
      { 
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie block header, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      return ret;
    }
    
    DEFINE_DESERIALIZE(ObSSTableBlockHeader)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size + pos > data_len) 
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, data_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &row_index_array_offset_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &row_count_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, reinterpret_cast<int16_t*>(&column_group_id_))))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&table_id_)))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie block header, buf=%p, "
                  "data_len=%ld, pos=%ld", buf, data_len, pos);
      }        
    
      return ret;
    }
    
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableBlockHeader)
    {
      return (encoded_length_i32(row_index_array_offset_) 
              + encoded_length_i32(row_count_) 
              + encoded_length_i16(reserved_)
              + encoded_length_i16(column_group_id_)
              + encoded_length_i32(table_id_));
    }

    int ObSSTableBlockBuilder::init(const int64_t block_size)
    {
      int ret                     = OB_SUCCESS;
      int64_t old_block_buf_size  = block_buf_size_;

      if (block_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid block size, block_size=%ld", block_size);
        ret = OB_ERROR;
      }
      else
      {
        //1k align
        //alloc 2M memory for block data buffer, 64k for row index buffer
        block_buf_size_     = BLOCK_DATA_BUFFER_SIZE + ROW_INDEX_BUFFER_SIZE;
        row_index_buf_size_ = ROW_INDEX_BUFFER_SIZE;
      }

      if (OB_SUCCESS == ret && NULL != block_buf_ 
          && block_buf_size_ > old_block_buf_size)
      {
        ob_free(block_buf_);
        block_buf_ = NULL;
      }

      /**
       * allocate memory for block buffer if necessary, the block 
       * buffer include three parts, at the beginning, it's the block 
       * header, the block data is in the middle, and the row index 
       * array is at the end. 
       *        -----------------------------------------------
       *        | block header | block data | row index array |
       *        -----------------------------------------------
       */
      if (OB_SUCCESS == ret && NULL == block_buf_)
      {
        //allocate memory for block buffer
        block_buf_ = static_cast<char *>(ob_malloc(block_buf_size_, 
                                                   ObModIds::OB_SSTABLE_WRITER));
        if (NULL == block_buf_)
        {
          TBSYS_LOG(ERROR, "Problem allocating memory for block buf");
          ret = OB_ERROR; 
        }
      }

      if (OB_SUCCESS == ret && NULL != block_buf_)
      {
        //reserve space to store block header
        block_buf_cur_ = block_buf_ + block_header_.get_serialize_size();
        row_index_buf_ = block_buf_ + block_buf_size_ - row_index_buf_size_;
        row_index_buf_cur_ = row_index_buf_;
      }

      if (OB_SUCCESS != ret)
      {
        row_index_buf_ = NULL;
        row_index_buf_cur_ = NULL;
        row_index_buf_size_ = 0;
        block_buf_ = NULL;
        block_buf_cur_ = NULL;
        block_buf_size_ = 0;
      }

      return ret;
    }

    int ObSSTableBlockBuilder::ensure_space(const int64_t size)
    {
      int ret               = OB_SUCCESS;
      int64_t index_remain  = get_row_index_remain() - MIN_ROW_INDEX_BUF_SIZE;
      int64_t block_remain  = get_block_data_remain();

      /**
       * if row index buffer can store extra 2 int32_t at least, and 
       * block buffer can store the block header(16 bytes), serialized
       * rows, current row index, extra 2 int32_t, and the size of new
       * serialized row at least, then we can ensure the space is 
       * enough to store a new row. 
       */
      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (index_remain < 0 || block_remain - size < 0)
      {
        //enlarge block buffer
        int new_buffer_size =  static_cast<int>(block_buf_size_);
        char * new_index_buffer = NULL;
        //if( block_remain + BLOCK_DATA_BUFFER_SIZE < size)
        if( (block_buf_size_ - row_index_buf_size_) * 2 - get_block_data_size() < size)
        {
          //1k align
          new_buffer_size =  static_cast<int32_t>((block_buf_cur_ - block_buf_ + row_index_buf_size_ * 2
                             + size + (SSTABLE_BLOCK_BUF_ALIGN_SIZE - 1))
                             & ~(SSTABLE_BLOCK_BUF_ALIGN_SIZE - 1));
        }
        else
        {
          new_buffer_size =  static_cast<int32_t>(block_buf_size_ * 2);
        }
        
        char * new_block_buffer = static_cast<char*>(ob_malloc(new_buffer_size, ObModIds::OB_SSTABLE_WRITER));
        if (NULL == new_block_buffer)
        {
          TBSYS_LOG(ERROR, "alloc memory failed new_block_buffer=%p", new_block_buffer);
          ret = OB_ERROR;
        }
        else
        {
          int64_t block_data_size = get_block_data_size();
          int64_t index_data_size = get_row_index_size();
          new_index_buffer = new_block_buffer + new_buffer_size - 2 * row_index_buf_size_;
          memcpy(new_block_buffer, block_buf_, block_data_size);
          memcpy(new_index_buffer, row_index_buf_, index_data_size);

          ob_free(block_buf_);
          block_buf_ = NULL;

          block_buf_size_      = new_buffer_size;
          block_buf_           = new_block_buffer;
          block_buf_cur_       = block_buf_ + block_data_size;
          row_index_buf_       = new_index_buffer;
          row_index_buf_cur_   = row_index_buf_ + index_data_size;
          row_index_buf_size_  = 2 * row_index_buf_size_;
        }
      }
      return ret;
    }

    int ObSSTableBlockBuilder::add_row(const ObSSTableRow &row, uint64_t &row_checksum, const int64_t row_serialize_size) 
    {
      int ret             = OB_SUCCESS;
      int32_t row_offset  = get_block_data_size();
      int64_t row_size    = row_serialize_size > 0 ? row_serialize_size : row.get_serialize_size();
      int64_t block_pos   = 0;
      int64_t index_pos   = 0;

      if (row.get_obj_count() <= 0 || OB_INVALID_ID == row.get_table_id()
          || 0 == row.get_table_id() || OB_INVALID_ID == row.get_column_group_id())
      {
        TBSYS_LOG(WARN, "invalid param, row column_count=%ld, row table id=%lu,"
                  "row column group id =%lu", row.get_obj_count(), row.get_table_id(), row.get_column_group_id());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && 0 == get_row_count())
      {
        block_header_.table_id_        =  static_cast<uint32_t>(row.get_table_id());
        block_header_.column_group_id_ =  static_cast<uint16_t>(row.get_column_group_id());
      }
      
      //check whether the row has same table id&&group id with current block
      if (OB_SUCCESS == ret && 0 != get_row_count() && (row.get_table_id() != block_header_.table_id_
                                   || row.get_column_group_id() != block_header_.column_group_id_))
      {
        TBSYS_LOG(WARN, "disargument  row (table_id=%lu, column_group_id=%lu)," 
                        "block (table_id=%u, column_group_id=%u)",row.get_table_id(),
                         row.get_column_group_id(), block_header_.table_id_, 
                         block_header_.column_group_id_);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && OB_SUCCESS == (ret = ensure_space(row_size)))
      {
        ret = row.serialize(block_buf_cur_, 
                            get_block_data_remain(), block_pos, row_size);
        if (OB_SUCCESS == ret)
        {
          //Calculate the row checksum
          row_checksum = common::ob_crc64(block_buf_cur_, block_pos);

          ret = encode_i32(row_index_buf_cur_, 
                           get_row_index_remain(),
                           index_pos, row_offset);
          if (OB_SUCCESS == ret)
          {
            block_buf_cur_ += block_pos;
            row_index_buf_cur_ += index_pos;
            block_header_.row_count_++;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to serialize row index");
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialize row");
        }
      }

      return ret;
    }

    int ObSSTableBlockBuilder::add_row(
        const uint64_t table_id, const uint64_t column_group_id, 
        const ObRow &row, const int64_t row_serialize_size) 
    {
      int ret             = OB_SUCCESS;
      int32_t row_offset  = get_block_data_size();
      int64_t row_size    = row_serialize_size;
      int64_t block_pos   = 0;
      int64_t index_pos   = 0;

      if (row.get_column_num() <= 0 || OB_INVALID_ID == table_id
          || 0 == table_id || OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid param, row column_count=%ld, row table id=%lu,"
                  "row column group id =%lu", row.get_column_num(), table_id, column_group_id);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && 0 == get_row_count())
      {
        block_header_.table_id_        =  static_cast<uint32_t>(table_id);
        block_header_.column_group_id_ =  static_cast<uint16_t>(column_group_id);
      }
      
      //check whether the row has same table id&&group id with current block
      if (OB_SUCCESS == ret && 0 != get_row_count() 
          && (table_id != block_header_.table_id_ 
            || column_group_id != block_header_.column_group_id_))
      {
        TBSYS_LOG(WARN, "disargument  row (table_id=%lu, column_group_id=%lu)," 
                        "block (table_id=%u, column_group_id=%u)", table_id,
                         column_group_id, block_header_.table_id_, 
                         block_header_.column_group_id_);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && OB_SUCCESS == (ret = ensure_space(row_size)))
      {
        ret = ObRowUtil::serialize_row(row, block_buf_cur_, 
                            get_block_data_remain(), block_pos);
        if (OB_SUCCESS == ret)
        {
          ret = encode_i32(row_index_buf_cur_, 
                           get_row_index_remain(),
                           index_pos, row_offset);
          if (OB_SUCCESS == ret)
          {
            block_buf_cur_ += block_pos;
            row_index_buf_cur_ += index_pos;
            block_header_.row_count_++;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to serialize row index");
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialize row");
        }
      }

      return ret;
    }
    
    int ObSSTableBlockBuilder::build_block() 
    {
      int ret               = OB_SUCCESS;
      int32_t row_offset    = get_block_data_size();
      int64_t index_remain  = get_row_index_remain() - ROW_INDEX_ITEM_SIZE;
      int64_t block_remain  = get_block_data_remain() - ROW_INDEX_ITEM_SIZE;
      int64_t block_pos     = 0;
      int64_t index_pos     = 0;

      //check if there is enough space to store the "row_count + 1" index
      if (index_remain < 0 || block_remain < 0)
      {
        TBSYS_LOG(WARN, "there is not enough space to store the 'row_count + 1' index");
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        /**
         * add the "row_count + 1" index, this index store offset of the
         * end of the last row. it's used to calculate the length of the 
         * last row. 
         */
        ret = encode_i32(row_index_buf_cur_, 
                         get_row_index_remain(),
                         index_pos, row_offset);
        if (OB_SUCCESS == ret)
        {
          row_index_buf_cur_ += index_pos;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialize 'row_count + 1' row index");
        }
      }

      if (OB_SUCCESS == ret)
      {
        // serialize block header
        block_header_.row_index_array_offset_ = get_block_data_size();
      }

      /**
       * store the block header into the reserved space at the
       * beginning of block buffer.
       */
      if (OB_SUCCESS == ret
          && OB_SUCCESS == block_header_.serialize(block_buf_, 
                           block_header_.get_serialize_size(), block_pos))
      {
        //copy the block index just behind block data
        memcpy(block_buf_cur_, row_index_buf_, get_row_index_size());
        block_buf_cur_ += get_row_index_size();
      }
      else 
      {
        TBSYS_LOG(WARN, "failed to serialize block header");
        ret = OB_ERROR;
      }
    
      return ret;
    }

    const uint64_t ObSSTableBlockBuilder::get_table_id() const
    {
      return block_header_.table_id_;
    }

    const uint64_t ObSSTableBlockBuilder::get_column_group_id() const
    {
      return block_header_.column_group_id_;
    }

    int ObSSTableBlockBuilder::set_table_id(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id=%lu", table_id);
        ret = OB_ERROR;
      }
      else
      {
        block_header_.table_id_ = table_id & TABLE_ID_MASK;
      }
      return ret;
    }

    int ObSSTableBlockBuilder::set_column_group_id(const uint64_t column_group_id)
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid argument column_group_id=%lu", column_group_id);
        ret = OB_ERROR;
      }
      else
      {
        block_header_.column_group_id_ = column_group_id & COLUMN_GROUP_ID_MASK;
      }
      return ret;
    }
  } // end namespace sstable
} // end namespace oceanbase
