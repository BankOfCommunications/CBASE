/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_sstable_block_index_builder.cpp for persistent ssatable 
 * index and store index. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "common/serialization.h"
#include "ob_sstable_block_index_builder.h"
#include "ob_sstable_block_index_buffer.h"

namespace oceanbase 
{ 
  namespace sstable 
  {
    using namespace common;
    using namespace common::serialization;

    ObSSTableBlockIndexBuilder::ObSSTableBlockIndexBuilder() 
    { 
      memset(&index_block_header_, 0, sizeof(ObSSTableBlockIndexHeader));
    }

    ObSSTableBlockIndexBuilder::~ObSSTableBlockIndexBuilder() 
    { 
      index_items_buf_.clear();
      end_keys_buf_.clear();
    }
  
    const int64_t ObSSTableBlockIndexBuilder:: get_block_count() const
    {
      return index_block_header_.sstable_block_count_;
    }

    const int64_t ObSSTableBlockIndexBuilder::get_end_key_char_stream_offset() const
    {
      return index_block_header_.end_key_char_stream_offset_;
    }

    const int64_t ObSSTableBlockIndexBuilder::get_index_block_size() const
    {
      return (index_block_header_.get_serialize_size() 
              + index_items_buf_.get_data_size() 
              + end_keys_buf_.get_data_size());
    }

    const int64_t ObSSTableBlockIndexBuilder::get_index_items_size() const
    {
      return index_items_buf_.get_data_size();
    }

    const int64_t ObSSTableBlockIndexBuilder::get_end_keys_size() const
    {
      return end_keys_buf_.get_data_size();
    }

    const ObSSTableBlockIndexBuffer* ObSSTableBlockIndexBuilder::get_index_items_buffer() const
    {
      return &index_items_buf_;
    }

    const ObSSTableBlockIndexBuffer* ObSSTableBlockIndexBuilder::get_end_keys_buffer() const
    {
      return &end_keys_buf_;
    }

    void ObSSTableBlockIndexBuilder::reset()
    {
      index_items_buf_.reset();
      end_keys_buf_.reset();
      memset(&index_block_header_, 0, sizeof(ObSSTableBlockIndexHeader));
    }

    DEFINE_SERIALIZE(ObSSTableBlockIndexHeader)
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
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, sstable_block_count_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, end_key_char_stream_offset_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, rowkey_flag_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved16_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, reserved64_[0])))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, reserved64_[1]))))
      { 
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie block index header, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      return ret;
    }
    
    DEFINE_DESERIALIZE(ObSSTableBlockIndexHeader)
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
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &sstable_block_count_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &end_key_char_stream_offset_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &rowkey_flag_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved16_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &reserved64_[0])))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &reserved64_[1]))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie block index header, buf=%p, "
                        "data_len=%ld, pos=%ld", buf, data_len, pos);
      }        
    
      return ret;
    }
    
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableBlockIndexHeader)
    {
      return (encoded_length_i64(sstable_block_count_) 
              + encoded_length_i32(end_key_char_stream_offset_) 
              + encoded_length_i16(rowkey_flag_)
              + encoded_length_i16(reserved16_)
              + encoded_length_i64(reserved64_[0])
              + encoded_length_i64(reserved64_[1]));
    }
    
    //TODO remove this function after modify 
    int ObSSTableBlockIndexBuilder::init()
    {
      return OB_SUCCESS;
    }

    int ObSSTableBlockIndexBuilder::add_entry(const uint64_t table_id,
                                              const uint64_t column_group_id,
                                              const ObRowkey &key, 
                                              const int32_t record_size) 
    {
      int ret     = OB_SUCCESS;
      ObSSTableBlockIndexItem index_item;

      if (record_size < 0 || key.get_obj_cnt() <= 0 || NULL == key.get_obj_ptr()
          || table_id == OB_INVALID_ID || table_id == 0 || OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, key_len=%ld,"
                        "key_ptr=%p, record_size=%d, column_group_id=%lu", 
                  table_id, key.get_obj_cnt(), key.get_obj_ptr(), record_size, column_group_id);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        index_item.rowkey_column_count_ = static_cast<int16_t>(key.get_obj_cnt());
        index_item.column_group_id_ = static_cast<uint16_t>(column_group_id);
        index_item.table_id_ = static_cast<uint32_t>(table_id);
        index_item.block_record_size_ = record_size;
        index_item.block_end_key_size_ = static_cast<int16_t>(key.get_serialize_objs_size());
        index_item.reserved_ = 0; 
      
        ret = index_items_buf_.add_index_item(index_item);
        if (OB_SUCCESS == ret) 
        {
          ret = end_keys_buf_.add_key(key);
          if (OB_ERROR == ret)
          {
            TBSYS_LOG(WARN, "failed to add end key");
          }
          else
          {
            index_block_header_.sstable_block_count_++;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to add index item");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObSSTableBlockIndexBuilder::add_entry(const uint64_t table_id,
                                              const uint64_t column_group_id,
                                              const ObString &key, 
                                              const int32_t record_size) 
    {
      int ret     = OB_SUCCESS;
      ObSSTableBlockIndexItem index_item;

      if (record_size < 0 || key.length() <= 0 || NULL == key.ptr()
          || table_id == OB_INVALID_ID || table_id == 0 || OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, key_len=%d,"
                        "key_ptr=%p, record_size=%d, column_group_id=%lu", 
                  table_id, key.length(), key.ptr(), record_size, column_group_id);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        index_item.rowkey_column_count_ = 0;
        index_item.column_group_id_ = static_cast<uint16_t>(column_group_id);
        index_item.table_id_ = static_cast<uint32_t>(table_id);
        index_item.block_record_size_ = record_size;
        index_item.block_end_key_size_ = static_cast<int16_t>(key.length());
        index_item.reserved_ = 0; 
      
        ret = index_items_buf_.add_index_item(index_item);
        if (OB_SUCCESS == ret) 
        {
          ret = end_keys_buf_.add_key(key);
          if (OB_ERROR == ret)
          {
            TBSYS_LOG(WARN, "failed to add end key");
          }
          else
          {
            index_block_header_.sstable_block_count_++;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to add index item");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObSSTableBlockIndexBuilder::build_block_index(const bool use_binary_rowkey, 
        char* index_block, const int64_t buffer_size, int64_t& index_size)
    {
      int ret                   = OB_SUCCESS;
      int64_t index_block_size  = get_index_block_size();
      int64_t index_items_size  = get_index_items_size();
      int64_t header_size       = index_block_header_.get_serialize_size();
      int64_t pos               = 0;

      if (NULL == index_block)
      {
        TBSYS_LOG(WARN, "invalid param, index_block=%p", index_block); 
        ret = OB_ERROR;
      }
      else if (index_block_size == header_size)
      {
        //no data in index block
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        index_block_header_.end_key_char_stream_offset_ 
            = static_cast<int32_t>(header_size + index_items_size);
        // new rowkey obj array format, force set to 1.
        index_block_header_.rowkey_flag_ = use_binary_rowkey ? 0 : 1;
        if (OB_SUCCESS == index_block_header_.serialize(index_block,
                                                        header_size, pos))
        {
          char* ptr = index_block + pos;
          ret = index_items_buf_.get_data(ptr, buffer_size - header_size);
          if (OB_SUCCESS == ret)
          {
            ptr += index_items_size;
            ret = end_keys_buf_.get_data(ptr, buffer_size - header_size - index_items_size);
            if (OB_SUCCESS == ret)
            {
              index_size = index_block_size;
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialize index block header");
          ret = OB_ERROR;
        }
      }
      
      return ret;
    }
  } // end namespace sstable
} // end namespace oceanbase
