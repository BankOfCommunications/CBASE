/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_trailer.cpp for define sstable trailer. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tbsys.h>
#include "common/utility.h"
#include "common/ob_rowkey_helper.h"
#include "common/page_arena.h"
#include "common/ob_schema.h"
#include "ob_sstable_trailer.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace tbsys;
    using namespace common;
    using namespace common::serialization;

    ObSSTableTrailer::ObSSTableTrailer() 
    : start_key_buf_(DEFAULT_KEY_BUF_SIZE), end_key_buf_(DEFAULT_KEY_BUF_SIZE)
    {
      reset();
    }

    ObSSTableTrailer::~ObSSTableTrailer()
    {

    }

    DEFINE_SERIALIZE(ObTrailerOffset)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size + pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, trailer_record_offset_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie trailer offset, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTrailerOffset)
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

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &trailer_record_offset_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie trailer offset, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTrailerOffset)
    {
      return encoded_length_i64(trailer_record_offset_);
    }

    const int64_t ObSSTableTrailer::get_range_record_size() const
    {
      return range_record_size_;
    }

    int ObSSTableTrailer::set_range_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        range_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }

    const int64_t ObSSTableTrailer::get_range_record_offset() const
    {
      return range_record_offset_;
    }

    int ObSSTableTrailer::set_range_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        range_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }

    const ObNewRange& ObSSTableTrailer::get_range() const
    {
      return range_;
    }

    int ObSSTableTrailer::copy_range(const ObNewRange& range)
    {
      int ret = OB_SUCCESS;
      ObMemBufAllocatorWrapper start_key_allocator(start_key_buf_);
      ObMemBufAllocatorWrapper end_key_allocator(end_key_buf_);

      if (OB_SUCCESS == (ret = range.start_key_.deep_copy(range_.start_key_, start_key_allocator))
          && OB_SUCCESS == (ret = range.end_key_.deep_copy(range_.end_key_, end_key_allocator)))
      {
        range_.table_id_ = range.table_id_;
        range_.border_flag_ = range.border_flag_;
        if (range_.end_key_.is_max_row())
        {
          range_.border_flag_.unset_inclusive_end();
        }

        // min,max is stored in start_key_ and end_key_ already
        // this field of range_.border_flag_ should not be set
        range_.border_flag_.unset_min_value();
        range_.border_flag_.unset_max_value();
      }

      return ret;
    }

    int ObSSTableTrailer::set_range(const ObNewRange& range)
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == range.table_id_)
      {
        TBSYS_LOG(WARN, "table id[%ld] should not be OB_INVALID_ID", range.table_id_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        range_ = range;
      }
      return ret;
    }

    //serialize ObSSTableTrailer only support V0.2.0
    DEFINE_SERIALIZE(ObSSTableTrailer)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();
      size_ = static_cast<int32_t>(serialize_size);

      if (NULL == buf || serialize_size + pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }
      
      //skip key stream offset && record size in version0.2.0
      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, size_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, trailer_version_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, table_version_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, first_block_data_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_count_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_index_record_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_index_record_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, bloom_filter_hash_count_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, bloom_filter_record_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, bloom_filter_record_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, schema_record_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, schema_record_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, row_count_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, sstable_checksum_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, first_table_id_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, frozen_time_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, range_record_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, range_record_size_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie the fourth part of trailer, buf=%p, "
                  "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < RESERVED_LEN && OB_SUCCESS == ret; ++i)
        {
          if (OB_SUCCESS != (ret = encode_i64(buf, buf_len, pos, reserved64_[i])))
          {
            TBSYS_LOG(WARN, "failed to serialzie trailer reserved, buf=%p, "
                            "buf_len=%ld, pos=%ld, index=%ld", 
                      buf, buf_len, pos, i);
            break;
          }
        }

        if ((OB_SUCCESS == ret ) && (buf_len >= OB_MAX_COMPRESSOR_NAME_LENGTH + pos))
        {
          memcpy(buf + pos, compressor_name_, OB_MAX_COMPRESSOR_NAME_LENGTH);
          pos += OB_MAX_COMPRESSOR_NAME_LENGTH;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialzie trailer comprssor name, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, buf_len, pos);
          ret = OB_ERROR;
        }

        if ((OB_SUCCESS == ret) 
            && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, row_value_store_style_)))
            && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved_))))
        {
          //do nothing here
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialzie the fourth part of trailer, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        }
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableTrailer)
    {
      int ret            = OB_SUCCESS;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld", 
                  buf, data_len, pos);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &size_))))
      {
        if (size_ > data_len)
        {
          TBSYS_LOG(WARN, "data length is less than expected, buf=%p, "
                          "data_len=%ld, pos=%ld, expected_len=%d", 
                    buf, data_len, pos, size_);
          ret = OB_ERROR;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie trailer size, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }

      if ((OB_SUCCESS == ret) 
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &trailer_version_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &table_version_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &first_block_data_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_count_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_index_record_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_index_record_size_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &bloom_filter_hash_count_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &bloom_filter_record_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &bloom_filter_record_size_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &schema_record_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &schema_record_size_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_size_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &row_count_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, 
                                              reinterpret_cast<int64_t*> (&sstable_checksum_))))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, 
                                              reinterpret_cast<int64_t*> (&first_table_id_))))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &frozen_time_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &range_record_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &range_record_size_))))
      {
        for (int64_t i = 0; i < RESERVED_LEN && OB_SUCCESS == ret; ++i)
        {
          if (OB_SUCCESS != (ret = decode_i64(buf, data_len, pos, &reserved64_[i])))
          {
            TBSYS_LOG(WARN, "failed to deserialzie trailer reserved, buf=%p, "
                            "buf_len=%ld, pos=%ld, index=%ld", 
                      buf, data_len, pos, i);
            break;
          }
        }
        if ((OB_SUCCESS == ret ) && (data_len >= OB_MAX_COMPRESSOR_NAME_LENGTH + pos))
        {
          memcpy(compressor_name_, buf + pos, OB_MAX_COMPRESSOR_NAME_LENGTH);
          pos += OB_MAX_COMPRESSOR_NAME_LENGTH;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialzie trailer compressor name, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, data_len, pos);
          ret = OB_ERROR;
        }

        if ((OB_SUCCESS == ret) 
            && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &row_value_store_style_)))
            && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved_))))
        {
          //do nothing here
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialzie the fourth part of trailer, buf=%p, "
                        "buf_len=%ld, pos=%ld", 
                        buf, data_len, pos);
            }
          }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie first part of trailer, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }   

      return ret;
    }

    //when you serialize and  deserialize, you should first give TAILER_LEN 
    //length memory for destination or source
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableTrailer)
    {
      int64_t total_len = 0;

      total_len += (encoded_length_i32(size_) 
                    + encoded_length_i32(trailer_version_)
                    + encoded_length_i64(table_version_)
                    + encoded_length_i64(first_block_data_offset_) 
                    + encoded_length_i64(block_count_)
                    + encoded_length_i64(block_index_record_offset_) 
                    + encoded_length_i64(block_index_record_size_)
                    + encoded_length_i64(bloom_filter_hash_count_)
                    + encoded_length_i64(bloom_filter_record_offset_) 
                    + encoded_length_i64(bloom_filter_record_size_)
                    + encoded_length_i64(schema_record_offset_) 
                    + encoded_length_i64(schema_record_size_)
                    + encoded_length_i64(block_size_)
                    + encoded_length_i64(row_count_)
                    + encoded_length_i64(sstable_checksum_)
                    + encoded_length_i64(first_table_id_)
                    + encoded_length_i64(frozen_time_)
                    + encoded_length_i64(range_record_offset_)
                    + encoded_length_i64(range_record_size_));

      for (int64_t i = 0; i < RESERVED_LEN; ++i)
      {
        total_len += encoded_length_i64(reserved64_[i]);
      }

      total_len += OB_MAX_COMPRESSOR_NAME_LENGTH;
      total_len += (encoded_length_i16(row_value_store_style_) 
                    + encoded_length_i16(reserved_));
      
      return total_len;
    }

    const int32_t ObSSTableTrailer::get_size() const
    {
      return size_;
    }
    
    const int32_t ObSSTableTrailer::get_trailer_version() const
    {
      return trailer_version_;
    }
         
    int ObSSTableTrailer::set_trailer_version(const int32_t version)
    {
      int ret = OB_SUCCESS;

      if (version >= 0 )
      {
        trailer_version_ = version;
      }
      else
      {
         ret = OB_ERROR;
      }

      return ret;
    }

    const int64_t ObSSTableTrailer::get_table_version() const
    {
      return table_version_;
    }
         
    int ObSSTableTrailer::set_table_version(const int64_t version)
    {
      int ret = OB_SUCCESS;

      if (version >= 0 )
      {
        table_version_ = version;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, version=%ld", version);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_count() const
    {
      return block_count_;
    }
         
    int ObSSTableTrailer::set_block_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count >= 0 )
      {
        block_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, block_count=%ld", count);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_index_record_offset() const
    {
      return block_index_record_offset_;
    }
         
    int ObSSTableTrailer::set_block_index_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        block_index_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_index_record_size() const
    {
      return block_index_record_size_;
    }
         
    int ObSSTableTrailer::set_block_index_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        block_index_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_bloom_filter_hash_count() const
    {
      return bloom_filter_hash_count_;
    }
         
    int ObSSTableTrailer::set_bloom_filter_hash_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count > 0 )
      {
        bloom_filter_hash_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, hash_count=%ld", count);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_bloom_filter_record_offset() const
    {
      return bloom_filter_record_offset_;
    }
         
    int ObSSTableTrailer::set_bloom_filter_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        bloom_filter_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_bloom_filter_record_size() const
    {
      return bloom_filter_record_size_;
    }
         
    int ObSSTableTrailer::set_bloom_filter_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        bloom_filter_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_schema_record_offset() const
    {
      return schema_record_offset_;
    }
         
    int ObSSTableTrailer::set_schema_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset >= 0 )
      {
        schema_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_schema_record_size() const
    {
      return schema_record_size_;
    }
         
    int ObSSTableTrailer::set_schema_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        schema_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_size() const
    {
      return block_size_;
    }
         
    int ObSSTableTrailer::set_block_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        block_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, block_size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_row_count() const
    {
      return row_count_;
    }
         
    int ObSSTableTrailer::set_row_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count >= 0)
      {
        row_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, row_count=%ld", count);
        ret = OB_ERROR;
      }

      return ret;
    }

    const uint64_t ObSSTableTrailer::get_sstable_checksum() const
    {
      return sstable_checksum_;
    }

    void ObSSTableTrailer::set_sstable_checksum(uint64_t sstable_checksum)
    {
      sstable_checksum_ = sstable_checksum;
    }
    
    const char* ObSSTableTrailer::get_compressor_name() const
    {
      return compressor_name_;
    }
    
    int ObSSTableTrailer::set_compressor_name(const char* name)
    { 
      int ret = OB_SUCCESS;
      int len = static_cast<int>(strlen(name));

      if ((NULL != name) && (len <= OB_MAX_COMPRESSOR_NAME_LENGTH))
      {
         memcpy(compressor_name_, name, strlen(name));
      }
      else 
      {
        TBSYS_LOG(WARN, "compessor name length is too big, name_length=%d,"
                        "name_buf_length=%ld", len, OB_MAX_COMPRESSOR_NAME_LENGTH);
        ret = OB_ERROR; 
      }

      return ret;
    }
    
    const int16_t ObSSTableTrailer::get_row_value_store_style() const
    {
      return row_value_store_style_;
    }
         
    int ObSSTableTrailer::set_row_value_store_style(const int16_t style)
    {
      int ret = OB_SUCCESS;

      if (style >= OB_SSTABLE_STORE_DENSE && style <= OB_SSTABLE_STORE_MIXED)
      {
        row_value_store_style_ = style;
      }
      else
      {
        TBSYS_LOG(WARN, "unknow store style, style=%d", style);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_first_block_data_offset() const
    {
      return first_block_data_offset_;
    }
         
    int ObSSTableTrailer::set_first_block_data_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset >= 0 )
      {
        first_block_data_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }

    const uint64_t ObSSTableTrailer::get_first_table_id() const
    {
      return first_table_id_;
    }

    int ObSSTableTrailer::set_first_table_id(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%ld", table_id);
        ret = OB_ERROR;
      }
      else
      {
        first_table_id_ = table_id;
        range_.table_id_ = table_id;
      }

      return ret;
    }

    const int64_t ObSSTableTrailer::get_frozen_time() const
    {
      return frozen_time_;
    }

    int ObSSTableTrailer::set_frozen_time(const int64_t frozen_time)
    {
      int ret = OB_SUCCESS;

      if (frozen_time < 0)
      {
        TBSYS_LOG(WARN, "invalid param, frozen_time=%ld", frozen_time);
        ret = OB_ERROR;
      }
      else
      {
        frozen_time_ = frozen_time;
      }

      return ret;
    }

    void ObSSTableTrailer::reset()
    {
      size_ = 0;
      trailer_version_ = 0;
      table_version_ = 0;
      first_block_data_offset_ = 0;
      block_count_ = 0;
      block_index_record_offset_ = 0;
      block_index_record_size_ = 0;
      bloom_filter_hash_count_ = 0;
      bloom_filter_record_offset_ = 0;
      bloom_filter_record_size_ = 0;
      schema_record_offset_ = 0;
      schema_record_size_ = 0;
      block_size_ = 0;
      row_count_ = 0;
      sstable_checksum_ = 0;
      first_table_id_ = 0;
      frozen_time_ = 0;
      range_record_offset_ = 0;
      range_record_size_ = 0;
      memset(reserved64_, 0, sizeof(int64_t) * RESERVED_LEN);
      memset(compressor_name_, 0, OB_MAX_COMPRESSOR_NAME_LENGTH);
      row_value_store_style_ = 0;
      reserved_ = 0;

      //default range is (MIN,MAX)
      range_.reset();
      range_.set_whole_range();
    }

    bool ObSSTableTrailer::is_valid()
    {
      bool ret  = false;
      int64_t i = 0;

      if (size_ <= 0)
      {
        TBSYS_LOG(WARN, "size error, size_=%d", size_);
      }
      else if (trailer_version_ < 0)
      {
        TBSYS_LOG(WARN, "trailer version error, trailer_version_=%d", trailer_version_);
      }
      else if (table_version_ < 0)
      {
        TBSYS_LOG(WARN, "table version error, table_version_=%ld", table_version_);
      }
      else if (first_block_data_offset_ != 0)
      {
        TBSYS_LOG(WARN, "first block data offset error, first_block_data_offset_=%ld", 
                  first_block_data_offset_);
      }
      else if (block_count_ < 0 )
      {
        TBSYS_LOG(WARN, "block_count_ should be greater than 0, block_count_=%ld",block_count_);
      }
      else if (block_index_record_offset_ < 0)
      {
        TBSYS_LOG(WARN, "block_index_record_offset_ should be greater than 0 [real:%ld]", 
                  block_index_record_offset_);
      }
      else if (block_index_record_size_ < 0)
      {
        TBSYS_LOG(WARN, "block_index_record_size_ should be greater than 0 [real:%ld]", 
                  block_index_record_size_);
      }
      else if (bloom_filter_hash_count_ < 0)
      {
        TBSYS_LOG(WARN, "bloom_filter_hash_count_ should be greater than 0 [real:%ld]", 
                  bloom_filter_hash_count_);
      }
      else if (bloom_filter_record_offset_ < 0)
      {
        TBSYS_LOG(WARN, "bloom_filter_record_offset_ should be greater than 0 [real:%ld]", 
                  bloom_filter_record_offset_);
      }
      else if (bloom_filter_record_size_ < 0)
      {
        TBSYS_LOG(WARN, "bloom_filter_record_size_ should be greater than 0 [real:%ld]", 
                  bloom_filter_record_size_);
      }
      else if (schema_record_offset_ < 0)
      {
        TBSYS_LOG(WARN, "schema_record_offset_ should be greater than 0 [real:%ld]", 
                  schema_record_offset_);
      }
      else if (schema_record_size_ < 0)
      {
        TBSYS_LOG(WARN, "schema_record_size_ should be greater than 0 [real:%ld]", 
                  schema_record_size_);
      }
      else if (block_size_ <= 0)
      {
        TBSYS_LOG(WARN, "blocksize_ should be greater than 0 [real:%ld]", 
                  block_size_);
      }
      else if (row_count_ < 0)
      {
        TBSYS_LOG(WARN, "row_count_ should be greater than 0 [real:%ld]", 
                  row_count_);
      }
      else if (OB_INVALID_ID == first_table_id_)
      {
        TBSYS_LOG(WARN, "first_table_id_ is invalid [real:%ld]", 
                  first_table_id_);
      }
      else if (frozen_time_ < 0)
      {
        TBSYS_LOG(WARN, "frozen_time_ should be greater than or equal to 0 [real:%ld]", 
                  frozen_time_);
      }
      else if (strlen(compressor_name_) == 0)
      {
        TBSYS_LOG(WARN, "compressor_name_ is NULL, length=%lu", strlen(compressor_name_));
      }
      else if (OB_SSTABLE_STORE_DENSE != row_value_store_style_ 
               && OB_SSTABLE_STORE_SPARSE != row_value_store_style_)
      {
        TBSYS_LOG(WARN, "row_value_store_style_ error [real:%hd,exp:%hd]", 
                  row_value_store_style_, OB_SSTABLE_STORE_DENSE);
      }
      else if (reserved_ != 0)
      {
        TBSYS_LOG(WARN, "reserved_ should be 0 [real:%d]", reserved_);
      }
      else
      {
        for (i = 0; i < RESERVED_LEN; ++i)
        {
          if (reserved64_[i] != 0)
          {
            TBSYS_LOG(WARN, "reserved64_ should be 0 [real:%ld,index:%ld]", reserved64_[i], i);
            break;
          }
        }
        if (RESERVED_LEN == i)
        {
          ret = true;
        }
      }

      return ret;
    }

    void ObSSTableTrailer::dump() const
    {
      TBSYS_LOG(WARN, "size_: %d \n"
                      "trailer_version_: %d \n"
                      "table_version_: %ld \n"
                      "first_block_data_offset_: %ld \n"
                      "block_count_: %ld \n"
                      "block_index_record_offset_: %ld \n"
                      "block_index_record_size_: %ld \n"
                      "bloom_filter_hash_count_: %ld \n"
                      "bloom_filter_record_offset_: %ld \n"
                      "bloom_filter_record_size_: %ld \n"
                      "schema_record_offset_: %ld \n"
                      "schema_record_size_: %ld \n"
                      "block_size_: %ld \n"
                      "row_count_: %ld \n"
                      "sstable_checksum_: %lu \n"
                      "first_table_id_: %lu \n"
                      "frozen_time_: %ld \n"
                      "range_record_offset_: %ld \n"
                      "range_record_size_: %ld \n"
                      "compressor_name_: %s \n"
                      "row_value_store_style_: %s \n"
                      "range: %s \n",
              size_, 
              trailer_version_, 
              table_version_,
              first_block_data_offset_, 
              block_count_,
              block_index_record_offset_, 
              block_index_record_size_,
              bloom_filter_hash_count_, 
              bloom_filter_record_offset_,
              bloom_filter_record_size_,
              schema_record_offset_, 
              schema_record_size_,
              block_size_, 
              row_count_, 
              sstable_checksum_, 
              first_table_id_,
              frozen_time_,
              range_record_offset_,
              range_record_size_,
              compressor_name_,
              row_value_store_style_ == OB_SSTABLE_STORE_DENSE ? "dense" : "sparse",
              to_cstring(range_));
    }
  } // end namespace sstable
} // end namespace oceanbase
