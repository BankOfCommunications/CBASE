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
#include "ob_sstable_trailerV1.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace common;
    using namespace common::serialization;

    ObSSTableTrailerV1::ObSSTableTrailerV1()
    {
      memset(this, 0, sizeof(ObSSTableTrailerV1));
      key_buf_size_ = DEFAULT_KEY_BUF_SIZE;
      own_key_buf_ = false;
    }

    ObSSTableTrailerV1::~ObSSTableTrailerV1()
    {
      if (NULL != table_info_)
      {
        ob_free(table_info_);
        table_info_ = NULL;
      }
      if (own_key_buf_ && NULL != key_buf_)
      {
        ob_free(key_buf_);
        key_buf_ = NULL;
      }
    }

    DEFINE_SERIALIZE(ObTrailerOffsetV1)
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

    DEFINE_DESERIALIZE(ObTrailerOffsetV1)
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

    DEFINE_GET_SERIALIZE_SIZE(ObTrailerOffsetV1)
    {
      return encoded_length_i64(trailer_record_offset_);
    }

    DEFINE_SERIALIZE(ObTableTrailerInfoV1)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, table_id_))
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, column_count_))
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, start_row_key_length_))
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, end_row_key_length_))
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, reversed_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie trailer table info, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTableTrailerInfoV1)
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
          && (OB_SUCCESS == decode_i64(buf, data_len, pos,
                                       reinterpret_cast<int64_t*>(&table_id_)))
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &column_count_))
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &start_row_key_length_))
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &end_row_key_length_))
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &reversed_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie trailer table info, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTableTrailerInfoV1)
    {
      return(encoded_length_i64(table_id_)
             + encoded_length_i16(column_count_)
             + encoded_length_i16(start_row_key_length_)
             + encoded_length_i16(end_row_key_length_)
             + encoded_length_i16(reversed_));
    }

    DEFINE_SERIALIZE(ObSSTableTrailerV1)
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

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, size_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, trailer_version_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, table_version_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, first_block_data_offset_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, block_count_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, block_index_record_offset_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, block_index_record_size_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, bloom_filter_hash_count_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, bloom_filter_record_offset_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, bloom_filter_record_size_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, schema_record_offset_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, schema_record_size_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, key_stream_record_offset_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, key_stream_record_size_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, block_size_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, row_count_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, sstable_checksum_)))
      {
        ret = OB_SUCCESS;
        for (int64_t i = 0; i < RESERVED_LEN; ++i)
        {
          if (OB_SUCCESS != encode_i64(buf, buf_len, pos, reserved64_[i]))
          {
            TBSYS_LOG(WARN, "failed to serialzie trailer reserved, buf=%p, "
                            "buf_len=%ld, pos=%ld, index=%ld",
                      buf, buf_len, pos, i);
            ret = OB_ERROR;
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
            && (OB_SUCCESS == encode_i16(buf, buf_len, pos, row_value_store_style_))
            && (OB_SUCCESS == encode_i16(buf, buf_len, pos, reserved_))
            && (OB_SUCCESS == encode_i32(buf, buf_len, pos, table_count_)))
        {
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialzie the fourth part of trailer, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, buf_len, pos);
          ret = OB_ERROR;
        }

        for (int64_t i = 0; i < table_count_ && OB_SUCCESS == ret; ++i)
        {
          ret = table_info_[i].serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to serialzie trailer table info, buf=%p, "
                            "buf_len=%ld, pos=%ld", buf, buf_len, pos);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie first part of trailer, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableTrailerV1)
    {
      int ret            = OB_SUCCESS;
      int32_t key_offset = 0;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &size_)))
      {
        ret = OB_SUCCESS;
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
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &trailer_version_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &table_version_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &first_block_data_offset_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &block_count_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &block_index_record_offset_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &block_index_record_size_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &bloom_filter_hash_count_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &bloom_filter_record_offset_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &bloom_filter_record_size_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &schema_record_offset_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &schema_record_size_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &key_stream_record_offset_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &key_stream_record_size_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &block_size_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &row_count_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos,
                                       reinterpret_cast<int64_t*> (&sstable_checksum_))))
      {
        for (int64_t i = 0; i < RESERVED_LEN; ++i)
        {
          if (OB_SUCCESS != decode_i64(buf, data_len, pos, &reserved64_[i]))
          {
            TBSYS_LOG(WARN, "failed to deserialzie trailer reserved, buf=%p, "
                            "buf_len=%ld, pos=%ld, index=%ld",
                      buf, data_len, pos, i);
            ret = OB_ERROR;
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
            && (OB_SUCCESS == decode_i16(buf, data_len, pos, &row_value_store_style_))
            && (OB_SUCCESS == decode_i16(buf, data_len, pos, &reserved_))
            && (OB_SUCCESS == decode_i32(buf, data_len, pos, &table_count_)))
        {
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialzie the fourth part of trailer, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, data_len, pos);
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret && table_count_ > 0)
        {
          ret = init_table_info(table_count_);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to init table info");
          }
        }

        for (int64_t i = 0; i < table_count_ && OB_SUCCESS == ret; ++i)
        {
          ret = table_info_[i].deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
            table_info_[i].start_row_key_offset_ = key_offset;
            key_offset += table_info_[i].start_row_key_length_;
            table_info_[i].end_row_key_offset_ = key_offset;
            key_offset += table_info_[i].end_row_key_length_;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to deserialzie trailer table info, buf=%p, "
                            "buf_len=%ld, pos=%ld, index=%ld",
                      buf, data_len, pos, i);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie first part of trailer, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    //when you serialize and  deserialize, you should first give TAILER_LEN
    //length memory for destination or source
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableTrailerV1)
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
                    + encoded_length_i64(key_stream_record_offset_)
                    + encoded_length_i64(key_stream_record_size_)
                    + encoded_length_i64(block_size_)
                    + encoded_length_i64(row_count_)
                    + encoded_length_i64(sstable_checksum_));

      for (int64_t i = 0; i < RESERVED_LEN; ++i)
      {
        total_len += encoded_length_i64(reserved64_[i]);
      }

      total_len += OB_MAX_COMPRESSOR_NAME_LENGTH;
      total_len += (encoded_length_i16(row_value_store_style_)
                    + encoded_length_i16(reserved_)
                    + encoded_length_i32(table_count_));
      total_len += table_info_[0].get_serialize_size() * table_count_;

      return total_len;
    }

    int32_t ObSSTableTrailerV1::get_size() const
    {
      return size_;
    }

    int32_t ObSSTableTrailerV1::get_trailer_version() const
    {
      return trailer_version_;
    }

    int ObSSTableTrailerV1::set_trailer_version(const int32_t version)
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

    int64_t ObSSTableTrailerV1::get_table_version() const
    {
      return table_version_;
    }

    int ObSSTableTrailerV1::set_table_version(const int64_t version)
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

    int64_t ObSSTableTrailerV1::get_block_count() const
    {
      return block_count_;
    }

    int ObSSTableTrailerV1::set_block_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count > 0 )
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

    int64_t ObSSTableTrailerV1::get_block_index_record_offset() const
    {
      return block_index_record_offset_;
    }

    int ObSSTableTrailerV1::set_block_index_record_offset(const int64_t offset)
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

    int64_t ObSSTableTrailerV1::get_block_index_record_size() const
    {
      return block_index_record_size_;
    }

    int ObSSTableTrailerV1::set_block_index_record_size(const int64_t size)
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

    int64_t ObSSTableTrailerV1::get_bloom_filter_hash_count() const
    {
      return bloom_filter_hash_count_;
    }

    int ObSSTableTrailerV1::set_bloom_filter_hash_count(const int64_t count)
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

    int64_t ObSSTableTrailerV1::get_bloom_filter_record_offset() const
    {
      return bloom_filter_record_offset_;
    }

    int ObSSTableTrailerV1::set_bloom_filter_record_offset(const int64_t offset)
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

    int64_t ObSSTableTrailerV1::get_bloom_filter_record_size() const
    {
      return bloom_filter_record_size_;
    }

    int ObSSTableTrailerV1::set_bloom_filter_record_size(const int64_t size)
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

    int64_t ObSSTableTrailerV1::get_schema_record_offset() const
    {
      return schema_record_offset_;
    }

    int ObSSTableTrailerV1::set_schema_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
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

    int64_t ObSSTableTrailerV1::get_schema_record_size() const
    {
      return schema_record_size_;
    }

    int ObSSTableTrailerV1::set_schema_record_size(const int64_t size)
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

    int64_t ObSSTableTrailerV1::get_key_stream_record_offset() const
    {
      return key_stream_record_offset_;
    }

    int ObSSTableTrailerV1::set_key_stream_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        key_stream_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }

    int64_t ObSSTableTrailerV1::get_key_stream_record_size() const
    {
      return key_stream_record_size_;
    }

    int ObSSTableTrailerV1::set_key_stream_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        key_stream_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }

    int64_t ObSSTableTrailerV1::get_block_size() const
    {
      return block_size_;
    }

    int ObSSTableTrailerV1::set_block_size(const int64_t size)
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

    uint64_t ObSSTableTrailerV1::get_table_id(const int64_t index) const
    {
      uint64_t table_id = OB_INVALID_ID;

      if (index >= 0 && index < table_count_)
      {
        table_id = table_info_[index].table_id_;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%ld, table_count=%d",
                  index, table_count_);
      }

      return table_id;
    }

    int ObSSTableTrailerV1::set_table_id(const int64_t index, const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      if (index >= 0 && index < table_count_ && NULL != table_info_
          && table_id != OB_INVALID_ID && table_id != 0)
      {
        if (index > 0 && table_info_[index - 1].table_id_ >= table_id)
        {
          TBSYS_LOG(WARN, "table id isn't in ascending ording, prev_table_id=%lu, "
                          "cur_table_id=%lu",
                    table_info_[index - 1].table_id_, table_id);
          ret = OB_ERROR;
        }
        else
        {
          table_info_[index].table_id_ = table_id;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%ld, table_id=%lu, table_count=%d",
                  index, table_id, table_count_);
        ret = OB_ERROR;
      }

      return ret;
    }

    uint64_t ObSSTableTrailerV1::get_column_count(const int64_t index) const
    {
      int64_t column_count = 0;

      if (index >= 0 && index < table_count_)
      {
        column_count = table_info_[index].column_count_;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%ld, table_count=%d",
                  index, table_count_);
      }

      return column_count;
    }

    int ObSSTableTrailerV1::set_column_count(const int64_t index,
                                           const int64_t column_count)
    {
      int ret = OB_SUCCESS;

      if (index >= 0 && index < table_count_ && NULL != table_info_)
      {
        table_info_[index].column_count_ = static_cast<int16_t>(column_count);
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%ld, column_count=%ld, "
                        "table_count=%d, table_info=%p",
                  index, column_count, table_count_, table_info_);
        ret = OB_ERROR;
      }

      return ret;
    }

    int32_t ObSSTableTrailerV1::get_table_count() const
    {
      return table_count_;
    }

    int ObSSTableTrailerV1::set_table_count(const int32_t count)
    {
      int ret = OB_SUCCESS;

      if (count > 0)
      {
        table_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, table_count=%d", count);
        ret = OB_ERROR;
      }

      return ret;
    }

    int64_t ObSSTableTrailerV1::get_row_count() const
    {
      return row_count_;
    }

    int ObSSTableTrailerV1::set_row_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count > 0)
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

    uint64_t ObSSTableTrailerV1::get_sstable_checksum() const
    {
      return sstable_checksum_;
    }

    void ObSSTableTrailerV1::set_sstable_checksum(uint64_t sstable_checksum)
    {
      sstable_checksum_ = sstable_checksum;
    }

    const char* ObSSTableTrailerV1::get_compressor_name() const
    {
      return compressor_name_;
    }

    int ObSSTableTrailerV1::set_compressor_name(const char* name)
    {
      int ret = OB_SUCCESS;
      int len = static_cast<int32_t>(strlen(name));

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

    int16_t ObSSTableTrailerV1::get_row_value_store_style() const
    {
      return row_value_store_style_;
    }

    int ObSSTableTrailerV1::set_row_value_store_style(const int16_t style)
    {
      int ret = OB_SUCCESS;

      if (style >= OB_SSTABLE_STORE_DENSEV1 && style <= OB_SSTABLE_STORE_MIXEDV1)
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

    int64_t ObSSTableTrailerV1::get_first_block_data_offset() const
    {
      return first_block_data_offset_;
    }

    int ObSSTableTrailerV1::set_first_block_data_offset(const int64_t offset)
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

    int ObSSTableTrailerV1::init_table_info(const int64_t table_count)
    {
      int ret = OB_SUCCESS;

      if (table_count < 0)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && NULL != table_info_)
      {
        TBSYS_LOG(WARN, "table info is inited, no need init again");
        ob_free(table_info_);
        table_info_ = NULL;
      }

      if (OB_SUCCESS == ret)
      {
        table_info_ = static_cast<ObTableTrailerInfoV1*>(
          ob_malloc(table_count * sizeof(ObTableTrailerInfoV1), ObModIds::TEST));
        if (NULL == table_info_)
        {
          TBSYS_LOG(ERROR, "Problem allocating memory for table trailer info");
          ret = OB_ERROR;
        }
        else
        {
          memset(table_info_, 0, table_count * sizeof(ObTableTrailerInfoV1));
        }
      }

      return ret;
    }

    int ObSSTableTrailerV1::ensure_key_buf_space(const int64_t size)
    {
      int ret             = OB_SUCCESS;
      char *new_buf       = NULL;
      int64_t reamin_size = key_buf_size_ - key_data_size_;
      int64_t key_buf_len = 0;

      if (size <= 0)
      {
        ret = OB_ERROR;
      }
      else if (NULL == key_buf_ || (NULL != key_buf_ && size > reamin_size))
      {
        key_buf_len = size > reamin_size
                      ? (key_buf_size_ * 2) : key_buf_size_;
        if (key_buf_len - key_data_size_ < size)
        {
          key_buf_len = key_data_size_ + size * 2;
        }
        new_buf = static_cast<char*>(ob_malloc(key_buf_len, ObModIds::TEST));
        if (NULL == new_buf)
        {
          TBSYS_LOG(ERROR, "Problem allocating memory for key buffer");
          ret = OB_ERROR;
        }
        else
        {
          memset(new_buf, 0, key_buf_len);
          if (NULL != key_buf_)
          {
            memcpy(new_buf, key_buf_, key_data_size_);
            if (own_key_buf_)
            {
              ob_free(key_buf_);
            }
            key_buf_ = NULL;
          }
          key_buf_size_ = key_buf_len;
          key_buf_ = new_buf;
          own_key_buf_ = true;
        }
      }

      return ret;
    }

    int ObSSTableTrailerV1::set_start_key(const int64_t table_index, ObString& start_key)
    {
      int ret   = OB_SUCCESS;
      int size  = start_key.length();
      char* ptr = start_key.ptr();

      if (table_index < 0 || table_index >= table_count_
          || size <= 0 || size > OB_MAX_ROW_KEY_LENGTH
          || NULL == table_info_ || NULL == ptr)
      {
        TBSYS_LOG(WARN, "invalid param, table_index=%ld, table_count=%d, "
                        "key_len=%d, key_ptr=%p",
                  table_index, table_count_, size, ptr);
        ret = OB_ERROR;
      }
      else
      {
        table_info_[table_index].start_row_key_length_ = static_cast<int16_t>(size);
        ret = ensure_key_buf_space(size);
        if (OB_SUCCESS == ret)
        {
          memcpy(key_buf_ + key_data_size_, ptr, size);
          key_data_size_ += size;
        }
        else
        {
          TBSYS_LOG(WARN, "no enough memory to store start key, key_len=%d, "
                          "key_ptr=%p", size, ptr);
        }
      }

      return ret;
    }

    int ObSSTableTrailerV1::set_end_key(const int64_t table_index, ObString& end_key)
    {
      int ret   = OB_SUCCESS;
      int size  = end_key.length();
      char* ptr = end_key.ptr();

      if (table_index < 0 || table_index >= table_count_
          || size <= 0 || size > OB_MAX_ROW_KEY_LENGTH
          || NULL == table_info_ || NULL == ptr)
      {
        TBSYS_LOG(WARN, "invalid param, table_index=%ld, table_count=%d, "
                        "key_len=%d, key_ptr=%p",
                  table_index, table_count_, size, ptr);
        ret = OB_ERROR;
      }
      else
      {
        table_info_[table_index].end_row_key_length_ = static_cast<int16_t>(size);
        ret = ensure_key_buf_space(size);
        if (OB_SUCCESS == ret)
        {
          memcpy(key_buf_ + key_data_size_, ptr, size);
          key_data_size_ += size;
        }
        else
        {
          TBSYS_LOG(WARN, "no enough memory to store end key, key_len=%d, "
                          "key_ptr=%p", size, ptr);
        }
      }

      return ret;
    }

    int64_t ObSSTableTrailerV1::find_table_id(const uint64_t table_id) const
    {
      int64_t ret     = -1;
      int64_t left    = 0;
      int64_t middle  = 0;
      int64_t right   = table_count_ - 1;

      if (NULL != table_info_ && table_count_ > 0)
      {
        while (left <= right)
        {
          middle = (left + right) / 2;
          if (table_info_[middle].table_id_ > table_id)
          {
            right = middle - 1;
          }
          else if (table_info_[middle].table_id_ < table_id)
          {
            left = middle + 1;
          }
          else
          {
            ret = middle;
            break;
          }
        }
      }

      return ret;
    }

    const ObString ObSSTableTrailerV1::get_start_key(const uint64_t table_id) const
    {
      ObString start_key;
      int64_t table_index = -1;

      if (table_id == OB_INVALID_ID || table_id == 0)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, table_count=%d, table_info=%p",
                  table_id, table_count_, table_info_);
      }
      else
      {
        table_index = find_table_id(table_id);
        if (table_index >= 0 && table_index < table_count_)
        {
          start_key.assign(key_buf_ + table_info_[table_index].start_row_key_offset_,
                           table_info_[table_index].start_row_key_length_);
        }
        else
        {
          TBSYS_LOG(WARN, "can't find table id in trailer, table_id=%lu, "
                          "table_count=%d, table_info=%p",
                    table_id, table_count_, table_info_);
        }
      }

      return start_key;
    }

    const ObString ObSSTableTrailerV1::get_end_key(const uint64_t table_id) const
    {
      ObString end_key;
      int64_t table_index = -1;

      if (table_id == OB_INVALID_ID || table_id == 0)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, table_count=%d, table_info=%p",
                  table_id, table_count_, table_info_);
      }
      else
      {
        table_index = find_table_id(table_id);
        if (table_index >= 0 && table_index < table_count_)
        {
          end_key.assign(key_buf_ + table_info_[table_index].end_row_key_offset_,
                         table_info_[table_index].end_row_key_length_);
        }
        else
        {
          TBSYS_LOG(WARN, "can't find table id in trailer, table_id=%lu, "
                          "table_count=%d, table_info=%p",
                    table_id, table_count_, table_info_);
        }
      }

      return end_key;
    }

    int ObSSTableTrailerV1::set_key_buf(ObString key_buf)
    {
      int ret         = OB_SUCCESS;
      int64_t len     = key_buf.length();
      const char* ptr = key_buf.ptr();

      if (NULL == ptr || len == 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, key bufer length=%ld, ptr=%p",
                  len, ptr);
        ret = OB_ERROR;
      }
      else
      {
        if (own_key_buf_ && NULL != key_buf_)
        {
          ob_free(key_buf_);
          key_buf_ = NULL;
        }
        key_buf_ = const_cast<char*>(ptr);
        key_buf_size_ = len;
        key_data_size_ = len;
        own_key_buf_ = false;
      }

      return ret;
    }

    ObString ObSSTableTrailerV1::get_key_buf() const
    {
      return ObString(static_cast<int32_t>(key_data_size_), static_cast<int32_t>(key_data_size_), key_buf_);
    }

    void ObSSTableTrailerV1::reset()
    {
      if (NULL != table_info_)
      {
        ob_free(table_info_);
        table_info_ = NULL;
      }
      if (own_key_buf_ && NULL != key_buf_)
      {
        ob_free(key_buf_);
        key_buf_ = NULL;
      }
      own_key_buf_ = false;
      memset(this, 0, sizeof(ObSSTableTrailerV1));
      key_buf_size_ = DEFAULT_KEY_BUF_SIZE;
    }
  } // end namespace sstable
} // end namespace oceanbase
