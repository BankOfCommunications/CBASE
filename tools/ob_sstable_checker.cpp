/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sstable_checker.cpp is for checking validity of a given sstable file
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_sstable_checker.h"
#include <arpa/inet.h>
#include <bits/endian.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/utility.h"
#include "tbsys.h"
#include "common/ob_crc64.h"
#include "sstable/ob_sstable_writer.h"

#undef OB_SSTABLE_CHECKER_DEBUG
namespace
{
  const int16_t AVERSION = 0;
  const int32_t TRAILER_VERSION3 = 0x300;

  uint64_t decode_uint64(const char* bufp)
  {
    uint64_t val = 0;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    val = ((uint64_t)((uint8_t)*bufp++) << 56);
    val |= ((uint64_t)((uint8_t)*bufp++) << 48);
    val |= ((uint64_t)((uint8_t)*bufp++) << 40);
    val |= ((uint64_t)((uint8_t)*bufp++) << 32);
    val |= ((uint64_t)((uint8_t)*bufp++) << 24);
    val |= ((uint64_t)((uint8_t)*bufp++) << 16);
    val |= ((uint64_t)((uint8_t)*bufp++) << 8);
    val |= (uint64_t)((uint8_t)*bufp++);
#else
    memcpy(&val, bufp, 8);
#endif
    return val;
  }

  uint32_t decode_uint32(const char* bufp)
  {
    uint32_t val;
    val = ntohl(*(reinterpret_cast<const uint32_t*>(bufp)));
    return val;
  }

  int8_t decode_int8(const char* bufp)
  {
    int8_t val;
    val = static_cast<int8_t>(*bufp & 0xf);
    return val;
  }

  int16_t decode_int16(const char* bufp)
  {
    int16_t val;
    val = static_cast<int16_t>((*bufp & 0xff) << 8);
    val = static_cast<int16_t>(val | (*(bufp + 1) & 0xff));
    //val = static_cast<short unsigned int>(ntohs(*(reinterpret_cast<const uint16_t*>(bufp))));
    return val;
  }

  int32_t decode_int32(const char* bufp)
  {
    int32_t val;
    val = ntohl(*(reinterpret_cast<const uint32_t*>(bufp)));
    return val;
  }

  void decode_int64_in_place(int64_t &value)
  {
    uint32_t *p_value = reinterpret_cast<uint32_t *>(&value);
    uint32_t tmp = 0;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    p_value[0] = ntohl(p_value[0]);
    p_value[1] = ntohl(p_value[1]);
    tmp = p_value[0];
    p_value[0]  = p_value[1];
    p_value[1] = tmp;
#endif
  }
  void decode_uint64_in_place(uint64_t &value)
  {
    int64_t tmp_value = static_cast<int64_t>(value);
    decode_int64_in_place(tmp_value);
    value = static_cast<uint64_t>(tmp_value);
  }
  void decode_int32_in_place(int32_t &value)
  {
    value = ntohl(value);
  }
  void decode_uint32_in_place(uint32_t &value)
  {
    value = ntohl(value);
  }

  void decode_int8_in_place(int8_t &value)
  {
    value = decode_int8(reinterpret_cast<const char*>(&value));
  }

  void decode_int16_in_place(int16_t &value)
  {
    value = decode_int16(reinterpret_cast<const char*>(&value));
    //value = static_cast<int16_t>(ntohs(value));
  }

  void decode_uint16_in_place(uint16_t &value)
  {
    value = decode_int16(reinterpret_cast<const char*>(&value));
    //value = ntohs(static_cast<uint16_t>(value));
  }

  /// @fn compare two buffer
  int compare_buf(const char *buf1, const int64_t buf1_len, const char *buf2, const int64_t buf2_len)
  {
    unsigned int commonlen = static_cast<uint32_t>(std::min<int64_t>(buf1_len, buf2_len));
    int ret = 0;
    if ((ret = memcmp(buf1, buf2, commonlen)) == 0)
    {
      ret = static_cast<int32_t>(buf1_len - buf2_len);
    }
    return ret;
  }
}

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

ObSCRecordHeader* ObSCRecordHeader::deserialize_and_check(char *data_buf,
                                                          const int64_t size,
                                                          int64_t &pos,
                                                          const int16_t magic)
{
  int err = OB_SUCCESS;
  ObSCRecordHeader *header = NULL;
  if (NULL == data_buf || size - pos < static_cast<int64_t>(sizeof(ObSCRecordHeader)))
  {
    TBSYS_LOG(ERROR, "param error [data_buf:%p,size:%ld]",data_buf, size);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    header = reinterpret_cast<ObSCRecordHeader*>(data_buf + pos);
    int16_t checksum_result = 0;
    int16_t *p_header_short_array
    = reinterpret_cast<int16_t*>(header);
    int32_t header_short_array_size
    = sizeof(ObSCRecordHeader)/sizeof(int16_t);
    pos += sizeof(ObSCRecordHeader);
    decode_int16_in_place(header->magic_);
    decode_int16_in_place(header->header_length_);
    decode_int16_in_place(header->version_);
    decode_int16_in_place(header->header_checksum_);
    decode_int32_in_place(header->data_length_);
    decode_int32_in_place(header->data_zlength_);
    decode_int64_in_place(header->data_checksum_);
    if (OB_SUCCESS == err && size  - pos < header->data_zlength_)
    {
      TBSYS_LOG(ERROR, "there is not enough space");
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && magic != header->magic_ )
    {
      TBSYS_LOG(ERROR, "magic number coincident [real:%d,exp:%d]",
                header->magic_,magic);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err
        && header->header_length_ != sizeof(ObSCRecordHeader))
    {
      TBSYS_LOG(ERROR, "header length error [real:%d,exp:%lu]",
                header->header_length_, sizeof(ObSCRecordHeader));
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err
        && AVERSION != header->version_)
    {
      TBSYS_LOG(ERROR, "version error [real:%d,exp:%d]",
                header->version_, AVERSION);
      err = OB_ERROR;
    }

    if (OB_SUCCESS == err
        && 0 != header->reserved_)
    {
      TBSYS_LOG(ERROR, "reserved error [real:%ld,exp:%d]",
                header->reserved_,0);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && header->data_zlength_ <= 0)
    {
      TBSYS_LOG(ERROR, "data_zlength should be greater than 0 [real:%d]",
                header->data_zlength_);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && header->data_length_ <= 0)
    {
      TBSYS_LOG(ERROR, "data_length should be greater than 0 [real:%d]",
                header->data_length_);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err)
    {
      for (int32_t int16_idx = 0;
          int16_idx < header_short_array_size;
          int16_idx ++)
      {
        checksum_result ^= p_header_short_array[int16_idx];
      }
      if (checksum_result != 0)
      {
        TBSYS_LOG(ERROR, "header checksum check fail");
        err = OB_ERROR;
      }
    }
    if (OB_SUCCESS == err
        && ob_crc64(data_buf + pos, header->data_zlength_) != (uint64_t)header->data_checksum_ )
    {
      TBSYS_LOG(ERROR, "data checksum check fail [real:%ld,exp:%ld]",
                header->data_checksum_,
                ob_crc64(data_buf + pos, header->data_zlength_));
      err = OB_ERROR;
    }
  }
  if (OB_SUCCESS == err)
  {
    pos += header->data_zlength_;
  }
  if (OB_SUCCESS != err)
  {
    header = NULL;
  }
  return header;
}

ObSCTrailerOffset* ObSCTrailerOffset::deserialize_and_check(char *data_buf,
                                                            const int64_t size,
                                                            int64_t &pos)
{
  int err = OB_SUCCESS;
  ObSCTrailerOffset *offset = NULL;
  if (NULL == data_buf || size - pos < static_cast<int64_t>(sizeof(ObSCTrailerOffset)))
  {
    TBSYS_LOG(ERROR, "param error [data_buf:%p,size:%ld]",data_buf, size);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    offset = reinterpret_cast<ObSCTrailerOffset*>(data_buf);
    decode_int64_in_place(offset->trailer_offset_);
    if (offset->trailer_offset_ <= 0)
    {
      TBSYS_LOG(ERROR, "trailer_offset should be greater than 0 [real:%ld]", offset->trailer_offset_);
      err = OB_ERROR;
    }
    pos += sizeof(ObSCTrailerOffset);
  }
  if (OB_SUCCESS != err)
  {
    offset = NULL;
  }
  return offset;
}

ObSCSSTableTrailer* ObSCSSTableTrailer::deserialize_and_check(char *data_buf,
                                                              const int64_t size,
                                                              int64_t &pos)
{
  int err = OB_SUCCESS;
  ObSCRecordHeader *record_header = NULL;
  ObSCSSTableTrailer *trailer = NULL;

  if (OB_SUCCESS == err)
  {
    record_header = ObSCRecordHeader::deserialize_and_check(data_buf,size,pos,
                                                            ObSSTableWriter::TRAILER_MAGIC);
    if (record_header == NULL)
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to deserialize sstable trailer's record head");
    }
  }
  if (OB_SUCCESS == err
      && record_header->data_length_ > static_cast<int64_t>(sizeof(ObSCSSTableTrailer)))
  {
    TBSYS_LOG(ERROR, "space not enough for ObSCSSTableTrailer, data_length=%d, "
                     "size_of_trailer=%lu", record_header->data_length_,
              sizeof(ObSCSSTableTrailer));
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    trailer = reinterpret_cast<ObSCSSTableTrailer*>(record_header->payload_);
    decode_int32_in_place(trailer->size_);
    decode_int32_in_place(trailer->version_);
    decode_int64_in_place(trailer->table_version_);
    decode_int64_in_place(trailer->first_block_data_offset_);
    decode_int64_in_place(trailer->block_count_);
    decode_int64_in_place(trailer->block_index_record_offset_);
    decode_int64_in_place(trailer->block_index_record_size_);
    decode_int64_in_place(trailer->bloom_filter_hash_count_);
    decode_int64_in_place(trailer->bloom_filter_record_offset_);
    decode_int64_in_place(trailer->bloom_filter_record_size_);
    decode_int64_in_place(trailer->schema_record_offset_);
    decode_int64_in_place(trailer->schema_record_size_);
    decode_int64_in_place(trailer->blocksize_);
    decode_int64_in_place(trailer->row_count_);
    decode_uint64_in_place(trailer->sstable_checksum_);
    decode_uint64_in_place(trailer->first_table_id_);
    decode_int64_in_place(trailer->frozen_time_);

    decode_int64_in_place(trailer->range_record_offset_);
    decode_int64_in_place(trailer->range_record_size_);

    decode_int16_in_place(trailer->row_value_store_style_);

    if (OB_SUCCESS == err && trailer->version_ != TRAILER_VERSION3)
    {
      TBSYS_LOG(ERROR, "version error [real:%d,exp:%d]", trailer->version_, TRAILER_VERSION3);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->block_count_ < 0 )
    {
      TBSYS_LOG(ERROR, "block_count_ should be greater than 0 [real:%ld]", trailer->block_count_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->block_index_record_offset_ < 0 )
    {
      TBSYS_LOG(ERROR, "block_index_record_offset_ should be greater than 0 [real:%ld]",
                trailer->block_index_record_offset_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->block_index_record_size_ < 0 )
    {
      TBSYS_LOG(ERROR, "block_index_record_size_ should be greater than 0 [real:%ld]",
                trailer->block_index_record_size_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->bloom_filter_hash_count_ < 0 )
    {
      TBSYS_LOG(ERROR, "bloom_filter_hash_count_ should be greater than 0 [real:%ld]",
                trailer->bloom_filter_hash_count_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->bloom_filter_record_offset_ < 0 )
    {
      TBSYS_LOG(ERROR, "bloom_filter_record_offset_ should be greater than 0 [real:%ld]",
                trailer->bloom_filter_record_offset_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->bloom_filter_record_size_ < 0 )
    {
      TBSYS_LOG(ERROR, "bloom_filter_record_size_ should be greater than 0 [real:%ld]",
                trailer->bloom_filter_record_size_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->blocksize_ < 0 )
    {
      TBSYS_LOG(ERROR, "blocksize_ should be greater than 0 [real:%ld]",
                trailer->blocksize_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->row_count_ < 0 )
    {
      TBSYS_LOG(ERROR, "row_count_ should be greater than 0 [real:%ld]",
                trailer->row_count_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->first_table_id_ == OB_INVALID_ID)
    {
      TBSYS_LOG(ERROR, "first_table_id_ shouldn't be OB_INVALID_ID [real:%lu]",
                trailer->first_table_id_);
      err  = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->frozen_time_ < 0 )
    {
      TBSYS_LOG(ERROR, "frozen_time_ should be greater than or equal 0 [real:%ld]",
                trailer->frozen_time_);
      err  = OB_ERROR;
    }
    for (int32_t i = 0;
        i < static_cast<int32_t>(sizeof(trailer->reserved64_)/sizeof(int64_t)) && OB_SUCCESS == err;
        i++)
    {
      if (trailer->reserved64_[i] != 0)
      {
        TBSYS_LOG(ERROR, "reserved64_ should be 0 [real:%ld,index:%d]", trailer->reserved64_[i], i);
        err = OB_ERROR;
      }
    }
    /// @todo (wushi wushi.ly@taobao.com) do not check compressor_name now,
    ///       check it after implemented
    if (OB_SUCCESS == err
        && memcmp(trailer->compressor_name_, "lzo", 3) != 0
        && memcmp(trailer->compressor_name_, "sna", 3) != 0
        && memcmp(trailer->compressor_name_, "non", 3) != 0)
    {
      TBSYS_LOG(ERROR, "compressor_name_ error [real:%.3s,exp:%.3s]",
                trailer->compressor_name_, "lzo");
      err = OB_ERROR;
    }
    /// @todo (wushi wushi.ly@taobao.com) row_value_store_stype_ can only be OB_SSTABLE_STORE_DENSE now
    if (OB_SUCCESS == err && OB_SSTABLE_STORE_DENSE != trailer->row_value_store_style_
        && OB_SSTABLE_STORE_SPARSE != trailer->row_value_store_style_)
    {
      TBSYS_LOG(ERROR, "row_value_store_style_ error [real:%hd,exp:%hd]",
                trailer->row_value_store_style_, OB_SSTABLE_STORE_DENSE);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && trailer->reserved_ != 0)
    {
      TBSYS_LOG(ERROR, "reserved_ should be 0 [real:%d]", trailer->reserved_);
      err = OB_ERROR;
    }
  }

  if (OB_SUCCESS != err)
  {
    trailer = NULL;
  }
  return trailer;
}

int ObSCTableSchemaHeader::deserialize_and_check(char *data_buf,
                                                 ObSCTableSchemaHeader& schema,
                                                 const int64_t size,
                                                 int64_t &pos)
{
  int err = OB_SUCCESS;
  ObSCRecordHeader *record_header = NULL;
  int64_t column_count = 0;

  if (OB_SUCCESS == err)
  {
    record_header = ObSCRecordHeader::deserialize_and_check(data_buf,size,pos,
                                                            ObSSTableWriter::SCHEMA_MAGIC);
    if (record_header == NULL)
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to deserialize sstable trailer's record head");
    }
  }
  if (OB_SUCCESS == err
      && record_header->data_zlength_ < ObSCTableSchemaHeader::get_serialize_size())
  {
    TBSYS_LOG(ERROR, "space not enough for ObSCTableSchemaHeader");
    err = OB_INVALID_ARGUMENT;
  }
  if ( OB_SUCCESS == err)
  {
    pos = sizeof(ObSCRecordHeader);
    schema.column_count_ = decode_int16(data_buf + pos);
    pos += sizeof(schema.column_count_);
    if (OB_SUCCESS == err && schema.column_count_ < 0)
    {
      TBSYS_LOG(ERROR, "column_count_ should be greater than or equal to 0 [real:%hd]",
                schema.column_count_);
      err = OB_ERROR;
    }

    schema.reserved16_ = *(reinterpret_cast<int16_t*>(data_buf + pos));
    if (schema.reserved16_ != 0)
    {
      TBSYS_LOG(ERROR, "reserved16_ should be 0 [real:%hd]", schema.reserved16_);
      err = OB_ERROR;
    }
    pos += sizeof(int16_t);

    schema.total_column_count_ = decode_int32(data_buf + pos);
    pos += sizeof(schema.total_column_count_);
    if (OB_SUCCESS == err && schema.total_column_count_ < 0)
    {
      TBSYS_LOG(ERROR, "total_column_count_ should be greater than or equal to 0 [real:%hd]",
                schema.total_column_count_);
      err = OB_ERROR;
    }

    if (OB_SUCCESS == err)
    {
      if (schema.total_column_count_ > schema.column_count_)
      {
        column_count = schema.total_column_count_;
      }
      else
      {
        column_count = schema.column_count_;
        schema.total_column_count_ = schema.column_count_;
      }
    }
    if (OB_SUCCESS == err
        && record_header->data_zlength_
        < static_cast<int64_t>(ObSCTableSchemaHeader::get_serialize_size()
                               + column_count * ObSCTableSchemaColumnDef::get_serialize_size()))
    {
      TBSYS_LOG(ERROR, "space not enough");
      err = OB_ERROR;
    }

    if (OB_SUCCESS == err)
    {
      uint64_t prev_column_id = static_cast<uint64_t>(-1);
      int16_t row_key_column_count = 0;
      for (int16_t i = 0; i < column_count && OB_SUCCESS == err; ++i)
      {
        schema.column_defs_[i].reserved_ = decode_int16(data_buf + pos);
        pos += sizeof(int16_t);
        schema.column_defs_[i].column_group_id_ = decode_int16(data_buf + pos);
        pos += sizeof(uint16_t);
        schema.column_defs_[i].column_name_id_ = decode_uint32(data_buf + pos);
        pos += sizeof(uint32_t);
        schema.column_defs_[i].column_value_type_ = decode_uint32(data_buf + pos);
        pos += sizeof(int32_t);
        schema.column_defs_[i].table_id_ = decode_uint32(data_buf + pos);
        pos += sizeof(uint32_t);

        if (schema.column_defs_[i].column_group_id_ == (uint16_t)(OB_INVALID_ID & 0xFFFF))
        {
          ++row_key_column_count;
        }
      }

      for (int16_t i = 0; i < column_count && OB_SUCCESS == err; i++)
      {
        if (schema.column_defs_[i].column_value_type_ < ObNullType
            || schema.column_defs_[i].column_value_type_ >= ObMaxType)
        {
          TBSYS_LOG(ERROR, "schema column type error [columnid:%u,type:%d]",
                    schema.column_defs_[i].column_name_id_,
                    schema.column_defs_[i].column_value_type_);
          err = OB_ERROR;
        }
        if (i > row_key_column_count && schema.column_defs_[i].is_less(schema.column_defs_[i - 1]))
        {
          TBSYS_LOG(ERROR, "shema column id not increase [index:%hd,current_columnid:%u,"
                    "prev_columnid:%lu], current_group_id=%u, prev_group_id=%u, "
                    "current_table_id=%u, prev_table_id=%u",
                    i, schema.column_defs_[i].column_name_id_, prev_column_id,
                    schema.column_defs_[i].column_group_id_,
                    schema.column_defs_[i - 1].column_group_id_,
                    schema.column_defs_[i].table_id_,
                    schema.column_defs_[i - 1].table_id_);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && schema.column_defs_[i].column_name_id_ == 1)
        {
          TBSYS_LOG(ERROR, "column id should not be 1 [real:%u]",
                    schema.column_defs_[i].column_name_id_);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && i > row_key_column_count &&
            schema.column_defs_[i].column_group_id_ == (uint16_t)(OB_INVALID_ID & 0xFFFF))
        {
          TBSYS_LOG(ERROR, "column def[%d] column_group_id_ should not be OB_INVALID_ID [real:%u]",
                    i, schema.column_defs_[i].column_group_id_);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && schema.column_defs_[i].table_id_
            == (uint32_t)(OB_INVALID_ID & 0xFFFFFFFF))
        {
          TBSYS_LOG(ERROR, "column def table_id_ should not be OB_INVALID_ID [real:%u]",
                    schema.column_defs_[i].table_id_);
          err = OB_ERROR;
        }
        prev_column_id = schema.column_defs_[i].column_name_id_;
      }
    }
  }

  return err;
}

ObSCSSTableBlockIndexHeader * ObSCSSTableBlockIndexHeader::deserialize_and_check(char *data_buf,
                                                                                 const int64_t size,
                                                                                 int64_t &pos)
{
  int err = OB_SUCCESS;
  ObSCRecordHeader *record_header = NULL;
  ObSCSSTableBlockIndexHeader *block_index = NULL;
  if (OB_SUCCESS == err)
  {
    record_header = ObSCRecordHeader::deserialize_and_check(data_buf,size,pos,
                                                            ObSSTableWriter::BLOCK_INDEX_MAGIC);
    if (record_header == NULL)
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to deserialize sstable trailer's record head");
    }
  }
  if (OB_SUCCESS == err
      && record_header->data_zlength_ < static_cast<int64_t>(sizeof(ObSCRecordHeader)))
  {
    err = OB_ERROR;
    TBSYS_LOG(ERROR,"space not enough");
  }
  if (OB_SUCCESS == err)
  {
    block_index = reinterpret_cast<ObSCSSTableBlockIndexHeader*>(record_header->payload_);
    decode_int64_in_place(block_index->sstable_block_count_);
    decode_int32_in_place(block_index->end_key_char_stream_offset_);
    decode_int16_in_place(block_index->rowkey_flag_);
    decode_int16_in_place(block_index->reserved16_);
    for(uint32_t i=0; i<sizeof(block_index->reserved64_)/sizeof(int64_t); ++i)
    {
      decode_int64_in_place(block_index->reserved64_[i]);
    }

    if (OB_SUCCESS == err && block_index->sstable_block_count_ <= 0)
    {
      TBSYS_LOG(ERROR, "sstable_block_count_ should be greater than 0 [real:%ld]",
                block_index->sstable_block_count_);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && block_index->end_key_char_stream_offset_ <= 0)
    {
      TBSYS_LOG(ERROR, "end_key_char_stream_offset_ should be greater than 0 [real:%ld]",
                block_index->sstable_block_count_);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && block_index->rowkey_flag_ != 1)
    {
      TBSYS_LOG(ERROR, "rowkey_flag_ should be 1[real:%d]",
                block_index->rowkey_flag_);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && block_index->reserved16_ != 0)
    {
      TBSYS_LOG(ERROR, "reserved16_ should be 0 [real:%d]",  block_index->reserved16_);
      err = OB_ERROR;
    }
    for (uint32_t i = 0;
        i < sizeof(block_index->reserved64_)/sizeof(int64_t) && OB_SUCCESS == err;
        i++)
    {
      if (block_index->reserved64_[i] != 0)
      {
        TBSYS_LOG(ERROR, "reserved64_ shoul be 0 [real:%ld,index:%u]",
                  block_index->reserved64_[i], i);
        err = OB_ERROR;
      }
    }
    if (OB_SUCCESS == err
        && record_header->data_zlength_ < static_cast<int64_t>(sizeof(ObSCSSTableBlockIndexHeader)
                                                               + block_index->sstable_block_count_*sizeof(ObSCBlockInfo)))
    {
      TBSYS_LOG(ERROR, "space not enough");
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err)
    {
      int64_t total_end_key_size = 0;
      for (int32_t i = 0; i < block_index->sstable_block_count_ && OB_SUCCESS == err; i++)
      {
        decode_int16_in_place(block_index->block_info_array_[i].reserved16_);
        decode_uint16_in_place(block_index->block_info_array_[i].column_group_id_);
        decode_uint32_in_place(block_index->block_info_array_[i].table_id_);
        decode_int32_in_place(block_index->block_info_array_[i].block_record_size_);
        decode_int16_in_place(block_index->block_info_array_[i].block_end_key_size_);
        if (OB_SUCCESS == err && block_index->block_info_array_[i].reserved16_ != 0)
        {
          TBSYS_LOG(ERROR, "block index resreved16_ should be 0 [real:%d]",
                    block_index->block_info_array_[i].reserved16_);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && block_index->block_info_array_[i].column_group_id_
            == (uint16_t)(OB_INVALID_ID & 0xFFFF))
        {
          TBSYS_LOG(ERROR, "block index column_group_id_ should not be OB_INVALID_ID [real:%u]",
                    block_index->block_info_array_[i].column_group_id_);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && block_index->block_info_array_[i].table_id_
            == (uint32_t)(OB_INVALID_ID & 0xFFFFFFFF))
        {
          TBSYS_LOG(ERROR, "block index table_id_ should not be OB_INVALID_ID [real:%u]",
                    block_index->block_info_array_[i].table_id_);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && block_index->block_info_array_[i].block_record_size_ <= 0)
        {
          TBSYS_LOG(ERROR, "block_record_size_ should be greater than 0 [real:%d,index:%d]",
                    block_index->block_info_array_[i].block_record_size_, i);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && block_index->block_info_array_[i].block_end_key_size_ <= 0)
        {
          TBSYS_LOG(ERROR, "block_end_key_size_ should be greater than 0 [real:%d,index:%d]",
                    block_index->block_info_array_[i].block_end_key_size_, i);
          err = OB_ERROR;
        }
        total_end_key_size += block_index->block_info_array_[i].block_end_key_size_;
      }
      if (OB_SUCCESS == err
          && record_header->data_zlength_ < static_cast<int64_t>(sizeof(ObSCSSTableBlockIndexHeader)
                                                                 + block_index->sstable_block_count_*sizeof(ObSCBlockInfo)
                                                                 + total_end_key_size))
      {
        TBSYS_LOG(ERROR, "space not enough");
        err = OB_ERROR;
      }
    }
    /// @note 在check全局key有序的时候check block endkey有序
  }

  if (OB_SUCCESS != err)
  {
    block_index = NULL;
  }
  return block_index;
}

ObSCSSTableBlockHeader * ObSCSSTableBlockHeader::deserialize_and_check(char *data_buf,
                                                                       const int64_t size,
                                                                       int64_t &pos,
                                                                       ObMemBuffer& mem_buf,
                                                                       const char* compressor_name)
{
  int err = OB_SUCCESS;
  ObSCRecordHeader *record_header = NULL;
  ObSCSSTableBlockHeader *block = NULL;
  int64_t uncompress_size = 0;
  char* uncompress_buf = NULL;
  ObCompressor* compressor = NULL;

  if (OB_SUCCESS == err)
  {
    record_header = ObSCRecordHeader::deserialize_and_check(data_buf,size,pos,
                                                            ObSSTableWriter::DATA_BLOCK_MAGIC);
    if (record_header == NULL)
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to deserialize sstable trailer's record head");
    }
  }
  if (OB_SUCCESS == err
      && record_header->data_zlength_ < static_cast<int64_t>(sizeof(ObSCSSTableBlockHeader)))
  {
    TBSYS_LOG(ERROR, "space not enough");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err && record_header->data_length_ > record_header->data_zlength_)
  {
    uncompress_buf = static_cast<char*>(mem_buf.malloc(record_header->data_length_));
    if (NULL == uncompress_buf)
    {
      TBSYS_LOG(ERROR, "failed to allocate memory for uncompress buffer");
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err
        && NULL == (compressor = create_compressor(compressor_name)))
    {
      TBSYS_LOG(ERROR, "failed to create compressor");
      err = OB_ERROR;
    }

    if (OB_SUCCESS == err)
    {
      err = compressor->decompress(record_header->payload_, record_header->data_zlength_,
                                   uncompress_buf, record_header->data_length_,
                                   uncompress_size);
      if (OB_SUCCESS != err || uncompress_size != record_header->data_length_)
      {
        TBSYS_LOG(ERROR, "failed to compress block data");
        err = OB_ERROR;
      }
    }
  }
  else if (OB_SUCCESS == err && record_header->data_length_ == record_header->data_zlength_)
  {
    uncompress_buf = record_header->payload_;
  }
  if (OB_SUCCESS == err)
  {
    block = reinterpret_cast<ObSCSSTableBlockHeader*>(uncompress_buf);
    decode_int32_in_place(block->row_index_array_offset_);
    decode_int32_in_place(block->row_count_);
    if (OB_SUCCESS == err && block->row_index_array_offset_ <= 0)
    {
      TBSYS_LOG(ERROR, "row_index_array_offset_ should be greater than 0 [real:%d]",
                block->row_index_array_offset_);
      err = OB_ERROR;
    }
    if (OB_SUCCESS == err && block->row_count_ <= 0)
    {
      TBSYS_LOG(ERROR, "row_count_ should be greater than 0 [real:%d]",
                block->row_count_);
      err = OB_ERROR;
    }
  }
  if (OB_SUCCESS == err
      && static_cast<int64_t>(block->row_index_array_offset_
                              + (block->row_count_ + 1) * sizeof(int32_t))
      > record_header->data_length_)
  {
    TBSYS_LOG(WARN, "block space not enough [need_size:%ld,actual_size:%d]",
              block->row_index_array_offset_ + (block->row_count_ + 1) * sizeof(int32_t),
              record_header->data_length_);
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    int32_t prev_offset = 0;
    int32_t *row_index_array
    = reinterpret_cast<int32_t *>(reinterpret_cast<char*>(block) + block->row_index_array_offset_);

    for (int64_t rowid = 0;
        OB_SUCCESS == err && rowid <= block->row_count_;
        rowid ++)
    {
      decode_int32_in_place(row_index_array[rowid]);
      if (row_index_array[rowid] <= prev_offset)
      {
        TBSYS_LOG(WARN, "record overlapped with previous [current_rowid:%ld]", rowid);
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err && row_index_array[rowid] >= record_header->data_length_)
      {
        TBSYS_LOG(WARN, "record out of block area [rowid:%ld]", rowid);
        err = OB_ERROR;
      }
      prev_offset = row_index_array[rowid];
    }
  }

  if (OB_SUCCESS != err)
  {
    block = NULL;
  }
  return block;
}

ObSCSSTableChecker::ObSCSSTableChecker()
{
  memset(this, 0,sizeof(*this));
}

ObSCSSTableChecker::~ObSCSSTableChecker()
{
}


int oceanbase::chunkserver::ObSCSSTableChecker::check(const char *sstable_fname)
{
  int err = 0;
  int sstable_fd = -1;
  struct stat sstable_stat;
  int64_t read_offset = 0;
  int64_t need_size = 0;
  int64_t readed_size = 0;
  if (NULL == sstable_fname)
  {
    TBSYS_LOG(WARN, "param error [sstable_fname:%p]",sstable_fname);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err && (sstable_fd = open(sstable_fname, O_RDONLY)) < 0)
  {
    TBSYS_LOG(WARN, "fail to open disk file for read [fname:%s,errno:%d]",
              sstable_fname, errno);
    err = OB_ERR_SYS;
  }
  if (OB_SUCCESS == err && fstat(sstable_fd, &sstable_stat) < 0)
  {
    TBSYS_LOG(WARN, "fail to stat disk file [fname:%s,errno:%d]", sstable_fname, errno);
    err = OB_ERR_SYS;
  }
  /// reading trailer offset
  if ( OB_SUCCESS == err )
  {
    need_size = sizeof( ObSCTrailerOffset );
    read_offset = sstable_stat.st_size - need_size;
    readed_size = pread( sstable_fd, &trailer_offset_, need_size, read_offset );
    if (readed_size != need_size)
    {
      TBSYS_LOG( WARN, "fail to read trailer offset [offset:%ld,need_size:%ld,"
                 "readed_size:%ld,errno:%d]", read_offset, need_size, readed_size, errno );
      err = OB_ERR_SYS;
    }
    if (OB_SUCCESS == err)
    {
      int64_t pos = 0;
      ObSCTrailerOffset *offset
      = ObSCTrailerOffset::deserialize_and_check(reinterpret_cast< char* >( &trailer_offset_),
                                                 need_size,  pos);
      if (offset == NULL)
      {
        TBSYS_LOG( WARN, "fail to deserialize trailer offset" );
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err && pos != need_size)
      {
        TBSYS_LOG( ERROR, "internal error");
        err = OB_ERROR;
      }
    }
  }
  /// reading trailer
  if (OB_SUCCESS == err)
  {
    read_offset = trailer_offset_.trailer_offset_ ;
    need_size = sstable_stat.st_size - sizeof( ObSCTrailerOffset ) - read_offset;
    // @warning trailer record include trailer offset
    //need_size = sstable_stat.st_size -  read_offset;
    if (NULL == trailer_buffer_.malloc( need_size ))
    {
      TBSYS_LOG( WARN, "fail to allocate memory for trailer" );
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
    if (OB_SUCCESS == err)
    {
      readed_size = pread( sstable_fd, trailer_buffer_.get_buffer(  ), need_size,read_offset );
      if (readed_size != need_size)
      {
        TBSYS_LOG( WARN, "fail to read trailer [offset:%ld,need_size:%ld,"
                   "readed_size:%ld,errno:%d]", read_offset, need_size, readed_size, errno );
        err = OB_ERR_SYS;
      }
    }
    if (OB_SUCCESS == err)
    {
      int64_t pos = 0;
      trailer_
      = ObSCSSTableTrailer::deserialize_and_check(
                                                 reinterpret_cast<char*>(trailer_buffer_.get_buffer(  )),
                                                 need_size, pos );
      if (NULL == trailer_ )
      {
        TBSYS_LOG( WARN, "fail to deserilize trailer" );
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err && pos != need_size)
      {
        TBSYS_LOG( ERROR, "check trailer internal error, pos=%ld, need_size=%ld", pos, need_size);
        err = OB_ERROR;
      }
    }
  }

  /// reading schema
  if (OB_SUCCESS == err)
  {
    read_offset = trailer_->schema_record_offset_;
    need_size = trailer_->schema_record_size_;
    if (NULL == schema_buffer_.malloc(need_size))
    {
      TBSYS_LOG(WARN, "fail to allocate memory for schema");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
    if (OB_SUCCESS == err)
    {
      readed_size = pread( sstable_fd, schema_buffer_.get_buffer(  ), need_size,read_offset );
      if (readed_size != need_size)
      {
        TBSYS_LOG( WARN, "fail to read schema [offset:%ld,need_size:%ld,"
                   "readed_size:%ld,errno:%d]", read_offset, need_size, readed_size, errno );
        err = OB_ERR_SYS;
      }
    }
    if (OB_SUCCESS == err)
    {
      int64_t pos = 0;
      err = ObSCTableSchemaHeader::deserialize_and_check(reinterpret_cast<char*>(schema_buffer_.get_buffer()),
                                                         schema_, need_size, pos);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to deserialize schema");
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err && pos != need_size)
      {
        TBSYS_LOG( ERROR, "internal error, pos=%ld, need_size=%ld", pos, need_size);
        err = OB_ERROR;
      }
    }
  }

  /// reading bloom filter
  if (OB_SUCCESS == err)
  {
    read_offset = trailer_->bloom_filter_record_offset_;
    need_size = trailer_->bloom_filter_record_size_;
    if (need_size > 0)
    {
      if (NULL == bloom_filter_buffer_.malloc(need_size))
      {
        TBSYS_LOG(WARN, "fail to allocate memory for bloom filter");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      if (OB_SUCCESS == err)
      {
        readed_size = pread( sstable_fd, bloom_filter_buffer_.get_buffer(  ), need_size,read_offset );
        if (readed_size != need_size)
        {
          TBSYS_LOG( WARN, "fail to read bloom filter [offset:%ld,need_size:%ld,"
                     "readed_size:%ld,errno:%d]", read_offset, need_size, readed_size, errno );
          err = OB_ERR_SYS;
        }
      }
      if (OB_SUCCESS == err)
      {
        int64_t pos = 0;
        bloom_filter_record_ = ObSCRecordHeader::deserialize_and_check(reinterpret_cast<char*>(bloom_filter_buffer_.get_buffer()),
                                                                       need_size, pos, ObSSTableWriter::BLOOM_FILTER_MAGIC);
        if (NULL == bloom_filter_record_)
        {
          TBSYS_LOG(WARN, "fail to deserialize bloom filter");
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && pos != need_size)
        {
          TBSYS_LOG( ERROR, "internal error");
          err = OB_ERROR;
        }
      }
      /// load bloom filter
      if (OB_SUCCESS == err)
      {
        err = bloom_filter_readed_.init(BLOOM_FILTER_ITEMS_ESTIMATE);
        if (OB_SUCCESS == err)
        {
          err = bloom_filter_readed_.init(trailer_->bloom_filter_hash_count_, (int64_t)bloom_filter_record_->data_zlength_);
        }
        if (OB_SUCCESS == err)
        {
          err = bloom_filter_readed_.set_buffer((uint8_t*)bloom_filter_record_->payload_,
                                                bloom_filter_record_->data_zlength_);
        }
      }
      /// init self filter
      if (OB_SUCCESS == err)
      {
        err = bloom_filter_generated_.init(BLOOM_FILTER_ITEMS_ESTIMATE);
      }
    }
  }

  /// read range
  if (OB_SUCCESS == err)
  {
    int ret = OB_SUCCESS;
    int64_t range_offset = trailer_->range_record_offset_;
    int64_t range_size = trailer_->range_record_size_;

    range_.reset();

    if (range_offset <= 0 || range_size <= 0)
    {
      TBSYS_LOG(ERROR, "range_offset:%ld, range_size:%ld is illegal",
          range_offset, range_size);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      ObSCRecordHeader *record_header = NULL;
      if (NULL == range_buffer_.malloc(range_size))
      {
        TBSYS_LOG(ERROR, "failed to malloc range_buffer_");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      char *range_buf = static_cast<char *>(range_buffer_.get_buffer());
      int64_t readed_size = pread(sstable_fd, range_buf, range_size, range_offset);
      if (readed_size != range_size)
      {
        TBSYS_LOG(WARN, "fail to read range [offset:%ld,need_size:%ld,"
            "readed_size:%ld,errno:%d]error: %s",
            range_offset, range_size, readed_size, errno, strerror(errno));
        err = OB_ERR_SYS;
      }

      int64_t pos = 0;
      int64_t size = range_size;
      record_header = ObSCRecordHeader::deserialize_and_check(range_buf,size,pos,
          ObSSTableWriter::RANGE_MAGIC);
      if (record_header == NULL)
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "fail to deserialize sstable key_stream:'s record head");
      }

      if (OB_SUCCESS == ret)
      {
        if (record_header->data_length_ == record_header->data_zlength_)
        {
          pos = 0;
          range_.start_key_.assign(start_key_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
          range_.end_key_.assign(end_key_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);

          ret = range_.deserialize(record_header->payload_, record_header->data_length_, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "failed to deserialize range: payload_ptr %p,"
                " payload_size %d, pos %ld", record_header->payload_, record_header->data_length_, pos);
          }
        }
        else
        {
          // compressed range buf
          TBSYS_LOG(ERROR, "range is in compressed format, "
              "but expect uncompressed format");
          ret = OB_ERROR;
        }
      }
    }
  }

  /// reading block index
  if (OB_SUCCESS == err && trailer_->block_index_record_size_ != 0)
  {
    read_offset = trailer_->block_index_record_offset_;
    need_size = trailer_->block_index_record_size_;
    if (NULL == block_index_buffer_.malloc(need_size))
    {
      TBSYS_LOG(WARN, "fail to allocate memory for block index");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
    if (OB_SUCCESS == err)
    {
      readed_size = pread( sstable_fd, block_index_buffer_.get_buffer(  ), need_size,read_offset );
      if (readed_size != need_size)
      {
        TBSYS_LOG( WARN, "fail to read block index [offset:%ld,need_size:%ld,"
                   "readed_size:%ld,errno:%d]", read_offset, need_size, readed_size, errno );
        err = OB_ERR_SYS;
      }
    }
    if (OB_SUCCESS == err)
    {
      int64_t pos = 0;
      block_index_ = ObSCSSTableBlockIndexHeader::deserialize_and_check(reinterpret_cast<char*>(block_index_buffer_.get_buffer()),
                                                                        need_size, pos);
      if (NULL == block_index_)
      {
        TBSYS_LOG(WARN, "fail to deserialize block index");
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err && pos != need_size)
      {
        TBSYS_LOG( ERROR, "internal error");
        err = OB_ERROR;
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    err = check_offset_size(sstable_stat.st_size);
  }
  if (OB_SUCCESS == err && trailer_->block_index_record_size_ != 0 &&
      trailer_->block_count_ != block_index_->sstable_block_count_)
  {
    TBSYS_LOG(WARN, "block count in block index and trailer not coincident "
              "[block_index.block_count:%ld,trailer.block_count:%ld]",
              block_index_->sstable_block_count_, trailer_->block_count_);
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err && trailer_->block_index_record_size_ != 0)
  {
    err = check_rowkey_order(sstable_fd);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to check rowkey_order, ret=%d", err);
    }
  }
  if (sstable_fd >= 0)
  {
    close(sstable_fd);
    sstable_fd = -1;
  }
  return err;
}

int oceanbase::chunkserver::ObSCSSTableChecker::check_row(const char *current_row, const int64_t row_len,
                                                          common::ObRowkey& row_key, const int32_t column_count)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  common::ObObj obj;
  if (OB_SUCCESS != (ret = row_key.deserialize_objs(current_row, row_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to deserialize row key,ret=%d", ret);
  }
  else
  {
    for(int32_t i = static_cast<int32_t>(row_key.get_obj_cnt()); i<column_count && OB_SUCCESS == ret; ++i)
    {
      ret = obj.deserialize(current_row, row_len, pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to deserialize obj[%d] row_len=%ld pos=%ld ret=%d",
            i, row_len, pos, ret);
      }
    }
  }

  return ret;
}

int oceanbase::chunkserver::ObSCSSTableChecker::check_rowkey_order(const int sstable_fd)
{
  int err = 0;
  const char *cur_block_end_key = reinterpret_cast<char*>(block_index_) +
                                   block_index_->end_key_char_stream_offset_;
  int64_t cur_block_end_key_size = 0;
  const char * prev_key = NULL;
  int64_t prev_key_size = 0;
  int64_t block_size = 0;
  int64_t cur_offset = 0;
  int64_t readed_size = 0;
  int64_t total_row_count = 0;
  int64_t pos = 0;
  ObSCSSTableBlockHeader *block = NULL;
  int32_t * row_index_array = NULL;
  uint64_t prev_table_id = OB_INVALID_ID;
  uint64_t cur_table_id = OB_INVALID_ID;
  uint64_t prev_group_id = OB_INVALID_ID;
  uint64_t cur_group_id = OB_INVALID_ID;

  const int32_t column_count = schema_.total_column_count_;
  int32_t row_key_column_count = 0;
  ObRowkey prev_row_key;
  ObRowkey end_row_key;
  bool add_row_count = true;
  ObSSTableRow row;
  ObRowkey row_key;
  common::ObObj row_key_obj_array[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  common::ObObj end_row_key_obj_array[common::OB_MAX_ROWKEY_COLUMN_NUMBER];

  for (int32_t i = 0; i < column_count; ++i)
  {
    if (schema_.column_defs_[i].column_group_id_ == (uint16_t)(OB_INVALID_ID & 0xFFFF))
    {
      ++row_key_column_count;
    }
  }
  row_key.assign(row_key_obj_array, row_key_column_count);
  end_row_key.assign(end_row_key_obj_array, row_key_column_count);

  for (int64_t blockid = 0;
      blockid < trailer_->block_count_ && OB_SUCCESS == err;
      blockid ++)
  {
    TBSYS_LOG(DEBUG, "begin to check block [blockid:%ld]", blockid);
    cur_block_end_key_size = block_index_->block_info_array_[blockid].block_end_key_size_;
    int64_t tmp_pos = 0;
    err = end_row_key.deserialize_objs(cur_block_end_key, cur_block_end_key_size, tmp_pos);
    if (0 != err)
    {
      TBSYS_LOG(ERROR, "failed to deserialize end key of cur block, blockid=%ld, ret=%d", blockid,err);
    }

    block_size = block_index_->block_info_array_[blockid].block_record_size_;
    cur_table_id = block_index_->block_info_array_[blockid].table_id_;
    cur_group_id = block_index_->block_info_array_[blockid].column_group_id_;
    if (blockid == 0)
    {
      prev_table_id = cur_table_id;
      prev_group_id = cur_group_id;
    }
    else if (OB_SUCCESS == err)
    {
      if (cur_table_id != prev_table_id)
      {
        prev_table_id = cur_table_id;
        prev_group_id = cur_group_id;
        add_row_count = true;
        prev_row_key.set_min_row();
      }
      else if (cur_table_id == prev_table_id && prev_group_id != cur_group_id)
      {
        prev_group_id = cur_group_id;
        add_row_count = false;
        prev_row_key.set_min_row();
      }
    }
    pos = 0;
    if ( NULL == block_buffer_.malloc(block_size))
    {
      err = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "fail to allocate memory for block");
    }
    /// read and deserialize block
    /// @todo (wushi wushi.ly@taobao.com) decompress the block after implemented
    if ( OB_SUCCESS == err )
    {
      readed_size = pread(sstable_fd, block_buffer_.get_buffer(), block_size, cur_offset);
      if (readed_size != block_size)
      {
        err = OB_ERR_SYS;
        TBSYS_LOG(WARN, "read block [blockid:%ld,block_size:%ld,readed_size:%ld]",
                  blockid, block_size, readed_size);
      }
      if (OB_SUCCESS == err)
      {
        block = block->deserialize_and_check(reinterpret_cast<char*>(block_buffer_.get_buffer()),
                                             block_size, pos, decompress_block_buffer_,
                                             trailer_->compressor_name_);
        if (NULL == block)
        {
          TBSYS_LOG(WARN, "fail to deserialize block [blockid:%ld]", blockid);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err && pos != block_size)
        {
          TBSYS_LOG(WARN, "internal error");
          err = OB_ERROR;
        }
      }
    }
    /// check block
    if (OB_SUCCESS == err)
    {
      row_index_array  =  reinterpret_cast<int32_t*>(reinterpret_cast<char*>(block)
                                                     + block->row_index_array_offset_);
      const char *current_row = NULL;
      ObString key_str;
      int64_t current_row_len = 0;

      for (int64_t row_id = 0; row_id < block->row_count_ && OB_SUCCESS == err; row_id ++)
      {
        current_row_len = row_index_array[row_id + 1]  - row_index_array[row_id];
        current_row = reinterpret_cast<const char*>(block) + row_index_array[row_id];
        err = check_row(current_row, current_row_len, row_key, column_count);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to check row, ret=%d", err);
        }

        /// @fn check bloom filter
        int64_t bf_key_size = row_key.get_serialize_size() + sizeof(int16_t) + sizeof(int16_t) + sizeof(int32_t);
        if (OB_SUCCESS != (err = bf_key_buf_.ensure_space(bf_key_size)))
        {
          TBSYS_LOG(WARN, "failed to ensure space for bloom filter key buf, ret=%d", err);
        }
        char* bf_key_buf = bf_key_buf_.get_buffer();
        int64_t pos = 0;
        //generate bloom filter key with table_id, column_group_id, row_key
        if (OB_SUCCESS !=
            (err = serialization::encode_i16(bf_key_buf, bf_key_size, pos, 0)))
        {
          TBSYS_LOG(WARN, "failed to encode 0 of bloom filter, ret=%d", err);
        }
        else if (OB_SUCCESS !=
            (err = serialization::encode_i16(bf_key_buf, bf_key_size, pos, static_cast<int16_t>(cur_group_id))))
        {
          TBSYS_LOG(WARN, "failed to encode cur_groupd_id[%lu] of bloom filter, ret=%d", cur_group_id, err);
        }
        else if (OB_SUCCESS !=
            (err = serialization::encode_i32(bf_key_buf, bf_key_size, pos, static_cast<int32_t>(cur_table_id))))
        {
          TBSYS_LOG(WARN, "failed to encode cur_table_id[%lu] of bloom filter, ret=%d", cur_table_id, err);
        }
        else if (OB_SUCCESS !=
            (err = row_key.serialize(bf_key_buf, bf_key_size, pos)))
        {
        }
        /*
        else if (!bloom_filter_readed_.may_contain(bf_key_buf, bf_key_size))
        {
          TBSYS_LOG(WARN, "bloom filter check fail [rowid:%ld], %s",row_id, to_cstring(row_key));
          err = OB_ERROR;
        }
        */

        /// check key order
        if (OB_SUCCESS == err && blockid >0 && (cur_table_id < prev_table_id
             || (cur_table_id == prev_table_id && cur_group_id < prev_group_id)
             || (cur_table_id == prev_table_id && cur_group_id == prev_group_id
                 && prev_row_key < row_key)))
        {
          TBSYS_LOG(WARN, "current row key isn't bigger than previous one "
                          "cur_table_id=%lu, prev_table_id=%lu, "
                          "cur_group_id=%lu, prev_group_id=%lu, "
                          "[current_row_id:%ld]\n"
                          "prev_row_key: %s\n"
                          "row_key:      %s",
                    cur_table_id, prev_table_id, cur_group_id, prev_group_id, row_id,
                    to_cstring(prev_row_key), to_cstring(row_key));
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err)
        {
          ObMemBufAllocatorWrapper allocator(row_key_buf_);
          row_key.deep_copy(prev_row_key, allocator);
        }
      }
      /// check block index last key is equal to the last key in the block
      if (OB_SUCCESS == err && (cur_table_id != prev_table_id
             || cur_group_id != prev_group_id
             || row_key != end_row_key))
      {
        TBSYS_LOG(WARN, "block's end key not coincident with block index [blockid:%ld],"
                        "cur_table_id=%lu, prev_table_id=%lu, "
                        "cur_group_id=lu, prev_group_id=%lu, "
                        "cur_block_end_key_size=%ld, blockid=%ld\n"
                        "cur_row_key: %s\n"
                        "end_row_key: %s\n",
                  cur_table_id, prev_table_id, cur_group_id, prev_group_id,
                  cur_block_end_key_size, blockid,
                  to_cstring(row_key), to_cstring(end_row_key));
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err)
      {
        prev_key = cur_block_end_key;
        prev_key_size = cur_block_end_key_size;
        if (add_row_count)
        {
          total_row_count += block->row_count_;
        }
        cur_block_end_key += block_index_->block_info_array_[blockid].block_end_key_size_;
      }
    }
    if (OB_SUCCESS == err)
    {
      cur_offset += block_index_->block_info_array_[blockid].block_record_size_;
    }
  }

  if (OB_SUCCESS == err && total_row_count != trailer_->row_count_)
  {
    TBSYS_LOG(WARN, "row count not coincident [total_row_count:%ld,trailer.row_count:%ld]",
              total_row_count, trailer_->row_count_);
    err  = OB_ERROR;
  }
  return err;
}

int  oceanbase::chunkserver::ObSCSSTableChecker::check_offset_size(const int64_t sstable_file_size)
{
  /// check block offseto
  int err = 0;
  int64_t cur_offset = 0;
  if (trailer_->block_index_record_size_ != 0)
  {
    for (int64_t block_id = 0;
        OB_SUCCESS == err && block_id < block_index_->sstable_block_count_;
        block_id ++)
    {
      cur_offset += block_index_->block_info_array_[block_id].block_record_size_;
      if (cur_offset > sstable_file_size)
      {
        TBSYS_LOG(WARN, "sstable file size not enough [block_id:%ld,need_size:%ld,actual_size:%ld]",
            block_id, cur_offset, sstable_file_size);
        err = OB_ERROR;
      }
    }
  }
  else
  {
    TBSYS_LOG(INFO, "empty sstable, skip check block offset");
  }
  /// block index
  if (OB_SUCCESS == err && cur_offset > trailer_->block_index_record_offset_)
  {
    TBSYS_LOG(WARN, "content overlapped block and block index, "
              "[block_end:%ld,block_index_beg:%ld]", cur_offset,
              trailer_->block_index_record_offset_);
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    cur_offset = trailer_->block_index_record_offset_;
  }
  if (OB_SUCCESS == err)
  {
    cur_offset += trailer_->block_index_record_size_;
    if (cur_offset > sstable_file_size)
    {
      TBSYS_LOG(WARN, "sstable file size not enough [block_index,need_size:%ld,actual_size:%ld]",
                cur_offset, sstable_file_size);
      err = OB_ERROR;
    }
  }

  /// bloom filter
  if (OB_SUCCESS == err
      && trailer_->bloom_filter_record_offset_ > 0
      && cur_offset > trailer_->bloom_filter_record_offset_)
  {
    TBSYS_LOG(WARN, "content overlapped block index and bloom filter, "
              "[block_index_end:%ld,bloom_filter_beg:%ld]", cur_offset,
              trailer_->bloom_filter_record_offset_);
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    cur_offset = trailer_->bloom_filter_record_offset_;
  }
  if (OB_SUCCESS == err)
  {
    cur_offset += trailer_->bloom_filter_record_size_;
    if (cur_offset > sstable_file_size)
    {
      TBSYS_LOG(WARN, "sstable file size not enough [bloom_filter,need_size:%ld,actual_size:%ld]",
                cur_offset, sstable_file_size);
      err = OB_ERROR;
    }
  }

  /// schema
  if (OB_SUCCESS == err && cur_offset > trailer_->schema_record_offset_)
  {
    TBSYS_LOG(WARN, "content overlapped bloom filter and schema, "
              "[bloom_filter_end:%ld,schema_beg:%ld]", cur_offset,
              trailer_->schema_record_offset_);
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    cur_offset = trailer_->schema_record_offset_;
  }
  if (OB_SUCCESS == err)
  {
    cur_offset += trailer_->schema_record_size_;
    if (cur_offset > sstable_file_size)
    {
      TBSYS_LOG(WARN, "sstable file size not enough [schema,need_size:%ld,actual_size:%ld]",
                cur_offset, sstable_file_size);
      err = OB_ERROR;
    }
  }

  /// trailer
  if (OB_SUCCESS == err && cur_offset > trailer_offset_.trailer_offset_)
  {
    TBSYS_LOG(WARN, "content overlapped schema and trailer, "
              "[schema_end:%ld,trailer_beg:%ld]", cur_offset,
              trailer_offset_.trailer_offset_);
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    cur_offset = trailer_offset_.trailer_offset_;
    if (cur_offset > sstable_file_size)
    {
      TBSYS_LOG(WARN, "sstable file size not enough [trailer,need_size:%ld,actual_size:%ld]",
                cur_offset, sstable_file_size);
      err = OB_ERROR;
    }
  }

  /// trailer offset
  if (OB_SUCCESS == err
      && cur_offset > sstable_file_size -static_cast<int64_t>(sizeof(ObSCTrailerOffset)))
  {
    TBSYS_LOG(WARN, "content overlapped trailer and trailer offset, "
              "[trailer_end:%ld,trailer_offset_beg:%ld]", cur_offset,
              sstable_file_size - sizeof(ObSCTrailerOffset));
    err = OB_ERROR;
  }
  return err;
}

void  oceanbase::chunkserver::ObSCSSTableChecker::dump()
{
#ifdef OB_SSTABLE_CHECKER_DEBUG
  TBSYS_LOG(INFO, "size_: %d \n"
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
      "row_value_store_style_: %s \n",
    trailer_->size_,
    trailer_->version_,
    trailer_->table_version_,
    trailer_->first_block_data_offset_,
    trailer_->block_count_,
    trailer_->block_index_record_offset_,
    trailer_->block_index_record_size_,
    trailer_->bloom_filter_hash_count_,
    trailer_->bloom_filter_record_offset_,
    trailer_->bloom_filter_record_size_,
    trailer_->schema_record_offset_,
    trailer_->schema_record_size_,
    trailer_->blocksize_,
    trailer_->row_count_,
    trailer_->sstable_checksum_,
    trailer_->first_table_id_,
    trailer_->frozen_time_,
    trailer_->range_record_offset_,
    trailer_->range_record_size_,
    trailer_->compressor_name_,
    trailer_->row_value_store_style_ == OB_SSTABLE_STORE_DENSE ? "dense" : "sparse");
#endif
  TBSYS_LOG(INFO, "range: %s", to_cstring(range_));
}

int main( int argc , char ** argv)
{
  int result = 0;
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  if (argc <= 1)
  {
    printf("Usage: %s [sstable file name list]\n", argv[0]);
    result = OB_INVALID_ARGUMENT;
  }
  for (int i = 1; i < argc && 0 == result; i++ )
  {
    ObSCSSTableChecker* checker = new ObSCSSTableChecker();
    if(checker != NULL)
    {
      result = checker->check(argv[i]);
      checker->dump();
      delete checker;
    }
    else
    {
      TBSYS_LOG(WARN, "failed to new ObSCSSTableChecker");
    }
    if (OB_SUCCESS != result)
    {
      TBSYS_LOG(WARN, "%s check fail", argv[i]);
    }
    else
    {
      TBSYS_LOG(INFO, "%s check success", argv[i]);
    }
  }
  return 0;
}
