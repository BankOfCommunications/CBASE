/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_record_header.cpp for record header. 
 *
 * Authors: 
 *    fanggang <fanggang@taobao.com>
 *    huating <huating.zmq@taobao.com>
 *
 */

#include "ob_record_header.h"

namespace oceanbase 
{ 
  namespace common 
  {
    ObRecordHeader::ObRecordHeader()
      :magic_(0), header_length_(0), version_(0), header_checksum_(0)
       , reserved_(0), data_length_(0), data_zlength_(0), data_checksum_(0)
    {
    }
    void ObRecordHeader::set_header_checksum()
    {  
      header_checksum_ = 0;
      int16_t checksum = 0;

      format_i64(magic_, checksum);
      checksum = checksum ^ header_length_;
      checksum = checksum ^ version_;
      checksum = checksum ^ header_checksum_;
      checksum = static_cast<int16_t>(checksum ^ reserved_);
      format_i32(data_length_, checksum);
      format_i32(data_zlength_, checksum);
      format_i64(data_checksum_, checksum);
      header_checksum_ = checksum;
    }

    int ObRecordHeader::check_header_checksum() const
    { 
      int ret           = OB_ERROR;
      int16_t checksum  = 0;

      format_i64(magic_, checksum);
      checksum = checksum ^ header_length_;
      checksum = checksum ^ version_;
      checksum = checksum ^ header_checksum_;
      checksum = static_cast<int16_t>(checksum ^ reserved_);
      format_i32(data_length_, checksum);
      format_i32(data_zlength_, checksum);
      format_i64(data_checksum_, checksum);
      if (checksum == 0)
      {
        ret = OB_SUCCESS;
      }

      return ret;
    }

    int ObRecordHeader::check_magic_num(const int16_t magic) const
    { 
      int ret = OB_SUCCESS;
      if(magic_ != magic)
      {
        ret = OB_ERROR;
      }
      return ret;
    }

    int ObRecordHeader::check_check_sum(const char *buf, const int64_t len) const
    { 
      int ret = OB_SUCCESS;

      /**
       * for network package, maybe there is only one recorder header 
       * without payload data, so the payload data lenth is 0, and 
       * checksum is 0. we skip this case and return success 
       */
      if (0 == len && (data_zlength_ != len || data_length_ != len || data_checksum_ != 0))
      {
        ret = OB_ERROR;
      }
      else 
      {
        if ((data_zlength_ != len) || (NULL == buf) || (len < 0))
        {
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        { 
          int64_t crc_check_sum = ob_crc64(buf, len);
          if (crc_check_sum !=  data_checksum_)
          { 
            ret = OB_ERROR;
          }
        }
      }

      return ret;
    }

    int ObRecordHeader::check_record(const char *buf, const int64_t len, 
                                     const int16_t magic)
    { 
      int ret = OB_SUCCESS;
      ObRecordHeader record_header;
      int64_t pos = 0;
      ret = record_header.deserialize(buf, len, pos);
      int record_len = static_cast<int32_t>(len - pos);

      if ((OB_SUCCESS == ret) && (record_len > 0)
          && (OB_SUCCESS == record_header.check_header_checksum())
          && (OB_SUCCESS == record_header.check_magic_num(magic))
          && (OB_SUCCESS == record_header.check_data_zlength_(record_len))
          && (OB_SUCCESS == record_header.check_check_sum(buf + pos, record_len)))
      {
        ret = OB_SUCCESS; 
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObRecordHeader::nonstd_check_record(const char *buf, const int64_t len, 
                                            const int16_t magic)
    { 
      int ret = OB_SUCCESS;
      ObRecordHeader record_header;
      int64_t pos = 0;
      ret = record_header.deserialize(buf, len, pos);
      int record_len = static_cast<int32_t>(len - pos);

      if (record_len <= 0 || record_len < record_header.data_zlength_)
      {
        ret = OB_ERROR;
      }
      else
      {
        record_len = record_header.data_zlength_;
      }

      if ((OB_SUCCESS == ret) && (record_len > 0)
          && (OB_SUCCESS == record_header.check_header_checksum())
          && (OB_SUCCESS == record_header.check_magic_num(magic))
          && (OB_SUCCESS == record_header.check_check_sum(buf + pos, record_len)))
      {
        ret = OB_SUCCESS; 
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObRecordHeader::check_record(const ObRecordHeader &record_header, 
                                     const char *payload_buf, 
                                     const int64_t payload_len, 
                                     const int16_t magic)
    { 
      int ret = OB_SUCCESS;

      if ((payload_len > 0) && (NULL != payload_buf)
          && (OB_SUCCESS == record_header.check_header_checksum())
          && (OB_SUCCESS == record_header.check_magic_num(magic))
          && (OB_SUCCESS == record_header.check_data_zlength_(static_cast<int32_t>(payload_len)))
          && (OB_SUCCESS == record_header.check_check_sum(payload_buf, payload_len)))
      {
        ret = OB_SUCCESS; 
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObRecordHeader::check_record(const char* ptr, const int64_t size, 
                                     const int16_t magic, ObRecordHeader& header, 
                                     const char*& payload_ptr, int64_t& payload_size)
    {
      int ret             = OB_SUCCESS;
      int64_t payload_pos = 0;

      if (NULL == ptr || size < OB_RECORD_HEADER_LENGTH)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      
      if (OB_SUCCESS == ret)
      {
        ret = header.deserialize(ptr, size, payload_pos);
      }

      if (OB_SUCCESS == ret)
      {
        payload_ptr = ptr + payload_pos;
        payload_size = size - payload_pos;
        if (header.header_length_ != payload_pos)
        {
          ret = OB_ERROR;
        }
        else
        {
          bool check = ((payload_size > 0)
                        && (OB_SUCCESS == header.check_magic_num(magic))
                        && (OB_SUCCESS == header.check_data_zlength_(static_cast<int32_t>(payload_size)))
                        && (OB_SUCCESS ==header.check_header_checksum())
                        && (OB_SUCCESS == header.check_check_sum(payload_ptr, payload_size))
                       );
          if (!check) ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObRecordHeader::get_record_header(const char* ptr, const int64_t size, 
                                          ObRecordHeader& header, 
                                          const char*& payload_ptr, 
                                          int64_t& payload_size)
    {
      int ret             = OB_SUCCESS;
      int64_t payload_pos = 0;

      if (NULL == ptr || size < OB_RECORD_HEADER_LENGTH)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      
      if (OB_SUCCESS == ret)
      {
        ret = header.deserialize(ptr, size, payload_pos);
      }

      if (OB_SUCCESS == ret)
      {
        payload_ptr = ptr + payload_pos;
        payload_size = size - payload_pos;
        if (header.header_length_ != payload_pos)
        {
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    DEFINE_SERIALIZE(ObRecordHeader)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len)) 
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && (OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, magic_))
          && (OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, header_length_))
          && (OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, version_))
          && (OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, header_checksum_))
          && (OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, reserved_))
          && (OB_SUCCESS == serialization::encode_i32(buf, buf_len, pos, data_length_))
          && (OB_SUCCESS == serialization::encode_i32(buf, buf_len, pos, data_zlength_))
          && (OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, data_checksum_)))
      { 
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObRecordHeader)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size > data_len) 
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && (OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &magic_))
          && (OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &header_length_))
          && (OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &version_))
          && (OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &header_checksum_))
          && (OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &reserved_))
          && (OB_SUCCESS == serialization::decode_i32(buf, data_len, pos, &data_length_))
          && (OB_SUCCESS == serialization::decode_i32(buf, data_len, pos, &data_zlength_))
          && (OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &data_checksum_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_ERROR;
      }        

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObRecordHeader)
    {
      return (serialization::encoded_length_i16(magic_)
        + serialization::encoded_length_i16(header_length_) 
        + serialization::encoded_length_i16(version_)
        + serialization::encoded_length_i16(header_checksum_) 
        + serialization::encoded_length_i64(reserved_)
        + serialization::encoded_length_i32(data_length_) 
        + serialization::encoded_length_i32(data_zlength_) 
        + serialization::encoded_length_i64(data_checksum_));
    }
  } // end namespace common
} // end namespace oceanbase
