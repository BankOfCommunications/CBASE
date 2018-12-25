/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_schema.cpp for persistent schema. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "common/serialization.h"
#include "ob_sstable_schemaV1.h"

namespace oceanbase 
{ 
  namespace sstable 
  {
    using namespace common;
    using namespace common::serialization;

    ObSSTableSchemaV1::ObSSTableSchemaV1()
    {
      reset();
    }

    ObSSTableSchemaV1::~ObSSTableSchemaV1()
    {
    }

    int64_t ObSSTableSchemaV1::get_column_count() const
    {
      return schema_header_.column_count_;
    }

    const ObSSTableSchemaColumnDefV1* ObSSTableSchemaV1::get_column_def(
      const int32_t index) const
    {
      const ObSSTableSchemaColumnDefV1* ret = NULL;

      if (index >= 0 && index < schema_header_.column_count_)
      {
        ret = &column_def_[index];
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%d, column_count=%d", 
                  index, schema_header_.column_count_);
      }

      return ret;
    }

    DEFINE_SERIALIZE(ObSSTableSchemaHeaderV1)
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
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, column_count_))
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, reserved16_[0]))
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, reserved16_[1]))
          && (OB_SUCCESS == encode_i16(buf, buf_len, pos, reserved16_[2])))
      { 
        ret = OB_SUCCESS;
      }
       else
      {
         TBSYS_LOG(WARN, "failed to serialize schema header");
        ret = OB_ERROR;
      }

      return ret;
    }
    
    DEFINE_DESERIALIZE(ObSSTableSchemaHeaderV1)
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
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &column_count_))
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &reserved16_[0]))
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &reserved16_[1]))
          && (OB_SUCCESS == decode_i16(buf, data_len, pos, &reserved16_[2])))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialize schema header");
        ret = OB_ERROR;
      }        
    
      return ret;
    }
    
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaHeaderV1)
    {
      return (encoded_length_i16(column_count_) 
              + encoded_length_i16(reserved16_[0]) 
              + encoded_length_i16(reserved16_[1])
              + encoded_length_i16(reserved16_[2]));
    }

    DEFINE_SERIALIZE(ObSSTableSchemaColumnDefV1)
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
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, column_name_id_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, column_value_type_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, reserved_)))
      { 
        ret = OB_SUCCESS;
      }
       else
      {
        TBSYS_LOG(WARN, "failed to serialzie schema def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    DEFINE_DESERIALIZE(ObSSTableSchemaColumnDefV1)
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
          && OB_SUCCESS == decode_i64(buf, data_len, pos, 
                                      reinterpret_cast<int64_t*>(&column_name_id_))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &column_value_type_))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &reserved_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialize schema column def");
        TBSYS_LOG(WARN, "failed to serialzie schema column def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }        
    
      return ret;
    }
    
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaColumnDefV1)
    {
      return (encoded_length_i64(column_name_id_) 
              + encoded_length_i32(column_value_type_)
              + encoded_length_i32(reserved_));
    }

    void ObSSTableSchemaV1::reset()
    {
      memset(column_def_, 0, sizeof(ObSSTableSchemaColumnDefV1) 
             * OB_MAX_COLUMN_NUMBER);
      memset(&schema_header_, 0, sizeof(ObSSTableSchemaHeaderV1));
    }

    int ObSSTableSchemaV1::add_column_def(ObSSTableSchemaColumnDefV1& column_def)
    {
      int ret = OB_SUCCESS;

      if (schema_header_.column_count_ < OB_MAX_COLUMN_NUMBER)
      {
        //ensure the column id is in order
        if (column_def_[schema_header_.column_count_].column_name_id_ 
            < column_def.column_name_id_)
        {
          column_def_[schema_header_.column_count_++] = column_def;
        }
        else
        {
          TBSYS_LOG(WARN, "column id must be in ascending order, prev_column_id=%lu, "
                          "current_column_id=%lu", 
                    column_def_[schema_header_.column_count_].column_name_id_,
                    column_def.column_name_id_);
          ret = OB_ERROR;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "no space to store more column def, "
                        "max_columns_count=%ld, column_count=%d", 
                  OB_MAX_COLUMN_NUMBER, schema_header_.column_count_ );
        ret = OB_ERROR;
      }

      return ret;      
    }

    int32_t ObSSTableSchemaV1::find_column_id(const uint64_t column_id) const
    {
      int32_t ret     = -1;
      int64_t left    = 0; 
      int64_t middle  = 0;
      int64_t right   = schema_header_.column_count_ - 1;
    
      while (left <= right)
      {
        middle = (left + right) / 2;
        if (column_def_[middle].column_name_id_ > column_id)
        {
          right = middle - 1;
        }
        else if (column_def_[middle].column_name_id_ < column_id)
        {
          left = middle + 1;
        }
        else
        {
          ret = static_cast<int32_t>(middle);
          break;
        }
      }

      return ret;
    }

    DEFINE_SERIALIZE(ObSSTableSchemaV1)
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

      if (OB_SUCCESS == ret)
      {
        ret = schema_header_.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to serialize schema header, buf=%p, "
                          "buf_len=%ld, pos=%ld, column_count=%d",
                    buf, buf_len, pos, schema_header_.column_count_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; OB_SUCCESS == ret 
             && i < schema_header_.column_count_; ++i)
        {
          ret = column_def_[i].serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to serialize schema column def, buf=%p, "
                            "buf_len=%ld, pos=%ld, column index=%ld, "
                            "column_count=%d",
                      buf, buf_len, pos, i, schema_header_.column_count_);
            break;
          }
        }          
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchemaV1)
    {
      int ret = OB_SUCCESS;

      if (NULL == buf || data_len <= 0 || pos > data_len) 
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = schema_header_.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to deserialize schema header, buf=%p, "
                          "data_len=%ld, pos=%ld, column_count=%d",
                    buf, data_len, pos, schema_header_.column_count_);
        }
      }

      if (OB_SUCCESS == ret && schema_header_.column_count_ > 0)
      {
        for (int64_t i = 0; OB_SUCCESS == ret 
             && i < schema_header_.column_count_; ++i)
        {
          ret = column_def_[i].deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to deserialize schema column def, buf=%p, "
                            "data_len=%ld, pos=%ld, column index=%ld, "
                            "column_count=%d",
                      buf, data_len, pos, i, schema_header_.column_count_);
            break;
          }
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaV1)
    {
      int64_t total_size  = 0;

      total_size += schema_header_.get_serialize_size();
      total_size += column_def_[0].get_serialize_size() 
                    * schema_header_.column_count_;

      return total_size;    
    }
  } // end namespace sstable
} // end namespace oceanbase
