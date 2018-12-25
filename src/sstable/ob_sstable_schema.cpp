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
 *   fangji  <fangji.hcm@taobao.com>
 *
 */
#include <algorithm>
#include <tblog.h>
#include "common/serialization.h"
#include "common/ob_schema_manager.h"
#include "ob_sstable_schema.h"
#include "ob_sstable_trailer.h"

namespace oceanbase
{
  namespace
  {
    common::ObMergerSchemaManager* global_sstable_schema_manager = NULL;
  }
  namespace sstable
  {
    using namespace common;
    void set_global_sstable_schema_manager(common::ObMergerSchemaManager* manager)
    {
      global_sstable_schema_manager = manager;
    }

    common::ObMergerSchemaManager* get_global_sstable_schema_manager(void)
    {
      return global_sstable_schema_manager;
    }

    int get_table_schema_from_global(const uint64_t table_id,
        const ObSchemaManagerV2* &schema, const ObTableSchema* &table_schema)
    {
      int iret = OB_SUCCESS;
      schema = NULL;
      if (NULL == global_sstable_schema_manager)
      {
        TBSYS_LOG(ERROR, "old fashion sstable, NOT set SCHEMA compatible info, table=%ld", table_id);
        iret = OB_ERROR;
      }
      else if (NULL == (schema = global_sstable_schema_manager->get_schema(table_id)))
      {
        TBSYS_LOG(ERROR, "old fashion sstable, can NOT get SCHEMA compatible info, table=%ld", table_id);
        iret = OB_ERROR;
      }
      else if (NULL == (table_schema = schema->get_table_schema(table_id)))
      {
        TBSYS_LOG(ERROR, "old fashion sstable, can NOT get table %ld SCHEMA ", table_id);
        iret = OB_ERROR;
      }
      return iret;
    }

    int release_global_sstable_schema_manager(const ObSchemaManagerV2* schema)
    {
      int iret = OB_SUCCESS;
      if (NULL == global_sstable_schema_manager)
      {
        TBSYS_LOG(WARN, "old fashion sstable, NOT set SCHEMA compatible info.");
        iret = OB_ERROR;
      }
      else if (OB_SUCCESS != (iret =
            global_sstable_schema_manager->release_schema(schema)))
      {
        TBSYS_LOG(WARN, "old fashion sstable, release schema :%ld error: %d.",
            schema->get_version(), iret);
      }
      return iret;
    }

    int get_global_schema_rowkey_info(const uint64_t table_id, ObRowkeyInfo& rowkey_info)
    {
      int iret = OB_SUCCESS;
      const ObSchemaManagerV2* schema = NULL;
      const ObTableSchema* table_schema = NULL;
      iret = get_table_schema_from_global(table_id, schema, table_schema);
      if (OB_SUCCESS == iret && NULL != table_schema)
      {
        // copy rowkey info
        rowkey_info = table_schema->get_rowkey_info();
      }
      if (NULL != schema)
      {
        int err = release_global_sstable_schema_manager(schema);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "release global sstable schema failed");
          if (OB_SUCCESS == iret)
          {
            iret = err;
          }
        }
      }
      return iret;
    }
  }

  namespace sstable
  {
    using namespace std;
    using namespace common;
    using namespace common::serialization;

    int64_t CombineIdKey::hash() const
    {
      hash::hash_func<uint64_t> hash;
      return hash(table_id_) + hash(sec_id_);
    }

    bool CombineIdKey::operator==(const CombineIdKey& key) const
    {
      bool ret = false;
      if (table_id_ == key.table_id_ && sec_id_ == key.sec_id_)
      {
        ret = true;
      }
      return ret;
    }

    ObSSTableSchema::ObSSTableSchema():version_(OB_SSTABLE_SCHEMA_VERSION_TWO),column_def_(column_def_array_), hash_init_(false),
    curr_group_head_(0)
    {
      memset(&column_def_array_, 0, sizeof(ObSSTableSchemaColumnDef) * OB_MAX_COLUMN_NUMBER);
      memset(&table_trun_array_, 0, sizeof(ObSSTableSchemaTableDef) * OB_MAX_TABLE_NUMBER); //add zhaoqiong [Truncate Table]:20160318
      memset(&schema_header_, 0, sizeof(ObSSTableSchemaHeaderV2));
    }

    ObSSTableSchema::~ObSSTableSchema()
    {
      if (NULL != column_def_ && column_def_array_ != column_def_)
      {
        ob_free(column_def_);
        column_def_ = NULL;
      }

      if (hash_init_)
      {
        table_schema_hashmap_.destroy();
        group_schema_hashmap_.destroy();
        group_index_hashmap_.destroy();
        table_rowkey_hashmap_.destroy();
      }
    }

    //In order to compatible with v0.1.0
    //if total_column_count equals zero, use column_count instead
    const int64_t ObSSTableSchema::get_column_count() const
    {
      int64_t count = 0;
      if (0 == schema_header_.total_column_count_)
      {
        count = schema_header_.column_count_;
      }
      else
      {
        count = schema_header_.total_column_count_;
      }
      return count;
    }


    //add zhaoqiong [Truncate Table]:20160318:b
    const int32_t ObSSTableSchema::get_trun_table_count() const
    {
      return schema_header_.trun_table_count_;
    }
    //add:e

    int ObSSTableSchema::get_rowkey_column_count(const uint64_t table_id, int64_t& rowkey_column_count) const
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }
      else
      {
        ValueSet rowkey_set;
        int ret_hash = table_rowkey_hashmap_.get(table_id, rowkey_set);
        if ( hash::HASH_EXIST != ret_hash )
        {
          TBSYS_LOG(ERROR, "Get (table_id=%lu) from table rowkey hashmap failed", table_id);
          ret = OB_ERROR;
        }
        else
        {
          rowkey_column_count = rowkey_set.size_;
        }
      }
      return ret;
    }

    bool ObSSTableSchema::is_binary_rowkey_format(const uint64_t table_id) const
    {
      int64_t rowkey_column_count = 0;
      get_rowkey_column_count(table_id, rowkey_column_count);
      return 0 == rowkey_column_count;
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_rowkey_column(
        const uint64_t table_id, const uint64_t rowkey_seq) const
    {
      const ObSSTableSchemaColumnDef* def = NULL;
      ValueSet rowkey_set;
      int ret_hash = table_rowkey_hashmap_.get(table_id, rowkey_set);
      if ( hash::HASH_EXIST != ret_hash )
      {
        TBSYS_LOG(WARN, "Get (table_id=%lu) from table rowkey hashmap failed", table_id);
      }
      else
      {
        int32_t offset = -1;
        for (int32_t index = 0; index < rowkey_set.size_; ++index)
        {
          if (0 == index)
          {
            offset = rowkey_set.head_offset_;
          }
          else if (NULL != def)
          {
            offset = def->rowkey_column_next_;
          }

          if (offset < 0 || offset >= get_column_count())
          {
            break;
          }
          else
          {
            def = column_def_ + offset;

            if (def->rowkey_seq_ == rowkey_seq)
            {
              break;
            }

          }

        }
      }

      return def;

    }

    int ObSSTableSchema::get_rowkey_columns(const uint64_t table_id,
        const ObSSTableSchemaColumnDef** defs, int64_t& size) const
    {
      int ret = OB_SUCCESS;
      ValueSet rowkey_set;
      const ObSSTableSchemaColumnDef* def = NULL;
      int ret_hash = table_rowkey_hashmap_.get(table_id, rowkey_set);
      if ( hash::HASH_EXIST != ret_hash )
      {
        TBSYS_LOG(WARN, "Get (table_id=%lu) from table rowkey hashmap failed", table_id);
      }
      else
      {
        int32_t offset = -1;
        int32_t cnt = 0;
        for (int32_t index = 0; index < rowkey_set.size_; ++index)
        {
          if (0 == index)
          {
            offset = rowkey_set.head_offset_;
          }
          else if (NULL != def)
          {
            offset = def->rowkey_column_next_;
          }

          if (offset < 0 || offset >= get_column_count() || cnt > size)
          {
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          else
          {
            def = column_def_ + offset;
            defs[cnt++] = def;
          }
        }

        if (OB_SUCCESS == ret)
        {
          size = cnt;
        }
      }

      return ret;

    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_column_def(
      const int32_t index) const
    {
      const ObSSTableSchemaColumnDef* ret = NULL;
      if (index >= 0 && index < get_column_count())
      {
        ret = &column_def_[index];
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%d, column_count=%ld",
                  index, get_column_count());
      }

      return ret;
    }


    //add zhaoqiong [Truncate Table]:20160318:b
    //trun table def
    const ObSSTableSchemaTableDef* ObSSTableSchema::get_truncate_def(const uint64_t table_id) const
    {
      const ObSSTableSchemaTableDef* ret = NULL;
      for (int64_t idx = 0; idx < schema_header_.trun_table_count_ ; idx ++)
      {
        if (table_trun_array_[idx].table_id_ == table_id )
        {
          ret = &table_trun_array_[table_id];
          break;
        }
      }
      return ret;
    }

    const ObSSTableSchemaTableDef* ObSSTableSchema::get_truncate_def(const int32_t idx) const
    {
      const ObSSTableSchemaTableDef* ret = NULL;
      if (idx >=0 && idx < schema_header_.trun_table_count_ )
      {
        ret = &table_trun_array_[idx];
      }
      return ret;
    }
    //add:e

    DEFINE_SERIALIZE(ObSSTableSchemaHeader)
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
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_count_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved16_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, total_column_count_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialize schema header");
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchemaHeader)
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
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &column_count_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved16_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &total_column_count_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialize schema header");
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaHeader)
    {
      return (encoded_length_i16(column_count_)
              + encoded_length_i16(reserved16_)
              + encoded_length_i32(total_column_count_));
    }

    DEFINE_SERIALIZE(ObSSTableSchemaHeaderV2)
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
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_count_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved16_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, total_column_count_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, trun_table_count_)))) //add zhaoqiong [Truncate Table]:20160318
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialize schema header");
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchemaHeaderV2)
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
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &column_count_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved16_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &total_column_count_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &trun_table_count_)))) //add zhaoqiong [Truncate Table]:20160318
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialize schema header");
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaHeaderV2)
    {
      return (encoded_length_i16(column_count_)
              + encoded_length_i16(reserved16_)
              + encoded_length_i32(total_column_count_)
              + encoded_length_i32(trun_table_count_)); //add zhaoqiong [Truncate Table]:20160318:b
    }


    //add zhaoqiong [Truncate Table]:20160318:b
    DEFINE_SERIALIZE(ObSSTableSchemaTableDef)
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
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, table_id_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, trun_timestamp_))))
      {
        //do nothing here
      }
       else
      {
        TBSYS_LOG(WARN, "failed to serialzie schema def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchemaTableDef)
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
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos,
                                              reinterpret_cast<int32_t*>(&table_id_))))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos,
                                              reinterpret_cast<int64_t*>(&trun_timestamp_)))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie schema column def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaTableDef)
    {
      return (encoded_length_i32(table_id_)
              + encoded_length_i64(trun_timestamp_));
    }
    //add:e

    DEFINE_SERIALIZE(ObSSTableSchemaColumnDef)
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
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_group_id_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, rowkey_seq_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_name_id_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, column_value_type_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, table_id_))))
      {
        //do nothing here
      }
       else
      {
        TBSYS_LOG(WARN, "failed to serialzie schema def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchemaColumnDef)
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
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos,
                                              reinterpret_cast<int16_t*>(&column_group_id_))))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos,
                                              reinterpret_cast<int16_t*>(&rowkey_seq_))))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos,
                                              reinterpret_cast<int16_t*>(&column_name_id_))))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &column_value_type_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos,
                                              reinterpret_cast<int32_t*>(&table_id_)))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie schema column def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaColumnDef)
    {
      return (encoded_length_i16(reserved_)
              + encoded_length_i16(column_group_id_)
              + encoded_length_i16(rowkey_seq_)
              + encoded_length_i16(column_name_id_)
              + encoded_length_i32(column_value_type_)
              + encoded_length_i32(table_id_));
    }

    void ObSSTableSchema::reset()
    {
      memset(&column_def_array_, 0, sizeof(ObSSTableSchemaColumnDef) * OB_MAX_COLUMN_NUMBER);
      memset(&schema_header_, 0, sizeof(ObSSTableSchemaHeader));
      if (column_def_ != column_def_array_ && column_def_ != NULL)
      {
        ob_free(column_def_);
      }
      column_def_ = column_def_array_;

      if (hash_init_)
      {
        table_schema_hashmap_.clear();
        group_schema_hashmap_.clear();
        group_index_hashmap_.clear();
        table_rowkey_hashmap_.clear();
      }
    }

    void ObSSTableSchema::dump() const
    {
      int64_t column_count = get_column_count();
      for (int64_t i = 0; i < column_count; ++i)
      {
        ObSSTableSchemaColumnDef* def = column_def_ + i;
        TBSYS_LOG(INFO, "column %ld: table:%d, group:%d, rowkey seq:%d, id:%d, type:%d, offset:%d",
            i, def->table_id_, def->column_group_id_, def->rowkey_seq_, def->column_name_id_,
            def->column_value_type_, def->offset_in_group_);
      }
    }

    int ObSSTableSchema::update_hashmaps(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = update_table_schema_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update table schema hashmap failed. table_id=%u, column_group_id=%u,"
                  "column_name_id=%u", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
      }
      else if (OB_SUCCESS != (ret = update_group_schema_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update group schema hashmap failed. table_id=%u, column_group_id=%u,"
                  "column_name_id=%u", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
      }
      else if (OB_SUCCESS != (ret = update_group_index_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update group index  hashmap failed. table_id=%u, column_group_id=%u,"
                  "column_name_id=%u", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
      }
      else if (OB_SUCCESS != (ret = update_table_rowkey_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update rowkey hashmap failed. table_id=%u, column_group_id=%u,"
                  "column_name_id=%u", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
      }
      return ret;
    }

    int ObSSTableSchema::update_table_rowkey_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }
      else
      {
        ValueSet rowkey_set;
        int64_t size = get_column_count();
        int ret_hash = 0;
        const ObSSTableSchemaColumnDef * last = NULL;

        if ( 0 == size )
        {
          // first table, group
          rowkey_set.head_offset_ = static_cast<int32_t>(size);
          rowkey_set.tail_offset_ = static_cast<int32_t>(size);
          rowkey_set.size_ = column_def.rowkey_seq_ > 0 ? 1 : 0;
          ret_hash = table_rowkey_hashmap_.set(column_def.table_id_, rowkey_set, 1); //overwrite
        }
        else if (column_def.rowkey_seq_ > 0)
        {
          last = &column_def_[size - 1];
          if (last->table_id_ != column_def.table_id_)
          {
            // different table, recount.
            rowkey_set.head_offset_ = static_cast<int32_t>(size);
            rowkey_set.tail_offset_ = static_cast<int32_t>(size);
            rowkey_set.size_ = 1;
            ret_hash = table_rowkey_hashmap_.set(column_def.table_id_, rowkey_set, 1); //overwrite
          }
          else if (last->column_group_id_ != column_def.column_group_id_)
          {
            // same table, but different column group, ignore;
          }
          else
          {
            // same table, same column group;
            ret_hash = table_rowkey_hashmap_.get(column_def.table_id_, rowkey_set);
            if (hash::HASH_EXIST != ret_hash)
            {
              TBSYS_LOG(ERROR, "get rowkey hashmap table=%d, column group =%d error",
                  column_def.table_id_, column_def.column_group_id_);
              ret = OB_ERROR;
            }
            else
            {
              ObSSTableSchemaColumnDef* def = column_def_ + rowkey_set.tail_offset_;
              def->rowkey_column_next_ = static_cast<int32_t>(size);
              rowkey_set.tail_offset_ = static_cast<int32_t>(size);
              ++rowkey_set.size_;
              ret_hash = table_rowkey_hashmap_.set(column_def.table_id_, rowkey_set, 1); //overwrite
            }
          }
        }

        if (-1 == ret_hash)
        {
          TBSYS_LOG(ERROR, "insert table_rowkey_hashmap_ error,"
              "table_id=%u, column_group_id=%u",
              column_def.table_id_, column_def.column_group_id_);
          ret = OB_ERROR;
        }

      }
      return ret;
    }

    int ObSSTableSchema::update_table_schema_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ValueSet table_value;
        int64_t size = get_column_count();
        const ObSSTableSchemaColumnDef * last = NULL;
        if (size > 0)
        {
          last = &column_def_[size - 1];
        }
        int ret_hash = 0;

        if (0 == size || (NULL != last && column_def.table_id_ != last->table_id_))
        {
          table_value.head_offset_ = static_cast<int32_t>(size);
          table_value.tail_offset_ = static_cast<int32_t>(size);
          table_value.size_ = 1;
          ret_hash = table_schema_hashmap_.set(column_def.table_id_, table_value);
          if (-1 == ret_hash)
          {
            TBSYS_LOG(ERROR, "Insert table schema hashmap faied"
                      "key = %u", column_def.table_id_);
            ret = OB_ERROR;
          }
        }
        else if (NULL != last && column_def.column_group_id_ != last->column_group_id_)
        {
          ret_hash = table_schema_hashmap_.get(column_def.table_id_, table_value);
          if (-1 == ret_hash || hash::HASH_NOT_EXIST == ret_hash)
          {
            TBSYS_LOG(ERROR, "Get (table_id=%u) from table schema hashmap failed", column_def.table_id_);
            ret = OB_ERROR;
          }
          else if (hash::HASH_EXIST == ret_hash)
          {
            //update table value and set table group next of column def
            ObSSTableSchemaColumnDef* def = column_def_ + table_value.tail_offset_;
            def->table_group_next_   = static_cast<int32_t>(size);
            table_value.tail_offset_ = static_cast<int32_t>(size);
            ++table_value.size_;
            ret_hash = table_schema_hashmap_.set(column_def.table_id_, table_value, 1); //overwrite
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "insert table_schema_hashmap_ failed table_id=%u", column_def.table_id_);
              ret = OB_ERROR;
            }
          }
        }
      }
      return ret;
    }

    int ObSSTableSchema::update_group_schema_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        CombineIdKey group_key;
        group_key.table_id_ = column_def.table_id_;
        group_key.sec_id_   = column_def.column_group_id_;
        ValueSet group_value;
        int64_t size = get_column_count();
        int ret_hash = 0;

        if (OB_SUCCESS == ret)
        {
          ret_hash = group_schema_hashmap_.get(group_key, group_value);
          if (-1 == ret_hash)
          {
            TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_group_id=%lu)"
                      "from group schema hashmap failed", group_key.table_id_, group_key.sec_id_);
            ret = OB_ERROR;
          }
          else if (hash::HASH_NOT_EXIST == ret_hash)
          {
            group_value.head_offset_ = static_cast<int32_t>(size);
            group_value.size_ = 1;
            ret_hash = group_schema_hashmap_.set(group_key, group_value);
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "insert group_schemma_hashmap_ failed "
                        "key(table_id=%lu, column_group_id=%lu", group_key.table_id_, group_key.sec_id_);
              ret = OB_ERROR;
            }
          }
          else if (hash::HASH_EXIST == ret_hash)
          {
            ++group_value.size_;
            ret_hash = group_schema_hashmap_.set(group_key, group_value, 1); //overwrite
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "insert group_schemma_hashmap_ failed "
                        "key(table_id=%lu, column_group_id=%lu", group_key.table_id_, group_key.sec_id_);
              ret = OB_ERROR;
            }
          }
        }
      }
      return ret;
    }

    int ObSSTableSchema::update_group_index_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        CombineIdKey group_index_key;
        group_index_key.table_id_ = column_def.table_id_;
        group_index_key.sec_id_   = column_def.column_name_id_;
        ValueSet index_value;
        int64_t size = get_column_count();
        int ret_hash = 0;

        if (OB_SUCCESS == ret)
        {
          ret_hash = group_index_hashmap_.get(group_index_key, index_value);
          if (-1 == ret_hash)
          {
            TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_name_id=%lu)"
                      "from group index hashmap failed",
                      group_index_key.table_id_, group_index_key.sec_id_);
            ret = OB_ERROR;
          }
          else if (hash::HASH_NOT_EXIST == ret_hash)
          {
            index_value.head_offset_ = static_cast<int32_t>(size);
            index_value.tail_offset_ = static_cast<int32_t>(size);
            index_value.size_ = 1;
            ret_hash = group_index_hashmap_.set(group_index_key, index_value);
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_name_id=%lu)"
                        "from group index hashmap failed",
                        group_index_key.table_id_, group_index_key.sec_id_);
              ret = OB_ERROR;
            }
          }
          else if (hash::HASH_EXIST == ret_hash)
          {
            ObSSTableSchemaColumnDef* def = column_def_ + index_value.tail_offset_;
            def->group_column_next_  = static_cast<int32_t>(size);
            index_value.tail_offset_ = static_cast<int32_t>(size);
            ++index_value.size_;
            ret_hash = group_index_hashmap_.set(group_index_key, index_value, 1); //overwrite
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_name_id=%lu)"
                        "from group index hashmap failed",
                        group_index_key.table_id_, group_index_key.sec_id_);
              ret = OB_ERROR;
            }
          }
        }
      }
      return ret;
    }

    int ObSSTableSchema::add_column_def(const ObSSTableSchemaColumnDef& def)
    {
      int ret = OB_SUCCESS;

      int32_t size = get_column_count();
      // copy object for modify (ROWKEY column group id);
      ObSSTableSchemaColumnDef column_def = def;
      if (column_def.rowkey_seq_ != 0)
      {
        // set rowkey column in special column group, not in any user define column groups.
        column_def.column_group_id_ = ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID;
      }


      if (DEFAULT_COLUMN_DEF_SIZE <= size)
      {
        TBSYS_LOG(WARN, "Can not add any more column_def, column_count=%d", size);
        ret = OB_ERROR;
      }
      //be sure that column_def is in order of (table_id, column_group_id, column_name_id)
      ObSSTableSchemaColumnDefCompare compare;
      if ( OB_SUCCESS == ret && 0 < size && false == compare(column_def_[size - 1], column_def) )
      {
        TBSYS_LOG(WARN, "column def must be order in (table_id, group_id, column_id, rowkey_seq),"
                  "previous def is (%u,%hu,%u,%u)\t"
                  "current def is (%u,%hu,%u,%u)",
                  column_def_[size-1].table_id_, column_def_[size-1].column_group_id_,
                  column_def_[size-1].column_name_id_, column_def_[size-1].rowkey_seq_,
                  column_def.table_id_, column_def.column_group_id_,
                  column_def.column_name_id_, column_def.rowkey_seq_);
        ret = OB_ERROR;
      }

      if ( OB_SUCCESS == ret)
      {
        uint64_t column_group_id = 0;
        int64_t offset = find_offset_first_column_group_schema(
            column_def.table_id_, column_def.column_name_id_, column_group_id);
        // check column if is duplicate rowkey column?
        if (column_def.column_group_id_ == ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID)
        {
          if (offset >= 0)
          {
            TBSYS_LOG(WARN, "rowkey column(%u,%hu,%u,%u) duplicate with offset=%ld",
                  column_def.table_id_, column_def.column_group_id_,
                  column_def.column_name_id_, column_def.rowkey_seq_, offset);
            ret = OB_ERROR;
          }
        }
        else
        {
          if (offset >= 0 && column_group_id == ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID)
          {
            TBSYS_LOG(WARN, "normal column(%u,%hu,%u,%u) duplicate with rowkey column offset=%ld",
                  column_def.table_id_, column_def.column_group_id_,
                  column_def.column_name_id_, column_def.rowkey_seq_, offset);
            ret = OB_ERROR;
          }
        }
      }

      //create hashmaps
      if (OB_SUCCESS == ret && !hash_init_)
      {
        ret = create_hashmaps();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "create hashmaps failed");
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (common::OB_MAX_COLUMN_NUMBER <= size
            && column_def_ == column_def_array_)
        {
          column_def_ = reinterpret_cast<ObSSTableSchemaColumnDef*>
            (ob_malloc(sizeof(ObSSTableSchemaColumnDef)*(DEFAULT_COLUMN_DEF_SIZE), ObModIds::OB_SSTABLE_READER));
          if (NULL == column_def_)
          {
            TBSYS_LOG(WARN, "failed to alloc memory column_def_=%p", column_def_);
            ret = OB_ERROR;
          }
          else
          {
            memcpy(column_def_, column_def_array_, sizeof(ObSSTableSchemaColumnDef) * size);
          }
        }

        //set curr_group_head_
        if (OB_SUCCESS == ret)
        {
          if ( 0 == size )
          {
            // first table, column group
            curr_group_head_ = size;
          }
          else if (column_def.table_id_ != column_def_[size - 1].table_id_
              || column_def.column_group_id_ != column_def_[size - 1].column_group_id_)
          {
            // table or column group changed.
            // set current size as current group start offset.
            curr_group_head_ = size;
          }

        }

        //set offset in group of column def
        if (OB_SUCCESS == ret)
        {
          column_def.offset_in_group_ = static_cast<int32_t>(size - curr_group_head_);
        }

        if (OB_SUCCESS == ret)
        {
          ret = update_hashmaps(column_def);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "update hashmaps failed ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          column_def_[schema_header_.total_column_count_++] = column_def;
        }
      }
      return ret;
    }

//add zhaoqiong [Truncate Table]:20160318:b
    int ObSSTableSchema::add_table_def(const ObSSTableSchemaTableDef& table_def)
    {
      int ret = OB_SUCCESS;

      int64_t size = get_trun_table_count();
      ObSSTableSchemaTableDef def = table_def;

      if (common::OB_MAX_TABLE_NUMBER <= size)
      {
        TBSYS_LOG(WARN, "is largert than the table count");
        ret = OB_ERROR;
      }
      else
      {
        ObSSTableSchemaTableDefCompare compare;
        if (0 < size && false == compare(table_trun_array_[size - 1], def))
        {
          TBSYS_LOG(WARN, "compare error");
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        table_trun_array_[schema_header_.trun_table_count_] = def;
        schema_header_.trun_table_count_ ++;
        TBSYS_LOG(DEBUG, "add table truncate def succ, trun_table_count= %d", schema_header_.trun_table_count_);
      }
      return ret;
    }
//add:e

    int ObSSTableSchema::get_table_column_groups_id(const uint64_t table_id,
                                                    uint64_t* group_ids, int64_t & size)const
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid arguments table_id = %lu", table_id);
        ret = OB_ERROR;
      }
      else if (0 != get_column_count())
      {
        if (hash_init_)
        {
          ValueSet group_id_set;
          int hash_get = 0;
          int32_t offset = 0;
          int group_index = 0;
          ObSSTableSchemaColumnDef* def = NULL;

          hash_get = table_schema_hashmap_.get(table_id, group_id_set);
          if (-1 == hash_get)
          {
            TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
            ret = OB_ERROR;
          }
          else if (hash::HASH_NOT_EXIST == hash_get)
          {
            size = 0;
            ret = OB_ERROR;
          }
          else if (hash::HASH_EXIST == hash_get)
          {
            for (int index = 0; index < group_id_set.size_; ++index)
            {
              if (0 == index)
              {
                offset = group_id_set.head_offset_;
              }
              else
              {
                offset = def->table_group_next_;
              }

              def = column_def_ + offset;

              if (offset < 0 || offset >= get_column_count())
              {
                TBSYS_LOG(ERROR, "offset overflosw offset=%d total column count=%ld",
                    offset, get_column_count());
                ret = OB_ERROR;
                break;
              }
              else if (ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID != def->column_group_id_)
              {
                if (group_index >= size)
                {
                  ret = OB_SIZE_OVERFLOW;
                  break;
                }
                else
                {
                  group_ids[group_index++] = def->column_group_id_;
                }
              }
            }
            size = group_index;
          }
          else
          {
            TBSYS_LOG(INFO, "hashmap not init yet");
            size = 0;
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    uint64_t ObSSTableSchema::get_table_first_column_group_id(const uint64_t table_id) const
    {
      uint64_t column_group_id = 0;
      if (OB_INVALID_ID != table_id && 0 != table_id && get_column_count() > 0)
      {

        if (hash_init_)
        {
          ValueSet group_id_set;
          int hash_get = 0;
          int32_t offset = 0;
          ObSSTableSchemaColumnDef* def = NULL;

          hash_get = table_schema_hashmap_.get(table_id, group_id_set);
          if (hash::HASH_EXIST == hash_get)
          {
            for (int index = 0; index < group_id_set.size_; ++index)
            {
              if (0 == index)
              {
                offset = group_id_set.head_offset_;
              }
              else
              {
                offset = def->table_group_next_;
              }

              def = column_def_ + offset;
              if (offset >= 0 && offset < get_column_count()
                  && ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID != def->column_group_id_)
              {
                column_group_id = def->column_group_id_;
              }
            }
          }
        }
      }
      return column_group_id;
    }

    bool ObSSTableSchema::is_table_exist(const uint64_t table_id) const
    {
      int bret = false;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid arguments table_id = %lu", table_id);
      }
      else if (0 != get_column_count())
      {
        if (hash_init_)
        {
          ValueSet group_id_set;
          int hash_get = table_schema_hashmap_.get(table_id, group_id_set);
          if (-1 == hash_get)
          {
            TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
          }
          else if (hash::HASH_NOT_EXIST == hash_get)
          {

          }
          else if (hash::HASH_EXIST == hash_get)
          {
            bret = true;
          }
        }
        else
        {
          TBSYS_LOG(INFO, "hashmap not init yet");
        }
      }

      return bret;
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_group_schema(const uint64_t table_id,
        const uint64_t group_id,
        int64_t& size) const
    {
      const ObSSTableSchemaColumnDef* ret   = NULL;
      size = 0;
      if (OB_INVALID_ID == table_id || 0 == table_id || OB_INVALID_ID == group_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id=%lu, group_id=%lu", table_id, group_id);
      }
      else if ( 0 != get_column_count() )
      {
        if (hash_init_)
        {
          CombineIdKey key;
          key.table_id_ = table_id;
          key.sec_id_   = group_id;

          ValueSet column_set;
          int hash_get = 0;
          hash_get = group_schema_hashmap_.get(key, column_set);
          if (-1 == hash_get)
          {
            TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
          }
          else if (hash::HASH_NOT_EXIST == hash_get)
          {
            ret = NULL;
            size = 0;
          }
          else if (hash::HASH_EXIST == hash_get)
          {
            ret  = &column_def_[column_set.head_offset_];
            size = column_set.size_;
          }
        }
        else
        {
          TBSYS_LOG(INFO, "hashmap not init yet");
        }
      }
      return ret;
    }

    int ObSSTableSchema::get_column_groups_id(const uint64_t table_id, const uint64_t column_id,
                                              uint64_t *array, int64_t &size) const
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == table_id || 0 == table_id
          || OB_INVALID_ID == column_id)
      {
           TBSYS_LOG(WARN, "invalid argument table_id = %lu, column_id = %lu",
                  table_id, column_id);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (hash_init_)
        {
          CombineIdKey key;
          key.table_id_ = table_id;
          key.sec_id_   = column_id;

          ValueSet group_set;
          int hash_get = 0;
          hash_get = group_index_hashmap_.get(key, group_set);
          if (-1 == hash_get)
          {
            TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
            ret = OB_ERROR;
          }
          else if (hash::HASH_NOT_EXIST == hash_get)
          {
            ret = OB_ERROR;
          }
          else if (hash::HASH_EXIST == hash_get)
          {
            if (size < group_set.size_)
            {
              ret = OB_SIZE_OVERFLOW;
            }
            else
            {
              size = group_set.size_;
            }
            ObSSTableSchemaColumnDef* def = NULL;
            int32_t offset = 0;
            for (int index = 0; index < size; ++index)
            {
              if (0 == index)
              {
                offset = group_set.head_offset_;
              }
              else
              {
                offset = def->group_column_next_;
              }
              if (offset < 0 || offset >= get_column_count())
              {
                TBSYS_LOG(ERROR, "offset overflosw offset=%d total column count=%ld",
                          offset, get_column_count());
                ret = OB_ERROR;
                break;
              }
              else
              {
                def = column_def_ + offset;
                array[index] = def->column_group_id_;
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(INFO, "hashmap not init yet");
        }
      }
      return ret;
    }

    const int64_t ObSSTableSchema::find_offset_first_column_group_schema(const uint64_t table_id,
                                                                         const uint64_t column_id,
                                                                         uint64_t & column_group_id) const
    {
      int64_t ret = -1;
      if (OB_INVALID_ID == table_id || 0 == table_id
          || OB_INVALID_ID == column_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id=%lu, column_id=%lu", table_id, column_id);
      }

      else if (0 < get_column_count())
      {
        if (hash_init_)
        {
          CombineIdKey key;
          key.table_id_ = table_id;
          key.sec_id_   = column_id;

          ValueSet group_set;
          int hash_get = 0;
          hash_get = group_index_hashmap_.get(key, group_set);
          if (-1 == hash_get)
          {
            TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
          }
          else if (hash::HASH_NOT_EXIST == hash_get)
          {

          }
          else if (hash::HASH_EXIST == hash_get)
          {
            if (0 < group_set.size_)
            {
              ObSSTableSchemaColumnDef* def = column_def_ + group_set.head_offset_;
              if (group_set.head_offset_ < 0 ||
                  group_set.head_offset_ >= get_column_count())
              {
                TBSYS_LOG(ERROR, "offset overflosw offset=%d max_offset=%lu",
                          group_set.head_offset_, get_column_count());
                ret = OB_ERROR;
              }
              else
              {
                column_group_id = def->column_group_id_;
                ret = def->offset_in_group_;
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(INFO, "hashmap not init yet");
        }
      }
      return ret;
    }

    const int64_t ObSSTableSchema::find_offset_column_group_schema(const uint64_t table_id,
                                                                   const uint64_t column_group_id,
                                                                   const uint64_t column_id) const
    {
      int64_t ret = -1;
      if (OB_INVALID_ID == table_id || 0 == table_id
          ||OB_INVALID_ID == column_group_id || OB_INVALID_ID == column_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id = %lu, column_group_id = %lu"
                  "column_id = %lu", table_id, column_group_id, column_id);
      }

      else if (0 < get_column_count())
      {
        if (hash_init_)
        {
          CombineIdKey key;
          key.table_id_ = table_id;
          key.sec_id_   = column_id;

          ValueSet group_set;
          int hash_get = 0;
          hash_get = group_index_hashmap_.get(key, group_set);
          if (-1 == hash_get)
          {
            TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
          }
          else if (hash::HASH_NOT_EXIST == hash_get)
          {

          }
          else if (hash::HASH_EXIST == hash_get)
          {
            ObSSTableSchemaColumnDef* def = NULL;
            int64_t offset = 0;
            for (int32_t index = 0; index < group_set.size_; ++index)
            {
              if (0 == index)
              {
                offset = group_set.head_offset_;
              }
              else
              {
                offset = def->group_column_next_;
              }
              if (offset < 0 || offset >= get_column_count())
              {
                TBSYS_LOG(ERROR, "offset overflosw offset=%ld total colum count=%ld",
                          offset, get_column_count());
                ret = OB_ERROR;
                break;
              }
              else
              {
                def = column_def_ + offset;
                if (def->column_group_id_ == column_group_id)
                {
                  ret = def->offset_in_group_;
                  break;
                }
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(INFO, "hashmap not init yet");
        }
      }
      return ret;
    }

    bool ObSSTableSchema::is_column_exist(const uint64_t table_id, const uint64_t column_id) const
    {
      bool ret = false;
      uint64_t group_id = 0;
      int64_t offset = find_offset_first_column_group_schema(table_id, column_id, group_id);
      if (0 <= offset)
      {
        ret = true;
      }
      return ret;
    }

    int ObSSTableSchema::create_hashmaps()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = table_schema_hashmap_.create(hash::cal_next_prime(OB_MAX_TABLE_NUMBER))))
      {
        TBSYS_LOG(WARN, "create table schema hashmap failed");
      }
      else if (OB_SUCCESS != (ret = group_schema_hashmap_.create(hash::cal_next_prime(512))))
      {
        TBSYS_LOG(WARN, "creat group schema hashmap failed");
      }
      else if (OB_SUCCESS != (ret = group_index_hashmap_.create(hash::cal_next_prime(512))))
      {
        TBSYS_LOG(WARN, "creat group index hashmap failed");
      }
      else if (OB_SUCCESS != (ret = table_rowkey_hashmap_.create(hash::cal_next_prime(512))))
      {
        TBSYS_LOG(WARN, "creat group index hashmap failed");
      }
      else
      {
        hash_init_ = true;
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObSSTableSchema)
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
      //add zhaoqiong [Truncate Table] for upgrade :20160504:b
      if (OB_SUCCESS == ret)
      {
        ret = encode_i32(buf, buf_len, pos, OB_SSTABLE_SCHEMA_VERSION_TWO);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to serialize schema version_, buf=%p, "
                    "buf_len=%ld, pos=%ld, version_=%d",
                    buf, buf_len, pos, static_cast<int32_t>(OB_SSTABLE_SCHEMA_VERSION_TWO));
        }
      }
      //add:e

      if (OB_SUCCESS == ret)
      {
        ret = schema_header_.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to serialize schema header, buf=%p, "
                          "buf_len=%ld, pos=%ld, column_count=%d",
                    buf, buf_len, pos, schema_header_.total_column_count_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        //add zhaoqiong [Truncate Table]:20160318:b
        for (int64_t i = 0; OB_SUCCESS == ret && i < schema_header_.trun_table_count_; ++i)
        {
          ret = table_trun_array_[i].serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to serialize schema column def, buf=%p, "
                            "buf_len=%ld, pos=%ld, column index=%ld, "
                            "column_count=%d",
                      buf, buf_len, pos, i, schema_header_.trun_table_count_);
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        //add:e
        for (int64_t i = 0; OB_SUCCESS == ret
             && i < schema_header_.total_column_count_; ++i)
        {
          ret = column_def_[i].serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to serialize schema column def, buf=%p, "
                            "buf_len=%ld, pos=%ld, column index=%ld, "
                            "column_count=%d",
                      buf, buf_len, pos, i, schema_header_.total_column_count_);
            break;
          }
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchema)
    {
      int ret = OB_SUCCESS;
      int64_t column_count = 0;
      int32_t trun_table_count = 0; //add zhaoqiong [Truncate Table]:20160318

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }
      reset();

      //add zhaoqiong [Truncate Table] for upgrade :20160504:b
      if (OB_SUCCESS == ret)
      {
        ret = decode_i32(buf, data_len, pos,reinterpret_cast<int32_t*>(&version_));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to deserialize sstable schema version, buf=%p, "
                          "data_len=%ld, pos=%ld", buf, data_len, pos);
        }
      }
      //add:e


      if (OB_SUCCESS == ret )
      {
        if (version_ == OB_SSTABLE_SCHEMA_VERSION_TWO)
        {
          ret = schema_header_.deserialize(buf, data_len, pos);
          column_count = get_column_count();
          trun_table_count = get_trun_table_count(); //add zhaoqiong [Truncate Table]:20160318
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to deserialize schema header, buf=%p, "
                      "data_len=%ld, pos=%ld, column_count=%ld",
                      buf, data_len, pos, column_count);
          }
        }
        else if (version_ == OB_SSTABLE_SCHEMA_VERSION_ONE)
        {
          ObSSTableSchemaHeader tmp_header;
          ret = tmp_header.deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to deserialize schema header, buf=%p, "
                      "data_len=%ld, pos=%ld, column_count=%ld",
                      buf, data_len, pos, column_count);
          }
          else
          {
            schema_header_.column_count_ = tmp_header.column_count_;
            schema_header_.reserved16_ = tmp_header.reserved16_;
            schema_header_.total_column_count_ = tmp_header.total_column_count_;
            trun_table_count = 0;
            column_count = get_column_count();
            TBSYS_LOG(INFO, "compatiable sstable version buf=%p, "
                      "data_len=%ld, pos=%ld, version=%d",
                      buf, data_len, pos, version_);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "unsupported sstable schema version(%d)", version_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      //add zhaoqiong [Truncate Table]:20160318:b
      if (OB_SUCCESS == ret)
      {
        schema_header_.trun_table_count_ = 0;
        ObSSTableSchemaTableDef table_def;

        for (int64_t i = 0; OB_SUCCESS == ret && i < trun_table_count; ++i)
        {
          if (OB_SUCCESS != (ret = table_def.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "failed to deserialize schema table def, buf=%p, "
                      "data_len=%ld, pos=%ld, table index=%ld, "
                      "trun_table_count=%d",
                      buf, data_len, pos, i, trun_table_count);
            break;
          }
          else if (OB_SUCCESS != (ret = add_table_def(table_def)))
          {
              TBSYS_LOG(WARN, "add table def failed table_def(table_id=%d, "
                        "truncate_timestamp = %ld", table_def.table_id_,table_def.trun_timestamp_);
          }
        }
      }
      //add:e

      if (OB_SUCCESS == ret && common::OB_MAX_COLUMN_NUMBER <= column_count)
      {
        column_def_ = reinterpret_cast<ObSSTableSchemaColumnDef*>
          (ob_malloc(sizeof(ObSSTableSchemaColumnDef) * (DEFAULT_COLUMN_DEF_SIZE), ObModIds::OB_SSTABLE_SCHEMA));
        if (NULL == column_def_)
        {
          TBSYS_LOG(WARN, "failed to alloc memory");
          ret = OB_ERROR;
        }
      }


      if (OB_SUCCESS == ret)
      {
        schema_header_.total_column_count_ = 0;
        ObSSTableSchemaColumnDef column_def;
        for (int64_t i = 0; OB_SUCCESS == ret && i < column_count; ++i)
        {
          ret = column_def.deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to deserialize schema column def, buf=%p, "
                      "data_len=%ld, pos=%ld, column index=%ld, "
                      "column_count=%ld",
                      buf, data_len, pos, i, column_count);
              break;
          }
          if (OB_SUCCESS == ret)
          {
            ret = add_column_def(column_def);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "add column def failed cocolumn_def(table_id=%u, column_group_id=%u"
                        ", column_name_id=%u", column_def.table_id_,
                        column_def.column_group_id_, column_def.column_name_id_);;
            }
          }
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchema)
    {
      int64_t total_size  = 0;
      int32_t column_count = get_column_count();
      int32_t table_count = get_trun_table_count(); //add zhaoqiong [Truncate Table]:20160318

      total_size += encoded_length_i32(version_);
      total_size += schema_header_.get_serialize_size();
      total_size += column_def_[0].get_serialize_size() * column_count;
      total_size += table_trun_array_[0].get_serialize_size() * table_count; //add zhaoqiong [Truncate Table]:20160318
      return total_size;
    }


    int build_sstable_schema(const uint64_t new_table_id, const uint64_t schema_table_id,
        const common::ObSchemaManagerV2 & schema,
        ObSSTableSchema& sstable_schema, bool binary_format)
    {
      int ret = OB_SUCCESS;
      int32_t size = 0;
      int64_t col_index = 0;
      ObSSTableSchemaColumnDef column_def;
      const ObColumnSchemaV2 *col = schema.get_table_schema(schema_table_id, size);
      const ObTableSchema* table_schema = schema.get_table_schema(schema_table_id);
      const ObRowkeyInfo & rowkey_info = table_schema->get_rowkey_info();

      memset(&column_def, 0, sizeof(column_def));

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR,"cann't find this table:%lu in schema", schema_table_id);
        ret = OB_ERROR;
      }
      else
      {
        const ObRowkeyColumn* column = NULL;
        // add rowkey column first;
        for (col_index = 0; col_index < rowkey_info.get_size() && OB_SUCCESS == ret && !binary_format; ++col_index)
        {
          column = rowkey_info.get_column(col_index);
          if (NULL == column)
          {
            TBSYS_LOG(ERROR, "schema internal error, rowkey column %ld not exist.", col_index);
            ret = OB_ERROR;
            break;
          }

          column_def.table_id_ = static_cast<uint32_t>(new_table_id);
          column_def.column_group_id_ = 0;
          column_def.column_name_id_ = static_cast<uint16_t>(column->column_id_);
          column_def.column_value_type_ = column->type_;
          column_def.rowkey_seq_ = static_cast<uint16_t>(col_index + 1);

          if ( (ret = sstable_schema.add_column_def(column_def)) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR,"add column_def(%u,%u,%u,%u) failed col_index : %ld",column_def.table_id_,
                column_def.column_group_id_,column_def.column_name_id_,column_def.rowkey_seq_, col_index);
          }
        }

        for (col_index = 0; col_index < size && OB_SUCCESS == ret; ++col_index)
        {
          //modify wenghaixing [secondary index static_index_build]20150319
          //因为如果这张表是索引的话，表的列id是不连续的，索引要多加一个判定条�?
          if(rowkey_info.is_rowkey_column(col[col_index].get_id()) || NULL == (schema.get_column_schema(new_table_id,col[col_index].get_id())))
          //if (rowkey_info.is_rowkey_column(col[col_index].get_id()))
          //add e
          {
            // ignore rowkey columns;
            continue;
          }

          column_def.table_id_ = static_cast<uint32_t>(new_table_id);
          column_def.column_group_id_ = static_cast<uint16_t>(col[col_index].get_column_group_id());
          column_def.column_name_id_ = static_cast<uint16_t>(col[col_index].get_id());
          column_def.column_value_type_ = col[col_index].get_type();
          column_def.rowkey_seq_ = 0;

          if ( (ret = sstable_schema.add_column_def(column_def)) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR,"add column_def(%u,%u,%u,%u) failed col_index : %ld",column_def.table_id_,
                column_def.column_group_id_,column_def.column_name_id_,column_def.rowkey_seq_, col_index);
          }
        }
      }

      return ret;
    }

    int build_sstable_schema(const uint64_t table_id,
        const common::ObSchemaManagerV2 & schema,
        ObSSTableSchema& sstable_schema, bool binary_format)
    {
      return build_sstable_schema(table_id, table_id, schema, sstable_schema, binary_format);
    }

    int build_sstable_schema(const common::ObSchemaManagerV2 & schema,
        ObSSTableSchema& sstable_schema, bool binary_format)
    {
      int ret = OB_SUCCESS;
      ObSchemaSortByIdHelper helper(&schema);
      const ObSchemaSortByIdHelper::Item* item = helper.begin();
      const ObTableSchema* table_schema = NULL;
      for (; item != helper.end(); ++item)
      {
        if (NULL != item && NULL != (table_schema = helper.get_table_schema(item)))
        {
          ret = build_sstable_schema(table_schema->get_table_id(), schema, sstable_schema, binary_format);
        }
        else
        {
          TBSYS_LOG(DEBUG, "build_sstable_schema error item=%p, table_schema=%p", item, table_schema);
          break;
        }
      }
      return ret;
    }

  } // end namespace sstable
} // end namespace oceanbase
