#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_SCHEMA_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_SCHEMA_H_

#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/hash/ob_hashutils.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_rowkey.h"
#include "common/ob_row.h"

class TestSSTableSchema_construct_Test;
class TestSSTableSchema_add_column_def_Test;

namespace oceanbase
{
  namespace common
  {
    class ObSchemaManagerV2;
  }
  namespace compactsstablev2
  {
    struct ObSSTableSchemaHeader
    {
      int64_t column_count_;
    };

    struct ObSSTableSchemaColumnDef
    {
      uint64_t table_id_;
      uint64_t column_id_;
      int32_t column_value_type_;
      int64_t offset_;
      int64_t rowkey_seq_;

      bool is_rowkey_column() const
      {
        bool ret = false;

        if (rowkey_seq_ > 0)
        {
          ret = true;
        }

        return ret;
      }

      void operator=(const ObSSTableSchemaColumnDef& def)
      {
        table_id_ = def.table_id_;
        column_id_ = def.column_id_;
        column_value_type_ = def.column_value_type_;
        offset_ = def.offset_;
        rowkey_seq_ = def.rowkey_seq_;
      }

      bool operator==(const ObSSTableSchemaColumnDef& def) const
      {
        bool ret = true;

        if (table_id_ == def.table_id_
            && column_id_ == def.column_id_
            && column_value_type_ == def.column_value_type_
            && offset_ == def.offset_
            && rowkey_seq_ == def.rowkey_seq_)
        {
          ret = true;
        }

        return ret;
      }
    };

    struct ObSSTableSchemaColumnDefCompare
    {
      bool operator()(const ObSSTableSchemaColumnDef& lhs, 
          const ObSSTableSchemaColumnDef& rhs)
      {
        bool ret = true;

        if (lhs.table_id_ > rhs.table_id_)
        {//table id必须递增
          ret = false;
        }
        else if (lhs.table_id_ == rhs.table_id_)
        {//table id相同
          if (!lhs.is_rowkey_column() && !rhs.is_rowkey_column())
          {
            if (lhs.column_id_ >= rhs.column_id_)
            {//非rowkey列，后面的列id要比前面的大
              ret = false;
            }
          }
          else if (!lhs.is_rowkey_column() && rhs.is_rowkey_column())
          {//非rowkey列要在rowkey列后面
            ret = false;
          }
          else if (lhs.is_rowkey_column() && rhs.is_rowkey_column())
          {
            if (lhs.rowkey_seq_ + 1 != rhs.rowkey_seq_)
            {//rowkey列的顺序号必段递增
              ret = false;
            }
            else if (lhs.column_id_ == rhs.column_id_)
            {//column id不得重复
              ret = false;
            }
          }
        }

        return ret;
      }
    };//end struct ObSSTableSchemaColumnDefCompare

    struct TableKey
    {
      uint64_t table_id_;
      bool is_rowkey_;

      int64_t hash() const
      {
        common::hash::hash_func<uint64_t> hash_uint64;
        common::hash::hash_func<uint8_t> hash_uint8;
        return hash_uint64(table_id_) 
          + hash_uint8(static_cast<uint8_t>(is_rowkey_));
      }

      bool operator==(const TableKey& key) const
      {
        bool ret = false;

        if (table_id_ == key.table_id_ && is_rowkey_ == key.is_rowkey_)
        {
          ret = true;
        }

        return ret;
      }
    };

    struct TableValue
    {
      int64_t offset_;
      int64_t size_;

      TableValue()
      {
        offset_ = -1;
        size_ = 0;
      }
    };
    
    struct TableColumnKey
    {
      uint64_t table_id_;
      uint64_t column_id_;

      int64_t hash() const
      {
        common::hash::hash_func<uint64_t> hash_uint64;
        return hash_uint64(table_id_) + hash_uint64(column_id_);
      }
      
      bool operator==(const TableColumnKey& key) const
      {
        bool ret = false;

        if (table_id_ == key.table_id_ && column_id_ == key.column_id_)
        {
          ret = true;
        }

        return ret;
      }
    };

    struct TableColumnValue
    {
      int64_t offset_;

      TableColumnValue()
      {
        offset_ = -1;
      }
    };

    class ObSSTableSchema
    {
    public:
      friend class ::TestSSTableSchema_construct_Test;
      friend class ::TestSSTableSchema_add_column_def_Test;

    public:
      static const int32_t OB_SSTABLE_SCHEMA_COLUMN_DEF_SIZE = static_cast<int32_t>(sizeof(ObSSTableSchemaColumnDef));
      static const int32_t OB_SSTABLE_SCHEMA_HEADER_SIZE = static_cast<int32_t>(sizeof(ObSSTableSchemaHeader));
      static const int64_t DEFAULT_COLUMN_DEF_ARRAY_CNT = common::OB_MAX_TABLE_NUMBER * common::OB_MAX_COLUMN_NUMBER;

    public:
      ObSSTableSchema();

      ~ObSSTableSchema();

      void reset();

      //table id, rowkey/rowvalue ----> column def array
      const ObSSTableSchemaColumnDef* get_table_schema(
          const uint64_t table_id,
          const bool is_rowkey_column, int64_t& size) const;

      //table id, column id ----> column def
      const ObSSTableSchemaColumnDef* get_column_def(
          const uint64_t table_id,
          const uint64_t column_id) const;

      //table exist?
      bool is_table_exist(const uint64_t table_id) const;

      //column exist
      bool is_column_exist(const uint64_t table_id,
          const uint64_t column_id) const;

      //add column def
      int add_column_def(const ObSSTableSchemaColumnDef& column_def);

      //get rowkey column
      const ObSSTableSchemaColumnDef* get_rowkey_column(
          const uint64_t table_id, const int64_t rowkey_seq) const;

      //get rowvalue column(used for dense)
      const ObSSTableSchemaColumnDef* get_rowvalue_column(
          const uint64_t table_id, const int64_t rowvalue_seq) const;

      //column count
      inline const int64_t get_column_count() const
      {
        return schema_header_.column_count_;
      }

      const int get_rowkey_column_count(
          const uint64_t table_id, int64_t& column_count) const;

      //index--->column def
      inline const ObSSTableSchemaColumnDef* get_column_def(
          const int64_t index) const
      {
        const ObSSTableSchemaColumnDef* ret = NULL;

        if (index >= 0 && index < get_column_count())
        {
          ret = &column_def_[index];
        }
        else
        {
          TBSYS_LOG(WARN, "can not find the def:index=%ld", index);
        }

        return ret;
      }

      int operator=(const ObSSTableSchema& schema);

      int check_row(const uint64_t table_id, const common::ObRowkey& rowkey,
          const common::ObRow& rowvalue);

      int check_row(const uint64_t table_id, const common::ObRow& row);

      int64_t to_string(char* buf, const int64_t buf_len) const;

      inline const ObSSTableSchemaColumnDef* get_column_def_array(
          int64_t& column_count) const
      {
        column_count = schema_header_.column_count_;
        return column_def_;
      }

    private:
      //column id --> index
      int64_t find_column_offset(const uint64_t table_id,
          const uint64_t column_id) const;

      //create hashmap
      int create_hashmaps();

      //update hashmap
      int update_hashmaps(const ObSSTableSchemaColumnDef& column_def);

      //update table schema hashmap
      int update_table_schema_hashmap(const ObSSTableSchemaColumnDef& column_def);

      //update table column schema hashmap
      int update_table_column_schema_hashmap(const ObSSTableSchemaColumnDef& column_def);

    private:
      ObSSTableSchemaHeader schema_header_;
      ObSSTableSchemaColumnDef column_def_array_[common::OB_MAX_COLUMN_NUMBER];
      ObSSTableSchemaColumnDef* column_def_;

      //the head of table key_columns/value_clumns
      int64_t cur_head_;

      bool hash_init_;
      //table id,rowkey/rowvalue--->index set
      common::hash::ObHashMap<TableKey, TableValue,
        common::hash::NoPthreadDefendMode> table_schema_hashmap_;
      //table id,column id --->index
      common::hash::ObHashMap<TableColumnKey, TableColumnValue,
        common::hash::NoPthreadDefendMode> table_column_schema_hashmap_;
    };

    /**
     * only translate table %schema_table_id to sstable schema with new_table_id as table id
     */
    int build_sstable_schema(const uint64_t new_table_id, const uint64_t schema_table_id,
        const common::ObSchemaManagerV2 & schema, ObSSTableSchema& sstable_schema);

    /**
     * only translate table %table_id to sstable schema
     */
    int build_sstable_schema(const uint64_t table_id, 
        const common::ObSchemaManagerV2 & schema, ObSSTableSchema& sstable_schema);
    /**
     * trsanlate all tables to sstable schema
     */
    int build_sstable_schema(const common::ObSchemaManagerV2 & schema, ObSSTableSchema& sstable_schema);

  } // namespace oceanbase::sstable
} // namespace Oceanbase
#endif 
