#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_STORE_STRUCT_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_STORE_STRUCT_H_

#include "common/ob_compact_cell_iterator.h"
#include "common/ob_compact_store_type.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    //record magic
    static const int16_t OB_SSTABLE_BLOCK_DATA_MAGIC = 0x4461;
    static const int16_t OB_SSTABLE_SCHEMA_MAGIC = 0x4463;
    static const int16_t OB_SSTABLE_TABLE_INDEX_MAGIC = 0x4464;
    static const int16_t OB_SSTABLE_TABLE_BLOOMFILTER_MAGIC = 0x4465;
    static const int16_t OB_SSTABLE_TABLE_RANGE_MAGIC = 0x4466;
    static const int16_t OB_SSTABLE_HEADER_MAGIC = 0x4467;
    static const int16_t OB_SSTABLE_BLOCK_ENDKEY_MAGIC = 0x4468;
    static const int16_t OB_SSTABLE_BLOCK_INDEX_MAGIC = 0x4468;

    struct ObFrozenMinorVersionRange
    {
      int64_t major_version_;
      int64_t major_frozen_time_;
      int64_t next_transaction_id_;
      int64_t next_commit_log_id_;
      int32_t start_minor_version_;
      int32_t end_minor_version_;
      int32_t is_final_minor_version_;

      ObFrozenMinorVersionRange()
      {
        memset(this, 0, sizeof(ObFrozenMinorVersionRange));
      }

      /**
       * reset
       */
      void reset()
      {
        memset(this, 0, sizeof(ObFrozenMinorVersionRange));
      }

      /**
       * check row store type
       * --DENSE_DENSE
       *   (1)major_version >= 2
       * --DENSE_SPARSE
       *   (1)major_version >= 2
       *   (2)major_frozen_time_
       *   (3)next_transaction_id_ >= 2
       *   (4)next_commit_log_id_ >= 2
       *   (5)start_minor_version_ >= 1
       *   (6)end_minor_version_ >= 2
       *   (7)is_final_minor_version_ =0 | 1
       */
      bool is_legal(const common::ObCompactStoreType row_store_type) const
      {
        bool ret = false;

        if (common::DENSE_SPARSE == row_store_type)
        {
          if (major_version_ >= 2 && next_transaction_id_ >= 2
              && next_commit_log_id_ >=2 && start_minor_version_ >= 1
              && end_minor_version_ >= 2 
              && (0 == is_final_minor_version_
                || 1 == is_final_minor_version_))
          {
            ret = true;
          }
        }
        else if (common::DENSE_DENSE == row_store_type)
        {
          if (major_version_ >= 1)
          {
            ret = true;
          }
        }

        return ret;
      }

      /**
       * to_string
       * @param buf: buf
       * @param buf_len: buf size
       */
      int64_t to_string(char* buf, const int64_t buf_len) const
      {
        int64_t pos = 0;

        if (pos < buf_len)
        {
          common::databuff_printf(buf, buf_len, pos,
              "major_frozen_time_=[%ld],major_frozen_time_=[%ld],"
              "next_transaction_id_=[%ld],next_commit_log_id_=[%ld],"
              "start_minor_version_=[%d],end_minor_version_=[%d],"
              "is_final_minor_version_=[%d]",
              major_version_, major_frozen_time_, next_transaction_id_,
              next_commit_log_id_, start_minor_version_,
              end_minor_version_, is_final_minor_version_);
        }

        return pos;
      }
    };

    struct ObSSTableBlockRowIndex
    {
      int32_t row_offset_;

      ObSSTableBlockRowIndex()
      {
        memset(this, 0, sizeof(ObSSTableBlockRowIndex));
      }

      void reset()
      {
        memset(this, 0, sizeof(ObSSTableBlockRowIndex));
      }
    };

    struct ObSSTableBlockIndex
    {
      int64_t block_data_offset_;
      int64_t block_endkey_offset_;

      ObSSTableBlockIndex()
      {
        memset(this, 0, sizeof(ObSSTableBlockIndex));
      }

      void reset()
      {
        memset(this, 0, sizeof(ObSSTableBlockIndex));
      }

      bool operator == (const ObSSTableBlockIndex& other) const
      {
        return block_data_offset_ == other.block_data_offset_;
      }

      bool operator != (const ObSSTableBlockIndex& other) const
      {
        return block_data_offset_ != other.block_data_offset_;
      }

      bool operator < (const ObSSTableBlockIndex& other) const
      {
        return block_data_offset_ < other.block_data_offset_;
      }
    };

    struct ObSSTableBlockHeader
    {
      int32_t row_index_offset_;
      int32_t row_count_;
      int64_t reserved64_;

      ObSSTableBlockHeader()
      {
        memset(this, 0, sizeof(ObSSTableBlockHeader));
      }

      void reset()
      {
        memset(this, 0, sizeof(ObSSTableBlockHeader));
      }
    };

    struct ObSSTableTableSchemaItem
    {
      uint64_t table_id_;
      uint32_t column_id_;
      uint16_t rowkey_seq_;
      uint16_t column_attr_;
      uint16_t column_value_type_;
      uint16_t reserved16_[3];

      ObSSTableTableSchemaItem()
      {
        memset(this, 0, sizeof(ObSSTableTableSchemaItem));
      }

      void reset()
      {
        memset(this, 0, sizeof(ObSSTableTableSchemaItem));
      }
    };

    struct ObSSTableHeader
    {
      static const int32_t SSTABLE_HEADER_VERSION = 0x20000;
      static const int64_t MAX_COMPRESSOR_NAME_SIZE = 1024;

      int32_t header_size_;
      int32_t header_version_;
      uint64_t checksum_;
      int64_t major_version_;
      int64_t major_frozen_time_;
      int64_t next_transaction_id_;
      int64_t next_commit_log_id_;
      int32_t start_minor_version_;
      int32_t end_minor_version_;
      int32_t is_final_minor_version_;
      int16_t row_store_type_;
      int16_t reserved16_;
      uint64_t first_table_id_;
      int32_t table_count_;
      int32_t table_index_unit_size_;
      int64_t table_index_offset_;
      int32_t schema_array_unit_count_;
      int32_t schema_array_unit_size_;
      int64_t schema_record_offset_;
      int64_t block_size_;
      char compressor_name_[MAX_COMPRESSOR_NAME_SIZE];
      int64_t reserved64_[64];

      ObSSTableHeader()
      {
        memset(this, 0, sizeof(ObSSTableHeader));
        header_size_ = sizeof(ObSSTableHeader);
        header_version_ = SSTABLE_HEADER_VERSION;
      }

      void reset()
      {
        memset(this, 0, sizeof(ObSSTableHeader));
        header_size_ = sizeof(ObSSTableHeader);
        header_version_ = SSTABLE_HEADER_VERSION;
      }

      void switch_sstable_reset()
      {
        //header_size_
        //header_version_
        checksum_ = 0;
        //major_version_
        //major_frozen_time_
        //next_transaction_id_
        //next_commit_log_id_
        //start_minor_version_
        //end_minor_version_
        //is_final_minor_version_
        //row_store_type_
        //reserved16_;
        //first_table_id_;
        table_count_ = 0;
        table_index_unit_size_ = 0;
        table_index_offset_ = 0;
        schema_array_unit_count_ = 0;
        schema_array_unit_size_ = 0;
        schema_record_offset_ = 0;
        //block_size_
        //compressor_name_
        //reserved64_
      }
    };//end struct ObSSTableHeader

    struct ObSSTableTableIndex
    {
      int32_t size_;            //the size of table index
      int32_t version_;           //the version of the struct
      uint64_t table_id_;         //table id
      int32_t column_count_;      //the number of column
      int32_t columns_in_rowkey_;     //the number of columnof key
      int64_t row_count_;         //the total row count of row
      int64_t block_count_;         //the total block count
      int64_t block_data_offset_;   //the offset of block data
      int64_t block_data_size_;     //the size of block data
      int64_t block_index_offset_;    //the offset of block index
      int64_t block_index_size_;    //the size of block index
      int64_t block_endkey_offset_;   //the offset of block endkey
      int64_t block_endkey_size_;   //the size of block endkey
      int64_t bloom_filter_hash_count_;//bloom filter hash count
      int64_t bloom_filter_offset_;   //bloom filter offset
      int64_t bloom_filter_size_;   //bloom filter size
      int64_t range_keys_offset_;   //range key offset
      int32_t range_start_key_length_;  //range start key
      int32_t range_end_key_length_;  //range end key
      int64_t reserved_[8];       //reserverd

      static const int32_t TABLE_INDEX_VERSION = 0x20000;

      ObSSTableTableIndex()
      {
        memset(this, 0, sizeof(ObSSTableTableIndex));
        size_ = sizeof(ObSSTableTableIndex);
        version_ = TABLE_INDEX_VERSION;
      }

      void reset()
      {
        int64_t bloom_filter_hash_count_bak_ = bloom_filter_hash_count_;
        memset(this, 0, sizeof(ObSSTableTableIndex));
        size_ = sizeof(ObSSTableTableIndex);
        version_ = TABLE_INDEX_VERSION;
        bloom_filter_hash_count_ = bloom_filter_hash_count_bak_;
      }

      void switch_sstable_reset()
      {
        int64_t bloom_filter_hash_count_bak_ = bloom_filter_hash_count_;
        uint64_t table_id = table_id_;
        memset(this, 0, sizeof(ObSSTableTableIndex));
        size_ = sizeof(ObSSTableTableIndex);
        version_ = TABLE_INDEX_VERSION;
        bloom_filter_hash_count_ = bloom_filter_hash_count_bak_;
        table_id_ = table_id;
      }
    };

    struct ObSSTableTrailerOffset
    {
      int64_t offset_;

      ObSSTableTrailerOffset()
      {
        memset(this, 0, sizeof(ObSSTableTrailerOffset));
      }

      void reset()
      {
        memset(this, 0, sizeof(ObSSTableTrailerOffset));
      }
    };

    struct ObSSTableTableRange
    {
      common::ObCompactCellIterator start_key_;
      common::ObCompactCellIterator end_key_;
    };
  }//end namespace compactsstablev2
}

#endif
