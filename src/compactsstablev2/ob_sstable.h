#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "ob_sstable_schema.h"
#include "ob_sstable_table_index_builder.h"
#include "ob_sstable_table_schema_builder.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTable
    {
    public:
      static const int64_t OB_SSTABLE_HEADER_SIZE = sizeof(ObSSTableHeader);

    public:
      struct QueryStruct
      {
        uint64_t checksum_;
        int64_t row_count_;

        QueryStruct()
        {
          memset(this, 0, sizeof(QueryStruct));
        }

        void reset()
        {
          checksum_ = 0;
          row_count_ = 0;
        }
      };

    public:
      ObSSTable()
      {
        sstable_header_.reset();
        table_index_.reset();
      }

      ~ObSSTable()
      {
      }

      inline void reset()
      {
        table_index_.reset();
        table_schema_.reset();
        sstable_header_.reset();
        table_index_builder_.reset();
        table_schema_builder_.reset();
        query_struct_.reset();
      }

      inline void clear()
      {
        table_index_builder_.clear();
        table_schema_builder_.clear();
      }

      inline void switch_table_reset()
      {
        table_index_.reset();
        //memset(&table_index_, 0, sizeof(table_index_));
      }

      inline void switch_sstable_reset()
      {
        query_struct_.checksum_ = sstable_header_.checksum_;
        query_struct_.row_count_ = table_index_.row_count_;
        table_index_.switch_sstable_reset();
        sstable_header_.switch_sstable_reset();
        table_index_builder_.reset();
        table_schema_builder_.reset();
      }

      inline uint64_t get_sstable_check_sum(const int64_t flag) const
      {
        uint64_t ret = 0;
        if (0 == flag)
        {
          ret = query_struct_.checksum_;
        }
        else if (1 == flag)
        {
          ret = sstable_header_.checksum_;
        }
        return ret;
      }

      inline int64_t get_sstable_row_count(const int64_t flag) const
      {
        int64_t ret = 0;
        if (0 == flag)
        {
          ret = query_struct_.row_count_;
        }
        else if (1 == flag)
        {
          ret = table_index_.row_count_;
        }
        return ret;
      }

      inline void set_first_table_id(const uint64_t table_id)
      {
        sstable_header_.first_table_id_ = table_id;
      }
      
      inline void set_table_id(const uint64_t table_id)
      {
        table_index_.table_id_ = table_id;
      }

      inline void set_blocksize(const int64_t block_size)
      {
        sstable_header_.block_size_ = block_size;
      }

      inline void set_compressor_name(const common::ObString& compressor_name)
      {
        memcpy(sstable_header_.compressor_name_, compressor_name.ptr(), 
            compressor_name.length());
      }

      inline void add_row_count(const int64_t count)
      {
        table_index_.row_count_ += count;
      }

      inline void inc_block_count()
      {
        table_index_.block_count_ ++;
      }

      inline void add_table_schema_count(const int32_t schema_array_unit_count)
      {
        sstable_header_.schema_array_unit_count_ += schema_array_unit_count;
      }

      inline void inc_table_count()
      {
        sstable_header_.table_count_ ++;
      }

      inline void set_block_data_record(const int64_t block_data_offset, 
          const int64_t block_data_size)
      {
        table_index_.block_data_offset_ = block_data_offset;
        table_index_.block_data_size_ = block_data_size;
      }

      inline void set_block_index_record(const int64_t  block_index_offset, 
          const int64_t block_index_size)
      {
        table_index_.block_index_offset_ = block_index_offset;
        table_index_.block_index_size_ = block_index_size;
      }

      inline void set_block_endkey_record(const int64_t block_endkey_offset, 
          const int64_t block_endkey_size)
      {
        table_index_.block_endkey_offset_ = block_endkey_offset;
        table_index_.block_endkey_size_ = block_endkey_size;
      }

      inline void set_table_range_record(const int64_t range_keys_offset, 
          const int64_t range_start_key_length, 
          const int64_t range_end_key_length)
      {
        table_index_.range_keys_offset_ = range_keys_offset;
        table_index_.range_start_key_length_ = 
          static_cast<int32_t>(range_start_key_length);
        table_index_.range_end_key_length_ = 
          static_cast<int32_t>(range_end_key_length);
      }

      inline void set_bloomfilter_record(const int64_t bloom_filter_offset, 
          const int64_t bloom_filter_size)
      {
        table_index_.bloom_filter_offset_ = bloom_filter_offset;
        table_index_.bloom_filter_size_ = bloom_filter_size;
      }

      inline void set_table_index_record(const int64_t table_index_offset, 
          const int64_t table_index_unit_size)
      {
        sstable_header_.table_index_offset_ = table_index_offset;
        sstable_header_.table_index_unit_size_ = 
          static_cast<int32_t>(table_index_unit_size);
      }

      inline void set_table_schema_record(const int64_t schema_record_offset, 
          const int64_t schema_array_unit_size)
      {
        sstable_header_.schema_record_offset_ = schema_record_offset;
        sstable_header_.schema_array_unit_size_ = 
          static_cast<int32_t>(schema_array_unit_size);
      }

      inline void set_bloomfilter_hashcount(const int64_t bloomfilter_hashcount)
      {
        table_index_.bloom_filter_hash_count_ = bloomfilter_hashcount;
      }
        
      inline void set_checksum(const uint64_t checksum)
      {
        sstable_header_.checksum_ = checksum;
      }

      inline void set_frozen_minor_version_range(
          const ObFrozenMinorVersionRange& range)
      {
        sstable_header_.major_version_ = range.major_version_;
        sstable_header_.major_frozen_time_ = range.major_frozen_time_;
        sstable_header_.next_transaction_id_ = range.next_transaction_id_;
        sstable_header_.next_commit_log_id_ = range.next_commit_log_id_;
        sstable_header_.start_minor_version_ = range.start_minor_version_;
        sstable_header_.end_minor_version_ = range.end_minor_version_;
        sstable_header_.is_final_minor_version_ = range.is_final_minor_version_;
      }

      inline void set_table_schema(const ObSSTableSchema& schema)
      {
        table_schema_ = schema;
      }

      inline void set_row_store_type(
          common::ObCompactStoreType row_store_type)
      {
        sstable_header_.row_store_type_ = static_cast<int16_t>(row_store_type);
      }

      inline common::ObCompactStoreType get_row_store_type() const
      {
        return static_cast<common::ObCompactStoreType>(sstable_header_.row_store_type_);
      }

      inline int64_t get_block_size() const
      {
        return sstable_header_.block_size_;
      }

      inline int64_t get_table_id() const
      {
        return table_index_.table_id_;
      }

      inline int64_t get_table_index_length() const
      {
        return table_index_builder_.get_length();
      }

      inline int64_t get_table_schema_length() const
      {
        return table_schema_builder_.get_length();
      }

      inline int64_t get_sstable_header_length() const
      {
        return OB_SSTABLE_HEADER_SIZE;
      }

      inline bool is_alloc_table_index_buf() const
      {
        bool ret = false;

        if (1 < table_index_builder_.get_buf_block_count())
        {
          ret = true;
        }

        return ret;
      }

      inline bool is_alloc_table_schema_buf() const
      {
        bool ret = false;

        if (1 < table_schema_builder_.get_buf_block_count())
        {
          ret = true;
        }

        return ret;
      }
        
      inline int build_table_index(char* const buf, const int64_t buf_size, 
          int64_t& index_length)
      {
        return table_index_builder_.build_table_index(buf, buf_size, index_length);
      }

      inline int build_table_index(const char*& buf , int64_t& index_length)
      {
        return table_index_builder_.build_table_index(buf, index_length);
      }

      inline int build_table_schema(char* buf, const int64_t buf_size, 
          int64_t& schema_length)
      {
        return table_schema_builder_.build_table_schema(buf, buf_size, schema_length);
      }

      inline int build_table_schema(const char*& buf, int64_t& schema_length)
      {
        return table_schema_builder_.build_table_schema(buf, schema_length);
      }

      inline int build_sstable_header(const char*& buf, int64_t& header_length)
      {
        int ret = common::OB_SUCCESS;

        buf = reinterpret_cast<char*>(&sstable_header_);
        header_length = OB_SSTABLE_HEADER_SIZE;

        return ret;
      }

      int finish_one_table();

      int check_row(const common::ObRowkey& rowkey, 
          const common::ObRow& rowvalue);

      int check_row(const common::ObRow& row);

    private:
      ObSSTableTableIndex table_index_;
      ObSSTableSchema table_schema_;
      ObSSTableHeader sstable_header_;

      ObSSTableTableIndexBuilder table_index_builder_;
      ObSSTableTableSchemaBuilder table_schema_builder_;

      QueryStruct query_struct_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif
