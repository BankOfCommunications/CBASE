#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_TABLE_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_TABLE_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/bloom_filter.h"
#include "common/ob_rowkey.h"
#include "common/ob_record_header_v2.h"
#include "common/ob_range2.h"
#include "common/ob_malloc.h"
#include "ob_sstable_block_index_builder.h"
#include "ob_sstable_block_endkey_builder.h"
#include "ob_sstable_table_range_builder.h"

class TestSSTableTable_init_Test;
class TestSSTableTable_set_table_range_Test;
class TestSSTableTable_set_block_endkey_Test;
class TestSSTableTable_set_block_index_Test;

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableTable
    {
    public:
      friend class ::TestSSTableTable_init_Test;
      friend class ::TestSSTableTable_set_table_range_Test;
      friend class ::TestSSTableTable_set_block_endkey_Test;
      friend class ::TestSSTableTable_set_block_index_Test;

    public:
      struct TableRangeStruct
      {
        TableRangeStruct()
          : startkey_length_(0),
            endkey_length_(0),
            available_(false)
        {
        }

        common::ObNewRange table_range_;
        common::ObMemBuf start_key_buf_;
        common::ObMemBuf end_key_buf_;
        int64_t startkey_length_;
        int64_t endkey_length_;
        bool available_;

        void reset()
        {
          table_range_.reset();
          startkey_length_ = 0;
          endkey_length_ = 0;
          available_ = false;
        }
      };

    public:
      ObSSTableTable()
        : table_range_array_flag_(0)
      {
      }

      ~ObSSTableTable()
      {
      }

      int init(const int64_t bloomfilter_nhash, 
          const int64_t bloomfilter_nbyte);

      inline void reset()
      {
        table_range_array_[0].reset();
        table_range_array_[1].reset();
        table_bloomfilter_.clear();
        //block_index_.reset();
        memset(&block_index_, 0, sizeof(block_index_));
        block_index_builder_.reset();
        block_endkey_builder_.reset();
        table_range_builder_.reset();
      }

      inline void clear()
      {
      }

      int switch_table_reset();

      int switch_sstable_reset();

      bool check_rowkey_range(const common::ObRowkey& rowkey, 
          const bool is_table_first_row);

      bool check_rowkey_range(const common::ObRow& row, 
          const bool is_table_first_row);
    
      int set_table_range(const common::ObNewRange& range);

      inline int add_table_bloomfilter(
          const common::ObBloomFilterV1& table_bloomfilter)
      {
        int ret = common::OB_SUCCESS;
        (void)table_bloomfilter;
#ifdef OB_COMPACT_SSTABLE_ALLOW_BLOOMFILTER_
        if (common::OB_SUCCESS != (ret =
              table_bloomfilter_ | table_bloomfilter))
        {
          TBSYS_LOG(WARN, "failed to use or bloomfilter");
        }
#endif
        return ret;
      }

      int set_block_index(const int64_t data_offset);

      int set_block_endkey(const common::ObRowkey& block_endkey);

      int finish_last_block(const int64_t data_offset);

      inline bool is_alloc_block_index_buf() const
      {
        bool ret = false;

        if (1 != block_index_builder_.get_buf_block_count())
        {
          ret = true;
        }

        return ret;
      }

      inline bool is_alloc_block_endkey_buf() const
      {
        bool ret = false;

        if (1 != block_endkey_builder_.get_buf_block_count())
        {
          ret = true;
        }

        return ret;
      }

      inline bool is_alloc_table_range_buf() const
      {
        bool ret = false;

        if (1 != table_range_builder_.get_buf_block_count())
        {
          ret = true;
        }

        return ret;
      }
      
      inline int64_t get_block_index_length() const
      {
        return block_index_builder_.get_length();
      }

      inline int64_t get_block_endkey_length() const
      {
        return block_endkey_builder_.get_length();
      }

      inline int64_t get_table_range_length() const
      {
        return table_range_builder_.get_length();
      }

      inline int64_t get_table_bloomfilter_length() const
      {
        return table_bloomfilter_.get_nbyte();
      }

      int finish_table_range(const bool is_table_finish);

      int build_block_index(char* buf, const int64_t buf_size, 
          int64_t& index_length);

      int build_block_index(const char*& buf, int64_t& index_length);
      
      int build_block_endkey(char* buf, const int64_t buf_size, 
          int64_t& endkey_length);

      int build_block_endkey(const char*& buf, int64_t& endkey_length);

      int build_table_range(char* buf, const int64_t buf_size,
          int64_t& startkey_length, int64_t& endkey_length);

      int build_table_range(const char*& buf, 
          int64_t& startkey_length, int64_t& endkey_length);

      int build_table_bloomfilter(const char*& buf,
          int64_t& bloomfilter_length);

      //flag = 0:fetch old
      //flag = 1:fectch cur
      const common::ObNewRange* get_table_range(int64_t flag);

    private:
      TableRangeStruct table_range_array_[2];
      int64_t table_range_array_flag_;

      common::ObBloomFilterV1 table_bloomfilter_;
      
      ObSSTableBlockIndex block_index_;
      common::ObRowkey block_endkey_;
      common::ObMemBuf block_endkey_buf_;
      
      ObSSTableBlockIndexBuilder block_index_builder_;
      ObSSTableBlockEndkeyBuilder block_endkey_builder_;
      ObSSTableTableRangeBuilder table_range_builder_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif
