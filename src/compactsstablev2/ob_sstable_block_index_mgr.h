#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_INDEX_MGR_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_INDEX_MGR_H_

#include "common/ob_define.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"
#include "common/ob_tsi_factory.h"
#include "common/utility.h"
#include "ob_sstable_store_struct.h"

class TestSSTableBlockIndexMgr_construct_Test;

namespace oceanbase
{
  namespace compactsstablev2
  {
    enum SearchMode
    {
      OB_SEARCH_MODE_MIN_VALUE = 1,
      OB_SEARCH_MODE_EQUAL,
      OB_SEARCH_MODE_GREATER_EQUAL,
      OB_SEARCH_MODE_GREATER_THAN,
      OB_SEARCH_MODE_LESS_THAN,
      OB_SEARCH_MODE_LESS_EQUAL,
      OB_SEARCH_MODE_MAX_VALUE
    };

    inline bool is_looking_forward_mode(const SearchMode mode)
    {
      return mode >= OB_SEARCH_MODE_MIN_VALUE
        && mode <= OB_SEARCH_MODE_GREATER_THAN;
    }

    inline bool is_regular_mode(const SearchMode mode)
    {
      return OB_SEARCH_MODE_MIN_VALUE < mode
        && OB_SEARCH_MODE_MAX_VALUE > mode;
    }

    struct ObBlockIndexPositionInfo
    {
      uint64_t sstable_file_id_;
      int64_t index_offset_;
      int64_t index_size_;
      int64_t endkey_offset_;
      int64_t endkey_size_;
      int64_t block_count_;

      ObBlockIndexPositionInfo()
      {
        memset(this, 0, sizeof(ObBlockIndexPositionInfo));
      }

      void reset()
      {
        memset(this, 0, sizeof(ObBlockIndexPositionInfo));
      }

      int64_t hash() const
      {
        return common::murmurhash2(this, sizeof(ObBlockIndexPositionInfo), 0);
      }

      bool operator == (const ObBlockIndexPositionInfo& other) const
      {
        return (sstable_file_id_ == other.sstable_file_id_
            && index_offset_ == other.index_offset_
            && index_size_ == other.index_size_
            && endkey_offset_ == other.endkey_offset_
            && endkey_size_ == other.endkey_size_
            && block_count_ == other.block_count_);
      }

      uint64_t to_string(char* buf, const int64_t buf_len) const
      {
        int64_t pos = 0;

        if (pos < buf_len)
        {
          common::databuff_printf(buf, buf_len, pos,
              "<sstable_file_id_=%lu,"
              "index_offset_=%ld,index_size_=%ld,endkey_offset_=%ld,"
              "endkey_size_=%ld,block_count_=%ld", sstable_file_id_,
              index_offset_, index_size_, endkey_offset_, endkey_size_,
              block_count_);
        }

        return pos;
      }
    };

    struct ObBlockPositionInfo
    {
      int64_t offset_;
      int64_t size_;
    };

    struct ObBlockPositionInfos
    {
      static const int32_t NUMBER_OF_BATCH_BLOCK_INFO = 10000;
      int64_t block_count_;
      ObBlockPositionInfo position_info_[NUMBER_OF_BATCH_BLOCK_INFO];

      ObBlockPositionInfos()
        : block_count_(0)
      {
      }

      void reset()
      {
        block_count_ = 0;
        memset(position_info_, 0,
            NUMBER_OF_BATCH_BLOCK_INFO * sizeof(ObBlockPositionInfo));
      }

      uint64_t to_string(char* buf, const int64_t buf_len) const
      {
        int64_t pos = 0;

        if (pos < buf_len)
        {
          common::databuff_printf(buf, buf_len, pos,
              "block_count_=%ld:", block_count_);
        }

        if (pos < buf_len)
        {
          common::databuff_printf(buf, buf_len, pos, "<offset_,size_>:");
        }

        if (pos < buf_len)
        {
          for (int64_t i = 0; i < block_count_; i ++)
          {
            common::databuff_printf(buf, buf_len, pos, "<%ld,%ld>,",
                position_info_[i].offset_, position_info_[i].size_);
          }
        }

        return pos;
      }
    };

    class ObSSTableBlockIndexMgr
    {
    public:
      friend class ::TestSSTableBlockIndexMgr_construct_Test;

    public:
      typedef const ObSSTableBlockIndex* const_iterator;

    private:
      struct Bound
      {
        const char* base_;
        const_iterator begin_;
        const_iterator end_;
      };

    private:
      class Compare
      {
      public:
        Compare(const ObSSTableBlockIndexMgr& block_index) 
          : block_index_(block_index)
        {
        }

        bool operator()(const ObSSTableBlockIndex& index, 
            const common::ObRowkey& key)
        {
          common::ObRowkey tmp_key;
          block_index_.get_row_key(index, tmp_key);
          return tmp_key.compare(key) < 0;
        }

      private:
        const ObSSTableBlockIndexMgr& block_index_;
      };

    public:
      static const int32_t BLOCK_INDEX_ITEM_SIZE = 
        static_cast<int32_t>(sizeof(ObSSTableBlockIndex));

    public:
      ObSSTableBlockIndexMgr(const int64_t block_index_length = 0,
          const int64_t block_endkey_length = 0,
          const int64_t block_count = 0)
        : block_index_length_(block_index_length),
          block_endkey_length_(block_endkey_length),
          block_count_(block_count)
      {
        block_index_base_ = reinterpret_cast<char*>(this) 
          + sizeof(ObSSTableBlockIndexMgr);
        block_endkey_base_ = block_index_base_ + block_index_length_;
      }

      ~ObSSTableBlockIndexMgr()
      {
      }

      int search_batch_blocks_by_key(const common::ObRowkey& key,
          const SearchMode mode, ObBlockPositionInfos& pos_info) const;

      int search_batch_blocks_by_range(const common::ObNewRange& range,
          const bool is_reverse_scan, ObBlockPositionInfos& pos_info) const;

      int search_one_block_by_key(const common::ObRowkey& key,
          const SearchMode mode, ObBlockPositionInfo& pos_info) const;

      int search_batch_blocks_by_offset(const int64_t offset,
          const SearchMode mode, ObBlockPositionInfos& pos_info) const;

      ObSSTableBlockIndexMgr* copy(char* buffer) const;

      inline int64_t get_size() const
      {
        return (sizeof(*this) + block_index_length_ + block_endkey_length_);
      }

      inline const char* get_block_index_base() const
      {
        return block_index_base_;
      }

      inline const char* get_block_endkey_base() const
      {
        return block_endkey_base_;
      }

      inline const int64_t get_block_count() const
      {
        return block_count_;
      }

    private:
      int find_by_key(const common::ObRowkey& key, const SearchMode mode,
          const Bound& bound, const_iterator& find) const;

      int store_block_position_info(const_iterator find, const Bound& bound,
          const SearchMode mode, const common::ObRowkey& key,
          ObBlockPositionInfos& pos_info) const;

      int trans_range_to_search_key(const common::ObNewRange& range,
          const bool is_reverse_scan, common::ObRowkey& search_key,
          SearchMode& mode) const;

      int get_row_key(const ObSSTableBlockIndex& index, 
          common::ObRowkey& key) const;

      inline int get_bound(Bound& bound) const
      {
        int ret = common::OB_SUCCESS;
        bound.base_ = block_index_base_;
        bound.begin_ = reinterpret_cast<const_iterator>(bound.base_);
        bound.end_ = bound.begin_ + block_count_;
        return ret;
      }

      int check_border(const_iterator& find, const Bound& bound,
          const SearchMode mode) const;

   private:
      char* block_index_base_;
      int64_t block_index_length_;

      char* block_endkey_base_;
      int64_t block_endkey_length_;

      int64_t block_count_; //实际数目=block_count_ + 1
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif
