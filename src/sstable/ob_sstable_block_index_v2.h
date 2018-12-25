/**
 * (C) 2010-2011 Taobao Inc.
 *  
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_block_index_v2.h for block block index. 
 *  
 * Authors: 
 *   duanfei <duanfei@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_INDEX_V2_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_INDEX_V2_H_

#include "common/murmur_hash.h"
#include "common/ob_string.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"

namespace oceanbase
{
  namespace sstable
  {
    enum SearchMode
    {
      OB_SEARCH_MODE_MIN_VALUE = 1,
      OB_SEARCH_MODE_EQUAL,
      OB_SEARCH_MODE_GREATER_EQUAL,
      OB_SEARCH_MODE_GREATER_THAN,
      OB_SEARCH_MODE_LESS_THAN,
      OB_SEARCH_MODE_LESS_EQUAL,
      OB_SEARCH_MODE_MAX_VALUE,
    };

    inline bool is_looking_forward_mode(const SearchMode mode) 
    {
      return mode >= OB_SEARCH_MODE_MIN_VALUE && mode <= OB_SEARCH_MODE_GREATER_THAN;
    }

    inline bool is_regular_mode(const SearchMode mode) 
    {
      return mode > OB_SEARCH_MODE_MIN_VALUE && mode < OB_SEARCH_MODE_MAX_VALUE;
    }

    struct ObBlockIndexPositionInfo
    {
      uint64_t sstable_file_id_;
      int64_t offset_;
      int64_t size_;

      int64_t hash() const
      {
        return common::murmurhash2(this, sizeof(ObBlockIndexPositionInfo), 0);
      };

      bool operator == (const ObBlockIndexPositionInfo& other) const
      {
        return (sstable_file_id_ == other.sstable_file_id_
                && offset_ == other.offset_
                && size_ == other.size_);
      };
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
    };
      
    class ObBlockIndexCache;
    class ObSSTableSchema;

    class ObSSTableBlockIndexV2
    {
      private:
        friend class DumpSSTable;
        friend class ObBlockIndexCache;

        struct IndexEntryType
        {
          uint64_t table_id_;
          uint64_t column_group_id_;
          int64_t block_offset_;
          int64_t block_record_size_;
          common::ObRowkey rowkey_;
          inline bool operator<(const IndexEntryType& entry) const
          {
            bool ret = false;
            if (table_id_ == entry.table_id_)
            {
              if (column_group_id_ == entry.column_group_id_)
              {
                ret = block_offset_ < entry.block_offset_;
              }
              else
              {
                ret = column_group_id_ < entry.column_group_id_;
              }
            }
            else
            {
              ret = table_id_ < entry.table_id_;
            }
            return ret;
          }
          inline bool operator==(const IndexEntryType& entry) const
          {
            return (table_id_ == entry.table_id_ 
                && column_group_id_ == entry.column_group_id_
                && block_offset_ == entry.block_offset_);
          }
          inline bool operator!=(const IndexEntryType& entry) const
          {
            return (table_id_ != entry.table_id_ 
                || column_group_id_ != entry.column_group_id_
                || block_offset_ != entry.block_offset_);
          }
        };

        struct IndexLookupKey
        {
          uint64_t table_id_;
          uint64_t column_group_id_;
          common::ObRowkey rowkey_;
          IndexLookupKey(const uint64_t id, 
              const uint64_t column_group_id, const common::ObRowkey& key)
            : table_id_(id), column_group_id_(column_group_id), rowkey_(key) {}
        };

        class Compare
        {
          public:
            Compare(const ObSSTableBlockIndexV2& block_index) : block_index_(block_index) {}
            bool operator()(const IndexEntryType& index, const IndexLookupKey& key)
            {
              bool ret = false;
              if (index.table_id_ == key.table_id_)
              {
                if (index.column_group_id_ == key.column_group_id_)
                {
                  ret = index.rowkey_.compare(key.rowkey_) < 0;
                }
                else
                {
                  ret = index.column_group_id_ < key.column_group_id_;
                }
              }
              else
              {
                ret = index.table_id_ < key.table_id_;
              }
              return ret;
            }
          private:
            const ObSSTableBlockIndexV2& block_index_;
        };

      public:
        typedef IndexEntryType* iterator;
        typedef const IndexEntryType* const_iterator;

      public:
        ObSSTableBlockIndexV2(const int64_t serialize_size = 0, 
                              const bool deserialized = true);
        ~ObSSTableBlockIndexV2();

        /**
         * search block position information in block index data
         * @param [in] table_id search table id
         * @param [in] column_group_id search column group id
         * @param [in] key  search key
         * @param [in] search_mode @see enum SearchMode 
         * @param [in|out] pos_info 
         * block position info(block offset in sstable file and block size)
         * SearchMode:
         *  OB_SEARCH_MODE_LESS_THAN,
         *  OB_SEARCH_MODE_LESS_EQUAL
         *    search the block less or equal input %key, return blocks ordered by desc
         *  OB_SEARCH_MODE_EQUAL,
         *  OB_SEARCH_MODE_GREATER_EQUAL,
         *  OB_SEARCH_MODE_GREATER_THAN   
         *    search the block greater or equal input %key, return blocks ordered by asc
         *  
         * @return
         *  OB_SUCCESS, got block(s) meet the requirement.
         *  OB_BEYOND_THE_RANGE, there is no block meet the requirement.
         *  OB_ERROR,other error code, internal error.
         */
        int search_batch_blocks_by_key(const uint64_t table_id, 
            const uint64_t column_group_id, 
            const oceanbase::common::ObRowkey& key, 
            const SearchMode mode, 
            ObBlockPositionInfos& pos_info) const;


        /**
         * same as above, search with range.
         */
        int search_batch_blocks_by_range(const uint64_t table_id, 
            const uint64_t column_group_id, 
            const common::ObNewRange& range, 
            const bool is_reverse_scan, 
            ObBlockPositionInfos& pos_info) const;

        /**
         * same as above, but only get one block info position, used by get. 
         */
        int search_one_block_by_key(const uint64_t table_id, 
            const uint64_t column_group_id, 
            const oceanbase::common::ObRowkey& key, 
            const SearchMode mode, 
            ObBlockPositionInfo& pos_info) const;

        /**
         * search next batch block position information base on current block pos
         * @param [in] table_id search table id
         * @param [in] column_group_id search column group id
         * @param [in] offset current block position information
         * @param [in] search_mode @see SearchMode
         * @param [in|out] pos_info search result
         * @return
         *  OB_SUCCESS, got block(s) meet the requirement.
         *  OB_BEYOND_THE_RANGE, there is no block meet the requirement.
         *  OB_ERROR,other error code, internal error.
         */
        int search_batch_blocks_by_offset(const uint64_t table_id, 
            const uint64_t column_group_id, 
            const int64_t offset, 
            const SearchMode mode , 
            ObBlockPositionInfos& pos_info) const;

        /**
         * start key of sstable is end_key of the first block 
         * the first block is a empty block, fill nothing but
         * only represents start key.
         */
        common::ObRowkey get_start_key(const uint64_t table_id) const;
        /**
         * end key of sstable is end_key of the last block.
         */
        common::ObRowkey get_end_key(const uint64_t table_id) const;

        ObSSTableBlockIndexV2* deserialize_copy(char* buffer) const;

        const int64_t get_deserialize_size();

      private:
        struct Bound
        {
          const char* base_;
          const_iterator begin_;
          const_iterator end_;
        };

        int get_bound(Bound& bound) const;
        inline const char* get_base() const { return base_; }
        inline char* get_base() { return base_; }

        const_iterator begin() const; 
        const_iterator end() const; 


        inline bool match_table_group(const IndexEntryType &entry, 
            const uint64_t table_id, const uint64_t column_group_id) const
        {
          return entry.table_id_ == table_id 
            && entry.column_group_id_ == column_group_id;
        }


        int store_block_position_info(
            const_iterator find,
            const Bound& bound,
            const SearchMode mode,
            const common::ObRowkey& end_key,
            const uint64_t table_id,
            const uint64_t column_group_id,
            ObBlockPositionInfos &pos_info) const;

        int check_border(
            const_iterator &find,
            const Bound& bound,
            const SearchMode mode,
            const uint64_t table_id,
            const uint64_t column_group_id) const;

        int find_by_key(const uint64_t table_id, 
            const uint64_t column_group_id, 
            const oceanbase::common::ObRowkey& key, 
            const SearchMode mode, 
            const Bound& bond,
            const_iterator& find) const;

        int find_start_in_table(const uint64_t table_id, const_iterator& find_it) const;
        int find_end_in_table(const uint64_t table_id, const_iterator& find_it) const;
        int find_start_in_group(const uint64_t table_id, 
            const uint64_t column_group_id, const_iterator& find_it) const;
        int find_end_in_group(const uint64_t table_id, 
            const uint64_t column_group_id, const_iterator& find_it) const;
        int find_pos_helper(const IndexEntryType& entry, const bool is_start,
            const_iterator& find_it) const;

        int trans_range_to_search_key(
            const common::ObNewRange& range,
            const bool is_reverse_scan, 
            common::ObRowkey& search_key, 
            SearchMode& mode) const;

        /**
         * called by ObBlockIndexCache, get the size of object.
         * @param [in] buf  block index data buffer
         * @param [in] data_len length of buf
         * @param [in|out] pos 
         * @return  > 0 on success otherwise on failure.
         */
        int64_t get_deserialize_size(const char*buf, const int64_t data_len, 
                                     int64_t& pos) const;
        int deserialize(const char* buf, const int64_t data_len, int64_t& pos,
            const char* base, int64_t base_length);

      protected:
        bool deserialized_;         //whether block index data is deserialized
        int64_t serialize_size_;    //serialize block index data size
        char* base_;                //base buffer of block index data
        int64_t base_length_;       // block index data length
        int64_t block_index_count_; // block index entry count
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif
