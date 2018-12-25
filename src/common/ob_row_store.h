/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_store.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_STORE_H
#define _OB_ROW_STORE_H 1
#include <stdint.h>
#include <utility>
#include "ob_row.h"
#include "ob_row_iterator.h"
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_tsi_block_allocator.h"
#include "common/ob_kv_storecache.h"
#include "ob_ups_row.h"
#include "ob_ups_row_util.h"

namespace oceanbase
{
  namespace common
  {
    class ObRowStore
    {
      private:
        struct BlockInfo;
        class Iterator;
      public:
        struct StoredRow
        {
          int32_t compact_row_size_;
          int32_t reserved_cells_count_;
          common::ObObj reserved_cells_[0];
          // ... compact_row
          const common::ObString get_compact_row() const;
        };
        class iterator
        {
          friend class ObRowStore;
          public:
            iterator             ();
            iterator             (ObRowStore * row_store, int64_t cur_iter_pos,
                                  BlockInfo * cur_iter_block, bool got_first_next);
            iterator             (const iterator & r);
            int get_next_row     (ObRow &row,
                                  common::ObString *compact_row = NULL);
            int get_next_row     (const ObRowkey *&rowkey, ObRow &row,
                                  common::ObString *compact_row = NULL);
            int get_next_ups_row (ObUpsRow &row,
                                  common::ObString *compact_row = NULL);
            int get_next_ups_row (const ObRowkey *&rowkey, ObUpsRow &row,
                                  common::ObString *compact_row = NULL);
          protected:
            int get_next_row     (ObRowkey *rowkey, ObObj *rowkey_obj, ObRow &row,
                                  common::ObString *compact_row);
            int next_iter_pos    (BlockInfo *&iter_block, int64_t &iter_pos);
          protected:
            ObRowStore *       row_store_;
            int64_t            cur_iter_pos_;
            BlockInfo *        cur_iter_block_;
            bool               got_first_next_;
            ObRowkey           cur_rowkey_;
            ObObj              cur_rowkey_obj_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        };
      public:
        ObRowStore(const int32_t mod_id = ObModIds::OB_SQL_ROW_STORE, ObIAllocator* allocator = NULL);
        ~ObRowStore();
        int32_t get_copy_size() const;
        int32_t get_meta_size() const;
        void set_block_size(int64_t block_size = NORMAL_BLOCK_SIZE);
        ObRowStore *clone(char *buffer) const;
        int add_reserved_column(uint64_t tid, uint64_t cid);
        void clear();
        void reuse();
        /**
         * clear rows only, keep reserved columns info
         */
        void clear_rows();
        /**
         * rollback last added row
         * NOTE: can only be called once.
         * must NOT called continuously, otherwise will return err
         */
        int rollback_last_row();
        /**
         * add row into the store
         *
         * @param row [in]
         * @param sort_row [out] stored row
         *
         * @return error code
         */
        int add_row(const ObRow &row, const StoredRow *&stored_row);
        /**
         * add row into the store
         *
         * @param row [in]
         * @param cur_size_counter [out] total mem used by the store
         *
         * @return error code
         */
        int add_row(const ObRow &row, int64_t &cur_size_counter);

        int add_row(const ObRowkey &rowkey, const ObRow &row, int64_t &cur_size_counter);
        int add_row(const ObRowkey &rowkey, const ObRow &row, const StoredRow *&stored_row);

        int add_ups_row(const ObUpsRow &row, const StoredRow *&stored_row);
        int add_ups_row(const ObUpsRow &row, int64_t &cur_size_counter);

        //stored_row包含rowkey和row的内容
        int add_ups_row(const ObRowkey &rowkey, const ObUpsRow &row, const StoredRow *&stored_row);
        int add_ups_row(const ObRowkey &rowkey, const ObUpsRow &row, int64_t &cur_size_counter);

        bool is_empty() const;
        int64_t get_used_mem_size() const;

        int get_next_row(ObRow &row, common::ObString *compact_row = NULL);
        int get_next_row(const ObRowkey *&rowkey, ObRow &row, common::ObString *compact_row = NULL);
        int get_next_ups_row(ObUpsRow &row, common::ObString *compact_row = NULL);
        int get_next_ups_row(const ObRowkey *&rowkey, ObUpsRow &row, common::ObString *compact_row = NULL);
        void reset_iterator();

        iterator begin();

        int64_t to_string(char* buf, const int64_t buf_len) const;
        //add lbzhong [Update rowkey] 20151221:b
        inline void set_is_update_second_index(const bool is_update_second_index)
        {
          is_update_second_index_ = is_update_second_index;
        }
        //add:e
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        static const int64_t BIG_BLOCK_SIZE = ObTSIBlockAllocator::BIG_BLOCK_SIZE; 
        static const int64_t NORMAL_BLOCK_SIZE = ObTSIBlockAllocator::NORMAL_BLOCK_SIZE; 
      private:
        int new_block(int64_t block_size);
        int new_block();
        int64_t get_reserved_cells_size(const int64_t reserved_columns_count) const;
        int64_t get_compact_row_min_size(const int64_t row_columns_count) const;
        int add_row(const ObRowkey *rowkey, const ObRow &row, const StoredRow *&stored_row, int64_t &cur_size_counter);

        // @return OB_SIZE_OVERFLOW if buffer not enough
        int append_row(const ObRowkey *rowkey, const ObRow &row, BlockInfo &block, StoredRow &stored_row);

      private:
        common::ObArray<std::pair<uint64_t, uint64_t> > reserved_columns_;
        ObIAllocator* allocator_;
        BlockInfo *block_list_head_;
        BlockInfo *block_list_tail_;
        int64_t block_count_;
        int64_t cur_size_counter_;
        int64_t rollback_iter_pos_;
        BlockInfo *rollback_block_list_;
        int32_t mod_id_;
        int64_t block_size_;
        bool is_read_only_;
        iterator inner_;
        //add lbzhong [Update rowkey] 20151221:b
        bool is_update_second_index_;
        //add:e
    };

    namespace KVStoreCacheComponent
    {
      struct ObRowStoreDeepCopyTag {};
      template<>
      struct traits<ObRowStore>
      {
        typedef ObRowStoreDeepCopyTag Tag;
      };
      inline ObRowStore* do_copy(const ObRowStore &other, char *buffer, ObRowStoreDeepCopyTag)
      {
        return other.clone(buffer);
      }
      inline int32_t do_size(const ObRowStore &data, ObRowStoreDeepCopyTag)
      {
        return data.get_copy_size();
      }
      inline void do_destroy(ObRowStore *data, ObRowStoreDeepCopyTag)
      {
        UNUSED(data);
      }
    }

    inline int64_t ObRowStore::get_reserved_cells_size(const int64_t reserved_columns_count) const
    {
      return sizeof(StoredRow) + (sizeof(common::ObObj) * reserved_columns_count);
    }

    inline int64_t ObRowStore::get_compact_row_min_size(const int64_t row_columns_count) const
    {
      // 4 ==  SUM( len(TypeAttr) = 1, len(int8) = 1, len(column id) = 2 )
      // 8 is a padding value/magic number, try to avoid a useless deserialization when reaching the end of a block
      return 4 * row_columns_count + 8;
    }

    inline const common::ObString ObRowStore::StoredRow::get_compact_row() const
    {
      common::ObString ret;
      ret.assign_ptr(reinterpret_cast<char*>(const_cast<common::ObObj*>(&reserved_cells_[reserved_cells_count_])), compact_row_size_);
      return ret;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_STORE_H */
