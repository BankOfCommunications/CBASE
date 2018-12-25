/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_desc.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_DESC_H
#define _OB_ROW_DESC_H 1
#include "common/ob_define.h"
#include "common/ob_bit_set.h"

namespace oceanbase
{
  namespace common
  {
    /// 行描述
    class ObRowDesc
    {
      public:
        struct Desc
        {
          uint64_t table_id_;
          uint64_t column_id_;

          bool is_invalid() const;
          bool operator== (const Desc &other) const;

          uint64_t hash() const {return ((table_id_ << 16) | ((column_id_ * 29 + 7) & 0xFFFF));};
        };

        template <class K, class V, uint64_t N = 1031>
        class PlacementHashMap
        {
          public:
            PlacementHashMap() { clear(); }
            int  insert(const K & key, const V & value);
            int  remove(const K & key);
            int  find  (const K & key, V & value) const;
            void clear ();
          protected:
            int set_   (uint64_t pos, const K & key, const V & value);
            int search_(const K & key, uint64_t & pos) const;
          protected:
            ObBitSet<N> flags_;
            K           keys_[N];
            V           values_[N];
        };

      public:
        ObRowDesc();
        ~ObRowDesc();
        /**
         * 根据表ID和列ID获得改列在元素数组中的下标
         *
         * @param table_id 表ID
         * @param column_id 列ID
         *
         * @return 下标或者OB_INVALID_INDEX
         */
        int64_t get_idx(const uint64_t table_id, const uint64_t column_id) const;
        /**
         * 根据列下标获得表ID和列ID
         *
         * @param idx
         * @param table_id [out]
         * @param column_id [out]
         *
         * @return OB_SUCCESS或错误码
         */
        int get_tid_cid(const int64_t idx, uint64_t &table_id, uint64_t &column_id) const;

        /// 一行中列的数目
        int64_t get_column_num() const;

        /// 添加下一列的描述信息
        int add_column_desc(const uint64_t table_id, const uint64_t column_id);

        /// 重置
        void reset();

        int64_t get_rowkey_cell_count() const;
        void set_rowkey_cell_count(int64_t rowkey_cell_count);

        int64_t to_string(char* buf, const int64_t buf_len) const;

        NEED_SERIALIZE_AND_DESERIALIZE;

        /// 获得内部运行时遇到的散列冲撞总数，用于监控和调优
        static uint64_t get_hash_collisions_count();

        const Desc* get_cells_desc_array(int64_t& array_size) const;

        ObRowDesc & operator = (const ObRowDesc & r);

        int assign(const ObRowDesc& other);

        //add lbzhong [Update rowkey] 20151221:b
        inline bool exist_column_id(const int64_t column_id) const
        {
          for(int64_t i = 0; i < cells_desc_count_; i++)
          {
            if(cells_desc_[i].column_id_ == static_cast<uint64_t>(column_id) ||
               (column_id == -1 && OB_ACTION_FLAG_COLUMN_ID == cells_desc_[i].column_id_))
            {
              return true;
            }
          }
          return false;
        }

        //add:e

      private:
        struct DescIndex
        {
          Desc desc_;
          int64_t idx_;
        };
        int hash_find(const uint64_t table_id, const uint64_t column_id, int64_t & index) const;
        int hash_insert(const uint64_t table_id, const uint64_t column_id, const int64_t index);
      private:
        static const int64_t MAX_COLUMNS_COUNT = common::OB_ROW_MAX_COLUMNS_COUNT; // 512
        static uint64_t HASH_COLLISIONS_COUNT;
        // data members
        Desc cells_desc_[MAX_COLUMNS_COUNT];
        int64_t cells_desc_count_;
        PlacementHashMap<Desc, int64_t> hash_map_;
        int64_t rowkey_cell_count_;
    };

    template <class K, class V, uint64_t N>
    int ObRowDesc::PlacementHashMap<K, V, N>::insert(const K & key, const V & value)
    {
      int ret = OB_SUCCESS;
      //uint64_t pos = murmurhash2(&key, sizeof(key), 0) % N;
      uint64_t pos = key.hash() % N;
      uint64_t i = 0;
      for (; i < N; i++, pos++)
      {
        if (pos == N)
        {
          pos = 0;
        }
        ret = set_(pos, key, value);
        if (ret != OB_ITEM_NOT_SETTED)
        {
          break;
        }
        HASH_COLLISIONS_COUNT++;
      }
      if (N == i)
      {
        TBSYS_LOG(ERROR, "hash buckets are full");
        ret = OB_ERROR;
      }
      return ret;
    }

    template <class K, class V, uint64_t N>
    int ObRowDesc::PlacementHashMap<K, V, N>::remove(const K & key)
    {
      int ret = OB_SUCCESS;
      uint64_t pos = 0;
      ret = search_(key, pos);
      if (OB_SUCCESS = ret)
      {
        flags_.del_member(pos);
      }
      return ret;
    }

    template <class K, class V, uint64_t N>
    int ObRowDesc::PlacementHashMap<K, V, N>::find(const K & key, V & value) const
    {
      int ret = OB_SUCCESS;
      uint64_t pos = 0;
      ret = search_(key, pos);
      if (OB_SUCCESS == ret)
      {
        value = values_[pos];
      }
      return ret;
    }

    template <class K, class V, uint64_t N>
    void ObRowDesc::PlacementHashMap<K, V, N>::clear()
    {
      flags_.clear();
    }

    template <class K, class V, uint64_t N>
    int ObRowDesc::PlacementHashMap<K, V, N>::set_(uint64_t pos, const K & key, const V & value)
    {
      int ret = OB_SUCCESS;
      if (!flags_.has_member(static_cast<int32_t>(pos)))
      {
        keys_[pos]   = key;
        values_[pos] = value;
        flags_.add_member(static_cast<int32_t>(pos));
      }
      else if (keys_[pos] == key)
      {
        if (values_[pos] == value)
        {
          ret = OB_ENTRY_EXIST;
        }
        else
        {
          ret = OB_DUPLICATE_COLUMN;
          TBSYS_LOG(WARN, "insert encounters duplicated key");
        }
      }
      else
      {
        ret = OB_ITEM_NOT_SETTED;
      }
      return ret;
    }

    template <class K, class V, uint64_t N>
    int ObRowDesc::PlacementHashMap<K, V, N>::search_(const K & key, uint64_t & pos) const
    {
      int ret = OB_SUCCESS;
      //pos = murmurhash2(&key, sizeof(key), 0) % N;
      pos = key.hash() % N;
      uint64_t i = 0;
      for (; i < N; i++, pos++)
      {
        if (pos == N)
        {
          pos = 0;
        }
        if (!flags_.has_member(static_cast<int32_t>(pos)))
        {
          ret = OB_ENTRY_NOT_EXIST;
          break;
        }
        else if (keys_[pos] == key)
        {
          break;
        }
      }
      if (N == i)
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      return ret;
    }

    inline const ObRowDesc::Desc* ObRowDesc::get_cells_desc_array(int64_t& array_size) const
    {
      array_size = cells_desc_count_;
      return cells_desc_;
    }

    inline int64_t ObRowDesc::get_column_num() const
    {
      return cells_desc_count_;
    }

    inline int ObRowDesc::get_tid_cid(const int64_t idx, uint64_t &table_id, uint64_t &column_id) const
    {
      int ret = common::OB_SUCCESS;
      if (idx < 0
          || idx >= cells_desc_count_)
      {
        ret = common::OB_INVALID_ARGUMENT;
      }
      else
      {
        table_id = cells_desc_[idx].table_id_;
        column_id = cells_desc_[idx].column_id_;
      }
      return ret;
    }

    inline uint64_t ObRowDesc::get_hash_collisions_count()
    {
      return HASH_COLLISIONS_COUNT;
    }

    inline int64_t ObRowDesc::get_rowkey_cell_count() const
    {
      return rowkey_cell_count_;
    }

    inline void ObRowDesc::set_rowkey_cell_count(int64_t rowkey_cell_count)
    {
      this->rowkey_cell_count_ = rowkey_cell_count;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_DESC_H */
