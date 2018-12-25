/**
 * (C) 2011-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_row_cache.h for sstable row cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_SSTABLE_SSTABLE_ROW_CACHE_H_
#define  OCEANBASE_SSTABLE_SSTABLE_ROW_CACHE_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_rowkey.h"
#include "common/ob_kv_storecache.h"

namespace oceanbase
{
  namespace sstable
  {
    struct ObSSTableRowCacheValue
    {
      int64_t size_;
      char* buf_;

      ObSSTableRowCacheValue() : size_(0), buf_(NULL)
      {

      }
    };

    /**
     * cs uses sstable_id+column_group_id+rowkey to identify one row
     * ups uses sstable_id+table_id+rowkey to identify one row
     */
    struct ObSSTableRowCacheKey
    {
      uint64_t sstable_id_;
      union
      {
        uint32_t column_group_id_;
        uint32_t table_id_;
      };
      int32_t  row_key_size_;
      common::ObRowkey row_key_;

      ObSSTableRowCacheKey()
      {
        sstable_id_  = common::OB_INVALID_ID;
        column_group_id_ = static_cast<uint32_t>(common::OB_INVALID_ID);
        row_key_size_ = 0;
        row_key_.assign(NULL, 0);
      }

      ObSSTableRowCacheKey(const uint64_t sstable_id, 
        const uint64_t table_or_group_id, common::ObRowkey& row_key_in)
      {
        sstable_id_ = sstable_id;
        column_group_id_ = static_cast<uint32_t>(table_or_group_id);
        row_key_size_ = static_cast<int32_t>(row_key_in.get_deep_copy_size());
        row_key_ = row_key_in;
      }

      int64_t hash()const
      {
        uint32_t hash_val = 0;
        hash_val = common::murmurhash2(&sstable_id_, sizeof(sstable_id_), hash_val);
        hash_val = common::murmurhash2(&column_group_id_, sizeof(column_group_id_), hash_val);
        if (NULL != row_key_.get_obj_ptr() && 0 < row_key_.get_obj_cnt())
        {
          hash_val = row_key_.murmurhash2(hash_val);
        }
        return hash_val;
      }

      bool operator==(const ObSSTableRowCacheKey &other) const
      {
        bool ret = true;
        ret = ((sstable_id_ == other.sstable_id_)
               && (column_group_id_ == other.column_group_id_)
               && (row_key_size_ == other.row_key_size_));
        if (ret && row_key_size_ > 0)
        {
          if (NULL != row_key_.get_obj_ptr() && NULL != other.row_key_.get_obj_ptr())
          {
            ret = (row_key_.compare(other.row_key_) == 0);
          }
          else
          {
            ret = false;
          }
        }
        return ret;
      }
    };
  }

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct ObSSTableRowCacheDeepCopyTag
      {
      };

      struct ObSSTableRowCacheKeyDeepCopyTag
      {
      };

      template <>
      struct traits<sstable::ObSSTableRowCacheValue>
      {
        typedef ObSSTableRowCacheDeepCopyTag Tag;
      };

      template<>
      struct traits<sstable::ObSSTableRowCacheKey>
      {
        typedef ObSSTableRowCacheKeyDeepCopyTag Tag;
      };

      inline sstable::ObSSTableRowCacheValue* do_copy(
        const sstable::ObSSTableRowCacheValue& other, 
        char* buffer, ObSSTableRowCacheDeepCopyTag)
      {
        sstable::ObSSTableRowCacheValue *ret = 
          reinterpret_cast<sstable::ObSSTableRowCacheValue*>(buffer);

        if (NULL != ret)
        {
          ret->size_ = other.size_;
          ret->buf_ = buffer + sizeof(sstable::ObSSTableRowCacheValue);
          if (other.size_ > 0)
          {
            memcpy(ret->buf_, other.buf_, other.size_);
          }
        }

        return ret;
      }

      inline sstable::ObSSTableRowCacheKey *do_copy(
        const sstable::ObSSTableRowCacheKey &other,
        char *buffer, ObSSTableRowCacheKeyDeepCopyTag)
      {
        sstable::ObSSTableRowCacheKey *ret = 
          reinterpret_cast<sstable::ObSSTableRowCacheKey*>(buffer);
        if (NULL != ret)
        {
          ret->sstable_id_ = other.sstable_id_;
          ret->column_group_id_ = other.column_group_id_;
          ret->row_key_.assign(NULL, 0);
          ret->row_key_size_ = 0;
          if (NULL != other.row_key_.get_obj_ptr() && 0 < other.row_key_size_)
          {
            ObRawBufAllocatorWrapper temp_buf(buffer + sizeof(sstable::ObSSTableRowCacheKey), other.row_key_size_);
            other.row_key_.deep_copy(ret->row_key_, temp_buf);
            ret->row_key_size_ = other.row_key_size_;
          }
        }
        return ret;
      }

      inline int32_t do_size(const sstable::ObSSTableRowCacheValue& data,
                             ObSSTableRowCacheDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(sstable::ObSSTableRowCacheValue) + data.size_);
      }

      inline int32_t do_size(const sstable::ObSSTableRowCacheKey& key,
                             ObSSTableRowCacheKeyDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(sstable::ObSSTableRowCacheKey) + key.row_key_size_);
      }

      inline void do_destroy(sstable::ObSSTableRowCacheValue* data,
                             ObSSTableRowCacheDeepCopyTag)
      {
        UNUSED(data);
      }

      inline void do_destroy(sstable::ObSSTableRowCacheKey* key,
                             ObSSTableRowCacheKeyDeepCopyTag)
      {
        UNUSED(key);
      }
    }
  }

  namespace sstable
  {
    class ObSSTableRowCache
    {
      static const int64_t KVCACHE_ITEM_SIZE = 2 * 1024;         //2k
      static const int64_t KVCACHE_BLOCK_SIZE = 2 * 1024 * 1024; //2M

    public:
      typedef common::KeyValueCache<ObSSTableRowCacheKey, ObSSTableRowCacheValue, 
      KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE, 
      common::KVStoreCacheComponent::SingleObjFreeList, 
      common::hash::SpinReadWriteDefendMode> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObSSTableRowCache();
      ~ObSSTableRowCache();

      int init(const int64_t max_mem_size);
      int enlarg_cache_size(const int64_t cache_mem_size);

      inline bool is_inited() const
      {
        return inited_;
      }

      inline const int64_t get_cache_mem_size() const 
      {
        return kv_cache_.size();
      }

      //clear all items in SSTable row cache
      int clear();

      //destroy this cache in order to reuse it
      int destroy();

      /**
       * get one row from SSTable Row cache
       * 
       * @param key table_id + version + row_key
       * @param row_value [out] serialized cells array of row to 
       *                  return
       * @param row_buf row buffer to store the row data copied from 
       *                sstable row cache
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR, OB_ENTRY_NOT_EXIST, OB_NOT_INIT,
       *         OB_INVALID_ARGUMENT
       */
      int get_row(const ObSSTableRowCacheKey& key, ObSSTableRowCacheValue& row_value, 
        common::ObMemBuf& row_buf);

      /**
       * put one row into SSTable Row cache
       * 
       * @param key table_id + version + row_key
       * @param row_value [in] serialized cells array of row to put
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int put_row(const ObSSTableRowCacheKey& key, 
        const ObSSTableRowCacheValue& row_value);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableRowCache);

      bool inited_;
      KVCache kv_cache_;
    };
  }
}

#endif //OCEANBASE_SSTABLE_SSTABLE_ROW_CACHE_H_
