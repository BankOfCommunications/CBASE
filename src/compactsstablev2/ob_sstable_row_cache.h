#ifndef  OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_ROW_CACHE_H_
#define  OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_ROW_CACHE_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_rowkey.h"
#include "common/ob_kv_storecache.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    struct ObSSTableRowCacheValue
    {
      char* buf_;
      int64_t size_;

      ObSSTableRowCacheValue() 
        : buf_(NULL), size_(0)
      {
      }
    };

    struct ObSSTableRowCacheKey
    {
      uint64_t sstable_id_;
      uint64_t table_id_;
      int32_t row_key_size_;
      common::ObRowkey row_key_;

      ObSSTableRowCacheKey()
      {
        sstable_id_  = common::OB_INVALID_ID;
        table_id_ = common::OB_INVALID_ID;
        row_key_size_ = 0;
        row_key_.assign(NULL, 0);
      }

      ObSSTableRowCacheKey(const uint64_t sstable_id, 
        const uint64_t table_id, common::ObRowkey& row_key)
      {
        sstable_id_ = sstable_id;
        table_id_ = table_id;
        row_key_size_ = static_cast<int32_t>(row_key.get_deep_copy_size());
        row_key_ = row_key;
      }

      int64_t hash()const
      {
        uint32_t hash_val = 0;
        hash_val = common::murmurhash2(&sstable_id_, 
            sizeof(sstable_id_), hash_val);
        hash_val = common::murmurhash2(&table_id_, 
            sizeof(sstable_id_), hash_val);
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
               && (table_id_ == other.table_id_)
               && (row_key_size_ == other.row_key_size_));
        if (ret && row_key_size_ > 0)
        {
          if (NULL != row_key_.get_obj_ptr() 
              && NULL != other.row_key_.get_obj_ptr())
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
      struct traits<compactsstablev2::ObSSTableRowCacheValue>
      {
        typedef ObSSTableRowCacheDeepCopyTag Tag;
      };

      template<>
      struct traits<compactsstablev2::ObSSTableRowCacheKey>
      {
        typedef ObSSTableRowCacheKeyDeepCopyTag Tag;
      };

      inline compactsstablev2::ObSSTableRowCacheValue* do_copy(
        const compactsstablev2::ObSSTableRowCacheValue& other, 
        char* buffer, ObSSTableRowCacheDeepCopyTag)
      {
        compactsstablev2::ObSSTableRowCacheValue *ret = 
          reinterpret_cast<compactsstablev2::ObSSTableRowCacheValue*>(buffer);

        if (NULL != ret)
        {
          ret->size_ = other.size_;
          ret->buf_ = buffer + sizeof(compactsstablev2::ObSSTableRowCacheValue);
          if (other.size_ > 0)
          {
            memcpy(ret->buf_, other.buf_, other.size_);
          }
        }

        return ret;
      }

      inline compactsstablev2::ObSSTableRowCacheKey *do_copy(
        const compactsstablev2::ObSSTableRowCacheKey &other,
        char *buffer, ObSSTableRowCacheKeyDeepCopyTag)
      {
        compactsstablev2::ObSSTableRowCacheKey *ret = 
          reinterpret_cast<compactsstablev2::ObSSTableRowCacheKey*>(buffer);
        if (NULL != ret)
        {
          ret->sstable_id_ = other.sstable_id_;
          ret->table_id_ = other.table_id_;
          ret->row_key_.assign(NULL, 0);
          ret->row_key_size_ = 0;
          if (NULL != other.row_key_.get_obj_ptr()
              && 0 < other.row_key_size_)
          {
            ObRawBufAllocatorWrapper temp_buf(buffer 
                + sizeof(compactsstablev2::ObSSTableRowCacheKey), 
                other.row_key_size_);
            other.row_key_.deep_copy(ret->row_key_, temp_buf);
            ret->row_key_size_ = other.row_key_size_;
          }
        }
        return ret;
      }

      inline int32_t do_size(
          const compactsstablev2::ObSSTableRowCacheValue& data,
          ObSSTableRowCacheDeepCopyTag)
      {
        return static_cast<int32_t>(
            sizeof(compactsstablev2::ObSSTableRowCacheValue) 
            + data.size_);
      }

      inline int32_t do_size(const compactsstablev2::ObSSTableRowCacheKey& key,
          ObSSTableRowCacheKeyDeepCopyTag)
      {
        return static_cast<int32_t>(
            sizeof(compactsstablev2::ObSSTableRowCacheKey) 
            + key.row_key_size_);
      }

      inline void do_destroy(compactsstablev2::ObSSTableRowCacheValue* data,
          ObSSTableRowCacheDeepCopyTag)
      {
        UNUSED(data);
      }

      inline void do_destroy(compactsstablev2::ObSSTableRowCacheKey* key,
          ObSSTableRowCacheKeyDeepCopyTag)
      {
        UNUSED(key);
      }
    }//end namespace KVStoreCacheComponent
  }//end namespace common

  namespace compactsstablev2
  {
    class ObSSTableRowCache
    {
    public:
      static const int64_t KVCACHE_ITEM_SIZE = 1024;
      static const int64_t KVCACHE_BLOCK_SIZE = 1024 * 1024;

    public:
      typedef common::KeyValueCache<ObSSTableRowCacheKey, 
              ObSSTableRowCacheValue, KVCACHE_ITEM_SIZE, 
              KVCACHE_BLOCK_SIZE, 
              common::KVStoreCacheComponent::SingleObjFreeList,
              common::hash::SpinReadWriteDefendMode> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObSSTableRowCache()
        : inited_(false)
      {
      }

      ~ObSSTableRowCache()
      {
        destroy();
      }

      int init(const int64_t max_mem_size);

      inline bool is_inited() const
      {
        return inited_;
      }

      inline const int64_t get_cache_mem_size() const 
      {
        return kv_cache_.size();
      }

      inline int clear()
      {
        return kv_cache_.clear();
      }

      inline int destroy()
      {
        inited_ = false;
        return kv_cache_.destroy();
      }

      int get_row(const ObSSTableRowCacheKey& key, 
          ObSSTableRowCacheValue& row_value, common::ObMemBuf& row_buf);

      int put_row(const ObSSTableRowCacheKey& key, 
        const ObSSTableRowCacheValue& row_value);

    private:
      bool inited_;
      KVCache kv_cache_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase

#endif //OCEANBASE_SSTABLE_SSTABLE_ROW_CACHE_H_
