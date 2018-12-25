#ifndef OB_INSERT_CACHE_H_
#define OB_INSERT_CACHE_H_

#include "common/ob_kv_storecache.h"
#include "common/bloom_filter.h"
#include "common/ob_range2.h"

namespace oceanbase
{
  namespace mergeserver
  {
    struct InsertCacheKey
    {
      InsertCacheKey():
        range_(), tablet_version_(-1)
      {
      }
      common::ObNewRange range_;
      int tablet_version_;
      inline int64_t hash() const
      {
        uint64_t hash_code = 0;
        hash_code = static_cast<uint32_t>(range_.hash());
        hash_code = common::murmurhash64A(&tablet_version_, sizeof(tablet_version_), hash_code);
        return hash_code;
      }
      inline bool operator == (const InsertCacheKey &other) const
      {
        bool ret = false;
        if (other.tablet_version_ == tablet_version_
            && other.range_ == range_)
        {
          ret = true;
        }
        return ret;
      }
      inline int32_t size() const
      {
        return static_cast<int32_t>(sizeof(*this) + range_.start_key_.get_deep_copy_size() + range_.end_key_.get_deep_copy_size());
      }
    };
    class InsertCacheWrapValue
    {
      public:
        InsertCacheWrapValue();
        ~InsertCacheWrapValue();
        common::CacheHandle handle_;
        common::ObBloomFilterAdapterV1 *bf_;
        inline void reset()
        {
          bf_ = NULL;
        }
    };
    inline int deep_copy(const InsertCacheKey &other, char *buffer)
    {
      int err = common::OB_SUCCESS;
      InsertCacheKey *tmp = new (buffer) InsertCacheKey();
      common::ObRowkey *start_key = &(tmp->range_.start_key_);
      common::ObRowkey *end_key = &(tmp->range_.end_key_);
      tmp->range_.table_id_ = other.range_.table_id_;
      tmp->range_.border_flag_ = other.range_.border_flag_;
      tmp->tablet_version_ = other.tablet_version_;
      common::AdapterAllocator alloc;
      alloc.init(buffer + sizeof(InsertCacheKey));
      if (common::OB_SUCCESS != (err = other.range_.start_key_.deep_copy(*start_key, alloc)))
      {
        TBSYS_LOG(ERROR, "deep copy startkey to insert cache failed, ret=%d", err);
      }
      else
      {
        if (common::OB_SUCCESS != (err = other.range_.end_key_.deep_copy(*end_key, alloc)))
        {
          TBSYS_LOG(ERROR, "deep copy startkey to insert cache failed, ret=%d", err);
        }
      }
      return err;
    }
    class ObInsertCache
    {
      public:
        typedef common::ObBloomFilterAdapterV1 InsertCacheValue;
        static const int64_t BLOOM_FILTER_AVG_SIZE = 2L << 20;
        static const int64_t CacheItemSize = sizeof(InsertCacheKey)
            + sizeof(InsertCacheValue) + BLOOM_FILTER_AVG_SIZE * 2;
        static const int64_t BLOOM_FILTER_BLOCK_SIZE = 2L << 20;
        static const int64_t ITEM_COUNT_PRINT_FREQUENCY = 1L << 16;
      public:
        //TODO:换成指针 ？
        typedef common::KeyValueCache<InsertCacheKey, InsertCacheValue,
                CacheItemSize, BLOOM_FILTER_BLOCK_SIZE,
                common::KVStoreCacheComponent::SingleObjFreeList,
                common::hash::SpinReadWriteDefendMode> InsertCache;
        ObInsertCache();
        ~ObInsertCache();
        int init(int64_t total_size);
        int put(const InsertCacheKey & key, const InsertCacheValue & value);
        int revert(InsertCacheWrapValue & value);
        int get(const InsertCacheKey & key, InsertCacheWrapValue & value);
        bool get_in_use() const;
        int enlarge_total_size(int64_t total_size);
      private:
        bool in_use_;
        int64_t get_counter_;
        InsertCache bf_cache_;
    };
  }
  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct InsertCacheKeyDeepCopyTag {};
      template<>
        struct traits<mergeserver::InsertCacheKey>
        {
          typedef InsertCacheKeyDeepCopyTag Tag;
        };
      inline mergeserver::InsertCacheKey* do_copy(const mergeserver::InsertCacheKey &other, char *buffer, InsertCacheKeyDeepCopyTag)
      {
        mergeserver::InsertCacheKey *ret = NULL;
        int err = OB_SUCCESS;
        if (OB_SUCCESS == (err = deep_copy(other, buffer)))
        {
          ret = reinterpret_cast<mergeserver::InsertCacheKey*>(buffer);
        }
        else
        {
          TBSYS_LOG(INFO, "deep_copy failed, err=%d", err);
        }
        return ret;
      }
      inline int32_t do_size(const mergeserver::InsertCacheKey &data, InsertCacheKeyDeepCopyTag)
      {
        return data.size();
      }
      inline void do_destroy(mergeserver::InsertCacheKey *data, InsertCacheKeyDeepCopyTag)
      {
        UNUSED(data);
      }
    }
  }
}

#endif
