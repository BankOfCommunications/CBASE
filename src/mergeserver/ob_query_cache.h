/**
 * (C) 2011-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_query_cache.h for query cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_MERGESERVER_OB_QUERY_CACHE_H_
#define  OCEANBASE_MERGESERVER_OB_QUERY_CACHE_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_kv_storecache.h"

namespace oceanbase
{
  namespace mergeserver
  {
    struct ObQueryCacheValue
    {
      int64_t expire_time_;
      int64_t size_;
      char* buf_;

      ObQueryCacheValue() : expire_time_(0), size_(0), buf_(NULL)
      {

      }
    };
  }

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct ObQueryCacheDeepCopyTag
      {
      };

      template <>
      struct traits<mergeserver::ObQueryCacheValue>
      {
        typedef ObQueryCacheDeepCopyTag Tag;
      };

      inline mergeserver::ObQueryCacheValue* do_copy(
        const mergeserver::ObQueryCacheValue& other, 
        char* buffer, ObQueryCacheDeepCopyTag)
      {
        mergeserver::ObQueryCacheValue *ret = 
          reinterpret_cast<mergeserver::ObQueryCacheValue*>(buffer);

        if (NULL != ret)
        {
          ret->expire_time_ = other.expire_time_;
          ret->size_ = other.size_;
          ret->buf_ = buffer + sizeof(mergeserver::ObQueryCacheValue);
          if (other.size_ > 0)
          {
            memcpy(ret->buf_, other.buf_, other.size_);
          }
        }

        return ret;
      }

      inline int32_t do_size(const mergeserver::ObQueryCacheValue& data,
                             ObQueryCacheDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(mergeserver::ObQueryCacheValue) + data.size_);
      }

      inline void do_destroy(mergeserver::ObQueryCacheValue* data,
                             ObQueryCacheDeepCopyTag)
      {
        UNUSED(data);
      }
    }
  }

  namespace mergeserver
  {
    class ObQueryCache
    {
      static const int64_t KVCACHE_ITEM_SIZE = 1024;         //1KB
      static const int64_t KVCACHE_BLOCK_SIZE = 2 * 1024 * 1024; //2M

    public:
      typedef common::KeyValueCache<common::ObString, ObQueryCacheValue, 
      KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE,
      common::KVStoreCacheComponent::SingleObjFreeList, 
      common::hash::SpinReadWriteDefendMode> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObQueryCache();
      ~ObQueryCache();

      int init(const int64_t max_mem_size);

      inline bool is_inited() const
      {
        return inited_;
      }

      inline const int64_t get_cache_mem_size() const 
      {
        return kv_cache_.size();
      }

      //clear all items in query cache
      int clear();

      //destroy this cache in order to reuse it
      int destroy();

      /**
       * get query result from query cache
       * 
       * @param key serialized scan parameter or get parameter
       * @param out_buffer [out] to store the query result
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR, OB_ENTRY_NOT_EXIST, OB_NOT_INIT,
       *         OB_INVALID_ARGUMENT
       */
      int get(const common::ObString& key, common::ObDataBuffer& out_buffer);

      /**
       * put query result into query cache
       * 
       * @param key serialized scan parameter or get parameter
       * @param value [in] serialized obscanner to put
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR, OB_NOT_INIT, OB_INVALID_ARGUMENT
       */
      int put(const common::ObString& key, const ObQueryCacheValue& value);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObQueryCache);

      bool inited_;
      KVCache kv_cache_;
    };
  }
}

#endif //OCEANBASE_MERGESERVER_OB_QUERY_CACHE_H_
