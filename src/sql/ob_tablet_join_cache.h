/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join_cache.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_TABLET_JOIN_CACHE_H
#define _OB_TABLET_JOIN_CACHE_H 1

#include "common/ob_kv_storecache.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTabletJoinCacheKey
    {
      public:
        ObTabletJoinCacheKey();

        uint32_t murmurhash2(const uint32_t hash) const;
        inline int64_t hash() const
        {
          return murmurhash2(0);
        }

        bool operator ==(const ObTabletJoinCacheKey &other) const;

      public:
        uint64_t table_id_;
        common::ObRowkey rowkey_;
    };
  }

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct ObTabletJoinCacheKeyCopyTag {};

      template <>
      struct traits<ObTabletJoinCacheKeyCopyTag>
      {
        typedef ObTabletJoinCacheKeyCopyTag Tag;
      };

      inline sql::ObTabletJoinCacheKey *do_copy(const sql::ObTabletJoinCacheKey &other, char *buffer, ObTabletJoinCacheKeyCopyTag)
      {
        sql::ObTabletJoinCacheKey *ret = new(buffer) sql::ObTabletJoinCacheKey();
        if (NULL != ret)
        {
          ret->table_id_ = other.table_id_;
          BufferAllocator allocator(buffer + sizeof(sql::ObTabletJoinCacheKey));
          int tmp_ret = other.rowkey_.deep_copy(ret->rowkey_, allocator);
          if (OB_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "deep_copy rowkey fail ret=%d", tmp_ret);
            ret = NULL;
          }
        }
        return ret;
      }

      inline int32_t do_size(const sql::ObTabletJoinCacheKey &data, ObTabletJoinCacheKeyCopyTag)
      {
        return (static_cast<int32_t>(sizeof(sql::ObTabletJoinCacheKey)) + static_cast<int32_t>(data.rowkey_.get_deep_copy_size()));
      }

      inline void do_destroy(sql::ObTabletJoinCacheKey *data, ObTabletJoinCacheKeyCopyTag)
      {
        data->~ObTabletJoinCacheKey();
      }
    }
  }

  namespace sql
  {
    static const int64_t KVCACHE_SIZE = 4*1024L*1024L; //4M
    static const int64_t KVCACHE_ITEM_SIZE = 1024L; //1k
    static const int64_t KVCACHE_BLOCK_SIZE = 1*1024*1024L; //1M

    typedef common::KeyValueCache<ObTabletJoinCacheKey, common::ObString, KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE> ObTabletJoinCache;
  }
}

#endif /* _OB_TABLET_JOIN_CACHE_H */
  

