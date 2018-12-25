/*
 * (C) 2013-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_frozen_data_cache.h$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@alipay.com>
 *
 */

#ifndef OCEANBASE_MERGESERVER_OB_FROZEN_DATA_CACHE
#define OCEANBASE_MERGESERVER_OB_FROZEN_DATA_CACHE

#include <common/ob_string.h>
#include <common/ob_object.h>
#include <common/ob_iarray.h>
#include <common/ob_row.h>
#include <common/ob_row_desc.h>
#include <common/ob_kv_storecache.h>
#include <common/ob_row_store.h>
#include <common/utility.h>

namespace oceanbase
{
  namespace mergeserver
  {
    struct ObFrozenDataKey
    {
      common::ObVersion frozen_version;
      char *            param_buf;
      int64_t           len;

      ObFrozenDataKey();
      ~ObFrozenDataKey();

      int64_t hash() const
      {
        uint32_t hash_code = 0;
        hash_code = common::murmurhash2(&frozen_version,
            static_cast<int32_t>(sizeof(frozen_version)), hash_code);
        hash_code = common::murmurhash2(param_buf, static_cast<int32_t>(len),
            hash_code);
        return hash_code;
      };
      bool operator == (const ObFrozenDataKey & r) const
      {
        bool ret = false;
        if (frozen_version == r.frozen_version && len == r.len)
        {
          ret = (memcmp(param_buf, r.param_buf, len) == 0);
        }
        return ret;
      }
      int64_t to_string(char * buf, const int64_t buf_len) const;
    };
    struct ObFrozenDataKeyBuf
    {
      char * buf;
      int64_t buf_len;
      int64_t pos;

      ObFrozenDataKeyBuf();
      ~ObFrozenDataKeyBuf();
      int alloc_buf(int64_t bl);
    };

    // 这个结构体暂时不用，Query Cache模块会用到这个结构
    struct ObFrozenDataURLKey
    {
      common::ObString sql;
      uint64_t         subquery_id;
      uint64_t         frozen_version;
      int64_t          param_num;
      common::ObObj *  params;
      bool             need_free;

      ObFrozenDataURLKey();
      ~ObFrozenDataURLKey();
      void reset();
      void reuse();
      void set_sql(const common::ObString & s);
      void set_subquery_id(uint64_t id);
      void set_frozen_version(uint64_t version);
      int  set_params(const common::ObIArray<common::ObObj*> &ps);
      int64_t to_string(char * buf, const int64_t buf_len) const;

      int64_t hash() const
      {
        uint32_t hash_code = 0;
        hash_code = common::murmurhash2(&subquery_id, sizeof(subquery_id), hash_code);
        hash_code = common::murmurhash2(&frozen_version, sizeof(frozen_version), hash_code);
        hash_code = common::murmurhash2(&param_num, sizeof(param_num), hash_code);
        hash_code = common::murmurhash2(sql.ptr(), sql.length(), hash_code);
        for (int64_t i = 0; i < param_num; i++)
        {
          if (common::ObIntType != params[i].get_type()) abort();
          hash_code = params[i].murmurhash2(hash_code);
        }
        return hash_code;
      };
      bool operator == (const ObFrozenDataURLKey & r) const
      {
        bool ret = false;
        ret = frozen_version == r.frozen_version
               && subquery_id == r.subquery_id
               && sql == r.sql
               && compare_params_(r.param_num, r.params);
        return ret;
      }
    private:
      bool compare_params_(int64_t pn, const common::ObObj * ps) const
      {
        bool ret = true;
        if (param_num != pn)
        {
          ret = false;
        }
        else
        {
          for (int64_t i = 0; i < pn; i++)
          {
            if (params[i] != ps[i])
            {
              ret = false;
              break;
            }
          }
        }
        return ret;
      }
    };
    typedef common::ObRowStore ObFrozenData;
  } // namespace mergeserver

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      using mergeserver::ObFrozenDataURLKey;

      struct ObFrozenDataURLKeyDeepCopyTag {};
      template<>
      struct traits<ObFrozenDataURLKey>
      {
        typedef ObFrozenDataURLKeyDeepCopyTag Tag;
      };
      inline ObFrozenDataURLKey * do_copy(const ObFrozenDataURLKey  & key, char * buffer, ObFrozenDataURLKeyDeepCopyTag)
      {
        ObFrozenDataURLKey * ret = reinterpret_cast<ObFrozenDataURLKey *>(buffer);
        if (NULL != ret)
        {
          char * sql = buffer + sizeof(ObFrozenDataURLKey);
          int32_t sql_len = key.sql.length();
          memcpy(sql, key.sql.ptr(), sql_len);
          ret->sql = common::ObString(sql_len, sql_len, sql);
          ret->subquery_id = key.subquery_id;
          ret->frozen_version = key.frozen_version;
          ret->param_num = key.param_num;
          ret->need_free = false;
          ret->params = reinterpret_cast<ObObj *>(sql + key.sql.length());
          for (int64_t i = 0; i < key.param_num; i++)
          {
            ret->params[i] = key.params[i];
          }
        }
        return ret;
      }

      inline int32_t do_size(const ObFrozenDataURLKey & key, ObFrozenDataURLKeyDeepCopyTag)
      {
        int32_t ret = 0;
        ret += static_cast<int32_t>(sizeof(key));
        ret += static_cast<int32_t>(key.sql.length());
        ret += static_cast<int32_t>(sizeof(common::ObObj) * key.param_num);
        return ret;
      }

      inline void do_destroy(ObFrozenDataURLKey * key, ObFrozenDataURLKeyDeepCopyTag)
      {
        UNUSED(key);
      }


      using mergeserver::ObFrozenDataKey;

      struct ObFrozenDataKeyDeepCopyTag {};
      template<>
      struct traits<ObFrozenDataKey>
      {
        typedef ObFrozenDataKeyDeepCopyTag Tag;
      };
      inline ObFrozenDataKey * do_copy(const ObFrozenDataKey & key, char * buffer, ObFrozenDataKeyDeepCopyTag)
      {
        ObFrozenDataKey * ret = reinterpret_cast<ObFrozenDataKey *>(buffer);
        if (NULL != ret)
        {
          ret->frozen_version = key.frozen_version;
          ret->param_buf = buffer + sizeof(key);
          ret->len = key.len;
          memcpy(ret->param_buf, key.param_buf, ret->len);
        }
        return ret;
      }

      inline int32_t do_size(const ObFrozenDataKey & key, ObFrozenDataKeyDeepCopyTag)
      {
        int32_t ret = 0;
        ret += static_cast<int32_t>(sizeof(key));
        ret += static_cast<int32_t>(key.len);
        return ret;
      }

      inline void do_destroy(ObFrozenDataKey * key, ObFrozenDataKeyDeepCopyTag)
      {
        UNUSED(key);
      }

    } // namespace KVStoreCacheComponent
  } // namespace common

  namespace mergeserver
  {
    class ObFrozenDataCache;
    class ObCachedFrozenData
    {
      friend class ObFrozenDataCache;
      public:
        ObCachedFrozenData();
        ~ObCachedFrozenData();
        void reset();
        void reuse();
        void set_row_desc(const common::ObRowDesc & row_desc);
        bool has_data();
        int get_next_row(const common::ObRow * & row);
      protected:
        common::ObRowStore * row_store_;
        common::ObRowStore::iterator row_iter_;
        common::CacheHandle  handle_;
        common::ObRow        cur_row_;
    };

    class ObFrozenDataCache
    {
      public:
        ObFrozenDataCache();
        ~ObFrozenDataCache();
      public:
        int init(int64_t total_size);
        int get(const ObFrozenDataKey & key, ObCachedFrozenData & value);
        int revert(ObCachedFrozenData & value);
        int put(const ObFrozenDataKey & key, const ObFrozenData & data);
        bool get_in_use() const;
        int enlarge_total_size(int64_t total_size);
      protected:
        bool in_use_;
        static const int64_t ITEM_COUNT_PRINT_FREQUENCY = 1L << 16;
        static const int64_t FROZEN_DATA_AVG_SIZE = 2L << 10;
        static const int64_t CacheItemSize = sizeof(ObFrozenDataKey)
                + sizeof(ObFrozenData) + FROZEN_DATA_AVG_SIZE * 2;
        static const int64_t FROZEN_DATA_BLOCK_SIZE = 2L << 20;
        typedef common::KeyValueCache<ObFrozenDataKey, ObFrozenData,
                CacheItemSize, FROZEN_DATA_BLOCK_SIZE,
                common::KVStoreCacheComponent::SingleObjFreeList,
                common::hash::SpinReadWriteDefendMode> CacheData;
        int64_t get_counter_;
        CacheData cache_;
    };
  } // namespace mergeserver

} // namespace oceanbase

#endif // OCEANBASE_MERGESERVER_OB_FROZEN_DATA_CACHE
