/*
 * (C) 2013-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_sql_query_cache.h$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@alipay.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_SQL_QUERY_CACHE
#define OCEANBASE_SQL_OB_SQL_QUERY_CACHE

#include <common/ob_kv_storecache.h>
#include <common/ob_tl_store.h>
#include <common/ob_iarray.h>
#include <common/ob_object.h>
#include <mergeserver/ob_merge_server_config.h>

namespace oceanbase
{
  namespace sql
  {
    struct ObSQLQueryCacheKey
    {
      int64_t         sql_id;
      int64_t         param_num;
      common::ObObj * params;
      void reuse()
      {
        sql_id = 0;
        param_num = 0;
        params = NULL;
      }
      int64_t hash() const
      {
        uint32_t hash_code = 0;
        hash_code = common::murmurhash2(&sql_id,
            static_cast<int32_t>(sizeof(sql_id)), hash_code);
        for (int64_t i = 0; i < param_num; i++)
        {
          hash_code = params[i].murmurhash2(hash_code);
        }
        TBSYS_LOG(DEBUG, "CacheKey key: %s hash: %u",
            common::to_cstring(*this), hash_code);
        return hash_code;
      };
      bool operator == (const ObSQLQueryCacheKey & r) const
      {
        bool ret = true;
        if (sql_id != r.sql_id)
        {
          ret = false;
        }
        else
        {
          for (int64_t i = 0; i < param_num; i++)
          {
            if (params[i] != r.params[i])
            {
              ret = false;
              break;
            }
          }
        }
        TBSYS_LOG(DEBUG, "CacheKey %s == %s ret: %d",
            common::to_cstring(*this), common::to_cstring(r), ret);
        return ret;
      }
      int64_t to_string(char * buf, const int64_t buf_len) const;
    };
    class ObSQLQueryCacheItemHolder
    {
      public:
        class Buffer
        {
          public:
            Buffer();
            ~Buffer();
            int                init    (int64_t capacity);
            int                write   (const char* bytes, int64_t size);
            void               reuse   ();
            inline const char* ptr     () const { return ptr_; }
            inline int64_t     length  () const { return length_; }
            inline int64_t     capacity() const { return capacity_; }
          private:
            int64_t capacity_;
            int64_t length_;
            char *  ptr_;
        };
      public:
        ObSQLQueryCacheItemHolder(int64_t max_param_num, int64_t buffer_capacity);
        ~ObSQLQueryCacheItemHolder();
        void         reuse        ();
        ObSQLQueryCacheKey &       get_key();
        const ObSQLQueryCacheKey & get_key() const;
        int          set          (int64_t sql_id, const common::ObIArray<common::ObObj> & array);
        const char * buffer_ptr   () { return buffer_.ptr(); }
        int64_t      buffer_length() { return buffer_.length(); }
        int          buffer_write (const char* bytes, int64_t size);
        bool         has_save_flag() { return save_flag_; }
        void         set_save_flag(bool flag) { save_flag_ = flag; }
      protected:
        ObSQLQueryCacheKey   key_;
        int64_t              max_param_num_;
        common::ObObj *      params_;
        Buffer               buffer_;
        bool                 save_flag_;
        bool                 initialized_;
    };
    struct ObSQLQueryCacheValue
    {
      int64_t      store_ts;
      int64_t      res_len;
      const char * res;
    };
    struct ObResWrapper
    {
      ObSQLQueryCacheValue value;
      common::CacheHandle handle;
    };
    class ObSQLQueryCache
    {
      public:
        static const int64_t DEFAULT_MAX_PARAM_NUM = 512;
        static const int64_t DEFAULT_MAX_RES_SIZE = 4L << 10;
      public:
        ObSQLQueryCache();
        ~ObSQLQueryCache();
        int init               (const mergeserver::ObMergeServerConfig * config);
        int get                (const ObSQLQueryCacheKey & key, ObResWrapper & res);
        int revert             (ObResWrapper & res);
        int remove             (const ObSQLQueryCacheKey & key, ObResWrapper & res);
        int prepare_ongoing_res(int64_t sql_id, const common::ObIArray<common::ObObj> & params,
                                ObResWrapper & res);
        int append_ongoing_res (easy_buf_t * res);
        int finish_ongoing_res ();
        int stop_ongoing_res   ();
      protected:
        class HolderInitializer
        {
          public:
            HolderInitializer  ();
            void init          (int64_t max_param_num, int64_t buffer_capacity);
            void operator ()   (void *ptr);
          protected:
            int64_t max_param_num_;
            int64_t buffer_capacity_;
        };
      protected:
        static const int64_t CacheItemSize = sizeof(ObSQLQueryCacheKey)
                + sizeof(ObSQLQueryCacheValue) + (1L << 20);
        static const int64_t QUERY_CACHE_BLOCK_SIZE = 2L << 20;
        typedef common::KeyValueCache<ObSQLQueryCacheKey, ObSQLQueryCacheValue,
                CacheItemSize, QUERY_CACHE_BLOCK_SIZE,
                common::KVStoreCacheComponent::SingleObjFreeList,
                common::hash::SpinReadWriteDefendMode> QueryCache;
        typedef common::ObTlStore<ObSQLQueryCacheItemHolder, HolderInitializer>
                CacheItemHolder;
        QueryCache        cache_;
        CacheItemHolder   cache_item_holder_;
        HolderInitializer holder_initializer_;
        const mergeserver::ObMergeServerConfig * config_;
        bool              initialized_;
    };
  } // namespace sql

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      using sql::ObSQLQueryCacheKey;
      using sql::ObSQLQueryCacheValue;

      struct ObSQLQueryCacheKeyDeepCopyTag {};
      template<>
      struct traits<ObSQLQueryCacheKey>
      {
        typedef ObSQLQueryCacheKeyDeepCopyTag Tag;
      };
      inline ObSQLQueryCacheKey * do_copy(const ObSQLQueryCacheKey  & key, char * buffer, ObSQLQueryCacheKeyDeepCopyTag)
      {
        ObSQLQueryCacheKey * ret = NULL;
        if (NULL != buffer)
        {
          ret = reinterpret_cast<ObSQLQueryCacheKey *>(buffer);
          ret->sql_id = key.sql_id;
          ret->param_num = key.param_num;
          ret->params = reinterpret_cast<ObObj *>(buffer + sizeof(key));
          int64_t params_len = sizeof(ObObj) * ret->param_num;
          char * str = buffer + sizeof(key) + params_len;
          int64_t str_pos = 0;
          for (int64_t i = 0; i < key.param_num; i++)
          {
            if (key.params[i].get_type() == common::ObVarcharType)
            {
              ObString key_str;
              key.params[i].get_varchar(key_str);
              memcpy(str + str_pos, key_str.ptr(), key_str.length());
              ObString new_key(key_str.length(), key_str.length(), str + str_pos);
              ret->params[i].set_varchar(new_key);
              str_pos += key_str.length();
            }
            else if (key.params[i].get_type() == common::ObDecimalType)
            {
              TBSYS_LOG(ERROR, "ObDecimalType does not support");
            }
            else
            {
              ret->params[i] = key.params[i];
            }
          }
        }
        return ret;
      }
      inline int32_t do_size(const ObSQLQueryCacheKey & key, ObSQLQueryCacheKeyDeepCopyTag)
      {
        int32_t ret = sizeof(key);
        for (int64_t i = 0; i < key.param_num; i++)
        {
          ret += static_cast<int32_t>(sizeof(ObObj));
          if (key.params[i].get_type() == common::ObVarcharType)
          {
            ret += key.params[i].get_val_len();
          }
          else if (key.params[i].get_type() == common::ObDecimalType)
          {
            TBSYS_LOG(ERROR, "ObDecimalType does not support");
          }
        }
        return ret;
      }
      inline void do_destroy(ObSQLQueryCacheKey * key, ObSQLQueryCacheKeyDeepCopyTag)
      {
        UNUSED(key);
      }

      struct ObSQLQueryCacheValueDeepCopyTag {};
      template<>
      struct traits<ObSQLQueryCacheValue>
      {
        typedef ObSQLQueryCacheValueDeepCopyTag Tag;
      };
      inline ObSQLQueryCacheValue * do_copy(const ObSQLQueryCacheValue & value, char * buffer, ObSQLQueryCacheValueDeepCopyTag)
      {
        ObSQLQueryCacheValue * ret = NULL;
        if (NULL != buffer)
        {
          ret = reinterpret_cast<ObSQLQueryCacheValue *>(buffer);
          ret->store_ts = value.store_ts;
          ret->res_len = value.res_len;
          ret->res = buffer + sizeof(value);
          memcpy(const_cast<char *>(ret->res), value.res, value.res_len);
        }
        return ret;
      }
      inline int32_t do_size(const ObSQLQueryCacheValue & value, ObSQLQueryCacheValueDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(value) + value.res_len);
      }
      inline void do_destroy(ObSQLQueryCacheValue * value, ObSQLQueryCacheValueDeepCopyTag)
      {
        UNUSED(value);
      }
    } // namespace KVStoreCacheComponent
  } // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_SQL_QUERY_CACHE
