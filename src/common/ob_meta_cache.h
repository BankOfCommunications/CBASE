/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_meta_cache.h is for what ...
 *
 * Version: ***: ob_meta_cache.h  Thu Aug 18 09:45:46 2011 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *     -some work detail if you want
 *
 */
#ifndef OCEANBASE_COMMON_OB_META_CACHE_H_
#define OCEANBASE_COMMON_OB_META_CACHE_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"
#include "common/thread_buffer.h"
#include "common/ob_get_param.h"
#include "common/ob_scanner.h"
#include "common/ob_kv_storecache.h"

namespace oceanbase
{
  namespace common
  {
    static const int32_t GET_PARAM_ID = 16;
    static const int32_t SCANNDER_ID  = 32;
    class IRpcStub
    {
    public:
      IRpcStub()
      {}
      virtual ~IRpcStub()
      {}

      virtual int rpc_get(ObGetParam &get_param, ObScanner &scanner, const int64_t timeouut) = 0;
    };

    class ObPermInfoKey
    {
    public:
      ObPermInfoKey();

      ObPermInfoKey(const uint64_t table_id, ObString& user_name);

      ObPermInfoKey(const ObPermInfoKey& other);

      ~ObPermInfoKey();
    public:
      int build_get_param(ObGetParam & get_param);

      int64_t hash() const;

      bool operator==(const ObPermInfoKey &other) const;

    public:
      inline uint64_t get_table_id() const
      {
        return table_id_;
      }

      inline ObString get_user_name() const
      {
        return ObString(static_cast<int32_t>(length_), static_cast<int32_t>(length_), user_name_);
      }

      inline char* get_buffer() const
      {
        return buffer_;
      }
      
      inline int64_t get_length() const
      {
        return length_;
      }

      int set_table_id(uint64_t table_id);

      int set_user_name(ObString name);
      int set_user_name(char *name);

      int set_length(int64_t lenth);
    private:
      uint64_t table_id_;
      char*    user_name_;
      int64_t  length_;
      char*    buffer_;
    };

    class ObUserInfoKey
    {
    public:
      ObUserInfoKey();

      ObUserInfoKey(ObString& user_name);

      ObUserInfoKey(const ObUserInfoKey& other);

      ~ObUserInfoKey();
    public:
      int build_get_param(ObGetParam &get_param);

      int64_t hash() const;

      bool operator==(const ObUserInfoKey &other) const;

    public:
      inline ObString get_user_name() const
      {
        return ObString(static_cast<int32_t>(length_), static_cast<int32_t>(length_), user_name_);
      }

      inline char*  get_buffer() const
      {
        return buffer_;
      }
      
      inline int64_t get_length() const
      {
        return length_;
      }

      int set_user_name(ObString name);
      int set_user_name(char* name);
      
      int set_length(int64_t length);
    private:
      char* user_name_;
      int64_t length_;
      char* buffer_;
    };

    class ObSKeyInfoKey
    {
    public:
      ObSKeyInfoKey();

      ObSKeyInfoKey(const int64_t version);

      ObSKeyInfoKey(const ObSKeyInfoKey& other);

      ~ObSKeyInfoKey();
    public:
      int build_get_param(ObGetParam &get_param);

      int64_t hash() const;

      bool operator==(const ObSKeyInfoKey & other) const;

    public:
      inline int64_t get_skey_version() const
      {
        return skey_version_;
      }

      inline char* get_buffer() const
      {
        return buffer_;
      }

      int set_skey_version(int64_t version);

    private:
      int64_t skey_version_;
      char* buffer_;
    };

    class ObPermInfoValue
    {
    public:
      ObPermInfoValue();

      ObPermInfoValue(const int64_t perm);

    public:
      int trans_from_scanner(const ObScanner &scanner);

      bool expired(const int64_t curr_time, const int64_t timeout) const;

    public:
      inline int64_t get_perm_mask() const
      {
        return perm_mask_;
      }

      inline int64_t get_time_stamp() const
      {
        return timestamp_;
      }

      int set_perm_mask(int64_t mask);

      int set_time_stamp(int64_t timestamp);

    private:
      int64_t perm_mask_;
      int64_t timestamp_;
    };

    class ObUserInfoValue
    {
    public:
      ObUserInfoValue();

      ObUserInfoValue(ObString& user);

    public:
      int trans_from_scanner(const ObScanner &scanner);

      bool expired(const int64_t curr_time, const int64_t timeout) const;

    public:
      inline ObString get_passwd() const
      {
        return ObString(static_cast<int32_t>(length_), static_cast<int32_t>(length_), passwd_);
      }

      inline int64_t get_length() const
      {
        return length_;
      }

      inline int64_t get_time_stamp() const
      {
        return timestamp_;
      }

      int set_passwd(ObString pass);
      int set_passwd(char* pass);

      int set_length(int64_t length);

      int set_time_stamp(int64_t timestamp);

    private:
      char* passwd_;
      int64_t length_;
      int64_t timestamp_;
    };

    class ObSKeyInfoValue
    {
    public:
      ObSKeyInfoValue();

    public:
      int trans_from_scanner(const ObScanner &scanner);

      bool expired(const int64_t curr_time, const int64_t timeout) const;

    public:
      inline ObString get_skey() const
      {
        return ObString(static_cast<int32_t>(length_), static_cast<int32_t>(length_), skey_);
      }

      inline int64_t get_length() const
      {
        return length_;
      }

      inline int64_t get_time_stamp() const
      {
        return timestamp_;
      }

      int set_skey(ObString key);
      int set_skey(char* key);

      int set_length(int64_t length);

      int set_time_stamp(int64_t timestamp);

    private:
      char* skey_;
      int64_t length_;
      int64_t timestamp_;
    };

    namespace KVStoreCacheComponent
    {
      struct ObPermInfoKeyDeepCopyTag
      {
      };
      
      struct ObUserInfoKeyDeepCopyTag
      {
      };

      struct ObUserInfoValueDeepCopyTag
      {
      };
      
      struct ObSKeyInfoValueDeepCopyTag
      {
      };
      template <>
      struct traits<common::ObPermInfoKey>
      {
        typedef ObPermInfoKeyDeepCopyTag Tag;
      };

      template <>
      struct traits<common::ObUserInfoKey>
      {
        typedef ObUserInfoKeyDeepCopyTag Tag;
      };

      template <>
      struct traits<common::ObUserInfoValue>
      {
        typedef ObUserInfoValueDeepCopyTag Tag;
      };

      template <>
      struct traits<common::ObSKeyInfoValue>
      {
        typedef ObSKeyInfoValueDeepCopyTag Tag;
      };

      inline common::ObPermInfoKey* do_copy(const common::ObPermInfoKey& other,
                                            char * buffer, ObPermInfoKeyDeepCopyTag)
      {
        common::ObPermInfoKey * ret =new(buffer)common::ObPermInfoKey();

        if (NULL != ret && NULL != other.get_user_name().ptr() && 0 < other.get_length() 
            && 0 < other.get_table_id() && OB_INVALID_ID != other.get_table_id())
        {
          ret->set_table_id(other.get_table_id());
          ret->set_user_name(buffer + sizeof(common::ObPermInfoKey));
          memcpy(ret->get_user_name().ptr() , other.get_user_name().ptr(), other.get_length());
          ret->set_length(other.get_length());
        }
        return ret;
      }

      inline int32_t do_size(const common::ObPermInfoKey& key,
                             ObPermInfoKeyDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(common::ObPermInfoKey) + key.get_length());
      }

      inline void do_destroy(common::ObPermInfoKey* data,
                             ObPermInfoKeyDeepCopyTag)
      {
        UNUSED(data);
      }

      inline common::ObUserInfoKey* do_copy(const common::ObUserInfoKey& other,
                                             char *buffer, ObUserInfoKeyDeepCopyTag)
      {
        common::ObUserInfoKey * ret = new(buffer)common::ObUserInfoKey();
        if (NULL != ret && NULL != other.get_user_name().ptr() && 0 < other.get_length())
        {
          ret->set_user_name(buffer + sizeof(ObUserInfoKey));
          memcpy(ret->get_user_name().ptr(), other.get_user_name().ptr(), other.get_length());
          ret->set_length(other.get_length());
        }
        return ret;
      }

      inline int32_t do_size(const common::ObUserInfoKey& key,
                             ObUserInfoKeyDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(ObUserInfoKey) + key.get_length());
      }

      inline void do_destroy(const common::ObUserInfoKey* data,
                             ObUserInfoKeyDeepCopyTag)
      {
        UNUSED(data);
      }

      inline common::ObUserInfoValue* do_copy(const common::ObUserInfoValue& other,
                                              char* buffer, ObUserInfoValueDeepCopyTag)
      {
        common::ObUserInfoValue* ret = new(buffer)common::ObUserInfoValue();
        if (NULL != ret && NULL != other.get_passwd().ptr() && 0 < other.get_length())
        {
          ret->set_passwd(buffer + sizeof(ObUserInfoValue));
          memcpy(ret->get_passwd().ptr(), other.get_passwd().ptr(), other.get_length());
          ret->set_length(other.get_length());
          ret->set_time_stamp(other.get_time_stamp());
        }
        return ret;
      }

      inline int32_t do_size(const common::ObUserInfoValue& key,
                             ObUserInfoValueDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(ObUserInfoValue) + key.get_length());
      }

      inline void do_destroy(const common::ObUserInfoValue* data,
                             ObUserInfoValueDeepCopyTag)
      {
        UNUSED(data);
      }

      inline common::ObSKeyInfoValue* do_copy(const common::ObSKeyInfoValue& other,
                             char* buffer, ObSKeyInfoValueDeepCopyTag)
      {
        common::ObSKeyInfoValue* ret = new(buffer)common::ObSKeyInfoValue();
        if (NULL != ret && NULL != other.get_skey().ptr() && 0 < other.get_length())
        {
          ret->set_skey(buffer + sizeof(ObSKeyInfoValue));
          memcpy(ret->get_skey().ptr(), other.get_skey().ptr(), other.get_length());
          ret->set_length(other.get_length());
          ret->set_time_stamp(other.get_time_stamp());
        }
        return ret;
      }

      inline int32_t do_size(const common::ObSKeyInfoValue& key,
                             ObSKeyInfoValueDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(ObSKeyInfoValue) + key.get_length());
      }

      inline void do_destroy(const common::ObSKeyInfoValue* data,
                             ObSKeyInfoValueDeepCopyTag)
      {
        UNUSED(data);
      }
    }



    template <typename Key, typename Value>
    class RefGuard
    {
      static const int64_t KVCACHE_BLOCK_SIZE = sizeof(Key) + 
        sizeof(Value) + sizeof(common::CacheHandle) + 4 * 1024L;
      static const int64_t KVCACHE_ITEM_SIZE = KVCACHE_BLOCK_SIZE;
      typedef common::CacheHandle Handle;
      typedef common::KeyValueCache<Key, Value,
        KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE,
        common::KVStoreCacheComponent::MultiObjFreeList> KVCache;
    public:
      RefGuard() : kv_cache_(NULL)
      {}
      ~RefGuard()
      {
        if (NULL != kv_cache_)
        {
          kv_cache_->revert(handle_);
        }
      }

      void set_kv_cache(KVCache* cache)
      {
        if (NULL != cache)
        {
          kv_cache_ = cache;
        }
      }

      Handle& get_handle()
      {
        return handle_;
      }

    private:
      DISALLOW_COPY_AND_ASSIGN(RefGuard);
      Handle handle_;
      KVCache *kv_cache_;
    };

    template <typename Key, typename Value>
    class ObMetaCache
    {
    public:
      static const int64_t KVCACHE_BLOCK_SIZE = sizeof(Key) + 
        sizeof(Value) + sizeof(common::CacheHandle) + 4 * 1024L;
      static const int64_t KVCACHE_ITEM_SIZE = KVCACHE_BLOCK_SIZE;

    ObMetaCache(): inited_(false),rpc_stub_(NULL)
      {
        cache_timeout_ = 10 * 1000L * 1000L;  //10s
      }
      ~ObMetaCache()
      {}
      typedef common::KeyValueCache<Key, Value,
        KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE,
        common::KVStoreCacheComponent::MultiObjFreeList> KVCache;

      typedef common::CacheHandle Handle;
    public:
      int init(const int64_t cache_timeout, IRpcStub *rpc_stub, const int64_t max_cache_num)
      {
        int ret = OB_SUCCESS;
        if (inited_)
        {
          TBSYS_LOG(INFO, "have inited");
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret && (0 > cache_timeout || NULL == rpc_stub))
        {
          TBSYS_LOG(ERROR, "illegal param cache_timeout=%ld, rpc_stub=%p", cache_timeout, rpc_stub);
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          ret = kv_cache_.init(max_cache_num * KVCACHE_BLOCK_SIZE);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "init kv cache fail");
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          rpc_stub_ = rpc_stub;
          cache_timeout_ = cache_timeout;
          inited_ = true;
          TBSYS_LOG(DEBUG, "init cache succ, max_cache_num=%ld, cache_timeout=%ld",
                    max_cache_num, cache_timeout);
        }
        return ret;
      }

      void destroy()
      {
        kv_cache_.destroy();
      }

    public:

      /**
       * Get value from cahce, if not exist or time expired
       *                       get new value by rpc_stub
       *
       * @param key       key to find
       * @param value     value return back
       * @param timeout   rpc time out
       * @param refGuard  handle manager
       *
       * @return int      OB_SUCCESS if get the value, else return OB_ERROR
       */
      int get(Key& key, Value& value, const int64_t timeout, RefGuard<Key,Value> &refGuard)
      {
        int ret       = OB_SUCCESS;
        bool need_rpc = false;

        ObGetParam* get_param = GET_TSI_MULT(ObGetParam, GET_PARAM_ID);
        ObScanner* scanner    = GET_TSI_MULT(ObScanner, SCANNDER_ID);
        if(!inited_)
        {
          TBSYS_LOG(WARN, "have not inited");
          ret = OB_NOT_INIT;
        }
        else
        {
          ret = kv_cache_.get(key, value, refGuard.get_handle(), false);
          if (OB_SUCCESS == ret)
          {
            if (value.expired(tbsys::CTimeUtil::getTime(),cache_timeout_))
            {
              kv_cache_.erase(key);
              need_rpc = true;
              ret = OB_ERROR;
            }
            else
            {
              //auto revert by refGuard
              refGuard.set_kv_cache(&kv_cache_);
            }
          }
          else
          {
            need_rpc = true;
          }

          if (need_rpc && OB_SUCCESS != ret)
          {
            get_param->reset();
            ret = key.build_get_param(*get_param);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "build get param failed");
            }
            else
            {
              scanner->reset();
              ret = rpc_get_value(*get_param, *scanner, timeout);
              if (OB_SUCCESS == ret)
              {
                ret = value.trans_from_scanner(*scanner);
                if (OB_SUCCESS == ret)
                {
                  kv_cache_.put(key, value, false);
                }
                else
                {
                  kv_cache_.erase(key);  
                }
              }
              //RPC failed remove fake node in kv cache
              else
              {
                kv_cache_.erase(key);
              }
            }
          }
        }
        return ret;
      }

      /**
       * adjust cache, earase expired cache items
       * 
       * @param cacah_timeout   if not set use default tiem out of cache
       *
       */
      void adjust(const int64_t cache_timeout = 0)
      {
        Handle handle;
        kv_cache_.reset_iter(handle);
        int ret = OB_SUCCESS;
        Key key;
        Value value;
        int64_t timeout = (cache_timeout == 0? cache_timeout_: cache_timeout);
        int64_t curr_time = tbsys::CTimeUtil::getTime();
        while(OB_SUCCESS == ret)
        {
          ret = kv_cache_.get_next(key, value, handle);
          if (OB_SUCCESS == ret && value.expired(curr_time, timeout))
          {
            TBSYS_LOG(DEBUG, "erase cache item");
            kv_cache_.erase(key);
          }
        }
        kv_cache_.revert(handle);
      }
      
      int clear()
      {
        return kv_cache_.clear();
      }

      /**
       * Set default cache time out
       * 
       * @param cacah_timeout
       * @return  return OB_SUCCESS if set cache time out success
       *          else return OB_ERROR
       */
      int set_cache_timeout(const int64_t cache_timeout)
      {
        int ret = OB_SUCCESS;
        if (0 > cache_timeout)
        {
          TBSYS_LOG(ERROR, "illegal param cache_timeout=%ld", cache_timeout);
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          cache_timeout_ = cache_timeout;
        }
        return ret;
      }

    private:
      int rpc_get_value(ObGetParam& get_param, ObScanner& scanner, const int64_t timeout)
      {
        int ret = OB_SUCCESS;
        ret = rpc_stub_->rpc_get(get_param, scanner, timeout);
        if  (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN,"rpc get failed");
        }
        return ret;
      }

    private:
      bool inited_;
      IRpcStub *rpc_stub_;
      int64_t cache_timeout_;     //us
      KVCache kv_cache_;
    };
  }
}
#endif
