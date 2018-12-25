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
#include <tblog.h>
#include "ob_query_cache.h"

namespace oceanbase
{
  namespace mergeserver
  {
    using namespace oceanbase::common;

    ObQueryCache::ObQueryCache() 
    : inited_(false)
    {

    }

    ObQueryCache::~ObQueryCache()
    {
      destroy();
    }

    int ObQueryCache::init(const int64_t max_mem_size)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        //do nothing
      }
      else if (OB_SUCCESS != kv_cache_.init(max_mem_size))
      {
        TBSYS_LOG(WARN, "init kv cache fail");
        ret = OB_ERROR;
      }
      else
      {
        inited_ = true;
        TBSYS_LOG_US(DEBUG, "init query cache succ, cache_mem_size=%ld,",
                     max_mem_size);
      }

      return ret;
    }


    int ObQueryCache::clear()
    {
      return kv_cache_.clear();
    }

    int ObQueryCache::destroy()
    {
      inited_ = false;
      return kv_cache_.destroy();
    }

    int ObQueryCache::get(const ObString& key, ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      ObQueryCacheValue cache_val;
      Handle handle;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_NOT_INIT;
      }
      else if (NULL == key.ptr() || key.length() <= 0)
      {
        TBSYS_LOG(WARN, "invalid query cache get param, key_ptr=%p, key_len=%d", 
                  key.ptr(), key.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = kv_cache_.get(key, cache_val, handle);
        if (OB_SUCCESS == ret)
        {
          //expired, delete this item from kvcache
          if (cache_val.expire_time_ < tbsys::CTimeUtil::getTime())
          {
            if (OB_SUCCESS != kv_cache_.erase(key))
            {
              TBSYS_LOG(WARN, "failed to delete item from query cache");
            }
            ret = OB_ENTRY_NOT_EXIST;
          }
          else if (cache_val.size_ > 0)
          {
            if (out_buffer.get_capacity() - out_buffer.get_position() < cache_val.size_)
            {
              TBSYS_LOG(WARN, "out_buffer hasn't enough space to store the result data, "
                              "data_buf_size=%ld, result_size=%ld", 
                out_buffer.get_capacity() - out_buffer.get_position(), cache_val.size_);
            }
            else 
            {
              memcpy(out_buffer.get_data(), cache_val.buf_, cache_val.size_);
              out_buffer.get_position() += cache_val.size_;
            }
          }
          else
          {
            // cache_val.size_ == 0, do nothing
          }
          kv_cache_.revert(handle);
        }
      }

      return ret;
    }

    int ObQueryCache::put(const ObString& key, const ObQueryCacheValue& value)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_NOT_INIT;
      }
      else if (NULL == key.ptr() || key.length() <= 0)
      {
        TBSYS_LOG(WARN, "invalid query cache put param, key_ptr=%p, key_len=%d", 
                  key.ptr(), key.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        /**
         * put the ObQueryCacheValue instance into query cache, 
         * maybe the result are existent in query cache, so 
         * ignore the return value. 
         */
        kv_cache_.put(key, value, false);
      }

      return ret;
    }

  } //end namespace mergeserver
} //end namespace oceanbase
