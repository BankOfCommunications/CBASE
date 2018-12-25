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
#include <tblog.h>
#include "ob_sstable_row_cache.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace oceanbase::common;

    ObSSTableRowCache::ObSSTableRowCache() 
    : inited_(false)
    {

    }

    ObSSTableRowCache::~ObSSTableRowCache()
    {
      destroy();
    }

    int ObSSTableRowCache::init(const int64_t max_mem_size)
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
        TBSYS_LOG_US(DEBUG, "init sstble row cache succ, cache_mem_size=%ld,",
                     max_mem_size);
      }

      return ret;
    }

    int ObSSTableRowCache::enlarg_cache_size(const int64_t cache_mem_size)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(INFO, "not inited");
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = kv_cache_.enlarge_total_size(cache_mem_size)))
      {
        TBSYS_LOG(WARN, "enlarge total sstable row cache size of kv cache fail");
      }
      else
      {
        TBSYS_LOG(INFO, "success enlarge sstable row cache size to %ld", cache_mem_size);
      }

      return ret;
    }

    int ObSSTableRowCache::clear()
    {
      return kv_cache_.clear();
    }

    int ObSSTableRowCache::destroy()
    {
      inited_ = false;
      return kv_cache_.destroy();
    }

    int ObSSTableRowCache::get_row(const ObSSTableRowCacheKey& key, 
      ObSSTableRowCacheValue& row_value, ObMemBuf& row_buf)
    {
      int ret = OB_SUCCESS;
      ObSSTableRowCacheValue row_cache_val;
      Handle handle;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_NOT_INIT;
      }
      else if (NULL == key.row_key_.get_obj_ptr() || key.row_key_size_ <= 0)
      {
        TBSYS_LOG(WARN, "invalid sstable row cache get_row param,key=%s", to_cstring(key.row_key_));
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = kv_cache_.get(key, row_cache_val, handle);
        if (OB_SUCCESS == ret)
        {
          if (row_cache_val.size_ > 0)
          {
            ret = row_buf.ensure_space(row_cache_val.size_, ObModIds::OB_SSTABLE_GET_SCAN);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "can't allocate enough space to store the row data, "
                              "row_buf=%p, row_size=%ld, ret=%d", 
                row_cache_val.buf_, row_cache_val.size_, ret);
            }
            else 
            {
              memcpy(row_buf.get_buffer(), row_cache_val.buf_, row_cache_val.size_);
              row_value.buf_ = row_buf.get_buffer();
              row_value.size_ = row_cache_val.size_;
            }
          }
          else if (0 == row_cache_val.size_)
          {
            //if row isn't existent, we store the row cache value with size 0
            row_value.buf_ = row_cache_val.buf_;
            row_value.size_ = row_cache_val.size_;
          }
          kv_cache_.revert(handle);
        }
      }

      return ret;
    }

    int ObSSTableRowCache::put_row(const ObSSTableRowCacheKey& key, 
      const ObSSTableRowCacheValue& row_value)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_NOT_INIT;
      }
      else if (NULL == key.row_key_.get_obj_ptr() || key.row_key_size_ <= 0)
      {
        TBSYS_LOG(WARN, "invalid sstable row cache key, key=%s", to_cstring(key.row_key_));
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        /**
         * put the ObSSTableRowCacheValue instance into sstable row 
         * cache, maybe the row cells are existent in sstable row cache,
         * so ignore the return value. 
         */
        kv_cache_.put(key, row_value, false);
      }

      return ret;
    }

  } //end namespace sstable
} //end namespace oceanbase
