#include "common/utility.h"
#include "ob_sstable_row_cache.h"
    
using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableRowCache::init(const int64_t max_mem_size)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        TBSYS_LOG(WARN, "is not init");
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != kv_cache_.init(max_mem_size))
      {
        TBSYS_LOG(WARN, "init kv cache fail");
        ret = OB_ERROR;
      }
      else
      {
        inited_ = true;
      }

      return ret;
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
        TBSYS_LOG(WARN, "invalid sstable row cache get_row param,key=%s", 
            to_cstring(key.row_key_));
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = kv_cache_.get(key, row_cache_val, handle);
        if (OB_SUCCESS == ret)
        {
          if (row_cache_val.size_ > 0)
          {
            ret = row_buf.ensure_space(row_cache_val.size_, 
                ObModIds::OB_SSTABLE_GET_SCAN);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "can't allocate enough space to store the row data, "
                              "row_buf=%p, row_size=%ld, ret=%d", 
                row_cache_val.buf_, row_cache_val.size_, ret);
            }
            else 
            {
              memcpy(row_buf.get_buffer(), row_cache_val.buf_, 
                  row_cache_val.size_);
              row_value.buf_ = row_buf.get_buffer();
              row_value.size_ = row_cache_val.size_;
            }
          }
          else if (0 == row_cache_val.size_)
          {
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
      else if (NULL == key.row_key_.get_obj_ptr())
      {
        TBSYS_LOG(WARN, "invalid sstable row cache key, key=%s", 
            to_cstring(key.row_key_));
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        kv_cache_.put(key, row_value, false);
      }

      return ret;
    }

  }//end namespace compactsstablev2
}//end namespace oceanbase
