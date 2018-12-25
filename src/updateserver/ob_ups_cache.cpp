///===================================================================
 //
 // ob_ups_cache.h / updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-02-24 by Rongxuan (rongxuan.lc@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
 ////====================================================================

#include "common/ob_define.h"
#include "ob_ups_cache.h"
#include "common/ob_action_flag.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;

    ObUpsCache::ObUpsCache():inited_(false)
    {
    }

    ObUpsCache::~ObUpsCache()
    {
    }

    int ObUpsCache::init(const int64_t cache_size)
    {
      int ret = OB_SUCCESS;
      if(inited_)
      {
        TBSYS_LOG(WARN,"have inited");
        ret = OB_INIT_TWICE;
      }
      else
      {
        set_cache_size(cache_size);
        int64_t cache_size = get_cache_size();
        if(OB_SUCCESS != kv_cache_.init(cache_size))
        {
          TBSYS_LOG(WARN,"init kv_cache_ failed");
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO,"cache_size=%ldB",cache_size);
          inited_ = true;
        }
      }
      return ret;
    }

    int ObUpsCache::destroy()
    {
      int ret = OB_SUCCESS;
      if(inited_)
      {
        if(OB_SUCCESS != kv_cache_.destroy() )
        {
          TBSYS_LOG(ERROR,"destroy cache fail");
          ret = OB_ERROR;
        }
        else
        {
          inited_ = false;
        }
      }
      else
      {
        TBSYS_LOG(WARN,"have not inited, no need to destroy");
        ret = OB_ERROR;
      }
      return ret;
    }

    int ObUpsCache::clear()
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        TBSYS_LOG(WARN,"have not inited");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != kv_cache_.clear())
      {
        TBSYS_LOG(WARN,"clear cache fail");
        ret = OB_ERROR;
      }
      else
      {
        //do nothing;
      }
      return ret;
    }

    int ObUpsCache::get(const uint64_t table_id,
        const ObRowkey& row_key,ObBufferHandle &buffer_handle,
        const uint64_t column_id, ObUpsCacheValue& value)
    {

      int ret = OB_SUCCESS;
      if(!inited_)
      {
        TBSYS_LOG(WARN,"have not inited");
        ret = OB_ERROR;
      }
      else
      {
        if(OB_INVALID_ID == table_id || OB_INVALID_ID == column_id
            || NULL == row_key.ptr() || row_key.length() <= 0)
        {
          TBSYS_LOG(WARN,"invalid param table_id=%lu,column_id=%lu,row_key_ptr=%p,row_key_length=%ld",
              table_id,column_id,row_key.ptr(),row_key.length());
          ret = OB_ERROR;
        }
        else
        {
          ObUpsCacheKey date_key;
          date_key.table_id = static_cast<uint16_t>(table_id);
          date_key.row_key = row_key;
          date_key.column_id = static_cast<uint16_t>(column_id);
          //如果是查询行是否存在于缓存中
          if(0 == column_id)
          {
            TBSYS_LOG(WARN,"use func is_row_exist to check if the row exist");
            ret = OB_ERROR;
          }
          else  //在缓存中查询列的值
          {
            ret = kv_cache_.get(date_key,value,buffer_handle.handle_);
            TBSYS_LOG(DEBUG, "get from kv cache, table_id=%lu column_id=%lu, row_key=%s ret=%d version=%ld value=[%s]",
                table_id, column_id, to_cstring(row_key), ret, value.version, print_obj(value.value));
            if (OB_ENTRY_NOT_EXIST != ret && OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN,"get kv_cache_ failed,ret=%d",ret);
            }
            else if(OB_SUCCESS == ret)
            {
              buffer_handle.ups_cache_ = this;
              if(common::ObVarcharType == value.value.get_type())
              {
                ObString str_value;
                value.value.get_varchar(str_value);
                buffer_handle.buffer_ = const_cast<char*>( str_value.ptr());
              }
              //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
              else if(common::ObDecimalType == value.value.get_type())
              {
                ObString str_value;
                value.value.get_decimal(str_value);
                buffer_handle.buffer_ = const_cast<char*>( str_value.ptr());
              }
              //add:e
              else
              {
                buffer_handle.buffer_ = NULL;
              }
              ret = OB_SUCCESS;
            }
          }
        }
      }
      return ret;
    }


    // 加入缓存项
    int ObUpsCache::put(const uint64_t table_id,
        const ObRowkey& row_key,
        const uint64_t column_id,
        const ObUpsCacheValue& value)
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        TBSYS_LOG(WARN,"have not inited");
        ret = OB_ERROR;
      }
      else
      {
        if(OB_INVALID_ID == table_id || OB_INVALID_ID == column_id
            || NULL == row_key.ptr() || row_key.length() <= 0)
        {
          TBSYS_LOG(ERROR,"invalid param table_id=%lu,column_id=%lu,row_key_ptr=%p,row_key_length=%ld",table_id,column_id,row_key.ptr(),row_key.length());
          ret = OB_ERROR;
        }
      }

      if(OB_SUCCESS == ret)
      {
        //参数验证正确以后，将参数形成key和value 插入到缓存中
        ObUpsCacheKey cache_key;
        cache_key.table_id = static_cast<uint16_t>(table_id);
        cache_key.column_id = static_cast<uint16_t>(column_id);
        cache_key.row_key = row_key;
        ret = kv_cache_.put(cache_key,value);
        TBSYS_LOG(DEBUG, "put to kv cache, table_id=%lu column_id=%lu row_key=%s ret=%d",
            table_id, column_id, to_cstring(row_key), ret);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to add cache pair to kv cache, ret=%d", ret);
        }
      }

      return ret;
    }

    // 查询行是否存在
    // 如果缓存项存在，返回OB_SUCCESS; 如果缓存项不存在，返回OB_NOT_EXIST；否则，返回OB_ERROR；
    int ObUpsCache::is_row_exist(const uint64_t table_id,
        const ObRowkey& row_key,
        bool& is_exist,
        ObBufferHandle& buffer_handle)
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        TBSYS_LOG(WARN,"have not inited");
        ret = OB_ERROR;
      }
      else
      {
        if(OB_INVALID_ID == table_id || NULL == row_key.ptr() || row_key.length() <= 0)
        {
          TBSYS_LOG(WARN,"invalid param table_id=%lu,row_ke=%s",table_id, to_cstring(row_key));
          ret = OB_ERROR;
        }
        else
        {
          ObUpsCacheKey cache_key;
          ObUpsCacheValue cache_value;
          cache_key.table_id = static_cast<uint16_t>(table_id);
          cache_key.column_id = 0;
          cache_key.row_key = row_key;
          if(OB_SUCCESS == kv_cache_.get(cache_key, cache_value, buffer_handle.handle_) )
          {
            buffer_handle.ups_cache_ = this;
            buffer_handle.buffer_ = NULL;
            //判断cache_value的值
            int64_t exist_value = 0;
            if(OB_SUCCESS == cache_value.value.get_ext(exist_value) )
            {
              ret = OB_SUCCESS;
              if(ObActionFlag::OP_ROW_EXIST == exist_value)
              {
                is_exist = true;
              }
              else if(ObActionFlag::OP_ROW_DOES_NOT_EXIST == exist_value)
              {
                is_exist = false;
              }
              else
              {
                ret = OB_ERROR;
              }
            }
            else
            {
              ret = OB_ERROR;
            }
          }
          else
          {
            ret = OB_UPS_NOT_EXIST;
          }
        }
      }

      return ret;
    }

    // 设置行是否存在标志，行不存在也需要记录到缓存中
    int ObUpsCache::set_row_exist(const uint64_t table_id, const ObRowkey& row_key, const bool is_exist)
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        TBSYS_LOG(WARN,"have not inited");
        ret = OB_ERROR;
      }
      else
      {
        if(OB_INVALID_ID == table_id
            || NULL == row_key.ptr()
            || row_key.length() <=0)
        {
          TBSYS_LOG(WARN,"invalid param,table_id=%lu,row_key_ptr=%p,row_key_length=%ld",
              table_id, row_key.ptr(), row_key.length());
          ret = OB_ERROR;
        }
        else
        {
          ObUpsCacheKey cache_key;
          ObUpsCacheValue cache_value;
          cache_key.table_id = static_cast<uint16_t>(table_id);
          cache_key.row_key = row_key;
          cache_key.column_id = 0;

          if(is_exist)
          {
            cache_value.value.set_ext(ObActionFlag::OP_ROW_EXIST);
          }
          else
          {
            cache_value.value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
          }
          ret = kv_cache_.put(cache_key,cache_value, true);
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN,"set_row_exist failed,ret=%d",ret);
          }
        }
      }
      return ret;
    }

    // 设置缓存大小，单位为B
    void ObUpsCache::set_cache_size(const int64_t cache_size)
    {
      if(cache_size  < MIN_KVCACHE_SIZE)
      {
        TBSYS_LOG(INFO,"set to min cache size, min_cache_size=%ldB",MIN_KVCACHE_SIZE);
        cache_size_ = MIN_KVCACHE_SIZE;
      }
      else
      {
        cache_size_ = cache_size ;
      }
    }
    // 获取缓存占用的内存，单位为B
    int64_t ObUpsCache::get_cache_size(void)
    {
      return cache_size_;
    }

  }//end of updateserver
} //end of  oceanbase
