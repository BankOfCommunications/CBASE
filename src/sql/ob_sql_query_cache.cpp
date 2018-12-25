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

#include "ob_sql_query_cache.h"

#include <common/ob_malloc.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

int64_t ObSQLQueryCacheKey::to_string(char * buf, const int64_t buf_len) const
{
  int64_t pos = snprintf(buf, buf_len, "SQL_ID: %ld", sql_id);
  for (int64_t i = 0; i < param_num && pos < buf_len; i++)
  {
    buf[pos++] = ' ';
    pos += params[i].to_string(buf + pos, buf_len - pos);
  }
  return pos;
}

ObSQLQueryCacheItemHolder::Buffer::Buffer()
  : capacity_(0), length_(0), ptr_(NULL)
{
}

ObSQLQueryCacheItemHolder::Buffer::~Buffer()
{
  if (NULL != ptr_)
  {
    ob_free(ptr_);
    ptr_ = NULL;
  }
}

int ObSQLQueryCacheItemHolder::Buffer::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  ptr_ = static_cast<char *>(ob_malloc(capacity, ObModIds::OB_SQL_QUERY_CACHE));
  if (NULL == ptr_)
  {
    TBSYS_LOG(ERROR, "ob_malloc failed");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    capacity_ = capacity;
    length_   = 0;
  }
  return ret;
}

int ObSQLQueryCacheItemHolder::Buffer::write(const char* bytes, int64_t size)
{
  int ret = OB_SUCCESS;
  if (length_ + size > capacity_)
  {
    TBSYS_LOG(WARN, "expect more space than capacity, capacity_: %ld length_: %ld",
        capacity_, length_);
    ret = OB_ERROR;
  }
  else
  {
    memcpy(ptr_ + length_, bytes, size);
    length_ += size;
  }
  return ret;
}

void ObSQLQueryCacheItemHolder::Buffer::reuse()
{
  length_ = 0;
}

ObSQLQueryCacheItemHolder::ObSQLQueryCacheItemHolder(int64_t max_param_num,
    int64_t buffer_capacity)
  : save_flag_(false), initialized_(false)
{
  int err = OB_SUCCESS;
  params_ = static_cast<ObObj *>(ob_malloc(sizeof(ObObj) * max_param_num,
        ObModIds::OB_SQL_QUERY_CACHE));
  if (NULL == params_)
  {
    TBSYS_LOG(ERROR, "ob_malloc failed");
  }
  else if (OB_SUCCESS != (err = buffer_.init(buffer_capacity)))
  {
    TBSYS_LOG(ERROR, "Buffer init failed, err: %d", err);
  }
  else
  {
    max_param_num_ = max_param_num;
    initialized_ = true;
  }
}

ObSQLQueryCacheItemHolder::~ObSQLQueryCacheItemHolder()
{
  if (NULL != params_)
  {
    ob_free(params_);
    params_ = NULL;
  }
}

void ObSQLQueryCacheItemHolder::reuse()
{
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCacheItemHolder has not been intialized");
  }
  else
  {
    key_.reuse();
    buffer_.reuse();
    save_flag_ = false;
  }
}

ObSQLQueryCacheKey & ObSQLQueryCacheItemHolder::get_key()
{
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCacheItemHolder has not been intialized");
  }
  return key_;
}

const ObSQLQueryCacheKey & ObSQLQueryCacheItemHolder::get_key() const
{
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCacheItemHolder has not been intialized");
  }
  return key_;
}

int ObSQLQueryCacheItemHolder::set(int64_t sql_id, const ObIArray<ObObj> & array)
{
  int ret = OB_SUCCESS;
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCacheItemHolder has not been intialized");
    ret = OB_NOT_INIT;
  }
  else if (array.count() > max_param_num_)
  {
    TBSYS_LOG(WARN, "do not support params larger than %ld", max_param_num_);
    ret = OB_ERROR;
  }
  else
  {
    key_.sql_id = sql_id;
    for (int64_t i = 0; i < array.count(); i++)
    {
      params_[i] = array.at(i);
    }
    key_.param_num = array.count();
    key_.params = params_;
  }
  return ret;
}

int ObSQLQueryCacheItemHolder::buffer_write(const char* bytes, int64_t size)
{
  int ret = buffer_.write(bytes, size);
  return ret;
}

ObSQLQueryCache::ObSQLQueryCache()
  : cache_item_holder_(holder_initializer_), initialized_(false)
{
}

ObSQLQueryCache::~ObSQLQueryCache()
{
}

int ObSQLQueryCache::init(const mergeserver::ObMergeServerConfig * config)
{
  int ret = OB_SUCCESS;
  if (initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache has been initialized");
    ret = OB_INIT_TWICE;
  }
  else if (NULL == config)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache arguments error");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    holder_initializer_.init(config->query_cache_max_param_num, config->query_cache_max_result);
    int64_t query_cache_size_percent = int64_t(config->query_cache_size) > 0L ?
        config->query_cache_size : 1L;
    if (OB_SUCCESS != (ret = cache_item_holder_.init()))
    {
      TBSYS_LOG(ERROR, "ObTlStore init error, ret: %d", ret);
    }
    else if (OB_SUCCESS != (ret = cache_.init(query_cache_size_percent
                * sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE) / 100)))
    {
      TBSYS_LOG(ERROR, "KeyValueCache init error, ret: %d", ret);
    }
    else
    {
      config_ = config;
      initialized_ = true;
    }
  }
  return ret;
}

int ObSQLQueryCache::get(const ObSQLQueryCacheKey & key, ObResWrapper & res)
{
  int ret = OB_SUCCESS;
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache has not been initialized");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = cache_.get(key, res.value, res.handle);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret)
    {
      TBSYS_LOG(ERROR, "cache_ get error, ret: %d", ret);
    }
    else if (OB_SUCCESS == ret && tbsys::CTimeUtil::getTime() - res.value.store_ts
        > config_->query_cache_expiration_time)
    {
      int err = remove(key, res);
      if (OB_SUCCESS != err && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "remove stale result cache error, err: %d", err);
      }
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObSQLQueryCache::revert(ObResWrapper & res)
{
  int ret = OB_SUCCESS;
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache has not been initialized");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = cache_.revert(res.handle);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "cache_ revert error, ret: %d", ret);
    }
  }
  return ret;
}

int ObSQLQueryCache::remove(const ObSQLQueryCacheKey & key, ObResWrapper & res)
{
  int ret = OB_SUCCESS;
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache has not been initialized");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = cache_.erase(key);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret)
    {
      TBSYS_LOG(ERROR, "cache_ erase error, ret: %d", ret);
    }
    else
    {
      ret = revert(res);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "revert error, ret: %d", ret);
      }
    }
  }
  return ret;
}

int ObSQLQueryCache::prepare_ongoing_res(int64_t sql_id,
    const ObIArray<ObObj> & params, ObResWrapper & res)
{
  int ret = OB_SUCCESS;
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache has not been initialized");
    ret = OB_NOT_INIT;
  }
  else
  {
    ObSQLQueryCacheItemHolder * holder = cache_item_holder_.get();
    if (NULL == holder)
    {
      TBSYS_LOG(ERROR, "ObTlStore get return NULL");
      ret = OB_ERROR;
    }
    else
    {
      holder->reuse();
      if(OB_SUCCESS != (ret = holder->set(sql_id, params)))
      {
        TBSYS_LOG(ERROR, "ObSQLQueryCacheItemHolder set error, ret: %d", ret);
      }
      else
      {
        int64_t repeat_times = 0;
        do
        {
          ret = cache_.get(holder->get_key(), res.value, res.handle, true);
          if (OB_ENTRY_NOT_EXIST == ret)
          {
            holder->set_save_flag(true);
            TBSYS_LOG(DEBUG, "fake node inserted");
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "KeyValueCache get error, ret: %d", ret);
          }
          else if (tbsys::CTimeUtil::getTime() - res.value.store_ts
              > config_->query_cache_expiration_time)
          {
            TBSYS_LOG(INFO, "out_dated");
            int err = remove(holder->get_key(), res);
            if (OB_SUCCESS != err && OB_ENTRY_NOT_EXIST != err)
            {
              TBSYS_LOG(ERROR, "remove stale result cache error, err: %d", err);
            }
            ret = OB_EAGAIN;
          }
          repeat_times++;
        }
        while (ret == OB_EAGAIN && repeat_times <= 3);
        if (repeat_times > 3 && OB_EAGAIN == ret)
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
    }
  }
  return ret;
}

int ObSQLQueryCache::append_ongoing_res(easy_buf_t * res)
{
  int ret = OB_SUCCESS;
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache has not been initialized");
    ret = OB_NOT_INIT;
  }
  else if (NULL == res)
  {
    TBSYS_LOG(ERROR, "res is NULL");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObSQLQueryCacheItemHolder * holder = cache_item_holder_.get();
    if (NULL == holder)
    {
      TBSYS_LOG(ERROR, "ObTlStore get return NULL");
      ret = OB_ERROR;
    }
    else if (holder->has_save_flag())
    {
      ret = holder->buffer_write(res->pos, res->last - res->pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "buffer_write error, ret: %d", ret);
        holder->set_save_flag(false);
      }
    }
  }
  return ret;
}

int ObSQLQueryCache::finish_ongoing_res()
{
  int ret = OB_SUCCESS;
  if (!initialized_)
  {
    TBSYS_LOG(ERROR, "ObSQLQueryCache has not been initialized");
    ret = OB_NOT_INIT;
  }
  else
  {
    ObSQLQueryCacheItemHolder * holder = cache_item_holder_.get();
    if (NULL == holder)
    {
      TBSYS_LOG(ERROR, "ObTlStore returns NULL");
      ret = OB_ERROR;
    }
    else if (holder->has_save_flag())
    {
      ObSQLQueryCacheValue value;
      value.store_ts = tbsys::CTimeUtil::getTime();
      value.res      = holder->buffer_ptr();
      value.res_len  = holder->buffer_length();
      ret = cache_.put(holder->get_key(), value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "KeyValueCache put error, ret: %d", ret);
      }
    }
  }
  return ret;
}

int ObSQLQueryCache::stop_ongoing_res()
{
  int ret = OB_SUCCESS;
  ObSQLQueryCacheItemHolder * holder = cache_item_holder_.get();
  if (NULL == holder)
  {
    TBSYS_LOG(ERROR, "ObTlStore get return NULL");
    ret = OB_ERROR;
  }
  else
  {
    holder->set_save_flag(false);
  }
  return ret;
}

ObSQLQueryCache::HolderInitializer::HolderInitializer()
  : max_param_num_(DEFAULT_MAX_PARAM_NUM),
    buffer_capacity_(DEFAULT_MAX_RES_SIZE)
{
}

void ObSQLQueryCache::HolderInitializer::init(int64_t max_param_num, int64_t buffer_capacity)
{
  max_param_num_ = max_param_num;
  buffer_capacity_ = buffer_capacity;
}

void ObSQLQueryCache::HolderInitializer::operator()(void *ptr)
{
  new (ptr) ObSQLQueryCacheItemHolder(max_param_num_, buffer_capacity_);
}
