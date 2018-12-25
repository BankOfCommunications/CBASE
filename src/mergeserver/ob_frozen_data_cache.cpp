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

#include "ob_frozen_data_cache.h"
#include "sql/ob_sql_read_param.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::sql;

ObFrozenDataKey::ObFrozenDataKey()
  : param_buf(NULL), len(0)
{
}

ObFrozenDataKey::~ObFrozenDataKey()
{
  param_buf = NULL;
  len = 0;
}

int64_t ObFrozenDataKey::to_string(char * buf, const int64_t buf_len) const
{
  int64_t ret = 0;
  if (NULL == param_buf)
  {
    ret = snprintf(buf, buf_len, "NULL");
  }
  else
  {
    ObSqlReadParam read_param;
    int64_t pos = 0;
    int err = read_param.deserialize(param_buf, len, pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObSqlReadParam deserialize error, err: %d", err);
      ret = snprintf(buf, buf_len, "to_string ERROR");
    }
    else
    {
      ret = read_param.to_string(buf, buf_len);
    }
  }
  return ret;
}

ObFrozenDataKeyBuf::ObFrozenDataKeyBuf()
  : buf(NULL), buf_len(0), pos(0)
{
}

ObFrozenDataKeyBuf::~ObFrozenDataKeyBuf()
{
  if (NULL != buf)
  {
    ob_tc_free(buf, ObModIds::OB_FROZEN_DATA_KEY_BUF);
    buf = NULL;
    buf_len = 0;
    pos = 0;
  }
}

int ObFrozenDataKeyBuf::alloc_buf(int64_t bl)
{
  int ret = OB_SUCCESS;
  if (bl > buf_len || NULL == buf)
  {
    if (NULL != buf)
    {
      ob_tc_free(buf, ObModIds::OB_FROZEN_DATA_KEY_BUF);
    }
    buf_len = bl;
    pos = 0;
    buf = reinterpret_cast<char *>(ob_tc_malloc(buf_len, ObModIds::OB_FROZEN_DATA_KEY_BUF));
    if (NULL == buf)
    {
      TBSYS_LOG(ERROR, "ob_tc_malloc failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  else
  {
    pos = 0;
  }
  return ret;
}

ObFrozenDataURLKey::ObFrozenDataURLKey()
  : sql(), subquery_id(0), frozen_version(0), param_num(0), params(NULL), need_free(false)
{
}

ObFrozenDataURLKey::~ObFrozenDataURLKey()
{
  reset();
}

void ObFrozenDataURLKey::reset()
{
  sql.reset();
  subquery_id = 0;
  frozen_version = 0;
  param_num = 0;
  if (NULL != params)
  {
    if (need_free)
    {
      ob_tc_free(params - 10, ObModIds::OB_MS_FROZEN_DATA_CACHE);
    }
    params = NULL;
  }
  need_free = false;
}

void ObFrozenDataURLKey::reuse()
{
  sql.reset();
  subquery_id = 0;
  frozen_version = 0;
  if (!need_free)
  {
    param_num = 0;
    params = NULL;
  }
}

void ObFrozenDataURLKey::set_sql(const ObString & s)
{
  sql = s;
}

void ObFrozenDataURLKey::set_subquery_id(uint64_t id)
{
  subquery_id = id;
}

void ObFrozenDataURLKey::set_frozen_version(uint64_t version)
{
  frozen_version = version;
}

int  ObFrozenDataURLKey::set_params(const common::ObIArray<common::ObObj*> &ps)
{
  int ret = OB_SUCCESS;
  if (param_num < ps.count() || NULL == params)
  {
    if (need_free)
    {
      ob_tc_free(params - 10, ObModIds::OB_MS_FROZEN_DATA_CACHE);
      params = NULL;
    }
    param_num = ps.count();
    int64_t ss = sizeof(ObObj) * param_num + 1024;
    params = reinterpret_cast<ObObj * >(ob_tc_malloc(ss,
          ObModIds::OB_MS_FROZEN_DATA_CACHE));
    if (NULL == params)
    {
      TBSYS_LOG(ERROR, "ob_tc_malloc failed");
      ret = OB_ERROR;
    }
    params += 10;
    need_free = true;
  }
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < param_num; i++)
    {
      params[i] = *ps.at(i);
    }
  }
  return ret;
}

int64_t ObFrozenDataURLKey::to_string(char * buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "sql(%.*s) subquery_id(%lu) "
      "frozen_version(%lu) param_num(%ld) params(%p):", sql.length(),
      sql.ptr(), subquery_id, frozen_version, param_num, params);
  for (int64_t i = 0; i < param_num; i++)
  {
    char p[64];
    int32_t pl = static_cast<int32_t>(params[i].to_string(p, 64));
    databuff_printf(buf, buf_len, pos, " (%.*s)", pl, p);
  }
  return pos;
}

ObCachedFrozenData::ObCachedFrozenData()
  : row_store_(NULL)
{
}

ObCachedFrozenData::~ObCachedFrozenData()
{
}

void ObCachedFrozenData::reset()
{
  row_store_ = NULL;
  cur_row_.reset(false, ObRow::DEFAULT_NULL);
}

void ObCachedFrozenData::reuse()
{
  reset();
}

bool ObCachedFrozenData::has_data()
{
  return row_store_ != NULL;
}

int ObCachedFrozenData::get_next_row(const common::ObRow * & row)
{
  int ret = OB_SUCCESS;
  if (!has_data())
  {
    TBSYS_LOG(ERROR, "row_store_ is NULL");
    ret = OB_ERROR;
  }
  else
  {
    ret = row_iter_.get_next_row(cur_row_);
    row = &cur_row_;
  }
  return ret;
}

void ObCachedFrozenData::set_row_desc(const ObRowDesc & row_desc)
{
  cur_row_.set_row_desc(row_desc);
}

ObFrozenDataCache::ObFrozenDataCache()
  : in_use_(false), get_counter_(0)
{
}

ObFrozenDataCache::~ObFrozenDataCache()
{
}

int ObFrozenDataCache::init(int64_t total_size)
{
  int ret = OB_SUCCESS;
  if (0 == total_size)
  {
    in_use_ = false;
    TBSYS_LOG(INFO, "ObFrozenDataCache switch off");
  }
  else
  {
    ret = cache_.init(total_size);
    if (OB_SUCCESS != ret)
    {
      if (total_size <= CacheItemSize)
      {
        TBSYS_LOG(ERROR, "total_size(%ld) is smaller than on cache item size(%ld)",
            total_size, CacheItemSize);
      }
      else
      {
        TBSYS_LOG(ERROR, "KeyValueCache init error, ret: %d", ret);
      }
      in_use_ = false;
    }
    else
    {
      in_use_ = true;
      TBSYS_LOG(INFO, "ObFrozenDataCache init OK");
    }
  }
  return ret;
}

int ObFrozenDataCache::get(const ObFrozenDataKey & key, ObCachedFrozenData & value)
{
  int ret = OB_SUCCESS;
  if (!in_use_)
  {
    TBSYS_LOG(ERROR, "ObFrozenDataCache is not initialized to use");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = cache_.get(key, value.row_store_, value.handle_);
    if (OB_ENTRY_NOT_EXIST == ret)
    {
      ret = OB_SUCCESS;
      value.row_store_ = NULL;
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "KeyValueCache get error, ret: %d", ret);
    }
    else
    {
      value.row_iter_ = value.row_store_->begin();
    }
    if (__sync_fetch_and_add(&get_counter_, 1) % ITEM_COUNT_PRINT_FREQUENCY == 0)
    {
      TBSYS_LOG(INFO, "frozen data cache cached item number: %ld", cache_.count());
    }
  }
  return ret;
}

int ObFrozenDataCache::revert(ObCachedFrozenData & value)
{
  int ret = OB_SUCCESS;
  if (!in_use_)
  {
    TBSYS_LOG(ERROR, "ObFrozenDataCache is not initialized to use");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = cache_.revert(value.handle_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "KeyValueCache revert error, ret: %d", ret);
    }
    else
    {
      value.reuse();
    }
  }
  return ret;
}

int ObFrozenDataCache::put(const ObFrozenDataKey & key, const ObFrozenData & data)
{
  int ret = OB_SUCCESS;
  if (!in_use_)
  {
    TBSYS_LOG(ERROR, "ObFrozenDataCache is not initialized to use");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = cache_.put(key, data);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "KeyValueCache put error, ret: %d", ret);
    }
  }
  return ret;
}

bool ObFrozenDataCache::get_in_use() const
{
  return in_use_;
}

int ObFrozenDataCache::enlarge_total_size(int64_t total_size)
{
  return cache_.enlarge_total_size(total_size);
}

