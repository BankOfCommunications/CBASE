#include "ob_insert_cache.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

InsertCacheWrapValue::InsertCacheWrapValue()
  :bf_(NULL)
{
}
InsertCacheWrapValue::~InsertCacheWrapValue()
{
}
ObInsertCache::ObInsertCache()
  : in_use_(false)
{
}
ObInsertCache::~ObInsertCache()
{
}
int ObInsertCache::init(int64_t total_size)
{
  int ret = OB_SUCCESS;
  if (0 == total_size)
  {
    in_use_ = false;
    TBSYS_LOG(INFO, "Bloom Filter Cache switch off");
  }
  else
  {
    ret = bf_cache_.init(total_size);
    if (ret != OB_SUCCESS)
    {
      if (total_size <= CacheItemSize)
      {
        TBSYS_LOG(ERROR, "total_size(%ld) is smaller than on cache item size(%ld)",
            total_size, CacheItemSize);
      }
      else
      {
        TBSYS_LOG(ERROR, "init bloom filter cache failed, ret=%d", ret);
      }
      in_use_ = false;
    }
    else
    {
      in_use_ = true;
    }
  }
  return ret;
}
int ObInsertCache::put(const InsertCacheKey & key, const InsertCacheValue & value)
{
  int ret = OB_SUCCESS;
  if (!in_use_)
  {
    TBSYS_LOG(ERROR, "ObInsertCache is not initialized to use");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = bf_cache_.put(key, value);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "put item to bloom filter cache failed, ret=%d", ret);
    }
  }
  return ret;
}
int ObInsertCache::revert(InsertCacheWrapValue & value)
{
  int ret = OB_SUCCESS;
  if (!in_use_)
  {
    TBSYS_LOG(ERROR, "ObInsertCache is not initialized to use");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = bf_cache_.revert(value.handle_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "revert item to bloom filter cache failed, ret=%d", ret);
    }
  }
  return ret;
}
int ObInsertCache::get(const InsertCacheKey & key, InsertCacheWrapValue & value)
{
  int ret = OB_SUCCESS;
  if (!in_use_)
  {
    TBSYS_LOG(ERROR, "ObInsertCache is not initialized to use");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = bf_cache_.get(key, value.bf_, value.handle_);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret)
    {
      TBSYS_LOG(ERROR, "get item from bloom filter cache failed, ret=%d", ret);
    }
    if (__sync_fetch_and_add(&get_counter_, 1) % ITEM_COUNT_PRINT_FREQUENCY == 0)
    {
      TBSYS_LOG(INFO, "bloom filter cache cached item number: %ld", bf_cache_.count());
    }
  }
  return ret;
}

bool ObInsertCache::get_in_use() const
{
  return in_use_;
}

int ObInsertCache::enlarge_total_size(int64_t total_size)
{
  return bf_cache_.enlarge_total_size(total_size);
}
