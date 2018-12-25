/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_cache.cpp is for what ...
 *
 * Version: $id: ob_cache.cpp,v 0.1 8/19/2010 3:20p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_cache.h"
#include <sys/time.h>
#include "ob_malloc.h"
#include "ob_define.h"
#include "ob_string.h"
#include "ob_link.h"
#include "tbsys.h"
#include "tblog.h"
#include "./hash/ob_hashutils.h"


using namespace oceanbase::common;
using oceanbase::common::ObString;
using oceanbase::common::ObVarCache;

oceanbase::common::ObCacheBase::ObCacheBase(int64_t mod_id)
{
  cache_mod_id_ = mod_id;
}

oceanbase::common::ObCacheBase::~ObCacheBase()
{
}

oceanbase::common::ObCachePair::~ObCachePair()
{
  revert();
}

oceanbase::common::ObCachePair::ObCachePair()
{
  init();
}


void oceanbase::common::ObCachePair::init(ObCacheBase &cache, char *key, 
                                          const int32_t key_size, 
                                          char *value, const int32_t value_size,
                                          void *cache_item_handle)
{
  revert();
  key_.assign(key,key_size);
  value_.assign(value,value_size);
  cache_ = & cache;
  cache_item_handle_ = cache_item_handle;
}

void oceanbase::common::ObCachePair::init()
{
  key_.assign(NULL,0);
  value_.assign(NULL,0);
  cache_ = NULL;
  cache_item_handle_ = NULL;
}

void oceanbase::common::ObCachePair::revert()
{
  if (cache_ != NULL)
  {
    cache_->revert(*this);
  }
  init();
}

oceanbase::common::ObString& oceanbase::common::ObCachePair::get_key()
{
  return key_;
}

const oceanbase::common::ObString& oceanbase::common::ObCachePair::get_key() const
{
  return key_;
}

oceanbase::common::ObString& oceanbase::common::ObCachePair::get_value()
{
  return value_;
}

const oceanbase::common::ObString& oceanbase::common::ObCachePair::get_value() const
{
  return value_;
}


/// void oceanbase::common::ObVarCache::inc_ref(void * cache_item_handle)
/// {
///   int err = 0;
///   CacheItemHead *item = reinterpret_cast<CacheItemHead*>(cache_item_handle);
///   if (item->magic_ != CACHE_ITEM_MAGIC)
///   {
///     TBSYS_LOG(ERROR, "cache memory overflow [item_ptr:%p]", item);
///     err = oceanbase::common::OB_MEM_OVERFLOW;
///   }
///   tbsys::CThreadGuard guard(&mutex_);
///   if (0 == err && !inited_)
///   {
///     TBSYS_LOG(ERROR, "cache not initialized yet");
///     err = oceanbase::common::OB_NOT_INIT;
///   }
///   if (0 == err)
///   {
///     item->ref_num_ ++;
///     total_ref_num_ ++;
///   }
/// }



