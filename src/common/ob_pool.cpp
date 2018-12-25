/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_pool.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_pool.h"
#include "ob_atomic.h"
using namespace oceanbase::common;

ObPool::ObPool(int64_t obj_size, int64_t block_size /*= 64*1024*/)
  :obj_size_(obj_size), block_size_(block_size),
   in_use_count_(0), free_count_(0), total_count_(0),
   freelist_(NULL)
{
  OB_ASSERT(obj_size_ >= static_cast<int64_t>(sizeof(FreeNode)));
  if (block_size_ < obj_size_)
  {
    TBSYS_LOG(WARN, "obj size larger than block size, obj_size=%ld block_size=%ld", obj_size_, block_size_);
    block_size_ = obj_size_;
  }
}

ObPool::~ObPool()
{
  //TBSYS_LOG(DEBUG, "pool free blocks, count=%ld", blocks_.count());
  for (int i = 0; i < blocks_.count(); ++i)
  {
    ob_free(blocks_.at(i), ObModIds::OB_POOL);
  }
  blocks_.clear();
}

void ObPool::alloc_new_block()
{
  void* block = ob_malloc(block_size_, ObModIds::OB_POOL);
  if (NULL == block)
  {
    TBSYS_LOG(ERROR, "no memory");
  }
  else
  {
    if (OB_SUCCESS != blocks_.push_back(block))
    {
      TBSYS_LOG(ERROR, "failed to push back block");
    }
    const int64_t obj_count = block_size_ / obj_size_;
    if (0 >= obj_count)
    {
      TBSYS_LOG(ERROR, "invalid block size=%ld", block_size_);
    }
    else
    {
      for (int i = 0; i < obj_count; ++i)
      {
        atomic_inc(&total_count_);
        freelist_push(static_cast<char*>(block) + obj_size_ * i);
      }
    }
  }
}

void* ObPool::alloc()
{
  void* ret = NULL;
  if (NULL == (ret = freelist_pop()))
  {
    alloc_new_block();
    ret = freelist_pop();
  }
  return ret;
}

void ObPool::free(void *obj)
{
  if (NULL != obj)
  {
    atomic_dec(&in_use_count_);
  }
  freelist_push(obj);
}

void* ObPool::freelist_pop()
{
  void *ret = NULL;
  if (NULL != freelist_)
  {
    ret = freelist_;
    freelist_ = freelist_->next_;
    atomic_dec(&free_count_);
    atomic_inc(&in_use_count_);
  }
  return ret;
}

void ObPool::freelist_push(void *obj)
{
  if (NULL != obj)
  {
    FreeNode* node = static_cast<FreeNode*>(obj);
    node->next_ = freelist_;
    freelist_ = node;
    atomic_inc(&free_count_);
  }
}

////////////////////////////////////////////////////////////////
ObLockedPool::ObLockedPool(int64_t obj_size, int64_t block_size /*= 64*1024*/)
  :ObPool(obj_size, block_size)
{
}

ObLockedPool::~ObLockedPool()
{
}

void* ObLockedPool::alloc()
{
  ObSpinLockGuard guard(lock_);
  return ObPool::alloc();
}

void ObLockedPool::free(void *obj)
{
  ObSpinLockGuard guard(lock_);
  return ObPool::free(obj);
}
