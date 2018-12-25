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
#include "ob_atomic.h"
namespace oceanbase
{
  namespace common
  {

template <typename BlockAllocatorT, typename LockT>
ObPool<BlockAllocatorT, LockT>::ObPool(int64_t obj_size, int64_t block_size, const BlockAllocatorT &alloc)
  :obj_size_(obj_size), block_size_(block_size),
   in_use_count_(0), free_count_(0), total_count_(0),
   freelist_(NULL), blocklist_(NULL),
   block_allocator_(alloc)
{
  OB_ASSERT(obj_size_ >= static_cast<int64_t>(sizeof(FreeNode)));
  if (block_size_ < (obj_size_ + static_cast<int64_t>(sizeof(BlockHeader))))
  {
    TBSYS_LOG(WARN, "obj size larger than block size, obj_size=%ld block_size=%ld", obj_size_, block_size_);
    block_size_ = obj_size_ + sizeof(BlockHeader);
  }
}

template <typename BlockAllocatorT, typename LockT>
ObPool<BlockAllocatorT, LockT>::~ObPool()
{
  BlockHeader* curr = blocklist_;
  BlockHeader* next = NULL;
  while (NULL != curr)
  {
    next = curr->next_;
    block_allocator_.free(curr);
    curr = next;
  }
  blocklist_ = NULL;
}

template <typename BlockAllocatorT, typename LockT>
void ObPool<BlockAllocatorT, LockT>::alloc_new_block()
{
  BlockHeader* new_block = static_cast<BlockHeader*>(block_allocator_.alloc(block_size_));
  if (NULL == new_block)
  {
    TBSYS_LOG(ERROR, "no memory");
  }
  else
  {
    new_block->next_ = blocklist_;
    blocklist_ = new_block;

    const int64_t obj_count = (block_size_ - sizeof(BlockHeader)) / obj_size_;
    if (0 >= obj_count)
    {
      TBSYS_LOG(ERROR, "invalid block size=%ld", block_size_);
    }
    else
    {
      for (int i = 0; i < obj_count; ++i)
      {
        atomic_inc(&total_count_);
        freelist_push(reinterpret_cast<char*>(new_block) + sizeof(BlockHeader) + obj_size_ * i);
      }
    }
  }
}

template <typename BlockAllocatorT, typename LockT>
void* ObPool<BlockAllocatorT, LockT>::alloc()
{
  void* ret = NULL;
  ObLockGuard<LockT> guard(lock_);
  if (NULL == (ret = freelist_pop()))
  {
    alloc_new_block();
    ret = freelist_pop();
  }
  return ret;
}

template <typename BlockAllocatorT, typename LockT>
void ObPool<BlockAllocatorT, LockT>::free(void *obj)
{
  ObLockGuard<LockT> guard(lock_);
  if (NULL != obj)
  {
    atomic_dec(&in_use_count_);
  }
  freelist_push(obj);
}

template <typename BlockAllocatorT, typename LockT>
void* ObPool<BlockAllocatorT, LockT>::freelist_pop()
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

template <typename BlockAllocatorT, typename LockT>
void ObPool<BlockAllocatorT, LockT>::freelist_push(void *obj)
{
  if (NULL != obj)
  {
    FreeNode* node = static_cast<FreeNode*>(obj);
    node->next_ = freelist_;
    freelist_ = node;
    atomic_inc(&free_count_);
  }
}

  }
}
