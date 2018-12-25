/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_pooled_allocator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_POOLED_ALLOCATOR_H
#define _OB_POOLED_ALLOCATOR_H 1

#include "ob_pool.h"
namespace oceanbase
{
  namespace common
  {
    // @note thread-safe depends on LockT
    template <typename T, typename BlockAllocatorT = ObMalloc, typename LockT = ObNullLock>
    class ObPooledAllocator
    {
      public:
        ObPooledAllocator(int64_t block_size = common::OB_COMMON_MEM_BLOCK_SIZE, const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_POOL));
        virtual ~ObPooledAllocator();

        T *alloc();
        void free(T *obj);

        void inc_ref(){};
        void dec_ref(){};

      private:
        // disallow copy
        ObPooledAllocator(const ObPooledAllocator &other);
        ObPooledAllocator& operator=(const ObPooledAllocator &other);
      private:
        // data members
        ObPool<BlockAllocatorT, LockT> the_pool_;
    };

    template <typename T, typename BlockAllocatorT, typename LockT>
    ObPooledAllocator<T, BlockAllocatorT, LockT>::ObPooledAllocator(int64_t block_size, const BlockAllocatorT &alloc)
      :the_pool_(sizeof(T), block_size, alloc)
    {
    }

    template <typename T, typename BlockAllocatorT, typename LockT>
    ObPooledAllocator<T, BlockAllocatorT, LockT>::~ObPooledAllocator()
    {
    }

    template <typename T, typename BlockAllocatorT, typename LockT>
    T* ObPooledAllocator<T, BlockAllocatorT, LockT>::alloc()
    {
      T *ret = NULL;
      void *p = the_pool_.alloc();
      if (NULL == p)
      {
        TBSYS_LOG(ERROR, "no memory");
      }
      else
      {
        ret = new(p) T();
      }
      return ret;
    }

    template <typename T, typename BlockAllocatorT, typename LockT>
    void ObPooledAllocator<T, BlockAllocatorT, LockT>::free(T *obj)
    {
      if (NULL != obj)
      {
        obj->~T();
        the_pool_.free(obj);
      }
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_POOLED_ALLOCATOR_H */
