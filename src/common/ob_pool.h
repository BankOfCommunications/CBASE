/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_simple_pool.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SIMPLE_POOL_H
#define _OB_SIMPLE_POOL_H 1
#include "ob_spin_lock.h"
#include "ob_allocator.h"
namespace oceanbase
{
  namespace common
  {
    /**
     * fixed size objects pool
     * suitable for allocating & deallocating lots of objects dynamically & frequently
     * @note not thread-safe
     * @note obj_size >= sizeof(void*)
     * @see ObLockedPool
     */
    template <typename BlockAllocatorT = ObMalloc, typename LockT = ObNullLock>
    class ObPool
    {
      public:
        ObPool(int64_t obj_size, int64_t block_size = common::OB_COMMON_MEM_BLOCK_SIZE, const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_POOL));
        virtual ~ObPool();

        void* alloc();
        void free(void *obj);
        void set_mod_id(int32_t mod_id) {block_allocator_.set_mod_id(mod_id);};

        uint64_t get_free_count() const;
        uint64_t get_in_use_count() const;
        uint64_t get_total_count() const;
        int64_t get_obj_size() const {return obj_size_;};
      private:
        void* freelist_pop();
        void freelist_push(void *obj);
        void alloc_new_block();
        // disallow copy
        ObPool(const ObPool &other);
        ObPool& operator=(const ObPool &other);
      private:
        struct FreeNode
        {
          FreeNode* next_;
        };
        struct BlockHeader
        {
          BlockHeader *next_;
        };
      private:
        // data members
        int64_t obj_size_;
        int64_t block_size_;
        volatile uint64_t in_use_count_;
        volatile uint64_t free_count_;
        volatile uint64_t total_count_;
        FreeNode* freelist_;
        BlockHeader *blocklist_;
        BlockAllocatorT block_allocator_;
        LockT lock_;
    };

    template <typename BlockAllocatorT, typename LockT>
    inline uint64_t ObPool<BlockAllocatorT, LockT>::get_free_count() const
    {
      return free_count_;
    }

    template <typename BlockAllocatorT, typename LockT>
    inline uint64_t ObPool<BlockAllocatorT, LockT>::get_in_use_count() const
    {
      return in_use_count_;
    }

    template <typename BlockAllocatorT, typename LockT>
    inline uint64_t ObPool<BlockAllocatorT, LockT>::get_total_count() const
    {
      return total_count_;
    }

    ////////////////////////////////////////////////////////////////
    /// thread-safe pool by using spin-lock
    typedef ObPool<ObMalloc, ObSpinLock> ObLockedPool;

    ////////////////////////////////////////////////////////////////
    // A small block allocator which split the block allocated by the BlockAllocator into small blocks
    template <typename BlockAllocatorT = ObMalloc, typename LockT = ObNullLock>
    class ObSmallBlockAllocator : public ObIAllocator
    {
      public:
        ObSmallBlockAllocator(int64_t small_block_size, int64_t block_size, const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_POOL))
          :block_pool_(small_block_size, block_size, alloc)
        {};
        // Caution: for hashmap
        ObSmallBlockAllocator()
          :block_pool_(sizeof(void*))
        {};

        virtual ~ObSmallBlockAllocator(){};

        virtual void *alloc(const int64_t sz)
        {
          void *ret = NULL;
          if (sz > block_pool_.get_obj_size())
          {
            TBSYS_LOG(ERROR, "Wrong block size, sz=%ld block_size=%ld", sz, block_pool_.get_obj_size());
          }
          else
          {
            ret = block_pool_.alloc();
          }
          return ret;
        }
        virtual void free(void *ptr){ block_pool_.free(ptr);};
        void set_mod_id(int32_t mod_id) {block_pool_.set_mod_id(mod_id);};
      private:
        // types and constants
      private:
        // disallow copy
        ObSmallBlockAllocator(const ObSmallBlockAllocator &other);
        ObSmallBlockAllocator& operator=(const ObSmallBlockAllocator &other);
        // function members
      private:
        // data members
        ObPool<BlockAllocatorT, LockT> block_pool_;
    };

    class ObWrapperAllocator: public ObIAllocator
    {
      public:
        explicit ObWrapperAllocator(ObIAllocator *alloc):alloc_(alloc) {};
        ObWrapperAllocator(int32_t mod_id):alloc_(NULL) {UNUSED(mod_id);};
        virtual ~ObWrapperAllocator(){};
        void *alloc(const int64_t sz) {return alloc_->alloc(sz);};
        void free(void *ptr) {alloc_->free(ptr);};
        void set_mod_id(int32_t mod_id) {alloc_->set_mod_id(mod_id);};
        void set_alloc(ObIAllocator *alloc) {alloc_ = alloc;};
      private:
        // data members
        ObIAllocator *alloc_;
    };
  } // end namespace common
} // end namespace oceanbase

#include "ob_pool.ipp"

#endif /* _OB_SIMPLE_POOL_H */
