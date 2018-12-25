/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - A memory pool specifically used by Btree
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_MEM_POOL_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_MEM_POOL_H_

#include <stdlib.h>
#include "btree_default_alloc.h"
#include "btree_tid.h"
#include "btree_thread_store.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      template<class K, class V, class Alloc>
      class BtreeMemPool
      {
        public:
          static const int64_t DEFAULT_PAGE_SIZE = 4L << 20;
        protected:
          class Page
          {
            public:
              Page                               ();
              void *   alloc                     (int64_t size);
              void     set_next                  (Page * page);
              Page *   get_next                  () const;
            private:
              Page *   next_;
              int64_t  pos_;
              char     mem_[DEFAULT_PAGE_SIZE];
          };
          class Block
          {
            public:
              Block *  get_next                  () const;
              void     set_next                  (Block * block);
            private:
              Block *  next_;
          };
          class Pool
          {
            public:
              Pool                               (Alloc & alloc);
              ~Pool                              ();
              void *   alloc                     (int64_t size);
              void     put_block                 (Block * block);
              Block *  pick_block                ();
              int64_t  get_block_num             () const;
              int64_t  get_alloced_size          () const;
            private:
              void     free_pages_               ();
            private:
              Page *   page_;
              Block *  block_;
              int64_t  block_num_;
              int64_t  alloced_size_;
              Alloc &  alloc_;
          };
          class PoolInitializer
          {
            public:
              PoolInitializer(Alloc & alloc);
              void operator()(void *ptr);
            private:
              Alloc &  alloc_;
          };
        public:
          typedef BtreeThreadStore<Pool, PoolInitializer> TThreadStore;
        public:
          class AllocedSizeAdder
          {
            public:
              inline      AllocedSizeAdder       ();
              inline void operator()             (const Pool * pool);
              inline int64_t sum                 () const;
            protected:
              int64_t sum_;
          };
          class ReservedBlockAdder
          {
            public:
              inline      ReservedBlockAdder     ();
              inline void operator()             (const Pool * pool);
              inline int64_t sum                 () const;
            protected:
              int64_t sum_;
          };
          class MemInfoDumper
          {
            public:
              MemInfoDumper                      (int64_t block_size);
              inline void operator()             (const typename TThreadStore::Item * item);
            protected:
              int64_t block_size_;
          };
        public:
          BtreeMemPool                           (int64_t block_size, Alloc & alloc);
          ~BtreeMemPool                          ();
          int32_t  init                          ();
          void     destroy                       ();
          void *   alloc_block                   ();
          /**
           * Allocate a block if this block is malloced from lower allocator,
           * namely not reusing recycled block, this block will be memset to 0
           */
          void *   alloc_block_with_initial_memset();
          void     free_block                    (void * ptr);
          int64_t  get_block_size                () const;
          int64_t  get_reserved_block_num        () const;
          int64_t  get_alloced_size              () const;
          int64_t  get_reserved_size             () const;
          void     dump_thread_mem_info          () const;
        protected:
          void *   alloc_block_                  (bool is_memset);
        protected:
          int64_t         block_size_;       /// 定长内存块大小
          Alloc &         alloc_;            /// 内存分配器
          PoolInitializer pool_initializer_;
          TThreadStore    pool_;             /// 线程局部的内存池
      };

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::Page::Page()
        : next_(NULL), pos_(0)
      {
      }

      template<class K, class V, class Alloc>
      void * BtreeMemPool<K, V, Alloc>::Page::alloc(int64_t size)
      {
        void * ret = NULL;
        if (LIKELY(DEFAULT_PAGE_SIZE - pos_ >= size))
        {
          ret = mem_ + pos_;
          pos_ += size;
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::Page::set_next(
          typename BtreeMemPool<K, V, Alloc>::Page * page)
      {
        next_ = page;
      }

      template<class K, class V, class Alloc>
      typename BtreeMemPool<K, V, Alloc>::Page *
          BtreeMemPool<K, V, Alloc>::Page::get_next() const
      {
        return next_;
      }

      template<class K, class V, class Alloc>
      typename BtreeMemPool<K, V, Alloc>::Block *
          BtreeMemPool<K, V, Alloc>::Block::get_next() const
      {
        return next_;
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::Block::set_next(Block * block)
      {
        next_ = block;
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::Pool::Pool(Alloc & alloc)
        : page_(NULL), block_(NULL), block_num_(0), alloced_size_(0), alloc_(alloc)
      {
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::Pool::~Pool()
      {
        free_pages_();
      }

      template<class K, class V, class Alloc>
      void * BtreeMemPool<K, V, Alloc>::Pool::alloc(int64_t size)
      {
        void * ret = NULL;
        if (UNLIKELY(NULL == page_))
        {
          Page * page = new (alloc_.alloc(sizeof(Page))) Page();
          if (LIKELY(NULL != page))
          {
            alloced_size_ += sizeof(Page);
            page->set_next(page_);
            page_ = page;
          }
        }
        if (UNLIKELY(NULL == page_))
        {
          CMBTREE_LOG(ERROR, "memory is not enough");
        }
        else
        {
          ret = page_->alloc(size);
          if (UNLIKELY(NULL == ret))
          {
            Page * page = new (alloc_.alloc(sizeof(Page))) Page();
            if (UNLIKELY(NULL == page))
            {
              CMBTREE_LOG(ERROR, "memory is not enough");
            }
            else
            {
              alloced_size_ += sizeof(Page);
              page->set_next(page_);
              page_ = page;
              ret = page_->alloc(size);
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::Pool::put_block(Block * block)
      {
        if (LIKELY(NULL != block))
        {
          block->set_next(block_);
          block_ = block;
          ++block_num_;
        }
      }

      template<class K, class V, class Alloc>
      typename BtreeMemPool<K, V, Alloc>::Block *
          BtreeMemPool<K, V, Alloc>::Pool::pick_block()
      {
        Block * ret = block_;
        block_ = block_ != NULL ? --block_num_, block_->get_next() : NULL;
        return ret;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::Pool::get_block_num() const
      {
        return block_num_;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::Pool::get_alloced_size() const
      {
        return alloced_size_;
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::Pool::free_pages_()
      {
        CMBTREE_LOG(DEBUG, "Thread alloced_size=%ld reserved_block_num=%ld",
            alloced_size_, get_block_num());
        Page * p = page_;
        while (NULL != p)
        {
          Page * o = p;
          p = p->get_next();
          o->~Page();
          alloc_.free(o);
        }
        page_ = NULL;
        block_ = NULL;
        block_num_ = 0;
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::PoolInitializer::PoolInitializer(Alloc & alloc)
        : alloc_(alloc)
      {
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::PoolInitializer::operator()(void *ptr)
      {
        new (ptr) Pool(alloc_);
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::AllocedSizeAdder::AllocedSizeAdder()
        : sum_(0)
      {
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::AllocedSizeAdder::operator()(const Pool * pool)
      {
        if (NULL == pool)
        {
          CMBTREE_LOG(ERROR, "AllocedSizeAdder internal error, "
              "sum operator encountered NULL");
        }
        else
        {
          sum_ += pool->get_alloced_size();
        }
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::AllocedSizeAdder::sum() const
      {
        return sum_;
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::ReservedBlockAdder::ReservedBlockAdder()
        : sum_(0)
      {
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::ReservedBlockAdder::operator()(const Pool * pool)
      {
        if (NULL == pool)
        {
          CMBTREE_LOG(ERROR, "ReservedBlockAdder internal error, "
              "sum operator encountered NULL");
        }
        else
        {
          sum_ += pool->get_block_num();
        }
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::ReservedBlockAdder::sum() const
      {
        return sum_;
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::MemInfoDumper::MemInfoDumper(int64_t block_size)
        : block_size_(block_size)
      {
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::MemInfoDumper::operator()(
          const typename TThreadStore::Item * item)
      {
        if (NULL == item)
        {
          CMBTREE_LOG(ERROR, "MemInfoDumper internal error, "
              "dump operator encountered NULL");
        }
        else
        {
          const Pool & pool = item->obj;
          CMBTREE_LOG(TRACE, "[CMBTree Info] Thread(%-5d) alloced_size=%-10ld "
              "reserved_block_num=%-5ld reserved_size=%-8ld",
              item->thread_id, pool.get_alloced_size(), pool.get_block_num(),
              pool.get_block_num() * block_size_);
        }
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::BtreeMemPool(int64_t block_size, Alloc & alloc)
          : block_size_(block_size), alloc_(alloc),
            pool_initializer_(alloc_), pool_(pool_initializer_)
      {
      }

      template<class K, class V, class Alloc>
      BtreeMemPool<K, V, Alloc>::~BtreeMemPool()
      {
      }

      template<class K, class V, class Alloc>
      int32_t BtreeMemPool<K, V, Alloc>::init()
      {
        return pool_.init();
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::destroy()
      {
        pool_.destroy();
      }

      template<class K, class V, class Alloc>
      void * BtreeMemPool<K, V, Alloc>::alloc_block()
      {
        bool is_memset = false;
        return alloc_block_(is_memset);
      }

      template<class K, class V, class Alloc>
      void * BtreeMemPool<K, V, Alloc>::alloc_block_with_initial_memset()
      {
        bool is_memset = true;
        return alloc_block_(is_memset);
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::free_block(void * ptr)
      {
        Pool * pool = pool_.get();
        if (UNLIKELY(NULL == pool))
        {
          CMBTREE_LOG(ERROR, "allocate pool error");
        }
        else
        {
          pool->put_block(reinterpret_cast<Block *>(ptr));
        }
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::get_block_size() const
      {
        return block_size_;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::get_reserved_block_num() const
      {
        int64_t ret = 0;
        Pool * pool = const_cast<BtreeMemPool<K, V, Alloc> *>(this)->pool_.get();
        if (UNLIKELY(NULL == pool))
        {
          CMBTREE_LOG(ERROR, "allocate pool error");
        }
        else
        {
          ret = pool->get_block_num();
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::get_alloced_size() const
      {
        int64_t ret = 0;
        AllocedSizeAdder alloced_size_adder;
        int err = pool_.for_each_obj_ptr(alloced_size_adder);
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "BtreeMemPool internal error, "
              "for_each_obj_ptr returned error, err = %d", err);
        }
        else
        {
          ret = alloced_size_adder.sum();
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeMemPool<K, V, Alloc>::get_reserved_size() const
      {
        int64_t ret = 0;
        ReservedBlockAdder reserved_block_adder;
        int err = pool_.for_each_obj_ptr(reserved_block_adder);
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "BtreeMemPool internal error, "
              "for_each_obj_ptr returned error, err = %d", err);
        }
        else
        {
          ret = reserved_block_adder.sum() * block_size_;
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      void BtreeMemPool<K, V, Alloc>::dump_thread_mem_info() const
      {
        MemInfoDumper dumper(block_size_);
        int err = pool_.for_each_item_ptr(dumper);
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "BtreeMemPool internal error, "
              "for_each_obj_ptr returned error, err = %d", err);
        }
      }

      template<class K, class V, class Alloc>
      void * BtreeMemPool<K, V, Alloc>::alloc_block_(bool is_memset)
      {
        void * ptr = NULL;
        Pool * pool = pool_.get();
        if (UNLIKELY(NULL == pool))
        {
          CMBTREE_LOG(ERROR, "allocate pool error");
        }
        else
        {
          ptr = pool->pick_block();
          if (NULL == ptr)
          {
            ptr = pool->alloc(block_size_);
            if (NULL != ptr && is_memset)
            {
              memset(ptr, 0x00, block_size_);
            }
          }
        }
        return ptr;
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_MEM_POOL_H_
