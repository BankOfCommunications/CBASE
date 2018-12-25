/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_stack_allocator.h"
#include "ob_mod_define.h"
#include "ob_malloc.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    DefaultBlockAllocator::DefaultBlockAllocator(): mod_(ObModIds::BLOCK_ALLOC), limit_(INT64_MAX), allocated_(0)
    {}

    DefaultBlockAllocator::~DefaultBlockAllocator()
    {
      if (allocated_ != 0)
      {
        TBSYS_LOG(WARN, "allocated_[%ld] != 0", allocated_);
      }
    }

    void DefaultBlockAllocator::set_mod_id(int32_t mod)
    {
      mod_ = mod;
    }

    int DefaultBlockAllocator::set_limit(const int64_t limit)
    {
      int err = OB_SUCCESS;
      if (limit < 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        limit_ = limit;
        TBSYS_LOG(INFO, "block_allocator.set_limit(%ld)", limit );
      }
      return err;
    }

    const int64_t DefaultBlockAllocator::get_allocated() const
    {
      return allocated_;
    }

    void* DefaultBlockAllocator::alloc(const int64_t size)
    {
      int err = OB_SUCCESS;
      void* p = NULL;
      int64_t alloc_size = size + sizeof(alloc_size);
      if (0 >= size)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (allocated_ + alloc_size > limit_)
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "allocated[%ld] + size[%ld] > limit[%ld]", allocated_, size, limit_);
      }
      else if (__sync_add_and_fetch(&allocated_, alloc_size) > limit_)
      {
        err = OB_MEM_OVERFLOW;
        __sync_add_and_fetch(&allocated_, -alloc_size);
        TBSYS_LOG(ERROR, "allocated[%ld] + size[%ld] > limit[%ld]", allocated_, alloc_size, limit_);
      }
      else if (NULL == (p = ob_tc_malloc(alloc_size, mod_)))
      {
        err = OB_MEM_OVERFLOW;
        __sync_add_and_fetch(&allocated_, -alloc_size);
        TBSYS_LOG(ERROR, "ob_tc_malloc(size=%ld) failed", alloc_size);
      }
      else
      {
        *((int64_t*)p) = alloc_size;
        p = (char*)p + sizeof(alloc_size);
      }
      return p;
    }

    void DefaultBlockAllocator::free(void* p)
    {
      int err = OB_SUCCESS;
      int64_t alloc_size = 0;
      if (NULL == p)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        alloc_size = *((int64_t*)((char*)p - sizeof(alloc_size)));
      }
      if (OB_SUCCESS != err)
      {}
      else if (__sync_add_and_fetch(&allocated_, -alloc_size) < 0)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "free(size=%ld): allocated[%ld] < 0 after free", alloc_size, allocated_);
      }
      else
      {
        ob_tc_free((void*)((char*)p - sizeof(int64_t)), mod_);
      }
    }

    StackAllocator::StackAllocator(): allocator_(NULL), block_size_(0), top_(0), saved_top_(-1), head_(NULL), reserved_(NULL)
    {}

    StackAllocator::~StackAllocator()
    {
      clear();
    }

    int StackAllocator::Block::init(const int64_t limit)
    {
      int err = OB_SUCCESS;
      magic_ = 0x7878787887878787;
      next_ = NULL;
      limit_ = limit;
      pos_ = sizeof(*this);
      checksum_ = -limit;
      return err;
    }

    int64_t StackAllocator::Block::remain() const
    {
      return NULL == this? -1: limit_ - pos_;
    }

    int StackAllocator::init(ObIAllocator* allocator, const int64_t block_size)
    {
      int err = OB_SUCCESS;
      if (NULL == allocator || 0 >= block_size)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != allocator_ && 0 < block_size_)
      {
        err = OB_INIT_TWICE;
      }
      else
      {
        allocator_ = allocator;
        block_size_ = block_size;
      }
      return err;
    }

    // int StackAllocator::destroy()
    // {
    //   int err = OB_SUCCESS;
    //   Block* head = NULL;
    //   if (NULL == allocator_)
    //   {
    //     err = OB_NOT_INIT;
    //   }
    //   while(OB_SUCCESS == err && NULL != head_)
    //   {
    //     head = head_;
    //     head_ = head_->next_;
    //     err = allocator_->free((const char*)head, head->limit_);
    //   }
    //   return err;
    // }

    int StackAllocator::clear(const bool slow)
    {
      int err = OB_SUCCESS;
      if (NULL == allocator_)
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = restore_top(0, slow)))
      {
        TBSYS_LOG(ERROR, "restore_top(0)=>%d", err);
      }
      else if (OB_SUCCESS != (err = set_reserved_block(NULL)))
      {
        TBSYS_LOG(ERROR, "set_reserved_block()=>%d", err);
      }
      return err;
    }

    int StackAllocator::alloc_block(Block*& block, const int64_t size)
    {
      int err = OB_SUCCESS;
      int64_t limit = max(block_size_, size + sizeof(Block));
      if (NULL == allocator_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == (block = (Block*)allocator_->alloc(limit)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(WARN, "allocator->alloc(size=%ld)=>NULL", size);
      }
      else if (OB_SUCCESS != (err = block->init(limit)))
      {
        TBSYS_LOG(ERROR, "block->init()=>%d", err);
      }
      return err;
    }

    int StackAllocator::free_block(Block* block)
    {
      int err = OB_SUCCESS;
      if (NULL == allocator_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == block)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "free_block(block=NULL)");
      }
      else
      {
        allocator_->free(block);
      }
      return err;
    }

    int StackAllocator::alloc_head(const int64_t size)
    {
      int err = OB_SUCCESS;
      Block* new_block = reserved_;
      if (new_block->remain() < size
          && OB_SUCCESS != (err = alloc_block(new_block, size)))
      {
        TBSYS_LOG(WARN, "allocator->alloc(size=%ld)=>%d", size, err);
      }
      else
      {
        if (new_block == reserved_)
        {
          reserved_ = NULL;
        }
        new_block->next_ = head_;
        top_ += new_block->pos_;
        head_ = new_block;
      }
      return err;
    }

    int StackAllocator::free_head()
    {
      int err = OB_SUCCESS;
      Block* head = NULL;
      if (NULL == allocator_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == head_ || (top_ -= head_->pos_) < 0)
      {
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        head = head_;
        head_ = head_->next_;
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = free_block(head)))
      {
        TBSYS_LOG(ERROR, "free_block()=>%d", err);
      }
      return err;
    }

    int StackAllocator::set_reserved_block(Block* block)
    {
      int err = OB_SUCCESS;
      if (NULL != reserved_ && OB_SUCCESS != (err = free_block(reserved_)))
      {
        TBSYS_LOG(ERROR, "free(%p)=>%d", reserved_, err);
      }
      else
      {
        reserved_ = block;
      }
      return err;
    }

    int StackAllocator::reserve_block(const int64_t size)
    {
      int err = OB_SUCCESS;
      Block* new_block = NULL;
      if (OB_SUCCESS != (err = alloc_block(new_block, size)))
      {
        TBSYS_LOG(WARN, "allocator->alloc(size=%ld)=>%d", size, err);
      }
      else if (OB_SUCCESS != (err = set_reserved_block(new_block)))
      {
        TBSYS_LOG(ERROR, "set_reserved_block(new_block=%p)=>%d", new_block, err);
      }
      if (OB_SUCCESS != err && OB_SUCCESS != (err = free_block(new_block)))
      {
        TBSYS_LOG(ERROR, "free_block(new_block=%p)=>%d", new_block, err);
      }
      return err;
    }

    int StackAllocator::reserve(const int64_t size)
    {
      int err = OB_SUCCESS;
      if (head_->remain() >= size || reserved_->remain() >= size)
      {}
      else if (OB_SUCCESS != (err = reserve_block(size)))
      {
        TBSYS_LOG(ERROR, "reserve_block(size=%ld)=>%d", size, err);
      }
      return err;
    }

    void* StackAllocator::alloc(const int64_t size)
    {
      int err = OB_SUCCESS;
      void* p = NULL;
      if (size <= 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "alloc(size=%ld): INVALID_ARGUMENT", size);
      }
      else if (head_->remain() < size && OB_SUCCESS != (err = alloc_head(size)))
      {
        TBSYS_LOG(ERROR, "alloc_block(size=%ld)=>%d", size, err);
      }
      else
      {
        p = (char*)head_ + head_->pos_;
        head_->pos_ += size;
        top_ += size;
      }
      return p;
    }

    void StackAllocator::free(void* p)
    {
      UNUSED(p);
    }

    int StackAllocator::start_batch_alloc()
    {
      int err = OB_SUCCESS;
      if (saved_top_ >= 0)
      {
        err = OB_ERROR;
      }
      else if (OB_SUCCESS != (err = save_top(saved_top_)))
      {
        TBSYS_LOG(ERROR, "save_top(top=%ld)=>%d", top_, err);
      }
      return err;
    }

    int StackAllocator::end_batch_alloc(const bool rollback)
    {
      int err = OB_SUCCESS;
      if (rollback && OB_SUCCESS != (err = restore_top(saved_top_, false)))
      {
        TBSYS_LOG(ERROR, "restor_top(saved_top=%ld)=>%d", saved_top_, err);
      }
      else
      {
        saved_top_ = -1;
      }
      return err;
    }

    int StackAllocator::save_top(int64_t& top) const
    {
      int err = OB_SUCCESS;
      if (saved_top_ >= 0)
      {
        err = OB_ERROR;
      }
      else
      {
        top = top_;
      }
      return err;
    }

    int StackAllocator::restore_top(const int64_t top, const bool slow)
    {
      int err = OB_SUCCESS;
      int64_t last_top = top_;
      int64_t BATCH_FREE_LIMIT = 1<<27;
      if (top < 0 || top > top_)
      {
        err = OB_INVALID_ARGUMENT;
      }
      while(OB_SUCCESS == err && top_ > top)
      {
        if (NULL == head_)
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "restor_top(top=%ld): head_ == NULL", top);
        }
        else if (top + head_->pos_ <= top_)
        {
          err = free_head();
        }
        else if (head_->pos_ - (int64_t)sizeof(*head_) < top_ - top)
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "restore_top(head->pos_=%ld, top=%ld, top_=%ld)=>%d", head_->pos_, top, top_, err);
        }
        else
        {
          head_->pos_ -= top_ - top;
          top_ = top;
        }
        if (slow && last_top > top_ + BATCH_FREE_LIMIT)
        {
          last_top = top_;
          usleep(10000);
        }
      }
      return err;
    }

    TSIStackAllocator::TSIStackAllocator(): block_size_(0), block_allocator_(NULL)
    {}

    TSIStackAllocator::~TSIStackAllocator()
    {
      clear();
    }

    int TSIStackAllocator::init(ObIAllocator* block_allocator, int64_t block_size)
    {
      int err = OB_SUCCESS;
      if (NULL == block_allocator || 0 >= block_size)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != block_allocator_)
      {
        err = OB_INIT_TWICE;
      }
      for(int64_t i = 0; OB_SUCCESS == err && i < (int64_t)ARRAYSIZEOF(allocator_array_); i++)
      {
        err = allocator_array_[i].init(block_allocator, block_size);
      }
      if (OB_SUCCESS == err)
      {
        block_allocator_ = block_allocator;
        block_size_ = block_size;
      }
      return err;
    }

    int TSIStackAllocator::clear(const bool slow)
    {
      int err = OB_SUCCESS;
      for(int64_t i = 0; OB_SUCCESS == err && i < (int64_t)ARRAYSIZEOF(allocator_array_); i++)
      {
        err = allocator_array_[i].clear(slow);
      }
      return err;
    }

    int TSIStackAllocator::reserve(const int64_t size)
    {
      int err = OB_SUCCESS;
      StackAllocator* allocator = NULL;
      if (NULL == (allocator = get()))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "get_tsi_allocator() failed");
      }
      else
      {
        err = allocator->reserve(size);
      }
      return err;
    }

    void* TSIStackAllocator::alloc(const int64_t size)
    {
      int err = OB_SUCCESS;
      StackAllocator* allocator = NULL;
      void* p = NULL;
      if (NULL == (allocator = get()))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "get_tsi_allocator() failed");
      }
      else if (NULL == (p = allocator->alloc(size)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "allocator->alloc(%ld)=>NULL", size);
      }
      return p;
    }

    void TSIStackAllocator::free(void* p)
    {
      int err = OB_SUCCESS;
      StackAllocator* allocator = NULL;
      if (NULL == (allocator = get()))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "get_tsi_allocator() failed");
      }
      else
      {
        allocator->free(p);
      }
    }

    int TSIStackAllocator::start_batch_alloc()
    {
      int err = OB_SUCCESS;
      StackAllocator* allocator = NULL;
      if (NULL == (allocator = get()))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "get_tsi_allocator() failed");
      }
      else
      {
        err = allocator->start_batch_alloc();
      }
      return err;
    }

    int TSIStackAllocator::end_batch_alloc(const bool rollback)
    {
      int err = OB_SUCCESS;
      StackAllocator* allocator = NULL;
      if (NULL == (allocator = get()))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "get_tsi_allocator() failed");
      }
      else
      {
        err = allocator->end_batch_alloc(rollback);
      }
      return err;
    }

    StackAllocator* TSIStackAllocator::get()
    {
      int64_t idx = itid();
      return (idx >= 0 && idx < (int64_t)ARRAYSIZEOF(allocator_array_))? allocator_array_ + idx: NULL;
    }
  }; // end namespace common
}; // end namespace oceanbase
