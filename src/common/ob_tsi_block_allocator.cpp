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
#include "ob_tsi_block_allocator.h"

namespace oceanbase
{
  namespace common
  {
#define ENABLE_MALLOC_BT 0
    char* parray(char* buf, int64_t len, int64_t* array, int size)
    {
      int64_t pos = 0;
      int64_t count = 0;
      for(int64_t i = 0; i < size; i++)
      {
        count = snprintf(buf + pos, len - pos, "0x%lx ", array[i]);
        if (count >= 0 && pos + count + 1 < len)
        {
          pos += count;
        }
        else
        {
          TBSYS_LOG(WARN, "buf not enough, len=%ld, array_size=%d", len, size);
          break;
        }
      }
      buf[pos + 1] = 0;
      return buf;
    }

    char* lbt(char* buf, int64_t len)
    {
      static void *addrs[100];
      int size = backtrace(addrs, 100);
      return parray(buf, len, (int64_t*)addrs, size);
    }
#undef BACKTRACE
#define BACKTRACE(level, cond, format...) { char buf[1024]; TBSYS_LOG(level, format); TBSYS_LOG(ERROR, "backtrace: %s", lbt(buf, 1024)); }
    void TSIBlockCache::Block::print_bt()
    {
      char buf1[1024];
      char buf2[1024];
      parray(buf1, sizeof(buf1), (int64_t*)alloc_backtraces_, alloc_bt_count_);
      parray(buf2, sizeof(buf2), (int64_t*)free_backtraces_, free_bt_count_);
      TBSYS_LOG(INFO, "alloc_bt: %s free_bt: %s", buf1, buf2);
    }
    void TSIBlockCache::call_clear_block_list(BlockList* block_list)
    {
      if (NULL != block_list && NULL != block_list->host_)
      {
        ((TSIBlockCache*)(block_list->host_))->clear_block_list(block_list);
      }
    }

    void TSIBlockCache::clear_block_list(BlockList* block_list)
    {
      int err = OB_SUCCESS;
      if (NULL != block_list)
      {
        Block* block = NULL;
        while(NULL != (block = block_list->pop()))
        {
          if (OB_SUCCESS != (err = global_block_list_.push(block)))
          {
            TBSYS_LOG(WARN, "global_block_list.push(%p)=>%d, free to OS", block, err);
            free_block(block);
          }
        }
        if (OB_SUCCESS != (err = block_list_pool_.push(block_list)))
        {
          TBSYS_LOG(ERROR, "block_list_pool.push(%p)=>%d", block_list, err);
        }
      }
    }

    void TSIBlockCache::clear()
    {
      Block* block = NULL;
      for(int64_t i = 0; i < MAX_THREAD_NUM; i++)
      {
        while(NULL != (block = block_list_[i].pop()))
        {
          free_block(block);
        }
      }
      while(OB_SUCCESS == global_block_list_.pop(block))
      {
        free_block(block);
      }
    }

    void TSIBlockCache::free_block(TSIBlockCache::Block* block)
    {
      if (NULL != allocator_)
      {
        allocator_->mod_free((void*)block, block->mod_);
      }
    }

    TSIBlockCache::BlockList* TSIBlockCache::get_block_list_head()
    {
      int err = OB_SUCCESS;
      int syserr = 0;
      BlockList* block_list = NULL;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "not inited");
      }
      else if (NULL != (block_list = (BlockList*)pthread_getspecific(key_)))
      {}
      else if (OB_SUCCESS != (err = block_list_pool_.pop(block_list)))
      {
        TBSYS_LOG(ERROR, "block_list_pool.pop()=>%d, too many threads", err);
      }
      else if (0 != (syserr = pthread_setspecific(key_, block_list)))
      {
        block_list_pool_.push(block_list);
        block_list = NULL;
        TBSYS_LOG(ERROR, "pthread_setspecific()=>%d", syserr);
      }
      return block_list;
    }

    TSIBlockCache::Block* TSIBlockCache::get()
    {
      BlockList* block_list = NULL;
      Block* block = NULL;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "not inited");
      }
      else if (NULL == (block_list = get_block_list_head()))
      {
        TBSYS_LOG(ERROR, "get_block_list()=>NULL");
      }
      else if (NULL != (block = block_list->pop()))
      {
        // do nothing
      }
      else if (OB_SUCCESS == global_block_list_.pop(block))
      {
        // do nothing
      }
      return block;
    }

    int TSIBlockCache::put(Block* block, const int64_t limit, const int64_t global_limit)
    {
      int err = OB_SUCCESS;
      BlockList* block_list = NULL;
      if (!inited_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "not inited");
      }
      else if (NULL == block)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "block == NULL");
      }
      else if (NULL == (block_list = get_block_list_head()))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "get_block_list()=>NULL");
      }
      else if (OB_SUCCESS != (err = block_list->push(block, limit))
               && OB_SIZE_OVERFLOW != err)
      {
        TBSYS_LOG(ERROR, "block_list->push(%p, limit=%ld)=>%d", block, limit, err);
      }
      else if (OB_SUCCESS == err)
      {} // do nothing
      else if (global_block_list_.get_total() > global_limit)
      {
        err = OB_SIZE_OVERFLOW;
      }
      else if (OB_SUCCESS != (err = global_block_list_.push(block)))
      {
        free_block(block);
        TBSYS_LOG(WARN, "global_block_list->push(%p, %ld)=>%d", block, global_limit, err);
      }
      return err;
    }

    void ObTSIBlockAllocator::set_normal_block_tclimit(const int64_t limit)
    {
      normal_block_tclimit_ = limit;
    }

    void ObTSIBlockAllocator::set_big_block_tclimit(const int64_t limit)
    {
      big_block_tclimit_ = limit;
    }

    void ObTSIBlockAllocator::set_normal_block_glimit(const int64_t limit)
    {
      normal_block_glimit_ = limit;
    }

    void ObTSIBlockAllocator::set_big_block_glimit(const int64_t limit)
    {
      big_block_glimit_ = limit;
    }

    int ObTSIBlockAllocator::init(ObIAllocator* allocator)
    {
      int err = OB_SUCCESS;
      if (inited_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == allocator)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        allocator_ = allocator;
        normal_block_cache_.set_allocator(allocator);
        medium_block_cache_.set_allocator(allocator);
        big_block_cache_.set_allocator(allocator);
        inited_ = true;
      }
      return err;
    }

    void ObTSIBlockAllocator::after_alloc(Block* block)
    {
      if (NULL != block)
      {
        int ref = 0;
        if ((ref = __sync_add_and_fetch(&block->ref_, 1)) != 1)
        {
          BACKTRACE(ERROR, true, "after allock ref[%d] != 1", ref);
          block->print_bt();
        }
#if ENABLE_MALLOC_BT
        block->alloc_bt_count_ = backtrace(block->alloc_backtraces_, ARRAYSIZEOF(block->alloc_backtraces_));
        block->free_bt_count_ = 0;
#endif
      }
    }

    void ObTSIBlockAllocator::before_free(Block* block)
    {
      if (NULL != block)
      {
        int ref = 0;
        if ((ref = __sync_add_and_fetch(&block->ref_, -1)) != 0)
        {
          BACKTRACE(ERROR, true, "before free ref[%d] != 0", ref);
          block->print_bt();
        }
#if ENABLE_MALLOC_BT
        block->free_bt_count_ = backtrace(block->free_backtraces_, ARRAYSIZEOF(block->free_backtraces_));
        block->alloc_bt_count_ = 0;
#endif
      }
    }

    ObTSIBlockAllocator::Block* ObTSIBlockAllocator::alloc_block(const int64_t size)
    {
      Block* p = NULL;
      if (NULL == (p = (Block*)allocator_->alloc(size + sizeof(*p))))
      {
        TBSYS_LOG(ERROR, "alloc(%ld)=>NULL", size + sizeof(*p));
      }
      else
      {
        new(p) Block(size);
      }
      return p;
    }

    void ObTSIBlockAllocator::free_block(Block* p)
    {
      if (NULL == p)
      {}
      else
      {
        p->~Block();
        allocator_->free(p);
      }
    }

    void* ObTSIBlockAllocator::alloc(const int64_t size)
    {
      return mod_alloc(size, mod_id_);
    }

    void ObTSIBlockAllocator::free(void* p)
    {
      mod_free(p, mod_id_);
    }

    ObTSIBlockAllocator::Block* ObTSIBlockAllocator::mod_alloc_block(int64_t size, const int32_t mod_id)
    {
      int err = OB_SUCCESS;
      Block* block = NULL;
      int64_t alloc_size = 0;
      if (!inited_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "not inited.");
      }
      else if (size <= 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "size[%ld] <= 0", size);
      }
      else if (normal_block_size_ >= size)
      {
        alloc_size = normal_block_size_;
        block = normal_block_cache_.get();
      }
      else if (medium_block_size_ >= size)
      {
        alloc_size = medium_block_size_;
        block = medium_block_cache_.get();
      }
      else if (big_block_size_ >= size)
      {
        alloc_size = big_block_size_;
        block = big_block_cache_.get();
      }
      else
      {
        alloc_size = size;
        //TBSYS_LOG(TRACE, "alloc big block size[%ld]", size);
      }
      if (OB_SUCCESS == err && NULL == block)
      {
        block = alloc_block(alloc_size);
      }
      if (NULL != block)
      {
        block->real_size_ = size;
        block->mod_ = mod_id;
        after_alloc(block);
        g_malloc_size_stat.add(size);
        ob_mod_usage_update(alloc_size, mod_id);
        ob_mod_usage_update(alloc_size, ObModIds::OB_MOD_TC_ALLOCATED);
        //BACKTRACE(WARN, block->size_ > 65536, "block_size=%ld", block->size_);
      }
      return block;
    }

    ObTSIBlockAllocator::Block* ObTSIBlockAllocator::mod_realloc_block(Block* old_block, int64_t size, const int32_t mod_id)
    {
      int err = OB_SUCCESS;
      Block* new_block = NULL;
      if (size < 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "realloc(block=%p, size[%ld] < 0): INVALID_ARGUMENT", old_block, size);
      }
      else if (NULL == old_block)
      {}
      else if (!old_block->check_checksum())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "block->check_checksum() fail");
      }
      else if (old_block->ref_ != 1)
      {
        err = OB_INVALID_ARGUMENT;
        BACKTRACE(ERROR, true, "realloc an unused block, block->buf=%p", old_block->buf_);
        old_block->print_bt();
      }
      else if (old_block->size_ >= size)
      {
        new_block = old_block;
        new_block->real_size_ = size;
        old_block = NULL;
      }
      if (OB_SUCCESS != err || NULL != new_block)
      {}
      else if (NULL == (new_block = mod_alloc_block(size, mod_id)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "mod_alloc(%ld, %d): MEMORY_OVERFLOW", size, mod_id);
      }
      if (NULL != old_block && NULL != new_block)
      {
        memcpy(new_block->buf_, old_block->buf_, old_block->size_);
        mod_free_block(old_block, mod_id);
        old_block = NULL;
      }
      return new_block;
    }

    void ObTSIBlockAllocator::mod_free_block(Block* block, const int32_t mod_id)
    {
      int err = OB_SUCCESS;
      int64_t size = block? block->size_: 0;
      int block_mod_id = block? block->mod_: 0;
      if (!inited_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "not inited.");
      }
      else if (NULL == block)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "block == NULL");
      }
      else if (block->ref_ != 1)
      {
        err = OB_INVALID_ARGUMENT;
        BACKTRACE(ERROR, true, "double free, block->buf=%p", block->buf_);
        block->print_bt();
      }
      else
      {
        before_free(block);
      }
      if (OB_SUCCESS != err)
      {}
      else if (normal_block_size_ == block->size_)
      {
        if (OB_SUCCESS == normal_block_cache_.put(block, normal_block_tclimit_, normal_block_glimit_))
        {
          block = NULL;
        }
      }
      else if (medium_block_size_ == block->size_)
      {
        if (OB_SUCCESS == medium_block_cache_.put(block, medium_block_tclimit_, medium_block_glimit_))
        {
          block = NULL;
        }
      }
      else if (big_block_size_ == block->size_)
      {
        if (OB_SUCCESS == big_block_cache_.put(block, big_block_tclimit_, big_block_glimit_))
        {
          block = NULL;
        }
      }
      else
      {
        TBSYS_LOG(TRACE, "size[%ld] not equal %ld or %ld", block->size_, normal_block_size_, big_block_size_);
      }
      if (OB_SUCCESS == err && NULL != block)
      {
        free_block(block);
      }
      ob_mod_usage_update(-size, mod_id?: block_mod_id);
      ob_mod_usage_update(-size, ObModIds::OB_MOD_TC_ALLOCATED);
    }

    void* ObTSIBlockAllocator::mod_alloc(int64_t size, const int32_t mod_id)
    {
      Block* block = mod_alloc_block(size, mod_id);
      return block? block->buf_: NULL;
    }

    void ObTSIBlockAllocator::mod_free(void* p, const int32_t mod_id)
    {
      Block* block = p? (Block*)((char*)p - sizeof(*block)): NULL;
      mod_free_block(block, mod_id);
    }

    void* ObTSIBlockAllocator::mod_realloc(void* p, int64_t size, const int32_t mod_id)
    {
      Block* old_block = p? (Block*)((char*)p - sizeof(Block)): NULL;
      Block* new_block = mod_realloc_block(old_block, size, mod_id);
      return new_block? new_block->buf_: NULL;
    }
  }; // end namespace common
}; // end namespace oceanbase
