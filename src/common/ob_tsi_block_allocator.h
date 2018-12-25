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
#ifndef __OB_COMMON_OB_BLOCK_ALLOCATOR_H__
#define __OB_COMMON_OB_BLOCK_ALLOCATOR_H__
#include "ob_allocator.h"
#include "utility.h"
#include "ob_fixed_queue.h"

namespace oceanbase
{
  namespace common
  {
    class TSIBlockCache
    {
      public:
        struct Block
        {
          Block(int64_t size): next_(NULL), mod_(0), ref_(0), size_(size), real_size_(0), checksum_(-size), alloc_bt_count_(0), free_bt_count_(0){}
          ~Block() {}
          bool check_checksum() {return 0 == checksum_ + size_; }
          void print_bt();
          Block* next_;
          int mod_;
          volatile int ref_;
          const int64_t size_;
          int64_t real_size_;
          const int64_t checksum_;
          int alloc_bt_count_;
          int free_bt_count_;
          void* alloc_backtraces_[32];
          void* free_backtraces_[32];
          char buf_[0];
        };
        struct BlockList
        {
          BlockList(): host_(NULL), n_block_(0), next_(NULL), head_(NULL) {}
          ~BlockList(){}
          int push(Block* block, const int64_t limit) {
            int err = OB_SUCCESS;
            if (NULL == block)
            {
              err = OB_INVALID_ARGUMENT;
            }
            else if (n_block_ >= limit)
            {
              err = OB_SIZE_OVERFLOW;
            }
            else
            {
              block->next_ = head_;
              head_ = block;
              n_block_++;
            }
            return err;
          }

          Block* pop() {
            Block* block = NULL;
            if (NULL != head_)
            {
              block = head_;
              head_ = block->next_;
              n_block_--;
            }
            return block;
          }
          TSIBlockCache* host_;
          int64_t n_block_;
          BlockList* next_;
          Block* head_;
        };
      public:
        const static int64_t MAX_THREAD_NUM = OB_MAX_THREAD_NUM;
        const static int64_t MAX_BLOCK_NUM = 1<<22;
      public:
        TSIBlockCache(): inited_(false), allocator_(NULL)
        {
          int err = OB_SUCCESS;
          int syserr = 0;
          if (0 != (syserr = pthread_key_create(&key_, (void (*)(void*))call_clear_block_list)))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "pthread_key_create()=>%d", syserr);
          }
          else if (OB_SUCCESS != (err = block_list_pool_.init(MAX_THREAD_NUM)))
          {
            TBSYS_LOG(ERROR, "block_list_pool.init(%ld)=>%d", MAX_THREAD_NUM, err);
          }
          else if (OB_SUCCESS != (err = global_block_list_.init(MAX_BLOCK_NUM)))
          {
            TBSYS_LOG(ERROR, "global_block_list.init(%ld)=>%d", MAX_BLOCK_NUM, err);
          }
          for(int64_t i = 0; OB_SUCCESS == err && i < MAX_THREAD_NUM; i++)
          {
            block_list_[i].host_ = this;
            if (OB_SUCCESS != (err = block_list_pool_.push(block_list_ + i)))
            {
              TBSYS_LOG(ERROR, "block_list_pool.push(i=%ld)=>%d", i, err);
            }
          }
          if (OB_SUCCESS == err)
          {
            inited_ = true;
          }
        }
        ~TSIBlockCache()
        {
          if (inited_)
          {
            pthread_key_delete(key_);
          }
          inited_ = false;
        }
      public:
        Block* get();
        int put(Block* block, const int64_t limit, const int64_t global_limit);
        void clear();
        void set_allocator(ObIAllocator* allocator){ allocator_ = allocator; }
      protected:
        void free_block(Block* block);
        static void call_clear_block_list(BlockList* block_list);
        void clear_block_list(BlockList* block_list);
      private:
        BlockList* get_block_list_head();
      private:
        bool inited_;
        pthread_key_t key_;
        ObIAllocator* allocator_;
        ObFixedQueue<BlockList> block_list_pool_;
        BlockList block_list_[MAX_THREAD_NUM];
        ObFixedQueue<Block> global_block_list_;
    };

    class ObTSIBlockAllocator: public ObIAllocator
    {
      public:
        typedef TSIBlockCache::Block Block;
        const static int64_t BLOCK_RESERVED_SIZE = sizeof(Block);
        const static int64_t NORMAL_BLOCK_SIZE = OB_TC_MALLOC_BLOCK_SIZE;
        const static int64_t MEDIUM_BLOCK_SIZE = OB_TC_MALLOC_BLOCK_SIZE;
        const static int64_t BIG_BLOCK_SIZE = (1<<21) + (1<<10) - BLOCK_RESERVED_SIZE;
        const static int64_t DEFAULT_NORMAL_BLOCK_TCLIMIT = 128;
        const static int64_t DEFAULT_MEDIUM_BLOCK_TCLIMIT = 64;
        const static int64_t DEFAULT_BIG_BLOCK_TCLIMIT = 8;
        const static int64_t DEFAULT_NORMAL_BLOCK_GLIMIT = 1024;
        const static int64_t DEFAULT_MEDIUM_BLOCK_GLIMIT = 512;
        const static int64_t DEFAULT_BIG_BLOCK_GLIMIT = 64;
      public:
        ObTSIBlockAllocator(): inited_(false), mod_id_(ObModIds::TSI_BLOCK_ALLOC), allocator_(NULL),
                               normal_block_size_(NORMAL_BLOCK_SIZE),
                               medium_block_size_(MEDIUM_BLOCK_SIZE),
                               big_block_size_(BIG_BLOCK_SIZE),
                               normal_block_cache_(),
                               medium_block_cache_(),
                               big_block_cache_(),
                               normal_block_tclimit_(DEFAULT_NORMAL_BLOCK_TCLIMIT),
                               medium_block_tclimit_(DEFAULT_MEDIUM_BLOCK_TCLIMIT),
                               big_block_tclimit_(DEFAULT_BIG_BLOCK_TCLIMIT),
                               normal_block_glimit_(DEFAULT_NORMAL_BLOCK_GLIMIT),
                               medium_block_glimit_(DEFAULT_MEDIUM_BLOCK_GLIMIT),
                               big_block_glimit_(DEFAULT_BIG_BLOCK_GLIMIT)
        {}
        ~ObTSIBlockAllocator()
        {
          if (inited_)
          {
            normal_block_cache_.clear();
            medium_block_cache_.clear();
            big_block_cache_.clear();
          }
          inited_ = false;
        }
        friend class TSIBlockCache;
      public:
        int init(ObIAllocator* allocator);
        void set_mod_id(int32_t mod_id) {mod_id_ = mod_id;};
        void set_normal_block_tclimit(const int64_t limit);
        void set_big_block_tclimit(const int64_t limit);
        void set_normal_block_glimit(const int64_t limit);
        void set_big_block_glimit(const int64_t limit);
        void* alloc(const int64_t size);
        void free(void* p);
        void* mod_alloc(int64_t size, const int32_t mod_id = 0);
        void mod_free(void* p, const int32_t mod_id = 0);
        void* mod_realloc(void* p, int64_t size, const int32_t mod_id = 0);
      protected:
        void after_alloc(Block* block);
        void before_free(Block* block);
        Block* mod_alloc_block(int64_t size, const int32_t mod_id);
        Block* mod_realloc_block(Block* old_block, int64_t size, const int32_t mod_id);
        void mod_free_block(Block* block, const int32_t mod_id);
      private:
        Block* alloc_block(const int64_t size);
        void free_block(Block* block);
      private:
        bool inited_;
        int32_t mod_id_;
        ObIAllocator* allocator_;
        int64_t normal_block_size_;
        int64_t medium_block_size_;
        int64_t big_block_size_;
        TSIBlockCache normal_block_cache_;
        TSIBlockCache medium_block_cache_;
        TSIBlockCache big_block_cache_;
        int64_t normal_block_tclimit_;
        int64_t medium_block_tclimit_;
        int64_t big_block_tclimit_;
        int64_t normal_block_glimit_;
        int64_t medium_block_glimit_;
        int64_t big_block_glimit_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_BLOCK_ALLOCATOR_H__ */
