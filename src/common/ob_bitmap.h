/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ob_bitmap.h is for what ...
 *
 *  Version: $Id: ob_bitmap.h 2011年08月04日 14时37分25秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#ifndef OCEANBASE_COMMON_OB_BITMAP_H_
#define OCEANBASE_COMMON_OB_BITMAP_H_

#include "page_arena.h"

namespace oceanbase
{
  namespace common
  {

    template <typename Block = unsigned long,  typename Allocator = PageArena<Block> >
    class ObBitmap
    {
      public:
        typedef uint64_t size_type;
        typedef Block block_type;
        typedef block_type block_width_type;
        typedef Allocator allocator_type;

        static const size_type BYTES_PER_BLOCK = sizeof(block_type);
        static const size_type BITS_PER_BLOCK = BYTES_PER_BLOCK * 8;
        static const size_type MIN_MEM_BLOCK_BITS = 64 * BITS_PER_BLOCK; 
      public:
        explicit ObBitmap(const size_type num_bits);
        ObBitmap(const size_type num_bits, Allocator* allocator);
        ~ObBitmap();

        /**
         * check if bitmap is empty; not contains any data.
         */
        bool empty() const;

        /**
         * free all hold memory in bitmap.
         * empty will return true;
         */
        void destroy();

        /**
         * set bit at %pos if value true
         * or clear bit at %pos if value false, same as clear(pos)
         */
        int set(const size_type pos, const bool value = true);

        /**
         * same as set(pos, false);
         */
        int clear(const size_type pos);

        /**
         * reset all bits in bitmap;
         */
        int clear();

        /**
         * xor with origin bit.
         */
        int flip(const size_type pos);

        /**
         * check bit at %pos if set
         */
        int test(const size_type pos, bool& result) const;
        bool test(const size_type pos) const;

        /**
         * same as test(pos);
         */
        bool operator[](const size_type pos) const
        {
          return test(pos);
        }

        void resize(const size_type size)
        {
          size_type inner_pos = 0;
          if (size > num_bits_)
          {
            expand_block(size-1,inner_pos);
          }
        }

      public:
        size_type size() const
        {
          return num_bits_;
        }
        size_type num_blocks() const
        {
          return num_bits_ / BITS_PER_BLOCK;
        }

      private:
        struct MemBlock
        {
          size_type num_blocks_;
          MemBlock* next_;
          block_type bits_[0];
          MemBlock() 
            : num_blocks_(0), next_(NULL) {}
          MemBlock(const size_type num_blocks) 
            : num_blocks_(num_blocks), next_(NULL) {}
        };

      private:
        static size_type block_index(size_type pos) 
        { 
          return pos / BITS_PER_BLOCK; 
        }
        static size_type bit_index(size_type pos) 
        { 
          return static_cast<block_width_type>(pos % BITS_PER_BLOCK); 
        }
        static block_type bit_mask(size_type pos) 
        { 
          return static_cast<block_type>(1ULL << bit_index(pos)); 
        }
        static size_type round_up(size_type n, size_type align) 
        { 
          return (n +  align - 1) & ~(align - 1); 
        }

        void init_with_num_bits(const size_type num_bits)
        {
          num_bits_ = num_bits;
          if (num_bits_ < MIN_MEM_BLOCK_BITS)
          {
            num_bits_ = MIN_MEM_BLOCK_BITS;
          }
          num_bits_ = round_up(num_bits_, BITS_PER_BLOCK);
          size_type num_blocks = num_bits_ / BITS_PER_BLOCK;
          header_ = tailer_ = allocate_block(num_blocks);
        }

        MemBlock* allocate_block(const size_type num_blocks);
        MemBlock* find_block(const size_type pos, size_type& inner_pos) const;
        MemBlock* expand_block(const size_type pos, size_type& inner_pos);

        template <typename Pred> int for_each_block(Pred pred) const;

        int deallocate_block(MemBlock* mem_block)
        {
          if (NULL != mem_block->bits_ && NULL != allocator_)
          {
            allocator_->free(mem_block->bits_);
          }
          mem_block->next_ = NULL;
          mem_block->num_blocks_ = 0;
          return OB_SUCCESS;
        }
        static int clear_block_bits(MemBlock* mem_block)
        {
          memset(static_cast<void*>(mem_block->bits_), 0, 
              mem_block->num_blocks_ * BYTES_PER_BLOCK);
          return OB_SUCCESS;
        }

      private:
        size_type num_bits_;
        MemBlock* header_;
        MemBlock* tailer_;
        bool use_ext_allocator_;
        Allocator* allocator_;
        DISALLOW_COPY_AND_ASSIGN(ObBitmap);
    };

    template <typename Block, typename Allocator>
      ObBitmap<Block, Allocator>::ObBitmap(const size_type num_bits)
      : header_(NULL), tailer_(NULL), use_ext_allocator_(false), allocator_(NULL)
      {
        allocator_ = new (std::nothrow) Allocator();
        init_with_num_bits(num_bits);
      }

    template <typename Block, typename Allocator>
      ObBitmap<Block, Allocator>::ObBitmap(const size_type num_bits, Allocator* allocator)
      : header_(NULL), tailer_(NULL), use_ext_allocator_(true), allocator_(allocator)
      {
        init_with_num_bits(num_bits);
      }

    template <typename Block, typename Allocator>
      ObBitmap<Block, Allocator>::~ObBitmap()
      {
        destroy();
        if (!use_ext_allocator_ && NULL != allocator_)
        {
          delete allocator_;
          allocator_ = NULL;
        }
      }

    template <typename Block, typename Allocator>
      template<typename Pred>
      int ObBitmap<Block, Allocator>::for_each_block(Pred pred) const
      {
        MemBlock* p = header_;
        MemBlock* next = header_;
        int ret = OB_SUCCESS;
        while (NULL != p && OB_SUCCESS == ret)
        {
          next = p->next_;
          ret = pred(p);
          p = next;
        }
        return ret;
      }

    template <typename Block, typename Allocator>
      void ObBitmap<Block, Allocator>::destroy()
      {
        for_each_block(std::bind1st(std::mem_fun(&ObBitmap<Block,Allocator>::deallocate_block), this));
        header_ = tailer_ = NULL;
        num_bits_ = 0;
      }

    template <typename Block, typename Allocator>
      bool ObBitmap<Block, Allocator>::empty() const
      {
        return (NULL == header_ || 0 == num_bits_);
      }

    template <typename Block, typename Allocator>
      int ObBitmap<Block, Allocator>::set(const size_type pos, const bool value)
      {
        int ret = OB_SUCCESS;
        size_type inner_pos = pos;
        MemBlock* mem_block = expand_block(pos, inner_pos);
        if (NULL != mem_block)
        {
          if (value)
          {
            mem_block->bits_[block_index(inner_pos)] |= bit_mask(inner_pos);
          }
          else
          {
            mem_block->bits_[block_index(inner_pos)] &= static_cast<block_type>(~bit_mask(inner_pos));
          }
        }
        else
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        return ret;
      }

    template <typename Block, typename Allocator>
      int ObBitmap<Block, Allocator>::clear(const size_type pos)
      {
        int ret = OB_SUCCESS;
        size_type inner_pos = pos;
        MemBlock* mem_block = find_block(pos, inner_pos);
        if (NULL != mem_block)
        {
          mem_block->bits_[block_index(inner_pos)] &= ~bit_mask(inner_pos);
        }
        else
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
        }
        return ret;
      }

    template <typename Block, typename Allocator>
      int ObBitmap<Block, Allocator>::clear()
      {
        for_each_block(&clear_block_bits);
        return OB_SUCCESS;
      }

    template <typename Block, typename Allocator>
      int ObBitmap<Block, Allocator>::flip(const size_type pos)
      {
        int ret = OB_SUCCESS;
        size_type inner_pos = pos;
        MemBlock* mem_block = find_block(pos, inner_pos);
        if (NULL != mem_block)
        {
          mem_block->bits_[block_index(inner_pos)] ^= bit_mask(inner_pos);
        }
        else
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
        }
        return ret;
      }

    template <typename Block, typename Allocator>
      int ObBitmap<Block, Allocator>::test(const size_type pos, bool& result) const
      {
        int ret = OB_SUCCESS;
        size_type inner_pos = pos;
        MemBlock* mem_block = find_block(pos, inner_pos);
        if (NULL != mem_block)
        {
          result = (mem_block->bits_[block_index(inner_pos)] & bit_mask(inner_pos)) != 0;
        }
        else
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
        }
        return ret;
      }

    template <typename Block, typename Allocator>
      bool ObBitmap<Block, Allocator>::test(const size_type pos) const
      {
        bool result = false;
        test(pos, result);
        return result;
      }

    template <typename Block, typename Allocator>
      typename ObBitmap<Block, Allocator>::MemBlock* 
      ObBitmap<Block, Allocator>::allocate_block(const size_type num_blocks)
      {
        block_type* membuf = NULL; 
        MemBlock * mem_block = NULL;

        if (NULL != allocator_)
        {
          membuf = allocator_->alloc(num_blocks * BYTES_PER_BLOCK + sizeof(MemBlock));
        }
        if (NULL != membuf)
        {
          mem_block = new (membuf)MemBlock(num_blocks);
          clear_block_bits(mem_block);
        }
        return mem_block;
      }

    template <typename Block, typename Allocator>
      typename ObBitmap<Block, Allocator>::MemBlock* 
      ObBitmap<Block, Allocator>::find_block(const size_type pos, size_type& inner_pos) const
      {
        MemBlock* ret = NULL;
        size_type acc_bits = 0;
        size_type last_acc_bits = 0;
        inner_pos = 0;
        MemBlock* cur_mem_block = header_;

        while (NULL != cur_mem_block)
        {
          acc_bits += cur_mem_block->num_blocks_ * BITS_PER_BLOCK;
          if (acc_bits > pos)
          {
            ret = cur_mem_block;
            inner_pos = pos - last_acc_bits;
            break;
          }
          else
          {
            last_acc_bits = acc_bits;
            cur_mem_block = cur_mem_block->next_;
          }
        }


        return ret;
      }

    template <typename Block, typename Allocator>
      typename ObBitmap<Block, Allocator>::MemBlock* 
      ObBitmap<Block, Allocator>::expand_block(const size_type pos, size_type& inner_pos)
      {
        MemBlock* ret = NULL;
        size_type need_bits = 0;
        size_type num_blocks = 0;
        inner_pos = 0;
        MemBlock* new_mem_block = NULL;


        if (pos < num_bits_)
        {
          ret = find_block(pos, inner_pos);
        }
        else 
        {
          need_bits = pos - (num_bits_ - 1);
          if (need_bits < MIN_MEM_BLOCK_BITS)
          {
            need_bits = MIN_MEM_BLOCK_BITS;
          }
          need_bits = round_up(need_bits, BITS_PER_BLOCK);
          num_blocks = need_bits / BITS_PER_BLOCK;

          new_mem_block = allocate_block(num_blocks);
          if (NULL != new_mem_block)
          {
            if (NULL != tailer_)
            {
              OB_ASSERT(NULL != header_);
              tailer_->next_ = new_mem_block;
              tailer_ = new_mem_block;
            }
            else
            {
              OB_ASSERT(NULL == header_);
              header_ = tailer_ = new_mem_block;
            }

            inner_pos = pos - num_bits_;
            num_bits_ += need_bits;
          }
          ret = new_mem_block;
        }

        return ret;

      }


  }
}

#endif // OCEANBASE_COMMON_OB_BITMAP_H_
