/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_global_factory.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_GLOBAL_FACTORY_H
#define _OB_GLOBAL_FACTORY_H 1
#include "common/ob_spin_lock.h"
#include "common/ob_recursive_mutex.h"
#include "common/ob_malloc.h"
namespace oceanbase
{
  namespace common
  {
    class ObTCBlock: public common::DLink
    {
      public:
        static ObTCBlock* new_block(int32_t obj_size, int32_t mod_id);
        static void delete_block(ObTCBlock *blk);
        static ObTCBlock* get_block_by_obj(void *obj);
        static int32_t get_batch_count_by_obj_size(int32_t obj_size);

        bool can_delete() const;
        void *alloc();
        void free(void *ptr);
        bool is_empty() const {return NULL == freelist_;};
        // for stat purpose only
        int32_t get_in_use_count() const {return in_use_count_;};
        int32_t get_obj_size() const {return obj_size_;};
        static int32_t get_obj_num_by_obj_size(int32_t obj_size);
      private:
        ObTCBlock(int32_t obj_size);
        ~ObTCBlock(){};
        struct FreeNode
        {
          FreeNode *next_;
        };
        void freelist_push(void *ptr);
        void *freelist_pop();
        static int32_t get_obj_num_by_aligned_obj_size(int32_t aligned_obj_size);
        static int32_t get_aligned_obj_size(int32_t obj_size);
      private:
        static const int64_t BLOCK_SIZE_SHIFT = 21;
        static const int64_t BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT; // 2MB
        static const uint64_t BLOCK_SIZE_ALIGN_MASK = ~((~0ULL) << BLOCK_SIZE_SHIFT);
        static const uint32_t OBJ_SIZE_ALIGN_SHIFT = sizeof(void*);
        static const uint32_t OBJ_SIZE_ALIGN_MASK = (~0U) << OBJ_SIZE_ALIGN_SHIFT;
      private:
        int32_t obj_size_;
        int32_t in_use_count_;
        FreeNode *freelist_;
        char data[0];
    };

    inline ObTCBlock* ObTCBlock::new_block(int32_t obj_size, int32_t mod_id)
    {
      ObTCBlock *ret = NULL;
      void *ptr = ob_malloc(BLOCK_SIZE, mod_id);
      if (NULL == ptr)
      {
        TBSYS_LOG(WARN, "no memory");
      }
      else
      {
        ret = new(ptr) ObTCBlock(obj_size);
      }
      return ret;
    }

    inline void ObTCBlock::delete_block(ObTCBlock *blk)
    {
      OB_ASSERT(blk);
      OB_ASSERT(blk->can_delete());
      blk->~ObTCBlock();
      ob_free(blk);
    }

    inline int32_t ObTCBlock::get_batch_count_by_obj_size(int32_t obj_size)
    {
      OB_ASSERT(obj_size > 0);
      int32_t batch_count = static_cast<int32_t>(BLOCK_SIZE) / obj_size;
      if (batch_count < 2)
      {
        batch_count = 2;
      }
      else if (batch_count > 32)
      {
        batch_count = 32;
      }
      return batch_count;
    }

    inline ObTCBlock* ObTCBlock::get_block_by_obj(void *obj)
    {
      return *reinterpret_cast<ObTCBlock**>(reinterpret_cast<char*>(obj) - sizeof(void*));
    }

    inline void ObTCBlock::freelist_push(void *ptr)
    {
      FreeNode* node = reinterpret_cast<FreeNode*>(ptr);
      node->next_ = freelist_;
      freelist_ = node;
    }

    inline void *ObTCBlock::freelist_pop()
    {
      void *ret = NULL;
      if (NULL != freelist_)
      {
        ret = freelist_;
        freelist_ = freelist_->next_;
      }
      return ret;
    }

    inline int32_t ObTCBlock::get_aligned_obj_size(int32_t obj_size)
    {
      /* we make the object address 8 bytes aligned */
      int32_t aligned_obj_size = static_cast<int32_t>(obj_size + sizeof(void*));
      if (aligned_obj_size % OBJ_SIZE_ALIGN_SHIFT != 0)
      {
        aligned_obj_size = (aligned_obj_size + OBJ_SIZE_ALIGN_SHIFT) & OBJ_SIZE_ALIGN_MASK;
      }
      return aligned_obj_size;
    }

    inline int32_t ObTCBlock::get_obj_num_by_aligned_obj_size(int32_t aligned_obj_size)
    {
      return static_cast<int32_t>((BLOCK_SIZE - sizeof(ObTCBlock)) / aligned_obj_size);
    }

    inline int32_t ObTCBlock::get_obj_num_by_obj_size(int32_t obj_size)
    {
      return get_obj_num_by_aligned_obj_size(get_aligned_obj_size(obj_size));
    }

    inline ObTCBlock::ObTCBlock(int32_t obj_size)
      :obj_size_(obj_size),
       in_use_count_(0),
       freelist_(NULL)
    {
      int32_t aligned_obj_size = get_aligned_obj_size(obj_size);
      int32_t obj_count = get_obj_num_by_aligned_obj_size(aligned_obj_size);
      for (int32_t i = 0; i < obj_count; ++i)
	  {
	    char *ptr = &data[i*aligned_obj_size];
        *reinterpret_cast<void**>(ptr) = this; // pointer to this block
        ptr += sizeof(void*);
	    freelist_push(ptr);
	  }
    }

    inline bool ObTCBlock::can_delete() const
    {
      return (0 == in_use_count_);
    }

    inline void *ObTCBlock::alloc()
    {
      void *ret = freelist_pop();
      if (NULL != ret)
      {
        ++in_use_count_;
      }
      return ret;
    }

    inline void ObTCBlock::free(void *ptr)
    {
      OB_ASSERT(ptr);
      OB_ASSERT((char*)ptr - (char*)this < BLOCK_SIZE);
      freelist_push(ptr);
      --in_use_count_;
    }
    ////////////////////////////////////////////////////////////////
    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    class ObGlobalFactory;

    template<typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    class ObGlobalFreeList
    {
      typedef typename ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::create_method_t create_method_t;
      public:
        ObGlobalFreeList():next_cache_line_(0) {};
        virtual ~ObGlobalFreeList(){};

        void push_range(common::DList &range, int32_t num_to_move)
      {
        int32_t num = range.get_size();
        while (num >= num_to_move)
        {
          common::DList subrange;
          range.pop_range(num_to_move, subrange);
          num -= num_to_move;
          ObRecursiveMutexGuard guard(lock_);
          if (next_cache_line_ < CACHE_SIZE)
          {
            objs_cache_[next_cache_line_++].push_range(subrange);
          }
          else
          {
            // cache is full
            release_objs(subrange);
            break;
          }
        } // end while
        if (0 < num)
        {
          ObRecursiveMutexGuard guard(lock_);
          release_objs(range);
        }
      }

        void pop_range(int32_t num, int32_t num_to_move, int32_t obj_size, create_method_t creator, common::DList &range)
      {
        OB_ASSERT(range.get_size() <= num_to_move);
        ObRecursiveMutexGuard guard(lock_);
        if (num == num_to_move)
        {
          if (next_cache_line_ > 0)
          {
            range.push_range(objs_cache_[--next_cache_line_]);
          }
          else
          {
            // cache is empty
            create_objs(num, obj_size, creator, range);
          }
        }
        else
        {
          // allocate num new objects from blocks
          create_objs(num, obj_size, creator, range);
        }
      }

        void stat()
      {
        ObRecursiveMutexGuard guard(lock_);
        TBSYS_LOG(INFO, "[GFACTORY_STAT] freelist next_cache_line=%d", next_cache_line_);
        TBSYS_LOG(INFO, "[GFACTORY_STAT] freelist empty_blocks_num=%d", empty_blocks_.get_size());
        TBSYS_LOG(INFO, "[GFACTORY_STAT] freelist nonempty_blocks_num=%d", nonempty_blocks_.get_size());
        dlist_for_each(ObTCBlock, blk, nonempty_blocks_)
        {
          const int32_t obj_size = blk->get_obj_size();
          TBSYS_LOG(INFO, "[GFACTORY_STAT] nonempty_block, obj_size=%d in_use_count=%d total=%d",
                    obj_size, blk->get_in_use_count(), blk->get_obj_num_by_obj_size(obj_size));
        }
      }
      private:
        // types and constants
        static const int64_t CACHE_SIZE = 64;
      private:
        // disallow copy
        ObGlobalFreeList(const ObGlobalFreeList &other);
        ObGlobalFreeList& operator=(const ObGlobalFreeList &other);
        // function members
        void release_objs(common::DList &range)
      {
          dlist_for_each_safe(T, ptr, next, range)
        {
          UNUSED(next);
          dynamic_cast<T*>(ptr)->~T();
          ObTCBlock* blk = ObTCBlock::get_block_by_obj(ptr);
          if (blk->is_empty())
          {
            // move from empty list to nonempty list
            empty_blocks_.remove(blk);
            nonempty_blocks_.add_last(blk);
          }
          blk->free(ptr);
          if (blk->can_delete())
          {
            nonempty_blocks_.remove(blk);
            ObTCBlock::delete_block(blk);
          }
        }
      }

        void create_objs(int32_t num, int32_t obj_size, create_method_t creator, common::DList &range)
      {
        for (int32_t i = 0; i < num; ++i)
        {
          if (nonempty_blocks_.get_size() <= 0)
          {
            //
            ObTCBlock *blk = ObTCBlock::new_block(obj_size, MODID);
            if (NULL == blk)
            {
              TBSYS_LOG(ERROR, "no memory");
              break;
            }
            void *ptr = blk->alloc();
            OB_ASSERT(ptr);
            T* obj = creator(ptr);
            range.add_first(obj);
            if (blk->is_empty())
            {
              empty_blocks_.add_last(blk);
            }
            else
            {
              nonempty_blocks_.add_last(blk);
            }
          }
          else
          {
            ObTCBlock *blk = dynamic_cast<ObTCBlock*>(nonempty_blocks_.get_first());
            void *ptr = blk->alloc();
            OB_ASSERT(ptr);
            T* obj = creator(ptr);
            range.add_first(obj);
            if (blk->is_empty())
            {
              nonempty_blocks_.remove(blk);
              empty_blocks_.add_last(blk);
            }
          }
        } // end for
      }
      private:
        // data members
        ObRecursiveMutex lock_;
        int32_t next_cache_line_;
        common::DList objs_cache_[CACHE_SIZE];

        common::DList empty_blocks_;
        common::DList nonempty_blocks_;
    };
    ////////////////////////////////////////////////////////////////
    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    class ObGlobalFactory
    {
      public:
        typedef T* (*create_method_t)(void *ptr);

        static ObGlobalFactory* get_instance();

        int register_class(int32_t type_id, create_method_t creator, int32_t obj_size);
        int init();

        void get_objs(int32_t type_id, int32_t num, common::DList &objs);
        void put_objs(int32_t type_id, common::DList &objs);

        int32_t get_batch_count(int32_t type_id) const {return batch_count_[type_id];};
        int32_t get_obj_size(int32_t type_id) const {return obj_size_[type_id];};

        void stat();
      private:
        // types and constants
        typedef ObGlobalFactory<T, MAX_CLASS_NUM, MODID> self_t;
        typedef ObGlobalFreeList<T, MAX_CLASS_NUM, MODID> freelist_t;
      private:
        // disallow copy
        ObGlobalFactory(const ObGlobalFactory &other);
        ObGlobalFactory& operator=(const ObGlobalFactory &other);
        // function members
        ObGlobalFactory()
      {
        memset(create_methods_, 0, sizeof(create_methods_));
        memset(obj_size_, 0, sizeof(obj_size_));
        memset(batch_count_, 0, sizeof(batch_count_));
      };
        virtual ~ObGlobalFactory(){};
      private:
        // static members
        static self_t *INSTANCE;
        // data members
        freelist_t freelists_[MAX_CLASS_NUM];
        create_method_t create_methods_[MAX_CLASS_NUM];
        int32_t obj_size_[MAX_CLASS_NUM];
        int32_t batch_count_[MAX_CLASS_NUM];
    };

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    int ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::register_class(int32_t type_id, create_method_t creator, int32_t obj_size)
    {
      int ret = OB_SUCCESS;
      if (0 > type_id || type_id >= MAX_CLASS_NUM)
      {
        TBSYS_LOG(WARN, "invalid type_id=%d class_num=%ld", type_id, MAX_CLASS_NUM);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == creator || 0 >= obj_size)
      {
        TBSYS_LOG(WARN, "invalid arguments, creator=%p obj_size=%d", creator, obj_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL != create_methods_[type_id]
               || 0 != obj_size_[type_id])
      {
        TBSYS_LOG(WARN, "class already registered, type_id=%d", type_id);
        ret = OB_INIT_TWICE;
      }
      else
      {
        create_methods_[type_id] = creator;
        obj_size_[type_id] = obj_size;
      }
      return ret;
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    int ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::init()
    {
      int ret = OB_SUCCESS;
      for (int32_t type_id = 0; type_id < MAX_CLASS_NUM; ++type_id)
      {
        if (NULL == create_methods_[type_id])
        {
          //TBSYS_LOG(WARN, "class not registered, type_id=%d", type_id);
        }
        else
        {
          batch_count_[type_id] = ObTCBlock::get_batch_count_by_obj_size(obj_size_[type_id]);
        }
      }
      return ret;
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    ObGlobalFactory<T, MAX_CLASS_NUM, MODID>* ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::INSTANCE = NULL;

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    ObGlobalFactory<T, MAX_CLASS_NUM, MODID>* ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::get_instance()
    {
      if (NULL == INSTANCE)
      {
        INSTANCE = new(std::nothrow) self_t();
        if (NULL == INSTANCE)
        {
          TBSYS_LOG(ERROR, "failed to create singleton instance");
          abort();
        }
      }
      return INSTANCE;
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    void ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::get_objs(int32_t type_id, int32_t num, common::DList &objs)
    {
      freelist_t &l = freelists_[type_id];
      l.pop_range(num, get_batch_count(type_id), get_obj_size(type_id), create_methods_[type_id], objs);
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    void ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::put_objs(int32_t type_id, common::DList &objs)
    {
      freelist_t &l = freelists_[type_id];
      l.push_range(objs, get_batch_count(type_id));
    }

    template <typename T, int64_t MAX_CLASS_NUM, int32_t MODID>
    void ObGlobalFactory<T, MAX_CLASS_NUM, MODID>::stat()
    {
      for (int32_t type_id = 0; type_id < MAX_CLASS_NUM; ++type_id)
      {
        if (NULL != create_methods_[type_id])
        {
          TBSYS_LOG(INFO, "[GFACTORY_STAT] type_id=%d obj_size=%d batch_count=%d creator=%p",
                    type_id, obj_size_[type_id], batch_count_[type_id], create_methods_[type_id]);
          freelists_[type_id].stat();
        }
      }
    }
  } // end namespace common
} // end namespace oceanbase

// utility for register class to global factory
#define DEFINE_CREATOR(T, D)\
  static T* tc_factory_create_##D(void *ptr)           \
  {                                                    \
    T* ret = new(ptr) D();                             \
    return ret;                                        \
  }


#define REGISTER_CREATOR(FACTORY, T, D, TYPE_ID)                        \
  DEFINE_CREATOR(T, D);                                                 \
  struct D##RegisterToFactory                                           \
  {                                                                     \
    D##RegisterToFactory()                                           \
    {                                                                   \
      FACTORY::get_instance()->register_class(TYPE_ID, tc_factory_create_##D, sizeof(D)); \
    }                                                                   \
  } register_##D;

#endif /* _OB_GLOBAL_FACTORY_H */
