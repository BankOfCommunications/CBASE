/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_memory_pool.h is for what ...
 *
 * Version: $id: ob_memory_pool.h,v 0.1 8/19/2010 9:56a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   为了便于调试内存错误的bug，如果定义了名为__OB_MALLOC_DIRECT__的环境变量，
 *   内存池将直接调用new和delete分配和释放内存
 *
 */
#ifndef OCEANBASE_COMMON_OB_MEMORY_POOL_H_
#define OCEANBASE_COMMON_OB_MEMORY_POOL_H_

#include <pthread.h>
#include "ob_string.h"
#include "tbsys.h"
#include "ob_define.h"
#include "ob_link.h"
#include "ob_tsi_utils.h"

namespace oceanbase
{
  namespace common
  {
    /// @class  define module names need to allocate memory from memory pool
    /// @author wushi(wushi.ly@taobao.com)  (11/23/2010)
    class ObMemPoolModSet
    {
    public:
      virtual ~ObMemPoolModSet()
      {
      }
      virtual int32_t get_mod_id(const char * mod_name)const = 0;
      virtual const char * get_mod_name(const int32_t mod_id) const = 0;
      virtual int32_t get_max_mod_num()const = 0;
    };

    /// @class  virtual base class of memory pool
    /// @author wushi(wushi.ly@taobao.com)  (8/16/2010)
    class ObBaseMemPool
    {
    private:
      DISALLOW_COPY_AND_ASSIGN(ObBaseMemPool);
    public:
      /// @fn constructor
      ObBaseMemPool();
      /// @fn destructor
      virtual ~ObBaseMemPool();

      /// @fn init mod set
      int init(const ObMemPoolModSet * mod_set);

      /// @fn void* oceanbase/common/ObBaseMemPool::malloc(const int64_t nbyte)
      ///   malloc nbyte memory buffer
      ///
      /// @param nbyte bytes needed
      ///
      /// @return void* pointer to the buffer allocated
      void *malloc(const int64_t nbyte,const int32_t mod_id = 0, int64_t *got_size = NULL);
      void *malloc_emergency(const int64_t nbyte,const int32_t mod_id = 0, int64_t *got_size = NULL);

      /// @fn int oceanbase/common/ObBaseMemPool::free(const void *ptr)
      ///
      /// @param ptr pointer to buffer allocated by calling malloc
      ///
      /// @return int 0 on success, < 0 for failure
      void free(const void *ptr);

      virtual void print_mod_memory_usage(bool print_to_std = false);

      int64_t get_mod_memory_usage(int32_t mod_id);
      inline void mod_usage_update(const int64_t delta, const int32_t mod_id)
      {
        int32_t real_mod_id = mod_id;
        if ((NULL == mod_set_)  || (NULL == mem_size_each_mod_) )
        {
          mem_size_default_mod_.inc(delta);
        }
        else
        {
          if (mod_id <= 0 || mod_id >= mod_set_->get_max_mod_num())
          {
            real_mod_id = 0;
          }
          mem_size_each_mod_[real_mod_id].inc(delta);
        }
      }

      /// @fn free all memory allocated from system
      virtual void clear()=0;

      /// @fn recycle free memory, try to keep the size of memory handled by pool limited to
      ///     the given argument
      virtual int64_t shrink(const int64_t );

      /// @fn get memory size handled by this pool
      virtual int64_t get_memory_size_handled();
      virtual int64_t set_memory_size_limit(const int64_t mem_size_limit);
      virtual int64_t get_memory_size_limit() const;

      inline int64_t get_memory_size_direct_allocated()
      {
        return direct_allocated_mem_size_;
      }
    private:
      virtual void *malloc_(const int64_t nbyte, const int32_t mod_id=0, int64_t *got_size = NULL) = 0;
      virtual void free_(const void *ptr)=0;
    protected:
      /// @property mutex
      mutable tbsys::CThreadMutex pool_mutex_;
      /// @property memory size hold by pool
      int64_t             mem_size_handled_;
      int64_t             mem_size_limit_;

    protected:
      virtual void mod_malloc(const int64_t size, const int32_t mod_id);
      virtual void mod_free(const int64_t size, const int32_t mod_id);
      TCCounter mem_size_default_mod_;
      TCCounter *mem_size_each_mod_;
      const ObMemPoolModSet     *mod_set_;
      /// @property number of block directly allocated
      int64_t   direct_allocated_block_num_;
      /// @property size of memory allocated directly
      int64_t   direct_allocated_mem_size_;
    };

    /// @class  ObFixedMemPool fixed size memory pool
    /// @author wushi(wushi.ly@taobao.com)  (8/16/2010)
    class ObFixedMemPool : public ObBaseMemPool
    {
    private:
      DISALLOW_COPY_AND_ASSIGN(ObFixedMemPool);
      /// @fn members initializing function
      void property_initializer();
    public:
      /// @fn constructor
      ObFixedMemPool();
      /// @fn destructor
      ~ObFixedMemPool();

      /// @fn set memory pool parameters
      ///
      /// @param fixed_item_size   the minimum size of buffer returned by calling malloc
      /// @param item_num_each_block   memory size each time allocate from system
      int init(const int64_t fixed_item_size, const int64_t item_num_each_block,
               const ObMemPoolModSet *mod_set = NULL);

      /// @fn return the number of blocks hold by the pool
      int64_t get_block_num() const;

      /// @fn return the number of blocks in using hold by the pool
      int64_t get_used_block_num() const;
      int64_t get_free_block_num() const {return free_mem_block_num_;};
      /// @fn get memory block size
      int64_t get_block_size() const;

      virtual void print_mod_memory_usage(bool print_to_std = false);

      int64_t shrink(const int64_t remain_memory_size);
      virtual void clear();
    private:
      virtual void *malloc_(const int64_t nbyte, const int32_t mod_id=0, int64_t *got_size = NULL);
      virtual void free_(const void *ptr);
        int64_t get_free_block_limit_() const;
    private:
      /// @fn clear all allocated memory
      void clear(bool check_unfreed_mem);
      /// @fn 释放一个内存block
      int64_t recycle_memory_block(ObDLink * &block_it, bool check_unfreed_mem=true);
      /// block means memory buffer allocated from system
      /// item means memory buffer allocated from mempool
      /// @property list of used memory block
      ObDLink     used_mem_block_list_;
      /// @property list of used memory block
      ObDLink     free_mem_block_list_;
      /// @property size of memory block
      int64_t   mem_block_size_;
      /// @property size of fixed memory item
      int64_t   mem_fixed_item_size_;
      /// @property number of used memory block
      int64_t   used_mem_block_num_;
      /// @property number of free memory block
      int64_t   free_mem_block_num_;
    };

    /// @class  ObVarMemPool 非通用变长内存池
    /// @author wushi(wushi.ly@taobao.com)  (8/25/2010)
    /// @warning 该内存池不是通用的变长内存池，通用的内存分配请使用common/ob_malloc.h中的接口
    ///          内存池适用的地方当前只有任务队列，如果需要使用该内存池请联系作者充分的了解
    ///          实现机制，从而确定是否适合使用场景，同时用户必须确保该内存池的使用控制在
    ///          一个class的内部
    /// @note -# 必须保证从ObVarMemPool中分配的内存能够尽快释放
    ///       -# 非通用内存池，请控制它的使用范围，使用前请联系wushi.ly@taobao.com
    class ObVarMemPool : public ObBaseMemPool
    {
    private:
      /// @fn members initializing function
      void property_initializer();
      ObVarMemPool();
      DISALLOW_COPY_AND_ASSIGN(ObVarMemPool);
    public:
      /// @fn explicit oceanbase/ObVarMemPool::ObVarMemPool(const int64_t item_size)
      ///   constructor
      ///
      /// @param block_size sizeof memory block each allocated from system
      explicit ObVarMemPool(const int64_t block_size);

      /// @fn destructor
      ~ObVarMemPool();

      /// @fn return the number of blocks hold by the pool
      int64_t get_block_num() const;

      /// @fn return the number of blocks in using hold by the pool
      int64_t get_used_block_num() const;

      /// @fn get memory block size
      int64_t get_block_size() const;

      int64_t shrink(const int64_t remain_memory_size);
      virtual void clear();
    private:
      virtual void *malloc_(const int64_t nbyte, const int32_t mod_id=0, int64_t *got_size = NULL);
      virtual void free_(const void *ptr);
    private:
      /// @fn clear all allocated memory
      void clear(bool check_unfreed_mem);
      /// @fn 释放一个内存block
      int64_t recycle_memory_block(ObDLink * &block_it, bool check_unfreed_mem=true);
      /// block means memory buffer allocated from system
      /// item means memory buffer allocated from mempool
      /// @property list of used memory block
      ObDLink     used_mem_block_list_;
      /// @property list of used memory block
      ObDLink     free_mem_block_list_;
      /// @property size of memory block
      int64_t   mem_block_size_;
      /// @property number of used memory block
      int64_t   used_mem_block_num_;
      /// @property number of free memory block
      int64_t   free_mem_block_num_;
      /// @property current block used number of bytes
      int64_t   cur_block_used_nbyte_;
      /// @property current mem block info
      void *cur_block_info_;
    };
  }
}


#endif /* OCEANBASE_SRC_COMMON_OB_MEMORY_POOL_H_ */
