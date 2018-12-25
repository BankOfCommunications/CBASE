/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_malloc.cpp is for what ...
 *
 * Version: $id: ob_malloc.cpp,v 0.1 8/19/2010 9:57a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_malloc.h"
#include <errno.h>
#include "ob_memory_pool.h"
#include "ob_mod_define.h"
#include "ob_thread_mempool.h"
#include "ob_tsi_factory.h"
#include "utility.h"
#include "ob_tsi_block_allocator.h"
#include <algorithm>
#ifdef __OB_MTRACE__
#include <execinfo.h>
#endif

namespace oceanbase
{
  namespace common
  {
    ObTSIBlockAllocator& get_global_tsi_block_allocator()
    {
      static ObTSIBlockAllocator tsi_block_allocator;
      return tsi_block_allocator;
    }
  }
}
namespace
{
  /// @warning 这里使用了gcc的扩展属性，以及一些不常使用的语言特征，目的是解决
  ///          static变量的析构顺序问题，如果把全局的内存池申明为static对象，那么
  ///          它可能在使用他的其他static对象析构前就被析构了，这样程序在退出的
  ///          时候就会出core

  /// @note 提前在数据区分配内存，这样就不需要处理new调用失败的情况
  /// @property 全局的内存池对象的存放空间
  char g_fixed_memory_pool_buffer[sizeof(oceanbase::common::ObFixedMemPool)];
  char g_mod_set_buffer[sizeof(oceanbase::common::ObModSet)];

  /// @note 为了解决析构顺序的问题，必须使用指针
  /// @property global memory pool
  oceanbase::common::ObFixedMemPool *g_memory_pool = NULL;
  oceanbase::common::ObModSet *g_mod_set = NULL;

  /// @note 在main函数执行前初始化全局的内存池
  /// @fn initialize global memory pool
  void  __attribute__((constructor)) init_global_memory_pool()
  {
    g_memory_pool = new(g_fixed_memory_pool_buffer)oceanbase::common::ObFixedMemPool;
    g_mod_set = new (g_mod_set_buffer) oceanbase::common::ObModSet;
    oceanbase::common::thread_mempool_init();
    oceanbase::common::tsi_factory_init();
  }

  /// @note 在main函数退出后析构全局内存池
  /// @fn deinitialize global memory pool
  void  __attribute__((destructor)) deinit_global_memory_pool()
  {
    oceanbase::common::tsi_factory_destroy();
    oceanbase::common::thread_mempool_destroy();
    g_memory_pool->~ObBaseMemPool();
    g_mod_set->~ObModSet();
    g_memory_pool = NULL;
    g_mod_set = NULL;
  }

  /// @fn get global memory pool
  oceanbase::common::ObFixedMemPool & get_fixed_memory_pool_instance()
  {
    return *g_memory_pool;
  }
}

namespace oceanbase
{
  namespace common
  {
    void *ob_tc_malloc(const int64_t nbyte, const int32_t mod_id)
    {
      return get_global_tsi_block_allocator().mod_alloc(nbyte, mod_id);
    }

    void ob_tc_free(void *ptr, const int32_t mod_id)
    {
      get_global_tsi_block_allocator().mod_free(ptr, mod_id);
    }

    void *ob_tc_realloc(void* ptr, const int64_t nbyte, const int32_t mod_id)
    {
      return get_global_tsi_block_allocator().mod_realloc(ptr, nbyte, mod_id);
    }
  }
}


int oceanbase::common::ob_init_memory_pool(int64_t block_size)
{
  int ret = OB_SUCCESS;
  static ObMalloc sys_allocator;
  sys_allocator.set_mod_id(ObModIds::OB_MOD_TC_TOTAL);
  if (OB_SUCCESS != (ret = get_fixed_memory_pool_instance().init(block_size,1, g_mod_set)))
  {
    TBSYS_LOG(ERROR, "fixed_memory_pool.init(block_size=%ld)=>%d", block_size, ret);
  }
  else if (OB_SUCCESS != (ret = get_global_tsi_block_allocator().init(&sys_allocator)))
  {
    TBSYS_LOG(ERROR, "tsi_block_allocator.init()=>%d", ret);
  }
  else
  {
    get_global_tsi_block_allocator().set_mod_id(ObModIds::OB_MOD_TCDEFAULT);
  }
  return ret;
}

void oceanbase::common::ob_mod_usage_update(const int64_t delta, const int32_t mod_id)
{
  get_fixed_memory_pool_instance().mod_usage_update(delta, mod_id);
}

void __attribute__((weak)) memory_limit_callback()
{
  TBSYS_LOG(DEBUG, "common memory_limit_callback");
}

void * oceanbase::common::ob_malloc(void *ptr, size_t size)
{
  if (size)
  {
    if (NULL != ptr)
    {
      ob_free(ptr, ObModIds::OB_COMMON_NETWORK);
      ptr = NULL;
    }
    ptr = get_fixed_memory_pool_instance().malloc(size, ObModIds::OB_COMMON_NETWORK, NULL);
    if (OB_UNLIKELY(NULL == ptr && ENOMEM == errno))
    {
      if (REACH_TIME_INTERVAL(60L * 1000000L))
      {
        ob_print_mod_memory_usage();
      }
      memory_limit_callback();
    }
    else
    {
      memset(ptr, 0, size);
    }
  }
  else if (ptr)
  {
    ob_free(ptr, ObModIds::OB_COMMON_NETWORK);
    return 0;
  }
  return ptr;
}

static int64_t MON_BLOCK_SIZE_ARRAY[] = {
  1 << 8,
  1 << 9,
  1 << 10,
  1 << 13,
  1 << 16,
  1 << 17,
  1 << 20,
  1 << 21,
  1 << 22,
  INT64_MAX
};

static int64_t MON_BLOCK_ARRAY[ARRAYSIZEOF(MON_BLOCK_SIZE_ARRAY)] = {0};

void * oceanbase::common::ob_malloc(const int64_t nbyte, const int32_t mod_id, int64_t *got_size)
{
  void *result = NULL;
  if (nbyte <= 0)
  {
    TBSYS_LOG(WARN, "cann't allocate memory less than 0 byte [nbyte:%ld]",nbyte);
    errno = EINVAL;
  }
  else
  {
    result = get_fixed_memory_pool_instance().malloc(nbyte,mod_id, got_size);
    if (OB_UNLIKELY(NULL == result && ENOMEM == errno))
    {
      if (REACH_TIME_INTERVAL(60L * 1000000L))
      {
        ob_print_mod_memory_usage();
      }
      memory_limit_callback();
    }
    int64_t *p = std::lower_bound(MON_BLOCK_SIZE_ARRAY, MON_BLOCK_SIZE_ARRAY+ARRAYSIZEOF(MON_BLOCK_SIZE_ARRAY), nbyte);
    OB_ASSERT(p >= MON_BLOCK_SIZE_ARRAY && p < MON_BLOCK_SIZE_ARRAY+ARRAYSIZEOF(MON_BLOCK_SIZE_ARRAY));
    MON_BLOCK_ARRAY[p-MON_BLOCK_SIZE_ARRAY] ++;
#ifdef __OB_MTRACE__
    BACKTRACE(
        WARN,
        (nbyte > OB_MALLOC_BLOCK_SIZE && nbyte < OB_MAX_PACKET_LENGTH),
        "ob_malloc  addr=%p nbyte=%ld", result, nbyte
        );
#endif
  }
  return  result;
}

void * oceanbase::common::ob_malloc_emergency(const int64_t nbyte, const int32_t mod_id, int64_t *got_size)
{
  void *result = NULL;
  if (nbyte <= 0)
  {
    TBSYS_LOG(WARN, "cann't allocate memory less than 0 byte [nbyte:%ld]",nbyte);
    errno = EINVAL;
  }
  else
  {
    result = get_fixed_memory_pool_instance().malloc_emergency(nbyte,mod_id, got_size);
  }
  return  result;
}


void oceanbase::common::ob_free(void *ptr, const int32_t mod_id)
{
  UNUSED(mod_id);
  get_fixed_memory_pool_instance().free(ptr);
}

void oceanbase::common::ob_safe_free(void *&ptr, const int32_t mod_id)
{
  UNUSED(mod_id);
  get_fixed_memory_pool_instance().free(ptr);
  ptr = NULL;
}

void oceanbase::common::ob_print_mod_memory_usage(bool print_to_std)
{
  g_memory_pool->print_mod_memory_usage(print_to_std);
  TBSYS_LOG(INFO, "malloc size distribution\n%s", to_cstring(g_malloc_size_stat));
  for (uint32_t i = 0; i < ARRAYSIZEOF(MON_BLOCK_SIZE_ARRAY); ++i)
  {
    TBSYS_LOG(INFO, "[BLOCK_STAT] size=%ld times=%ld", MON_BLOCK_SIZE_ARRAY[i], MON_BLOCK_ARRAY[i]);
  } // end for 
}

int64_t oceanbase::common::ob_get_mod_memory_usage(int32_t mod_id)
{
  return g_memory_pool->get_mod_memory_usage(mod_id);
}

int64_t oceanbase::common::ob_get_memory_size_direct_allocated()
{
  return g_memory_pool->get_memory_size_direct_allocated();
}

int64_t oceanbase::common::ob_set_memory_size_limit(const int64_t mem_size_limit)
{
  return get_fixed_memory_pool_instance().set_memory_size_limit(mem_size_limit);
}

int64_t oceanbase::common::ob_get_memory_size_limit()
{
  return get_fixed_memory_pool_instance().get_memory_size_limit();
}

int64_t oceanbase::common::ob_get_memory_size_handled()
{
  return get_fixed_memory_pool_instance().get_memory_size_handled();
}

int64_t oceanbase::common::ob_get_memory_size_used()
{
  oceanbase::common::ObFixedMemPool & fixed_pool = get_fixed_memory_pool_instance();
  return fixed_pool.get_memory_size_handled()
         - (fixed_pool.get_block_size() * fixed_pool.get_free_block_num());
}

oceanbase::common::ObMemBuffer::ObMemBuffer()
{
  buf_size_ = 0;
  buf_ptr_ = NULL;
}

oceanbase::common::ObMemBuffer::ObMemBuffer(const int64_t nbyte)
{
  buf_size_ = 0;
  buf_ptr_ = NULL;
  buf_ptr_ = ob_malloc(nbyte, ObModIds::OB_MEM_BUFFER);
  if (buf_ptr_ != NULL)
  {
    buf_size_ = nbyte;
  }
}

oceanbase::common::ObMemBuffer::~ObMemBuffer()
{
  ob_free(buf_ptr_,mod_id_);
  buf_size_ = 0;
  buf_ptr_ = NULL;
}

void *oceanbase::common::ObMemBuffer::malloc(const int64_t nbyte, const int32_t mod_id)
{
  void *result = NULL;
  if (nbyte <= buf_size_ && buf_ptr_ != NULL)
  {
    result = buf_ptr_;
  }
  else
  {
    ob_free(buf_ptr_,mod_id_);
    buf_size_ = 0;
    buf_ptr_ = NULL;
    result = ob_malloc(nbyte,mod_id);
    if (result != NULL)
    {
      mod_id_ = mod_id;
      buf_size_ = nbyte;
      buf_ptr_ = result;
    }
  }
  return result;
}

void * oceanbase::common::ObMemBuffer::get_buffer()
{
  return buf_ptr_;
}

int64_t oceanbase::common::ObMemBuffer::get_buffer_size()
{
  return buf_size_;
}

int oceanbase::common::ObMemBuf::ensure_space(const int64_t size, const int32_t mod_id)
{
  int ret         = OB_SUCCESS;
  char* new_buf   = NULL;
  int64_t buf_len = size > buf_size_ ? size : buf_size_;

  if (size <= 0 || (NULL != buf_ptr_ && buf_size_ <= 0))
  {
    TBSYS_LOG(WARN, "invalid param, size=%ld, buf_ptr_=%p, "
                    "buf_size_=%ld",
              size, buf_ptr_, buf_size_);
    ret = OB_ERROR;
  }
  else if (NULL == buf_ptr_ || (NULL != buf_ptr_ && size > buf_size_))
  {
    new_buf = static_cast<char*>(ob_malloc(buf_len, mod_id));
    if (NULL == new_buf)
    {
      TBSYS_LOG(ERROR, "Problem allocate memory for buffer");
      ret = OB_ERROR;
    }
    else
    {
      if (NULL != buf_ptr_)
      {
        ob_free(buf_ptr_, mod_id_);
        buf_ptr_ = NULL;
      }
      buf_size_ = buf_len;
      buf_ptr_ = new_buf;
      mod_id_ = mod_id;
    }
  }

  return ret;
}
