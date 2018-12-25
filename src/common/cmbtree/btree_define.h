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
 *     - Basic stuff used for CMBtree
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_DEFINE_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_DEFINE_H_

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <tblog.h>
#include "btree_default_alloc.h"

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

#define CMBTREE_DEBUG

#define CMBTREE_CHECK
#ifdef CMBTREE_CHECK
#include <execinfo.h>
#define CMBTREE_CHECK_FALSE(cond, format, args...) if (LIKELY(cond)) \
  {char _buffer_stack_[256];{void *array[10];int i, idx=0, n = backtrace(array, 10);\
      for (i = 0; i < n; i++) idx += snprintf(idx+_buffer_stack_, 25, "%p,", array[i]);}\
    TBSYS_LOG(ERROR, "%s" format, _buffer_stack_, ## args);}
#define CMBTREE_CHECK_TRUE(cond, format, args...) CMBTREE_CHECK_FALSE(!(cond), format, ## args)
#else
#define CMBTREE_CHECK_TRUE(cond, format, ...)
#define CMBTREE_CHECK_FALSE(cond, format, ...)
#endif

#define CMBTREE_LOG(level, format, args...) \
  { \
    TBSYS_LOG(level, format, ## args); \
  }

#define CMBTREE_ATOMIC_CAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
#define CMBTREE_ATOMIC_ADD(val, inc) (void)__sync_add_and_fetch((val), (inc))
#define CMBTREE_ATOMIC_SUB(val, dec) (void)__sync_sub_and_fetch((val), (dec))
#define CMBTREE_ATOMIC_INC(val) (void)__sync_add_and_fetch((val), 1)
#define CMBTREE_ATOMIC_DEC(val) (void)__sync_sub_and_fetch((val), 1)
#define CMBTREE_ATOMIC_STORE(ptr, val, model) __atomic_store_n((ptr), (val), (model))

#define CMBTREE_DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);               \
    void operator=(const TypeName&)


namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      /**
       * 常量定义
       */

      enum
      {
        CONST_MAX_DEPTH               = 10,  /// 树最大深度
        CONST_NODE_OBJECT_COUNT       = 31,  /// 每个节点上object数量
        CONST_KEY_MAX_LENGTH          = 1024 /// KEY最大长度
      };

      /**
       * 错误代码定义
       */
      enum
      {
        ERROR_CODE_FAIL               = -1,  /// 错误
        ERROR_CODE_OK                 = 0,   /// 正确
        ERROR_CODE_NOT_FOUND          = 1,   /// KEY不存在
        ERROR_CODE_KEY_REPEAT         = 2,   /// KEY重复了
        ERROR_CODE_TOO_DEPTH          = 3,   /// 树超出最大深度了
        ERROR_CODE_ALLOC_FAIL         = 4,   /// 内存分配失败
        ERROR_CODE_NOT_EXPECTED       = 5,   /// 计划外错误
        ERROR_CODE_NEED_RETRY         = 6,   /// 需要重试
      };

      /**
       * 查找路径函数使用的查找类型
       */
      enum
      {
        FIND_PATH_PUT                 = 1,
        FIND_PATH_GET                 = 2,
        FIND_PATH_FORWARD_SCAN        = 3,
        FIND_PATH_BACKWARD_SCAN       = 4,
        FIND_PATH_MIN                 = 5,
        FIND_PATH_MAX                 = 6
      };

      // 比较函数
      typedef int64_t BtreeKeyCompare(const char *, const char *);

      const int OB_LOCK_NOT_MATCH = -200;
      const int OB_DEAD_LOCK = -201;

      template<class K, class V, class Alloc = BtreeDefaultAlloc>
      class BtreeBase;

      inline void CMBTreeMemoryBarrier()
      {
          __asm__ __volatile__("" : : : "memory");
      }

    }
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_DEFINE_H_
