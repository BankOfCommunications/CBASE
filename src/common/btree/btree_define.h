#ifndef OCEANBASE_COMMON_BTREE_BTREE_DEFINE_H_
#define OCEANBASE_COMMON_BTREE_BTREE_DEFINE_H_

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <tblog.h>

//#define OCEAN_BTREE_CHECK
#ifdef OCEAN_BTREE_CHECK
#include <execinfo.h>
#define OCEAN_BTREE_CHECK_FALSE(cond, format, args...) if ((cond)) \
  {char _buffer_stack_[256];{void *array[10];int i, idx=0, n = backtrace(array, 10);\
      for (i = 0; i < n; i++) idx += snprintf(idx+_buffer_stack_, 25, "%p,", array[i]);}\
    TBSYS_LOG(ERROR, "%s" format, _buffer_stack_, ## args);}
#define OCEAN_BTREE_CHECK_TRUE(cond, format, args...) OCEAN_BTREE_CHECK_FALSE(!(cond), format, ## args)
#else
#define OCEAN_BTREE_CHECK_TRUE(cond, format, ...)
#define OCEAN_BTREE_CHECK_FALSE(cond, format, ...)
#endif

namespace oceanbase
{
  namespace common
  {
    /**
     * 常量定义
     */

    enum
    {
      // 树最大深度
      CONST_MAX_DEPTH = 10,
      // BTree内部最多子树数量
      CONST_MAX_BTREE_COUNT = 2,
      // 每个节点上object数量
      CONST_NODE_OBJECT_COUNT = 31,
      // KEY最大长度
      CONST_KEY_MAX_LENGTH = 1024
    };

    /**
     * 错误代码定义
     */
    enum
    {
      // 错误
      ERROR_CODE_FAIL = -1,
      // 正确
      ERROR_CODE_OK = 0,
      // KEY不存在
      ERROR_CODE_NOT_FOUND = 1,
      // KEY重复了
      ERROR_CODE_KEY_REPEAT = 2,
      // 树超出最大深度了
      ERROR_CODE_TOO_DEPTH = 3,
      // 内存分配失败
      ERROR_CODE_ALLOC_FAIL = 4,
    };

    /**
     * Key-Value对基本结构定义
     */
    struct BtreeKeyValuePair
    {
      char *key_;
      char *value_;
    };

    // 比较函数
    typedef int64_t BtreeKeyCompare(const char *, const char *);

  } // end namespace common
} // end namespace oceanbase

#endif
