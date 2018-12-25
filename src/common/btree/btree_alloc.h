#ifndef OCEANBASE_COMMON_BTREE_BTREE_ALLOC_H_
#define OCEANBASE_COMMON_BTREE_BTREE_ALLOC_H_

#include "btree_define.h"

namespace oceanbase
{
  namespace common
  {

    /**
     * 分配定长空间, 线程不安全, 用之前需要加锁
     */

    class BtreeAlloc
    {
    public:
      /**
       * 构造函数
       */
      BtreeAlloc() {}

      /**
       * 析构函数
       */
      virtual ~BtreeAlloc() {}

      /**
       * 初始化
       */
      virtual void init(int32_t size) = 0;

      /**
       * 分配一个空间
       */
      virtual char *alloc() = 0;

      /**
       * 归还一个空间
       */
      virtual void release(const char* pdata) = 0;

      /**
       * 释放掉内存
       */
      virtual void destroy() = 0;

      /**
       * 得到每个item分配的大小
       */
      virtual int32_t get_alloc_size() = 0;

      /**
       * 用了多少块
       */
      virtual int64_t get_use_count() = 0;

      /**
       * 有多少块是free的
       */
      virtual int64_t get_free_count() = 0;

      /**
       * 分配多少次
       */
      virtual int32_t get_malloc_count() = 0;
    };

  } // end namespace common
} // end namespace oceanbase

#endif

