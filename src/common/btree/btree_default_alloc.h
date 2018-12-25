#ifndef OCEANBASE_COMMON_BTREE_BTREE_DEFAULT_ALLOC_H_
#define OCEANBASE_COMMON_BTREE_BTREE_DEFAULT_ALLOC_H_

#include "btree_define.h"
#include "btree_alloc.h"

namespace oceanbase
{
  namespace common
  {

    /**
     * 分配定长空间, 线程不安全, 用之前需要加锁
     */

    class BtreeDefaultAlloc : public BtreeAlloc
    {
    public:
      /**
       * 构造函数
       */
      BtreeDefaultAlloc();

      /**
       * 析构函数
       */
      ~BtreeDefaultAlloc();

      /**
       * 初始化
       */
      void init(int32_t size);

      /**
       * 分配一个空间
       */
      char *alloc();

      /**
       * 归还一个空间
       */
      void release(const char* pdata);

      /**
       * 释放掉内存
       */
      void destroy();

      /**
       * 得到每个item分配的大小
       */
      int32_t get_alloc_size();

      /**
       * 用了多少块
       */
      int64_t get_use_count();

      /**
       * 有多少块是free的
       */
      int64_t get_free_count();

      /**
       * 分配多少次
       */
      int32_t get_malloc_count();

    private:
      static const int32_t MALLOC_SIZE;

      // buffer
      struct MemBuffer
      {
        char *last;
        char *end;
        struct MemBuffer *next;
      } *buffer_;

      // free list
      char *free_list_;
      // size
      int32_t size_;
      // use count;
      int64_t use_count_;
      // free count
      int64_t free_count_;
      // malloc count
      int32_t malloc_count_;
    };

  } // end namespace common
} // end namespace oceanbase

#endif

