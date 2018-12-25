#ifndef OCEANBASE_UPDATESERVER_BTREE_ENGINE_ALLOC_H_
#define OCEANBASE_UPDATESERVER_BTREE_ENGINE_ALLOC_H_

#include "common/btree/btree_alloc.h"
#include "ob_memtank.h"

namespace oceanbase
{
  namespace updateserver
  {

    /**
     * 分配定长空间, 线程不安全, 用之前需要加锁
     */

    class UpsBtreeEngineAlloc : public common::BtreeAlloc
    {
    public:
      /**
       * 构造函数
       */
      UpsBtreeEngineAlloc();

      /**
       * 析构函数
       */
      ~UpsBtreeEngineAlloc();

      void set_mem_tank(MemTank *mem_tank);

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

      MemTank *mem_tank_;
    };

  } // end namespace updatesever
} // end namespace oceanbase

#endif //OCEANBASE_UPDATESERVER_BTREE_ENGINE_ALLOC_H_

