#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "btree_default_alloc.h"

namespace oceanbase
{
  namespace common
  {
    const int32_t BtreeDefaultAlloc::MALLOC_SIZE = (1024 * 1024);

    /**
     * 构造函数
     */
    BtreeDefaultAlloc::BtreeDefaultAlloc() : BtreeAlloc()
    {
      buffer_ = NULL;
      free_list_ = NULL;
      use_count_ = 0;
      free_count_ = 0;
      malloc_count_ = 0;
      init(sizeof(int64_t));
      OCEAN_BTREE_CHECK_TRUE(free_count_ >= 0, "xxx");
    }

    /**
     * 析构函数
     */
    BtreeDefaultAlloc::~BtreeDefaultAlloc()
    {
      destroy();
    }

    /**
     * 初始化
     */
    void BtreeDefaultAlloc::init(int32_t size)
    {
      OCEAN_BTREE_CHECK_TRUE(size >= static_cast<int32_t>(sizeof(int64_t)), "ret:%d", size);
      destroy();
      size_ = size;
      // 不能小于8个. 在free_list上会用到
      if (size_ < static_cast<int32_t>(sizeof(int64_t)))
        size_ = sizeof(int64_t);

      buffer_ = NULL;
      free_list_ = NULL;
      use_count_ = 0;
      free_count_ = 0;
      malloc_count_ = 0;
    }

    /**
     * 得到size
     */
    int32_t BtreeDefaultAlloc::get_alloc_size()
    {
      return size_;
    }

    /**
     * 分配一个空间
     */
    char *BtreeDefaultAlloc::alloc()
    {
      char *pret = NULL;
#ifdef OCEAN_BASE_BTREE_USE_SYS_MALLOC
      pret = (char*)malloc(size_);
      //OCEAN_BTREE_CHECK_TRUE(0, "ALLOC: %p", pret);
#else

      // 从free list取
      if (free_list_ != NULL)
      {
        pret = free_list_;
        int64_t *addr = reinterpret_cast<int64_t*>(free_list_);
        free_list_ =  reinterpret_cast<char*>(*addr);
        OCEAN_BTREE_CHECK_TRUE(free_count_ > 0, "free_count_: %d", free_count_);
        free_count_ --;
      }
      else if ((buffer_ != NULL) && ((buffer_->last + size_) <= buffer_->end))
      {
        // 从最后一个buffer上取
        pret = buffer_->last;
        buffer_->last += size_;
      }
      else
      {
        // malloc出一个新块, 每次1m
        malloc_count_ ++;
        // 分配出1m空间
        uint64_t n = BtreeDefaultAlloc::MALLOC_SIZE / size_;
        n = (n < 1 ? 1 : n) * size_ + sizeof(struct MemBuffer);
        char *pdata = reinterpret_cast<char*>(malloc(n));
        OCEAN_BTREE_CHECK_TRUE(pdata, "pdata is null.");
        if (NULL != pdata)
        {
          MemBuffer *mb = reinterpret_cast<MemBuffer*>(pdata);
          mb->last = pdata + sizeof(MemBuffer);
          mb->end = pdata + n;
          mb->next = buffer_;
          buffer_ = mb;
          // 从最后一个buffer上取
          pret = buffer_->last;
          buffer_->last += size_;
        }
      }
#endif
      OCEAN_BTREE_CHECK_TRUE(pret, "pret is null.");

      if (pret) memset(pret, 0, size_);
      use_count_  ++;

      return pret;
    }

    /**
     * 归还一个空间
     */
    void BtreeDefaultAlloc::release(const char* pdata)
    {
      OCEAN_BTREE_CHECK_TRUE(pdata, "pret is null.");
      if (pdata != NULL)
      {
        OCEAN_BTREE_CHECK_TRUE(use_count_ > 0, "use_count_: %d", use_count_);
        use_count_  --;
        free_count_ ++;

#ifdef OCEAN_BASE_BTREE_USE_SYS_MALLOC
        //OCEAN_BTREE_CHECK_TRUE(0, "FREE: %p", pdata);
        free((void*)pdata);
#else
        // 释放掉, 用pdata自身的内存把链串起来
        int64_t *addr = reinterpret_cast<int64_t*>(const_cast<char*>(pdata));
        (*addr) = reinterpret_cast<int64_t>(free_list_);
        free_list_ = const_cast<char*>(pdata);
#endif

      }
    }

    /**
     * 释放掉内存
     */
    void BtreeDefaultAlloc::destroy()
    {
      if (buffer_ != NULL)
      {
        struct MemBuffer *next_mb;
        struct MemBuffer *mb = buffer_;
        while(mb)
        {
          OCEAN_BTREE_CHECK_TRUE(mb, "mb is null");
          next_mb = mb->next;
          free(reinterpret_cast<void*>(mb));

          // 下一块
          mb = next_mb;
        }
        buffer_ = NULL;
        free_list_ = NULL;
        use_count_ = 0;
        free_count_ = 0;
        malloc_count_ = 0;
      }
    }

    /**
     * 用了多少块
     */
    int64_t BtreeDefaultAlloc::get_use_count()
    {
      return use_count_;
    }

    /**
     * 有多少块是free的
     */
    int64_t BtreeDefaultAlloc::get_free_count()
    {
      return free_count_;
    }

    /**
     * 分配多少次
     */
    int32_t BtreeDefaultAlloc::get_malloc_count()
    {
      return malloc_count_;
    }

  } // end namespace common
} // end namespace oceanbase

