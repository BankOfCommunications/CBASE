#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ob_btree_engine_alloc.h"

namespace oceanbase
{
  namespace updateserver
  {
    const int32_t UpsBtreeEngineAlloc::MALLOC_SIZE = (2 * 1024 * 1024);

    /**
     * 构造函数
     */
    UpsBtreeEngineAlloc::UpsBtreeEngineAlloc() : BtreeAlloc()
    {
      buffer_ = NULL;
      free_list_ = NULL;
      use_count_ = 0;
      free_count_ = 0;
      malloc_count_ = 0;
      init(sizeof(int64_t));
    }

    /**
     * 析构函数
     */
    UpsBtreeEngineAlloc::~UpsBtreeEngineAlloc()
    {
      destroy();
    }

    void UpsBtreeEngineAlloc::set_mem_tank(MemTank *mem_tank)
    {
      mem_tank_ = mem_tank;
    }

    /**
     * 初始化
     */
    void UpsBtreeEngineAlloc::init(int32_t size)
    {
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
    int32_t UpsBtreeEngineAlloc::get_alloc_size()
    {
      return size_;
    }

    /**
     * 分配一个空间
     */
    char *UpsBtreeEngineAlloc::alloc()
    {
      char *pret = NULL;
      // 从free list取
      if (free_list_ != NULL)
      {
        pret = free_list_;
        int64_t *addr = reinterpret_cast<int64_t*>(free_list_);
        free_list_ =  reinterpret_cast<char*>(*addr);
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
        // 分配出1m空间
        int32_t n = UpsBtreeEngineAlloc::MALLOC_SIZE / size_;
        n = static_cast<int32_t>((n < 1 ? 1 : n) * size_ + sizeof(struct MemBuffer));
        char *pdata = NULL;
        if (NULL != mem_tank_)
        {
          pdata = reinterpret_cast<char*>(mem_tank_->btree_engine_alloc(n));
        }
        if (NULL != pdata)
        {
          malloc_count_ ++;
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
      if (NULL != pret)
      {
        memset(pret, 0, size_);
      }
      use_count_  ++;

      return pret;
    }

    /**
     * 归还一个空间
     */
    void UpsBtreeEngineAlloc::release(const char* pdata)
    {
      if (pdata != NULL)
      {
        use_count_  --;
        free_count_ ++;

        // 释放掉, 用pdata自身的内存把链串起来
        int64_t *addr = reinterpret_cast<int64_t*>(const_cast<char*>(pdata));
        (*addr) = reinterpret_cast<int64_t>(free_list_);
        free_list_ = const_cast<char*>(pdata);
      }
    }

    /**
     * 释放掉内存
     */
    void UpsBtreeEngineAlloc::destroy()
    {
      if (buffer_ != NULL)
      {
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
    int64_t UpsBtreeEngineAlloc::get_use_count()
    {
      return use_count_;
    }

    /**
     * 有多少块是free的
     */
    int64_t UpsBtreeEngineAlloc::get_free_count()
    {
      return free_count_;
    }

    /**
     * 分配多少次
     */
    int32_t UpsBtreeEngineAlloc::get_malloc_count()
    {
      return malloc_count_;
    }

  } // end namespace updateserver
} // end namespace oceanbase

