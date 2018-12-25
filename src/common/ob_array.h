/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_array.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ARRAY_H
#define _OB_ARRAY_H 1
#include "ob_malloc.h"
#include "page_arena.h"         // for ModulePageAllocator
#include "utility.h"
#include "ob_iarray.h"
namespace oceanbase
{
  namespace common
  {
    template<typename T>
    class ObArrayDefaultCallBack
    {
    public:
      void operator () (T*ptr)
      {
        UNUSED(ptr);
      }
    };

    template<typename T>
    class ObArrayExpressionCallBack
    {
    public:
      void operator () (T*ptr)
      {
        ptr->reset();
      }
    };

    template<typename T, typename BlockAllocatorT = ModulePageAllocator, typename CallBack = ObArrayDefaultCallBack<T> >
    class ObArray : public ObIArray<T>
    {
      public:
        ObArray(int64_t block_size = 64*1024, const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_COMMON_ARRAY));
        virtual ~ObArray();

        int push_back(const T &obj);
        void pop_back();
        int pop_back(T &obj);

        int remove(int64_t idx);

        int at(int64_t idx, T &obj) const;
        T& at(int64_t idx);     // dangerous
        const T& at(int64_t idx) const; // dangerous

        int64_t count() const;
        void clear();
        void reserve(int64_t capacity);
        int64_t to_string(char* buffer, int64_t length) const;
        int64_t get_data_size() const {return data_size_;};
        bool error() const {return error_ != 0;};
        // deep copy
        ObArray(const ObArray &other);
        ObArray& operator=(const ObArray &other);
      private:
        void extend_buf();
        void extend_buf(int64_t new_size);
        int64_t get_capacity() const;
        void destroy();
      private:
        // data members
        T* data_;
        int64_t count_;
        int64_t data_size_;
        int64_t block_size_;
        int32_t error_;
        int32_t reserve_;
        BlockAllocatorT block_allocator_;
    };

    template<typename T, typename BlockAllocatorT, typename CallBack>
    ObArray<T, BlockAllocatorT, CallBack>::ObArray(int64_t block_size, const BlockAllocatorT &alloc)
      :data_(NULL), count_(0),
       data_size_(0), block_size_(block_size),
       error_(0), reserve_(0),
       block_allocator_(alloc)
    {
      block_size_ = std::max(static_cast<int64_t>(sizeof(T)), block_size);
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    ObArray<T, BlockAllocatorT, CallBack>::~ObArray()
    {
      destroy();
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    int ObArray<T, BlockAllocatorT, CallBack>::at(int64_t idx, T &obj) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(error_))
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "array in error state");
      }
      else if (OB_UNLIKELY(0 > idx || idx >= count_))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else
      {
        obj = data_[idx];
      }
      return ret;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    T& ObArray<T, BlockAllocatorT, CallBack>::at(int64_t idx)
    {
      OB_ASSERT(0 <= idx && idx < count_ && 0 == error_);
      return data_[idx];
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    const T& ObArray<T, BlockAllocatorT, CallBack>::at(int64_t idx) const
    {
      OB_ASSERT(0 <= idx && idx < count_ && 0 == error_);
      return data_[idx];
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    void ObArray<T, BlockAllocatorT, CallBack>::extend_buf(int64_t new_size)
    {
      OB_ASSERT(new_size > data_size_);
      T* new_data = (T*)block_allocator_.alloc(new_size);
      if (NULL != new_data)
      {
        int64_t max_obj_count = new_size/(int64_t)sizeof(T);
        new_data = new(new_data) T[max_obj_count];
        if (NULL != data_)
        {
          int64_t old_max_obj_count = data_size_/(int64_t)sizeof(T);
          OB_ASSERT(count_ <= old_max_obj_count);
          for (int64_t i = 0; i < old_max_obj_count; ++i)
          {
            if (i < count_)
            {
              // copy object
              new_data[i] = data_[i];
            }
            // destruct old objects
            data_[i].~T();
          }
          block_allocator_.free(data_);
        }
        if (0 < data_size_)
        {
          TBSYS_LOG(DEBUG, "array extend buf, old_size=%ld new_size=%ld old_ptr=%p new_ptr=%p "
                    "count=%ld obj_size=%ld block_size=%ld",
                    data_size_, new_size, data_, new_data,
                    count_, static_cast<int64_t>(sizeof(T)), block_size_);
        }
        data_size_ = new_size;
        data_ = new_data;
      }
      else
      {
        TBSYS_LOG(ERROR, "no memory");
      }
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    void ObArray<T, BlockAllocatorT, CallBack>::extend_buf()
    {
      int64_t new_size = data_size_ + block_size_;
      extend_buf(new_size);
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    void ObArray<T, BlockAllocatorT, CallBack>::reserve(int64_t capacity)
    {
      if (capacity > data_size_/(int64_t)sizeof(T))
      {
        int64_t new_size = capacity * sizeof(T);
        int64_t plus = new_size % block_size_;
        new_size += (0 == plus) ? 0 : (block_size_ - plus);
        extend_buf(new_size);
      }
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    int64_t ObArray<T, BlockAllocatorT, CallBack>::to_string(char *buffer, int64_t length) const
    {
      int64_t pos = 0;
      for (int64_t index = 0; index < count_; ++index)
      {
        databuff_printf(buffer, length, pos, "<%ld:", index);
        databuff_print_obj(buffer, length, pos, at(index));
        databuff_printf(buffer, length, pos, "> ");
      }
      return pos;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    int64_t ObArray<T, BlockAllocatorT, CallBack>::get_capacity() const
    {
      return data_size_/static_cast<int64_t>(sizeof(T));
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    int ObArray<T, BlockAllocatorT, CallBack>::push_back(const T &obj)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(error_))
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "array in error state");
      }
      else if (count_ >= data_size_/(int64_t)sizeof(T))
      {
        extend_buf();
      }
      if (OB_SUCCESS == ret && (count_ < data_size_/(int64_t)sizeof(T)))
      {
        data_[count_++] = obj;
      }
      else
      {
        TBSYS_LOG(WARN, "count_=%ld, data_size_=%ld, (int64_t)sizeof(T)=%ld, data_size_/(int64_t)sizeof(T)=%ld, ret=%d",
            count_, data_size_, static_cast<int64_t>(sizeof(T)), data_size_/static_cast<int64_t>(sizeof(T)), ret);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      return ret;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    void ObArray<T, BlockAllocatorT, CallBack>::pop_back()
    {
      if (0 < count_)
      {
        --count_;
      }
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    int ObArray<T, BlockAllocatorT, CallBack>::pop_back(T &obj)
    {
      int ret = OB_ENTRY_NOT_EXIST;
      if (0 < count_)
      {
        obj = data_[count_-1];
        --count_;
        ret = OB_SUCCESS;
      }
      return ret;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    int ObArray<T, BlockAllocatorT, CallBack>::remove(int64_t idx)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(0 > idx || idx >= count_))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else
      {
        for (int64_t i = idx; i < count_ - 1; ++i)
        {
          data_[i] = data_[i+1];
        }
        --count_;
      }
      return ret;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    int64_t ObArray<T, BlockAllocatorT, CallBack>::count() const
    {
      return count_;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    void ObArray<T, BlockAllocatorT, CallBack>::clear()
    {
      if (data_size_ <= block_size_)
      {
        CallBack cb;
        for (int64_t index = 0; index < count_; ++index)
        {
          T *obj = &data_[index];
          cb(obj);
        }
        count_ = 0;
        error_ = 0;
      }
      else
      {
        destroy();
      }
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    ObArray<T, BlockAllocatorT, CallBack>::ObArray(const ObArray<T, BlockAllocatorT, CallBack> &other)
      : ObIArray<T>()
    {
      *this = other;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    ObArray<T, BlockAllocatorT, CallBack>& ObArray<T, BlockAllocatorT, CallBack>::operator=(const ObArray<T, BlockAllocatorT, CallBack> &other)
    {
      if (this != &other)
      {
        this->clear();
        this->reserve(other.count());
        if (static_cast<uint64_t>(data_size_) < (sizeof(T)*other.count_) )
        {
          TBSYS_LOG(ERROR, "no memory");
          error_ = 1;
        }
        else
        {
          // copy objects
          for (int64_t i = 0; i < other.count_; ++i)
          {
            data_[i] = other.data_[i];
          }
          count_ = other.count_;
        }
      }
      return *this;
    }

    template<typename T, typename BlockAllocatorT, typename CallBack>
    void ObArray<T, BlockAllocatorT, CallBack>::destroy()
    {
      if (NULL != data_)
      {
        int64_t max_obj_count = data_size_/(int64_t)sizeof(T);
        for (int i = 0; i < max_obj_count; ++i)
        {
          data_[i].~T();
        }
        block_allocator_.free(data_);
        data_ = NULL;
      }
      count_ = data_size_ = 0;
      error_ = 0;
      reserve_ = 0;
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ARRAY_H */
