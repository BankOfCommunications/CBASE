/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_se_array.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SE_ARRAY_H
#define _OB_SE_ARRAY_H 1
#include "ob_array.h"
#include "page_arena.h"         // for ModulePageAllocator
#include "ob_iarray.h"
namespace oceanbase
{
  namespace common
  {
    template <typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT = ModulePageAllocator, typename CallBack = ObArrayDefaultCallBack<T> >
    class ObSEArray : public ObIArray<T>
    {
      public:
        ObSEArray(int64_t block_size = 64*1024, const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_SE_ARRAY));
        virtual ~ObSEArray();

        // deep copy
        ObSEArray(const ObSEArray &other);
        ObSEArray& operator=(const ObSEArray &other);
        ObSEArray& operator=(const ObIArray<T> &other);

        int push_back(const T &obj);
        void pop_back();
        int pop_back(T &obj);

        int remove(int64_t idx);

        int at(int64_t idx, T &obj) const;
        T& at(int64_t idx);     // dangerous
        const T& at(int64_t idx) const; // dangerous
        const T& operator[] (int64_t idx) const; // dangerous

        int64_t count() const;
        void clear();
        void reset();
        void reserve(int64_t capacity);
        int64_t to_string(char* buffer, int64_t length) const;
        int64_t get_data_size() const;
        bool error() const{return array_.error();};
      public:
        class iterator
        {
          friend class ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>;
          public:
          typedef T value_type;
          typedef int64_t difference_type;
          typedef T* pointer;
          typedef T& reference;
          typedef std::random_access_iterator_tag iterator_category;

          public:
          explicit iterator(ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT> *arr, int64_t index)
          {
            arr_ = arr;
            index_ = index;
          }
          T& operator*()
          {
            OB_ASSERT(arr_ != NULL);
            return arr_->at(index_);
          }
          iterator operator++(int)// iterator++
          {
            OB_ASSERT(arr_ != NULL);
            return iterator(arr_, index_++);
          }
          iterator operator++()
          {
            OB_ASSERT(arr_ != NULL);
            index_++;
            return *this;
          }
          iterator operator--(int)
          {
            OB_ASSERT(arr_ != NULL);
            return iterator(arr_, index_--);
          }
          iterator operator--()
          {
            OB_ASSERT(arr_ != NULL);
            index_--;
            return *this;
          }
          iterator operator+(int64_t off)
          {
            OB_ASSERT(arr_ != NULL);
            return iterator(arr_, index_ + off);
          }
          difference_type operator-(const iterator &rhs)
          {
            OB_ASSERT(arr_ == rhs.arr_);
            return index_ - rhs.index_;
          }
          iterator operator-(int64_t index)
          {
            OB_ASSERT(arr_ != NULL);
            return iterator(arr_, this->index_ - index);
          }
          bool operator==(const iterator &rhs) const
          {
            OB_ASSERT(arr_ == rhs.arr_);
            return (this->index_ == rhs.index_);
          }
          bool operator!=(const iterator &rhs) const
          {
            OB_ASSERT(arr_ == rhs.arr_);
            return (this->index_ != rhs.index_);
          }
          bool operator<(const iterator &rhs) const
          {
            OB_ASSERT(arr_ == rhs.arr_);
            return (index_ < rhs.index_);
          }
          private:
          ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack> *arr_;
          int64_t index_;
        };
        iterator begin()
        {
          return iterator(this, 0);
        }
        iterator end()
        {
          return iterator(this, count_);
        }
      private:
        // types and constants
      private:
        // function members
        int move_from_local();
        void move_to_local();
      private:
        // data members
        T *ptr_;                // optimize for read
        T local_data_[LOCAL_ARRAY_SIZE];
        int64_t count_;
        ObArray<T, BlockAllocatorT, CallBack> array_;
    };

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::ObSEArray(int64_t block_size, const BlockAllocatorT &alloc)
      : ObIArray<T>(),
        ptr_(local_data_),
        count_(0),
        array_(block_size, alloc)
    {
      OB_ASSERT(LOCAL_ARRAY_SIZE > 0);
    }
    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::~ObSEArray()
    {
      clear();
    }


    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::move_from_local()
    {
      int ret = OB_SUCCESS;
      OB_ASSERT(count_ == LOCAL_ARRAY_SIZE);
      OB_ASSERT(0 == array_.count());
      array_.reserve(count_);
      for (int64_t i = 0; i < count_; ++i)
      {
        if (OB_SUCCESS != (ret = array_.push_back(local_data_[i])))
        {
          break;
        }
      }
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        ptr_ = &array_.at(0);
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::push_back(const T &obj)
    {
      int ret = OB_SUCCESS;
      if (count_ < LOCAL_ARRAY_SIZE)
      {
        local_data_[count_++] = obj;
      }
      else
      {
        if (OB_UNLIKELY(count_ == LOCAL_ARRAY_SIZE))
        {
          ret = move_from_local();
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          ret = array_.push_back(obj);
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          ptr_ = &array_.at(0);
          ++count_;
        }
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::move_to_local()
    {
      OB_ASSERT(array_.count() == LOCAL_ARRAY_SIZE);
      for (int64_t i = 0; i < LOCAL_ARRAY_SIZE; ++i)
      {
        local_data_[i] = array_.at(i);
      }
      ptr_ = local_data_;
      array_.clear();
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::pop_back()
    {
      if (OB_UNLIKELY(count_ <= 0))
      {
      }
      else if (count_ <= LOCAL_ARRAY_SIZE)
      {
        --count_;
      }
      else
      {
        array_.pop_back();
        if (array_.count() == LOCAL_ARRAY_SIZE)
        {
          move_to_local();
        }
        --count_;
      }
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::pop_back(T &obj)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(count_ <= 0))
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (count_ <= LOCAL_ARRAY_SIZE)
      {
        obj = local_data_[--count_];
      }
      else
      {
        ret = array_.pop_back(obj);
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          if (array_.count() == LOCAL_ARRAY_SIZE)
          {
            move_to_local();
          }
          --count_;
        }
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::remove(int64_t idx)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(0 > idx || idx >= count_))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else if (count_ <= LOCAL_ARRAY_SIZE)
      {
        for (int64_t i = idx; i < count_ - 1; ++i)
        {
          local_data_[i] = local_data_[i+1];
        }
        --count_;
      }
      else
      {
        ret = array_.remove(idx);
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          if (array_.count() == LOCAL_ARRAY_SIZE)
          {
            move_to_local();
          }
          --count_;
        }
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::at(int64_t idx, T &obj) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(0 > idx || idx >= count_))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else
      {
        obj = ptr_[idx];
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    T& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::at(int64_t idx)
    {
      OB_ASSERT(0 <= idx && idx < count_);
      return ptr_[idx];
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    const T& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::at(int64_t idx) const
    {
      OB_ASSERT(0 <= idx && idx < count_);
      return ptr_[idx];
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    const T& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::operator[] (int64_t idx) const
    {
      OB_ASSERT(0 <= idx && idx < count_);
      return ptr_[idx];
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int64_t ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::count() const
    {
      return count_;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int64_t ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::get_data_size() const
    {
      return (count_ <= LOCAL_ARRAY_SIZE)?(LOCAL_ARRAY_SIZE*sizeof(T)):(array_.get_data_size());
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::clear()
    {
      //reset all datamember
      int64_t count = array_.count();
      CallBack cb;
      for (int64_t i = 0; i<count; ++i)
      {
        cb(&array_.at(i));
      }
      if (count_ > LOCAL_ARRAY_SIZE)
      {
        array_.clear();
        ptr_ = local_data_;
      }
      count_ = 0;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::reset()
    {
      clear();
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::reserve(int64_t capacity)
    {
      if (capacity > LOCAL_ARRAY_SIZE)
      {
        array_.reserve(capacity);
      }
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    int64_t ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::to_string(char* buffer, int64_t length) const
    {
      int64_t pos = 0;
      for (int64_t i = 0; i < count_; ++i)
      {
        databuff_printf(buffer, length, pos, "<%ld:%s> ", i, to_cstring(ptr_[i]));
      }
      return pos;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::operator=(const ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack> &other)
    {
      if (OB_LIKELY(this != &other))
      {
        this->clear();
        if (other.count_ <= LOCAL_ARRAY_SIZE)
        {
          for (int64_t i = 0; i < other.count_; ++i)
          {
            local_data_[i] = other.local_data_[i];
          }
          this->count_ = other.count_;
        }
        else
        {
          this->array_ = other.array_;
          if (this->array_.error())
          {
            TBSYS_LOG(WARN, "error when assign");
          }
          else
          {
            ptr_ = &array_.at(0);
            this->count_ = other.count_;
          }
        }
      }
      return *this;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::operator=(const ObIArray<T> &other)
    {
      int err = OB_SUCCESS;
      if (OB_LIKELY(this != &other))
      {
        this->clear();
        for (int64_t i = 0; OB_SUCCESS == err && i < other.count(); ++i)
        {
          err = this->push_back(other.at(i));
        }
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "ObSEArray assign error, err = %d", err);
        }
      }
      return *this;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack>::ObSEArray(const ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack> &other)
      :ptr_(local_data_),
       count_(0),
       array_(other.array_.block_size_, other.array_.block_allocator_)
    {
      *this = other;
    }
    /*
    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
      ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::iterator ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::begin()
      {
        return iterator(this, 0);
      }
    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
      ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::iterator ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::end()
      {
        return iterator(this, count_);
      }
      */
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_SE_ARRAY_H */
