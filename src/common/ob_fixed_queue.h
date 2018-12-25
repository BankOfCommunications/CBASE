////===================================================================
 //
 // ob_fixed_queue.h common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2012-03-04 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // 无锁的环形队列
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_FIXED_QUEUE_H_
#define  OCEANBASE_COMMON_FIXED_QUEUE_H_
#include "common/ob_define.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    template <typename T>
    class ObFixedQueue
    {
      struct ArrayItem
      {
        volatile uint64_t cur_pos;
        T *volatile data;
      };
      static const int64_t ARRAY_BLOCK_SIZE = 128L * 1024L;
      public:
        ObFixedQueue();
        ~ObFixedQueue();
      public:
        int init(const int64_t max_num);
        void destroy();
      public:
        int push(T *ptr);
        int pop(T *&ptr);
        inline int64_t get_total() const;
        inline int64_t get_free() const;
        bool inited() const {return inited_;};
      private:
        inline int64_t get_total_(const uint64_t consumer, const uint64_t producer) const;
        inline int64_t get_free_(const uint64_t consumer, const uint64_t producer) const;
      private:
        bool inited_;
        int64_t max_num_;
        ArrayItem *array_;
        volatile uint64_t consumer_ CACHE_ALIGNED;
        volatile uint64_t producer_ CACHE_ALIGNED;
    } __attribute__ ((aligned (64)));

    template <typename T>
    ObFixedQueue<T>::ObFixedQueue() : inited_(false),
                                      max_num_(0),
                                      array_(NULL),
                                      consumer_(0),
                                      producer_(0)
    {
    }

    template <typename T>
    ObFixedQueue<T>::~ObFixedQueue()
    {
      destroy();
    }

    template <typename T>
    int ObFixedQueue<T>::init(const int64_t max_num)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= max_num)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (array_ = (ArrayItem*)ob_malloc(sizeof(ArrayItem) * max_num, ObModIds::OB_FIXED_QUEUE)))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memset(array_, 0, sizeof(ArrayItem) * max_num);
        // prevent initial position of consumer equal to array_[0].cur_pos
        array_[0].cur_pos = UINT64_MAX;
        max_num_ = max_num;
        consumer_ = 0;
        producer_ = 0;
        inited_ = true;
      }
      return ret;
    }

    template <typename T>
    void ObFixedQueue<T>::destroy()
    {
      if (inited_)
      {
        ob_free(array_);
        array_ = NULL;
        max_num_ = 0;
        consumer_ = 0;
        producer_ = 0;
        inited_ = false;
      }
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_total() const
    {
      return get_total_(consumer_, producer_);
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_free() const
    {
      return get_free_(consumer_, producer_);
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_total_(const uint64_t consumer, const uint64_t producer) const
    {
      return (producer - consumer);
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_free_(const uint64_t consumer, const uint64_t producer) const
    {
      return max_num_ - get_total_(consumer, producer);
    }

    template <typename T>
    int ObFixedQueue<T>::push(T *ptr)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == ptr)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        register uint64_t old_pos = 0;
        register uint64_t new_pos = 0;
        while (true)
        {
          old_pos = producer_;
          new_pos = old_pos;

          if (0 >= get_free_(consumer_, old_pos))
          {
            ret = OB_SIZE_OVERFLOW;
            break;
          }

          new_pos++;
          if (old_pos == ATOMIC_CAS(&producer_, old_pos, new_pos))
          {
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          register uint64_t index = old_pos % max_num_;
          array_[index].data = ptr;
          array_[index].cur_pos = old_pos;
        }
      }
      return ret;
    }

    template <typename T>
    int ObFixedQueue<T>::pop(T *&ptr)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        T *tmp_ptr = NULL;
        register uint64_t old_pos = 0;
        register uint64_t new_pos = 0;
        while (true)
        {
          old_pos = consumer_;
          new_pos = old_pos;

          if (0 >= get_total_(old_pos, producer_))
          {
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }

          register uint64_t index = old_pos % max_num_;
          if (old_pos != array_[index].cur_pos)
          {
            continue;
          }
          tmp_ptr = array_[index].data;

          new_pos++;
          if (old_pos == ATOMIC_CAS(&consumer_, old_pos, new_pos))
          {
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          ptr = tmp_ptr;
        }
      }
      return ret;
    }
  }
}

#endif //OCEANBASE_COMMON_FIXED_QUEUE_H_
