#ifndef OCEANBASE_COMMON_SINGLE_POP_QUEUE_H_
#define OCEANBASE_COMMON_SINGLE_POP_QUEUE_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    // only one thread can get head item
    template <typename T>
    class ObSinglePopQueue
    {
    public:
      ObSinglePopQueue();
      virtual ~ObSinglePopQueue();
    public:
      // not thread safe only call once
      int init(const int64_t max_num);
      void destroy();
    public:
      // thread safe interface
      int head(T & item) const;
      int push(const T & item);
      int pop(T & item);
      int pop(void);
      int64_t size(void) const;
    private:
      static const int64_t ARRAY_BLOCK_SIZE = 128L * 1024L;
      bool inited_;
      int64_t max_num_;
      T * array_;
      mutable tbsys::CRWLock lock_;
      int64_t consumer_;
      int64_t producer_;
    };

    template <typename T>
    ObSinglePopQueue<T>::ObSinglePopQueue() : inited_(false), max_num_(0),
        array_(NULL), consumer_(0), producer_(0)
    {
    }

    template <typename T>
    ObSinglePopQueue<T>::~ObSinglePopQueue()
    {
      destroy();
    }

    template <typename T>
    int ObSinglePopQueue<T>::init(const int64_t max_num)
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
      else if (NULL == (array_ = (T*)ob_malloc(sizeof(T) * max_num, common::ObModIds::SPOP_QUEUE)))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memset(array_, 0, sizeof(T) * max_num);
        max_num_ = max_num;
        consumer_ = 0;
        producer_ = 0;
        inited_ = true;
      }
      return ret;
    }

    template <typename T>
    void ObSinglePopQueue<T>::destroy()
    {
      if (inited_)
      {
        max_num_ = 0;
        consumer_ = 0;
        producer_ = 0;
        ob_free(array_);
        array_ = NULL;
        inited_ = false;
      }
    }

    template <typename T>
    int64_t ObSinglePopQueue<T>::size(void) const
    {
      tbsys::CWLockGuard lock(lock_);
      return producer_ - consumer_;
    }

    template <typename T>
    int ObSinglePopQueue<T>::push(const T & item)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        tbsys::CWLockGuard lock(lock_);
        if (producer_ - consumer_ < max_num_)
        {
          array_[producer_++ % max_num_] = item;
        }
        else
        {
          ret = OB_SIZE_OVERFLOW;
        }
      }
      return ret;
    }

    template <typename T>
    int ObSinglePopQueue<T>::pop(T & item)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        tbsys::CWLockGuard lock(lock_);
        if (producer_ - consumer_ > 0)
        {
          item = array_[consumer_++ % max_num_];
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }

    template <typename T>
    int ObSinglePopQueue<T>::head(T & item) const
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        tbsys::CRLockGuard lock(lock_);
        if (producer_ - consumer_ > 0)
        {
          item = array_[consumer_ % max_num_];
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }

    template <typename T>
    int ObSinglePopQueue<T>::pop(void)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        tbsys::CWLockGuard lock(lock_);
        if (producer_ - consumer_ > 0)
        {
          ++consumer_;
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }
  }
}

#endif //OCEANBASE_COMMON_SINGLE_POP_QUEUE_H_
