#ifndef OCEANBASE_COMMON_OB_ARRAY_HELPER_H_
#define OCEANBASE_COMMON_OB_ARRAY_HELPER_H_
#include <stdint.h>
#include <stdlib.h>
#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    template<class T>
      class ObArrayHelper
      {
        public:
          ObArrayHelper(int64_t size, T* base_address, int64_t index = 0)
            :size_(size), index_(index), p_(base_address)
          {
          }
          ObArrayHelper()
            :size_(0), index_(0), p_(NULL)
          {
          }
          void init(int64_t size, T* base_address, int64_t index = 0)
          {
            size_ = size;
            p_ = base_address;
            index_ = index;
          }
          bool push_back(const T& value)
          {
            bool add_ok = true;
            if (index_ < size_ && p_ && index_ >= 0)
            {
              p_[index_++] = value;
            }
            else
            {
              add_ok = false;
            }
            return add_ok;
          }
          T* pop()
          {
            T* res = NULL;
            if (index_ > 0 && p_ && index_ <= size_)
            {
              res = &p_[--index_];
            }
            return res;
          }
          int64_t get_array_size() const
          {
            return size_;
          }
          int64_t get_array_index() const
          {
            return index_;
          }
          T* at(int64_t index) const
          {
            T* res = NULL;
            if (index < index_ && p_ && index >= 0)
            {
              res = &p_[index];
            }
            return res;
          }
          T* get_base_address() const
          {
            return p_;
          }
          void clear()
          {
            index_ = 0;
          }
        private:
          int64_t size_;
          int64_t index_;
          T* p_;
      };


      template<class T>
      class ObArrayHelpers
      {
      public:
        ObArrayHelpers()
        {
          memset(arrs_, 0x00, sizeof(arrs_));
          arr_count_ = 0;
        }

        int add_array_helper(ObArrayHelper<T> &helper)
        {
          int err = OB_SUCCESS;
          if(arr_count_ >= MAX_ARR_COUNT)
          {
            err = OB_ARRAY_OUT_OF_RANGE;
          }
          else
          {
            arrs_[arr_count_] = &helper;
            arr_count_ ++;
          }
          return err;
        }

        T* at(const int64_t index)
        {
          const ObArrayHelpers * pthis = static_cast<const ObArrayHelpers *>(this);
          return const_cast<T*>(pthis->at(index));
        }
        const T* at(const int64_t index) const
        {
          int64_t counter = 0;
          T* res = NULL;
          if (index < 0)
          {
            res = NULL;
          }
          else
          {
            for (int64_t i = 0; i < arr_count_; i++)
            {
              if (index < counter + arrs_[i]->get_array_index())
              {
                res = arrs_[i]->at(index - counter);
                break;
              }
              else
              {
                counter+= arrs_[i]->get_array_index();
              }
            }
          }
          return res;
        }

        void clear()
        {
          for(int64_t i = 0; i < arr_count_; i++)
          {
            arrs_[i]->clear();
          }
        }

        int64_t get_array_index()const
        {
          int64_t counter = 0;
          for(int64_t i = 0; i < arr_count_; i++)
          {
            counter+= arrs_[i]->get_array_index();
          }
          return counter;
        }
      private:
        static const int MAX_ARR_COUNT = 16;
        ObArrayHelper<T> * arrs_[MAX_ARR_COUNT];
        int64_t arr_count_;
      };
  }
}
#endif
