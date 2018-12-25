/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-26
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#ifndef OCEANBASE_COMMON_LIMIT_ARRAY_H_
#define OCEANBASE_COMMON_LIMIT_ARRAY_H_
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

#include <tblog.h>

#include "ob_array_helper.h"

namespace oceanbase 
{ 
  namespace common 
  {
    template <class T, int64_t N>
    class LimitArray
    {
      public:
        LimitArray()
          :holder_index_(0), size_(0), index_(0), tail_holer_index_(0)
        {
        }
        bool expand(ObArrayHelper<T>& new_array)
        {
          bool res = true;
          if (new_array.get_array_size() <= 0 ||
              new_array.get_array_index() !=0 ||
              new_array.get_base_address() == NULL)
          {
            res = false;
          }
          if (res && holder_index_ >= N)
          {
            res = false;
          }
          if (res)
          {
            holder[holder_index_++] = new_array;
            size_ += new_array.get_array_size();
          }
          return res;
        }
        //bool shrink(int64_t size_)

        T* at(const int64_t index_in) const
        {
          int64_t index = index_in;
          T* res = NULL;
          if (index < index_)
          {
            int64_t array_pos = 0;
            while (res == NULL)
            {
              if (index < holder[array_pos].get_array_size())
              {
                res = holder[array_pos].at(index);
                break;
              }
              else
              {
                index -= holder[array_pos].get_array_size();
                array_pos++;
              }
              if (index < 0 || array_pos >= N)
              {
                TBSYS_LOG(ERROR, "this can never be reached, bugs!!!");
                break;
              }
            }
          }
          return res;
        }

        bool push_back(const T& value)
        {
          bool res = true;
          if (index_ >= size_ )
          {
            res = false;
          }
          if (res && (tail_holer_index_ >= N || tail_holer_index_ < 0))
          {
            res = false;
            TBSYS_LOG(ERROR, "this can never be reached, bugs!!!");
          }
          if (res)
          {
            while(holder[tail_holer_index_].get_array_index() ==
                holder[tail_holer_index_].get_array_size())
            {
              tail_holer_index_++;
            }
            if (tail_holer_index_ >= N || tail_holer_index_ < 0)
            {
              TBSYS_LOG(ERROR, "this can never be reached, bugs!!!");
              res = false;
            }
            else
            {
              res = holder[tail_holer_index_].push_back(value);
            }
          }
          if (res) index_++;
          return res;
        }
        T* pop()
        {
          T* res = NULL;
          if (index_ > 0 && index_ <= size_ ) 
          {
            if (tail_holer_index_ < 0 || tail_holer_index_ >= N)
            {
              TBSYS_LOG(ERROR, "this can never be reached, bugs!!!");
            }
            else 
            {
              while (holder[tail_holer_index_].get_array_index() == 0)
              {
                tail_holer_index_--;
              }
              if (tail_holer_index_ < 0)
              {
                TBSYS_LOG(ERROR, "this can never be reached, bugs!!!");
              }
              else
              {
                res = holder[tail_holer_index_].pop();
                index_--;
              }
            }
          }
          return res;
        }
        int64_t get_size() const
        {
          return size_;
        }
        int64_t get_index() const
        {
          return index_;
        }
        

      private:
        ObArrayHelper<T> holder[N];
        int64_t holder_index_;
        int64_t size_;
        int64_t index_;
        int64_t tail_holer_index_;
    };

  }
}
#endif
