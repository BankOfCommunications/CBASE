#ifndef OCEANBASE_COMMON_OB_DATA_BUFFER_H_

#define OCEANBASE_COMMON_OB_DATA_BUFFER_H_
#include <stdint.h>
#include <stdlib.h>

namespace oceanbase
{
  namespace common
  {
    class ObDataBuffer
    {
      public:
        ObDataBuffer() : data_(NULL), capacity_(0), position_(0)
      {
      }

        ObDataBuffer(char* data, const int64_t capacity)
          : data_(data), capacity_(capacity), position_(0)
        {
        }

        ~ObDataBuffer() {}

        inline bool set_data(char* data, const int64_t capacity)
        {
          bool ret = false;

          if (data != NULL && capacity > 0)
          {
            data_ = data;
            capacity_ = capacity;
            position_ = 0;
            limit_ = 0;
            ret = true;
          }

          return ret;
        }

        /** WARNING: make sure the memory if freed before call this method */
        inline void reset()
        {
          data_ = NULL;
          capacity_ = 0;
          position_ = 0;
          limit_ = 0;
        }

        inline const char* get_data() const { return data_; }
        inline char* get_data() { return data_; }

        inline int64_t get_capacity() const { return capacity_; }
        inline int64_t get_remain() const { return capacity_ - position_; }
        inline int64_t get_remain_data_len() const {return limit_ - position_; }
        inline int64_t get_position() const { return position_; }
        inline int64_t& get_position() { return position_; }

        inline int64_t get_limit() const { return limit_; }
        inline int64_t& get_limit() { return limit_; }

      private:
        char* data_;
        int64_t capacity_;
        int64_t position_;
        int64_t limit_;
    };
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OB_DATA_BUFFER_H_ */
