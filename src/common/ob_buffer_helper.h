/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   jianming <jianming.cjq@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_BUFFER_HELPER_H_
#define OCEANBASE_BUFFER_HELPER_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace common 
  {

    #define INVALID_BUF_SIZE -1024 

    /*
     * read content from buffer
     * @warning if buf_size = INVALID_BUF_SIZE, then don't check buf size
     */
    class ObBufferReader
    {
      public:
        ObBufferReader(const char* buf = NULL, int64_t buf_size = INVALID_BUF_SIZE) 
          : pos_(0), buf_size_(buf_size)
        {
          this->buf_ = buf;
        }

        inline int assign(const char* buf, int64_t buf_size = INVALID_BUF_SIZE)
        {
          int ret = OB_SUCCESS;
          if(NULL == buf)
          {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(WARN, "buf is null");
          }
          else
          {
            pos_ = 0;
            this->buf_ = buf;
            this->buf_size_ = buf_size;
          }
          return ret;
        }

        template<class T>
        inline int get(const T *&ptr)
        {
          int ret = OB_SUCCESS;
          if(NULL == buf_)
          {
            ret = OB_NOT_INIT;
            TBSYS_LOG(WARN, "buf_ is null");
          }
          else if(buf_size_ != INVALID_BUF_SIZE && pos_ == buf_size_)
          {
            ret = OB_ITER_END; 
          }
          else if(buf_size_ != INVALID_BUF_SIZE && pos_ + (int64_t)sizeof(T) > buf_size_)
          {
            ret = OB_BUF_NOT_ENOUGH;
          }
          else
          {
            ptr = (const T*)(buf_ + pos_);
            pos_ += sizeof(T);
          }
          return ret;
        }

        /*
         * read a value from buffer
         * @return value
         */
        template<class T>
        inline T get() 
        {
          T ret = *((T*)(buf_ + pos_));
          pos_ += sizeof(T);
          return ret;
        };

        /*
         * read a value from buffer
         * @return value pointer
         */
        template<class T>
        inline T* get_ptr() 
        {
          T* ret = (T*)(buf_ + pos_);
          pos_ += sizeof(T);
          return ret;
        };

        /*
         * get current pointer of buffer
         */
        inline const char* cur_ptr() const
        {
          return buf_ + pos_;
        }

        inline int64_t pos() const
        {
          return pos_;
        }

        inline const char* buf() const
        {
          return buf_;
        }

        /*
         * skip n byte not read
         * @param step n-byte
         */
        inline void skip(int64_t step)
        {
          pos_ += step;
        }
        
        /*
         * reset the offset
         */
        inline void reset()
        {
          pos_ = 0;
        }

      private:
        const char* buf_;
        int64_t pos_;
        int64_t buf_size_;
    };

 
    /*
     * write value to buffer
     * @warning if buf_size = INVALID_BUF_SIZE, then don't check buf size
     */
    class ObBufferWriter
    {
    public:
      ObBufferWriter(int64_t buf_size = INVALID_BUF_SIZE) : buf_(NULL), pos_(0), buf_size_(buf_size)
      {
      }

      ObBufferWriter(char* buf, int64_t buf_size = INVALID_BUF_SIZE) : pos_(0), buf_size_(buf_size)
      {
        this->buf_ = buf;
      }

      inline int assign(char* buf, int64_t buf_size = INVALID_BUF_SIZE)
      {
        int ret = OB_SUCCESS;
        if(NULL == buf)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "buf is null");
        }
        else
        {
          pos_ = 0;
          this->buf_size_ = buf_size;
          this->buf_ = buf;
        }
        return ret;
      }

      inline int test(int64_t write_len)
      {
        int ret = common::OB_SUCCESS;
        if(pos_ + write_len > buf_size_)
        {
          ret = common::OB_BUF_NOT_ENOUGH;
        }
        return ret;
      }

      /*
       * @brief desc write a value to buffer
       * @param val the value to be written
       */
      template<class T>
      inline int write(const T& val)
      {
        int ret = common::OB_SUCCESS;
        if(NULL == buf_)
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN, "buf_ is null");
        }
        else if((buf_size_ != INVALID_BUF_SIZE) && ((pos_ + (int64_t)sizeof(T)) > buf_size_))
        {
          ret = common::OB_BUF_NOT_ENOUGH;
        }
        else
        {
          *((T*)(buf_ + pos_)) = val;
          pos_ += sizeof(T);
        }
        return ret;
      };

      /*
       * @brief desc write varchar to buffer
       * @param str varchar pointer
       * @param str_len varchar length
       */
      inline int write_varchar(const char *str, int64_t len)
      {
        int ret = common::OB_SUCCESS;
        if(NULL == buf_)
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN, "buf_ is null");
        }
        else if((buf_size_ != INVALID_BUF_SIZE) && ((pos_ + len) > buf_size_))
        {
          ret = common::OB_BUF_NOT_ENOUGH;
        }
        else
        {
          memcpy(buf_ + pos_, str, len);
          pos_ += len;
        }
        return ret;
      }

      inline int write_varchar(const ObString &str, ObString *str_written = NULL)
      {
        int ret = common::OB_SUCCESS;
        if(NULL == buf_)
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN, "buf_ is null");
        }
        else if((buf_size_ != INVALID_BUF_SIZE) && ((pos_ + str.length()) > buf_size_))
        {
          ret = common::OB_BUF_NOT_ENOUGH;
        }
        else
        {
          memcpy(buf_ + pos_, str.ptr(), str.length());
          if(NULL != str_written)
          {
            str_written->assign(buf_ + pos_, str.length());
          }
          pos_ += str.length();
        }
        return ret;
      }

      inline int revert(int64_t step)
      {
        int ret = common::OB_SUCCESS;
        if(step > pos_)
        {
          ret = common::OB_BUF_NOT_ENOUGH; 
          TBSYS_LOG(WARN, "step is too large, step[%ld], pos_[%ld]", step, pos_);
        }
        else
        {
          pos_ -= step;
        }
        return ret;
      }

      /*
       * get current pointer of buffer
       */
      inline char* cur_ptr() const
      {
        return buf_ + pos_;
      }

      inline int64_t pos() const
      {
        return pos_;
      }

      inline char* buf()
      {
        return buf_;
      }

      inline char* buf() const
      {
        return buf_;
      }

      inline void reset()
      {
        pos_ = 0;
      }

    private:
      char* buf_;
      int64_t pos_;
      int64_t buf_size_;
    };
  }
}

#endif

