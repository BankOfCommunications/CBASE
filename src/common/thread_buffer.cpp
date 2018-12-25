/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 */

#include "thread_buffer.h"
#include <stdint.h>
#include "tblog.h"
#include "ob_malloc.h"

namespace
{
  const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
}

namespace oceanbase
{
  namespace common
  {
    ThreadSpecificBuffer::ThreadSpecificBuffer(const int32_t size)
      : key_(INVALID_THREAD_KEY), size_(size)
    {
      create_thread_key();
    }

    ThreadSpecificBuffer::~ThreadSpecificBuffer()
    {
      delete_thread_key();
    }

    int ThreadSpecificBuffer::create_thread_key()
    {
      int ret = pthread_key_create(&key_, destroy_thread_key);
      if (0 != ret)
      {
        TBSYS_LOG(ERROR, "cannot create thread key:%d", ret);
      }
      return (0 == ret) ? OB_SUCCESS : OB_ERROR;
    }

    int ThreadSpecificBuffer::delete_thread_key()
    {
      int ret = -1;
      if (INVALID_THREAD_KEY != key_)
      {
        ret = pthread_key_delete(key_);
      }
      if (0 != ret)
      {
        TBSYS_LOG(WARN, "delete thread key key_ failed.");
      }
      return (0 == ret) ? OB_SUCCESS : OB_ERROR;
    }

    void ThreadSpecificBuffer::destroy_thread_key(void* ptr)
    {
      TBSYS_LOG(INFO, "delete thread specific buffer, ptr=%p", ptr);
      if (NULL != ptr) ob_free(ptr);
    }

    ThreadSpecificBuffer::Buffer* ThreadSpecificBuffer::get_buffer() const
    {
      Buffer * buffer = NULL;
      if (INVALID_THREAD_KEY != key_ && size_ > 0)
      {
        void* ptr = pthread_getspecific(key_);
        if (NULL == ptr)
        {
          ptr = ob_malloc(size_ + sizeof(Buffer), ObModIds::OB_THREAD_BUFFER);
          if (NULL != ptr)
          {
            int ret = pthread_setspecific(key_, ptr);
            if (0 != ret)
            {
              TBSYS_LOG(ERROR, "pthread_setspecific failed:%d", ret);
              ob_free(ptr);
              ptr = NULL;
            }
            else
            {
              TBSYS_LOG(DEBUG, "new thread specific buffer, addr=%p size=%d this=%p", ptr, size_, this);
              buffer = new (ptr) Buffer(static_cast<char*>(ptr) + sizeof(Buffer), size_);
            }
          }
          else
          {
            // malloc failed;
            TBSYS_LOG(ERROR, "malloc thread specific memeory failed.");
          }
        }
        else
        {
          // got exist ptr;
          buffer = reinterpret_cast<Buffer*>(ptr);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "thread key must be initialized "
            "and size must great than zero, key:%u,size:%d", key_, size_);
      }
      return buffer;
    }

    int ThreadSpecificBuffer::Buffer::advance(const int32_t size)
    {
      int ret = OB_SUCCESS;
      if (size < 0)
      {
        if (end_ + size < start_)
        {
          ret = OB_ERROR;
        }
      }
      else
      {
        if (end_ + size > end_of_storage_)
        {
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret) end_ += size;

      assert(end_ >= start_ && end_ <= end_of_storage_);
      return ret;
    }

    void ThreadSpecificBuffer::Buffer::reset()
    {
      end_ = start_;
    }

    int ThreadSpecificBuffer::Buffer::write(const char* bytes, const int32_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == bytes || size < 0)
      {
        ret = OB_ERROR;
      }
      else
      {
        if (size > remain()) ret = OB_ERROR;
        else
        {
          ::memcpy(end_, bytes, size);
          advance(size);
        }
      }
      return ret;
    }

  } // end namespace chunkserver
} // end namespace oceanbase
