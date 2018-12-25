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
#ifndef OCEANBASE_COMMON_THREAD_BUFFER_H_
#define OCEANBASE_COMMON_THREAD_BUFFER_H_

#include <pthread.h>
#include "ob_define.h"

namespace oceanbase 
{ 
  namespace common
  {

    /**
     * Provide a memeory allocate mechanism by allocate
     * a buffer associate with specific thread, whenever
     * this buffer allocate by user function in a thread
     * and free when thread exit.
     */
    class ThreadSpecificBuffer
    {
      public:
        static const int32_t MAX_THREAD_BUFFER_SIZE = 2*1024*1024L;
      public:
        class Buffer
        {
          public:
            Buffer(char* start, const int32_t size)
              : end_of_storage_(start + size), end_(start)
            {
            }

          public:
            int write(const char* bytes, const int32_t size);
            int advance(const int32_t size);
            void reset() ;

            inline int32_t used() const { return static_cast<int32_t>(end_ - start_); }
            inline int32_t remain() const { return static_cast<int32_t>(end_of_storage_ - end_); }
            inline int32_t capacity() const { return static_cast<int32_t>(end_of_storage_ - start_); }
            inline char* current() const { return end_; }

          private:
            char* end_of_storage_;
            char* end_;
            char start_[0];
        };
      public:
        explicit ThreadSpecificBuffer(const int32_t size = MAX_THREAD_BUFFER_SIZE);
        ~ThreadSpecificBuffer();

        Buffer* get_buffer() const;
      private:
        int create_thread_key();
        int delete_thread_key();
        static void destroy_thread_key(void* ptr);

        pthread_key_t key_;
        int32_t size_;
    };

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_THREAD_BUFFER_H_

