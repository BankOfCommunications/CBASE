////===================================================================
 //
 // ob_pointer_array.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-09-16 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_POINTER_ARRAY_H_
#define  OCEANBASE_COMMON_POINTER_ARRAY_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"

namespace oceanbase
{
  namespace common
  {
    template <typename T, int64_t SIZE>
    class ObPointerArray
    {
      public:
        ObPointerArray()
        {
          memset(array_, 0, sizeof(array_));
        };
        ~ObPointerArray()
        {
          for (int64_t i = 0; i < SIZE; i++)
          {
            if (NULL != array_[i])
            {
              array_[i]->~T();
              array_[i] = NULL;
            }
          }
        };
      public:
        T *operator [](const int64_t index)
        {
          T *ret = NULL;
          if (0 <= index
              && SIZE > index)
          {
            if (NULL == (ret = array_[index]))
            {
              char *buffer = allocator_.alloc(sizeof(T));
              if (NULL != buffer)
              {
                array_[index] = new(buffer) T();
                ret = array_[index];
              }
            }
          }
          return ret;
        };
      private:
        T *array_[SIZE];
        PageArena<char> allocator_;
    };
  }
}

#endif // OCEANBASE_COMMON_POINTER_ARRAY_H_

