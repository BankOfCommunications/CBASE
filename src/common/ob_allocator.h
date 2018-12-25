////===================================================================
 //
 // ob_allocator.h common / Oceanbase
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
#ifndef  OCEANBASE_COMMON_ALLOCATOR_H_
#define  OCEANBASE_COMMON_ALLOCATOR_H_
#include "common/ob_define.h"

namespace oceanbase
{
  namespace common
  {
    class ObIAllocator
    {
      public:
        virtual ~ObIAllocator() {};
      public:
        virtual void *alloc(const int64_t sz) = 0;
        virtual void free(void *ptr) = 0;
        virtual void* mod_alloc(const int64_t size, const int32_t mod_id = 0){ UNUSED(size); UNUSED(mod_id); return NULL; }
        virtual void mod_free(void* p, const int32_t mod_id = 0) { UNUSED(p); UNUSED(mod_id); }
        virtual void set_mod_id(int32_t mod_id) = 0;
    };
  }
}

#endif //OCEANBASE_COMMON_ALLOCATOR_H_
