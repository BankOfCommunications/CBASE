/**
 * ob_custom_allocator.h
 *
 * Authors:
 *   tianz
 * used to alloc memory for deepcopy rowkey in multibind and mergejoin operator
 */
 
/**
 * this file modified by peiouya
 * change class CustomAllocator:thread-unsafe  ----->  thread-safe
 */
#ifndef OCEANBASE_CUSTOM_ALLOCATOR_H_
#define OCEANBASE_CUSTOM_ALLOCATOR_H_

#include <stdlib.h>
#include <assert.h>
#include "ob_define.h"
#include "ob_malloc.h"
#include "ob_array.h"
#include "ob_spin_lock.h"

namespace oceanbase
{
  namespace common
  {
    
      class CustomAllocator
      {
        private:
          static const int MAX_ALLOC_NUM = 256;
          static const int64_t DEFAULT_BLOCK_SIZE = 2L*1024L*1024L;
          ObArray<char*> alloc_arr_prt_;
          char *cur_head_;
          int64_t cur_remain_size_;
          int32_t mod_id_;
          ObSpinLock lock_;
      public:
          CustomAllocator(int32_t mod_id = ObModIds::OB_MS_SUB_QUERY):cur_head_(NULL),cur_remain_size_(0),mod_id_(mod_id){}
          ~CustomAllocator();
          void set_mod_id(const int32_t mod_id);
          char* alloc(const int64_t size);
          void free();
      };
  } // end namespace common
} // end namespace oceanbase

#endif // end if OCEANBASE_CUSTOM_ALLOCATOR_H_
