/**
 * ob_custom_allocator.cpp
 *
 * Authors:
 *   tianz
 * used to alloc memory for deepcopy rowkey in multibind and mergejoin operator
 */
 
/**
 * this file modified by peiouya
 * change class CustomAllocator:thread-unsafe  ----->  thread-safe
*/
#include "ob_custom_allocator.h"
namespace oceanbase
{
  namespace common
  {
    CustomAllocator::~CustomAllocator()
    {
     free();
    }
    void CustomAllocator::set_mod_id(const int32_t mod_id)
    {
      mod_id_ = mod_id;
    }

    char* CustomAllocator::alloc(const int64_t size)
    {
      char * ret = NULL;

      ObSpinLockGuard guard(lock_);
      //mod peiouya [first_alloc_BUG] 20160525:b
      if (DEFAULT_BLOCK_SIZE < size)
      {
        ret = NULL;
      }
      //if ((NULL != cur_head_) && (cur_remain_size_ > size))
      else if ((NULL != cur_head_) && (cur_remain_size_ > size))
      //mod peiouya [first_alloc_BUG] 20160525:e
      {
        ret = cur_head_;
        cur_head_ += size;
        cur_remain_size_ -= size;
      }
      else if (alloc_arr_prt_.count () < MAX_ALLOC_NUM)
      {
        ret =  (char*)ob_malloc(DEFAULT_BLOCK_SIZE,mod_id_);

        if ((NULL != ret)  && (OB_SUCCESS == alloc_arr_prt_.push_back (ret)))
        {
          //mod peiouya [first_alloc_BUG] 20160525:b
          //cur_head_ = ret;
          //cur_remain_size_ = DEFAULT_BLOCK_SIZE;
          cur_head_ = ret + size;
          cur_remain_size_ = DEFAULT_BLOCK_SIZE - size;
          //mod peiouya [first_alloc_BUG] 20160525:e
        }
        else if (NULL != ret)
        {
          ob_free(ret, mod_id_);
          ret = NULL;
        }
      }

      return ret;
    }
       
    void CustomAllocator::free()
    {
      char * ret_ptr = NULL;
      ObSpinLockGuard guard(lock_);
      while(OB_SUCCESS == alloc_arr_prt_.pop_back (ret_ptr))
      {
         OB_ASSERT(NULL != ret_ptr);
         ob_free(ret_ptr, mod_id_);
      }
      OB_ASSERT(0 == alloc_arr_prt_.count ());
      cur_head_ =NULL;
      cur_remain_size_ = 0;
    }
  }
}