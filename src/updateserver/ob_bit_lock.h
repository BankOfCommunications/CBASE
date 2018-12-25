////===================================================================
 //
 // ob_bit_lock.h / updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2012-08-29 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_UPDATESERVER_BIT_LOCK_H_
#define  OCEANBASE_UPDATESERVER_BIT_LOCK_H_
#include <stdint.h>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace updateserver
  {
    static const uint8_t BIT_MASKS[8] = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};

    class BitLock
    {
      public:
        BitLock() : size_(0),
                    bits_(NULL)
        {
        };
        ~BitLock()
        {
          destroy();
        };
      public:
        inline int init(const int64_t size);
        inline void destroy();
        inline int lock(const uint64_t index);
        inline int unlock(const uint64_t index);
      private:
        int64_t size_;
        uint8_t *volatile bits_;
    };

    class BitLockGuard
    {
      public:
        BitLockGuard(BitLock &lock, const uint64_t index) : lock_(lock),
                                                           index_(index)
        {
          lock_.lock(index_);
        };
        ~BitLockGuard()
        {
          lock_.unlock(index_);
        };
      private:
        BitLock &lock_;
        const uint64_t index_;
    };

    int BitLock::init(const int64_t size)
    {
      int ret = common::OB_SUCCESS;
      if (0 < size_
          || NULL != bits_)
      {
        ret = common::OB_INIT_TWICE;
      }
      else if (0 >= size)
      {
        ret = common::OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t byte_size = common::upper_align(size, 8) / 8;
        if (NULL == (bits_ = (uint8_t*)common::ob_malloc(byte_size, common::ObModIds::BIT_LOCK)))
        {
          ret = common::OB_MEM_OVERFLOW;
        }
        else
        {
          memset(bits_, 0, byte_size);
          size_ = size;
        }
      }
      return ret;
    }

    void BitLock::destroy()
    {
      if (NULL != bits_)
      {
        common::ob_free(bits_);
        bits_ = NULL;
      }
      size_ = 0;
    }

    int BitLock::lock(const uint64_t index)
    {
      int ret = common::OB_SUCCESS;
      if (0 >= size_
          || NULL == bits_)
      {
        ret = common::OB_NOT_INIT;
      }
      else if ((int64_t)index >= size_)
      {
        ret = common::OB_INVALID_ARGUMENT;
      }
      else
      {
        uint64_t byte_index = index / 8;
        uint64_t bit_index = index % 8;
        while (true)
        {
          uint8_t ov = bits_[byte_index];
          if (ov & BIT_MASKS[bit_index])
          {
            continue;
          }
          if (ov == ATOMIC_CAS(&(bits_[byte_index]), ov, ov | BIT_MASKS[bit_index]))
          {
            break;
          }
        }
      }
      return ret;
    }

    int BitLock::unlock(const uint64_t index)
    {
      int ret = common::OB_SUCCESS;
      if (0 >= size_
          || NULL == bits_)
      {
        ret = common::OB_NOT_INIT;
      }
      else if ((int64_t)index >= size_)
      {
        ret = common::OB_INVALID_ARGUMENT;
      }
      else
      {
        uint64_t byte_index = index / 8;
        uint64_t bit_index = index % 8;
        while (true)
        {
          uint8_t ov = bits_[byte_index];
          if (!(ov & BIT_MASKS[bit_index]))
          {
            // have not locked
            break;
          }
          if (ov == ATOMIC_CAS(&(bits_[byte_index]), ov, ov & ~BIT_MASKS[bit_index]))
          {
            break;
          }
        }
      }
      return ret;
    }
  }
}

#endif // OCEANBASE_UPDATESERVER_BIT_LOCK_H_
