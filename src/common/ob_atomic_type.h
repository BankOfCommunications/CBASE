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
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#ifndef __OCEANBASE_COMMON_OB_ATOMIC_TYPE_H__
#define __OCEANBASE_COMMON_OB_ATOMIC_TYPE_H__
#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    // 支持并发读写
    template<typename T>
    struct ObAtomicType
    {
      volatile int64_t modify_version_;
      ObAtomicType(): modify_version_(0)
      {}
      ~ObAtomicType()
      {}
      int copy(T& that)
      {
        int err = OB_SUCCESS;
        int64_t old_version = modify_version_;
        if (0 >= old_version)
        {
          err = OB_EAGAIN;
        }
        else
        {
          that = *(T*)this;
        }
        if (OB_SUCCESS == err && old_version != modify_version_)
        {
          err = OB_EAGAIN;
        }
        return err;
      }

      int set(T& that)
      {
        int err = OB_SUCCESS;
        int64_t old_version = modify_version_;
        if (0 > old_version
            || !__sync_bool_compare_and_swap(&modify_version_, old_version, -1))
        {
          err = OB_EAGAIN;
        }
        else
        {
          that.modify_version_ = -1;
          *(T*)this = that;
        }
        if (OB_SUCCESS == err
            && !__sync_bool_compare_and_swap(&modify_version_, -1, old_version+1))
        {
          err = OB_ERR_UNEXPECTED;
        }
        return err;
      }
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif //__OCEANBASE_COMMON_OB_ATOMIC_TYPE_H__
