/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_array_lock.h,v 0.1 2012/09/14 10:59:30 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_ARRAY_LOCK_H_
#define OCEANBASE_ARRAY_LOCK_H_

#include <tbsys.h>

namespace oceanbase
{
  namespace common
  {
    /// multi lock holder
    class ObLockHolder
    {
    public:
      ObLockHolder();
      virtual ~ObLockHolder();
    public:
      // not thread-safe init
      // create locks array size can not create twice rightnow
      int init(const uint64_t size);
      uint64_t size(void) const;
    protected:
      uint64_t lock_size_;
      tbsys::CThreadMutex * lock_holder_;
    };

    /// just like hash lock but not for more concurrency,
    /// using sequential array for sequential lock acquire
    /// this class assure that not collide for different
    /// holder acquire different lock according to different id
    class ObArrayLock:public ObLockHolder
    {
    public:
      ObArrayLock();
      ~ObArrayLock();
    public:
      // multi-thread can get the same lock because of the array is static
      // acquire the lock according different id (0 -> size-1)
      tbsys::CThreadMutex * acquire_lock(const uint64_t id);
    };

    /// using sequence round-robin algorithm lock allocat algorithm
    /// strict the concurrency to the fixed lock size
    /// acquire different lock according the sequnce of calling time
    class ObSequenceLock:public ObLockHolder
    {
    public:
      ObSequenceLock();
      ~ObSequenceLock();
    public:
      // multi-thread can get the same lock because of the array is static
      // the different time to call this will return different lock using
      // round robin algorithm
      tbsys::CThreadMutex * acquire_lock(void);
      // for test return the lock index
      tbsys::CThreadMutex * acquire_lock(uint64_t & index);
    private:
      uint64_t lock_index_;
    };
  }
}

#endif // OCEANBASE_ARRAY_LOCK_H_
