/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_recursive_mutex.h is for what ...
 *
 * Version: ***: ob_recursive_mutex.h  Mon Aug  5 13:28:58 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_RECURSIVE_MUTEX_H_
#define OB_RECURSIVE_MUTEX_H_

#include <pthread.h>
#include "tbsys.h"
#include "ob_spin_lock.h"

namespace oceanbase
{
  namespace common
  {
    class ObRecursiveMutex
    {
    public:
      ObRecursiveMutex();
      ~ObRecursiveMutex();
      int lock();
      int unlock();
    private:
      pthread_mutexattr_t attr_;
      pthread_mutex_t  lock_;
    };

    inline ObRecursiveMutex::ObRecursiveMutex()
    {
      int ret = pthread_mutexattr_init(&attr_);
      if (0 != ret)
      {
        TBSYS_LOG(ERROR, "init mutex attr failed, err=%s", strerror(ret));
      }
      else
      {
        ret = pthread_mutexattr_settype(&attr_, PTHREAD_MUTEX_RECURSIVE);
        if (0 != ret)
        {
          TBSYS_LOG(ERROR, "set mutex attr failed, err=%s", strerror(ret));
        }
        else
        {
          ret = pthread_mutex_init(&lock_, &attr_);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "mutex init failed, err=%s", strerror(ret));
          }
        }
      }
    }

    inline ObRecursiveMutex::~ObRecursiveMutex()
    {
      pthread_mutex_destroy(&lock_);
    }

    inline int ObRecursiveMutex::lock()
    {
      return pthread_mutex_lock(&lock_);
    }

    inline int ObRecursiveMutex::unlock()
    {
      return pthread_mutex_unlock(&lock_);
    }

    typedef ObLockGuard<ObRecursiveMutex> ObRecursiveMutexGuard;
  }
}
#endif
