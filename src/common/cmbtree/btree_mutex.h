/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - An encapsulation of pthread_mutex_t
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_MUTEX_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_MUTEX_H_

#include <pthread.h>

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      class BtreeMutex
      {
        public:
          inline BtreeMutex                      ();
          inline ~BtreeMutex                     ();
          inline int lock                        () const;
          inline int unlock                      () const;

        private:
          BtreeMutex                             (const BtreeMutex&);
          BtreeMutex& operator=                  (const BtreeMutex&);

          mutable pthread_mutex_t mutex_;
      };

      BtreeMutex::BtreeMutex()
      {
        const int err = pthread_mutex_init(&mutex_, NULL);
        if (err != 0)
        {
          CMBTREE_LOG(ERROR, "%s", "pthread_mutex_init error");
        } 
      }

      BtreeMutex::~BtreeMutex()
      {
        const int err = pthread_mutex_destroy(&mutex_); 
        if (err != 0)
        {
          CMBTREE_LOG(ERROR, "%s", "pthread_mutex_destroy error");
        } 
      }

      int BtreeMutex::lock() const
      {
        int ret = ERROR_CODE_OK;
        const int err = pthread_mutex_lock(&mutex_);
        if (err != 0)
        {
          if (err == EDEADLK)
          {
            CMBTREE_LOG(ERROR, "%s", "pthread_mutex_lock deadlock"); 
          }
          else
          {
            CMBTREE_LOG(ERROR, "%s", "pthread_mutex_lock error"); 
          }
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        return ret;
      }

      int BtreeMutex::unlock() const
      {
        int ret = ERROR_CODE_OK;
        const int err = pthread_mutex_unlock(&mutex_);
        if (err != 0)
        {
          CMBTREE_LOG(ERROR, "%s", "pthread_mutex_unlock error");
          ret = ERROR_CODE_NOT_EXPECTED;
        } 
        return ret;
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_MUTEX_H_
