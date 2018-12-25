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
 *     - Thread ID encapsulation
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_TID_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_TID_H_

#include <sys/syscall.h>
#include <unistd.h>

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      class BtreeTID
      {
        public:
          static inline int gettid()
          {
            static __thread int tid = -1;
            if (UNLIKELY(tid == -1))
            {
              tid = static_cast<int>(syscall(__NR_gettid));
            }
            return tid;
          }
      };

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_TID_H_
