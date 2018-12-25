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
 *     - Default allocator allocates memory from glibc malloc/free
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_DEFAULT_ALLOC_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_DEFAULT_ALLOC_H_

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      class BtreeDefaultAlloc
      {
        public:
          inline void * alloc(int64_t size)
          {
            return ::malloc(size);
          }

          inline void free(void * ptr)
          {
            ::free(ptr);
          }
      };
    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_DEFAULT_ALLOC_H_
