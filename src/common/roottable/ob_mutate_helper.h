/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_mutate_helper.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MUTATE_HELPER_H
#define _OB_MUTATE_HELPER_H 1

namespace oceanbase
{
  namespace common
  {
    class ObMutator;
    class ObMutateHelper
    {
      public:
        ObMutateHelper(){}
        virtual ~ObMutateHelper(){}
        virtual int mutate(const ObMutator& mutator) = 0;

      private:
        // disallow copy
        ObMutateHelper(const ObMutateHelper &other);
        ObMutateHelper& operator=(const ObMutateHelper &other);
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_MUTATE_HELPER_H */

