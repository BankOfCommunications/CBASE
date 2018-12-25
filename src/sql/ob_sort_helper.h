/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sort_helper.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SORT_HELPER_H
#define _OB_SORT_HELPER_H 1
#include "common/ob_row.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSortHelper
    {
      public:
        ObSortHelper(){}
        virtual ~ObSortHelper(){}

        virtual int get_next_row(const common::ObRow *&row) = 0;
      private:
        // disallow copy
        ObSortHelper(const ObSortHelper &other);
        ObSortHelper& operator=(const ObSortHelper &other);
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SORT_HELPER_H */

