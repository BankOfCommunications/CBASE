/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_row_iterator.h,v 0.1 2012/06/19 13:24:51 yuhuang Exp $
 *
 * Authors:
 *   yuhuang <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_ROW_ITERATOR_H_
#define __OCEANBASE_CHUNKSERVER_OB_ROW_ITERATOR_H_

#include "common/ob_common_param.h"
#include "ob_row.h"

namespace oceanbase
{
  namespace common
  {
    // interface of iterator
    class ObRowIterator
    {
      public:
        ObRowIterator()
        {
          // empty
        }
        virtual ~ObRowIterator()
        {
          // empty
        }
      public:
        // Moves the cursor to next row.
        // Gets the that next row.
        // @return OB_SUCCESS if sucess, OB_ITER_END if iter ends, or other error code
        virtual int get_next_row(ObRow &row) = 0;
    };
  }
}

#endif //__OB_ROW_ITERATOR_H__

