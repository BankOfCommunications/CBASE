/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_iterator.h,v 0.1 2010/08/18 13:24:51 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_ITERATOR_H_
#define __OCEANBASE_CHUNKSERVER_OB_ITERATOR_H_

#include "common/ob_common_param.h"

namespace oceanbase
{
  namespace common
  {
    // interface of iterator
    template <typename T>
    class ObIteratorTmpl
    {
      public:
        ObIteratorTmpl()
        {
          // empty
        }
        virtual ~ObIteratorTmpl()
        {
          // empty
        }
      public:
        // Moves the cursor to next cell.
        // @return OB_SUCCESS if sucess, OB_ITER_END if iter ends, or other error code
        virtual int next_cell() = 0;
        // Gets the current cell.
        virtual int get_cell(T** cell) = 0;
        virtual int get_cell(T** cell, bool* is_row_changed) = 0;
        virtual int is_row_finished(bool* is_row_finished) {UNUSED(is_row_finished); return OB_NOT_IMPLEMENT;};
    };
    typedef ObIteratorTmpl<ObCellInfo> ObIterator;
    typedef ObIteratorTmpl<ObInnerCellInfo> ObInnerIterator;
  }
}

#endif //__OB_ITERATOR_H__

