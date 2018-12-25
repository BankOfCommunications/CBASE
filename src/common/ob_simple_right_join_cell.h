/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 *
 * Authors:
 *
 */

#ifndef OB_SIMPLE_RIGHT_JOIN_CELL_H_
#define OB_SIMPLE_RIGHT_JOIN_CELL_H_

#include "ob_vector.h"
#include "ob_object.h"
#include "ob_rowkey.h"

namespace oceanbase
{
  namespace common
  {
    struct ObSimpleRightJoinCell
    {
      uint64_t table_id;
      ObRowkey rowkey;
    };
    template <> 
    struct ob_vector_traits<ObSimpleRightJoinCell>
    {
      typedef ObSimpleRightJoinCell* pointee_type;
      typedef ObSimpleRightJoinCell value_type;
      typedef const ObSimpleRightJoinCell const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type; 
    };
  }
}

#endif //OB_SIMPLE_RIGHT_JOIN_CELL_H_

