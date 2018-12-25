/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_util.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef OCEANBASE_COMMON_COMPACT_CELL_UTIL_H_
#define OCEANBASE_COMMON_COMPACT_CELL_UTIL_H_

#include "ob_define.h"
#include "ob_object.h"
#include "ob_cell_meta.h"

namespace oceanbase
{
  namespace common
  {
    /* 计算ObObj转换成紧凑格式占空间大小 */
    int get_compact_cell_size(const ObObj &value);

    /* 计算每种ObObj类型转成紧凑格式最大所占空间大小, ObVarcharType类型不能计算 */
    int get_compact_cell_max_size(enum ObObjType type);

    /* 计算int压缩后大小 */
    int get_int_byte(int64_t int_value);
  }
}

#endif /* OCEANBASE_COMMON_COMPACT_CELL_UTIL_H_ */

