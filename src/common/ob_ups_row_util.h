/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_row_util.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_UPS_ROW_UTIL_H
#define _OB_UPS_ROW_UTIL_H 1
#include "common/ob_string.h"
#include "ob_ups_row.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace common 
  {
    class ObUpsRowUtil
    {
      public:
        static int convert(const ObUpsRow &row, ObString &compact_row);
        static int convert(uint64_t table_id, const ObString &compact_row, ObUpsRow &row);

        static int convert(uint64_t table_id, const ObString &compact_row, ObUpsRow &row, ObRowkey *rowkey, ObObj *rowkey_buf);
    };
  }
}

#endif /* _OB_UPS_ROW_UTIL_H */

