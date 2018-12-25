/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * nb_scan_cond.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _NB_SCAN_COND_H
#define _NB_SCAN_COND_H 1

#include "common/ob_simple_condition.h"
#include "common/ob_easy_array.h"

namespace oceanbase
{
namespace common
{
  namespace nb_accessor
  {
    class ScanConds : public EasyArray<ObSimpleCond>
    {
    public:
      ScanConds();
      ScanConds(const char* column_name, const ObLogicOperator& cond_op, int64_t value);
      ScanConds(const char* column_name, const ObLogicOperator& cond_op, ObString& value);

      ScanConds& operator()(const char* column_name, const ObLogicOperator& cond_op, int64_t value);
      ScanConds& operator()(const char* column_name, const ObLogicOperator& cond_op, ObString& value);
    };
  }
}
}

#endif /* _NB_SCAN_COND_H */

