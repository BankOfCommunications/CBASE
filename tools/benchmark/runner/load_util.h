/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_util.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_UTIL_H_
#define LOAD_UTIL_H_

#include "common/ob_scanner.h"
#include "common/ob_get_param.h"
#include "common/ob_fifo_stream.h"

namespace oceanbase
{
  namespace tools
  {
    class LoadUtil
    {
    public:
      static bool compare_cell(const common::ObCellInfo & first, const common::ObCellInfo & second);
      static bool compare_result(common::ObScanner & first, common::ObScanner & second);
    public:
      static bool check_request(common::Request * request);
      static int dump_param(const common::ObGetParam & param);
    };
  }
}

#endif // LOAD_UTIL_H_
