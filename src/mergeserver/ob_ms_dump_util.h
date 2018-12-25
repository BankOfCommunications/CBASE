/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_dump_util.h,v 0.1 2012/02/29 11:19:30 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGER_DUMP_UTIL_H_
#define OCEANBASE_MERGER_DUMP_UTIL_H_

namespace oceanbase
{
  namespace common
  {
    class ObObj;
    class ObRange;
    class ObCellInfo;
    class ObGetParam;
    class ObScanParam;
    class ObScanner;
  }

  namespace mergeserver
  {
    // dump param to debug log
    class ObMergerDumpUtil
    {
    public:
      // dump scan param
      static void dump(const common::ObScanParam & param);
      // dump get param
      static void dump(const common::ObGetParam & param);
      // dump scanner result
      static void dump(const common::ObScanner & result);
    };
  }
}

#endif //OCEANBASE_MERGER_DUMP_UTIL_H_
