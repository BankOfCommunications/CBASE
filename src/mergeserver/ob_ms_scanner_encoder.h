/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_scanner_encoder.h,v 0.1 2011/12/29 15:49:30 zhidong Exp $
 *
 * Authors:
 *   xielun.szd <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_SCANNER_ENCODER_H_
#define OCEANBASE_SCANNER_ENCODER_H_

namespace oceanbase
{
  namespace common
  {
    class ObGetParam;
    class ObScanParam;
    class ObScanner;
  }

  namespace mergeserver
  {
    class ObMergerScannerEncoder
    {
    public:
      // output cellinfo
      static void output(const common::ObCellInfo & cell);
      // convert id to name for scanner result
      static int encode(const common::ObGetParam & get_param,
          const common::ObScanner & org_scanner, common::ObScanner & encoded_scanner);
    };
  }
}

#endif // OCEANBASE_SCANNER_ENCODER_H_
