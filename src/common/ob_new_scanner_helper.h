/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_new_scanner_helper.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_NEW_SCANNER_HELPER_H
#define _OB_NEW_SCANNER_HELPER_H 1

#include "ob_scanner.h"
#include "ob_new_scanner.h"
#include "ob_row_desc.h"
#include "ob_scan_param.h"
#include "ob_get_param.h"
#include "ob_string_buf.h"

namespace oceanbase
{
  namespace common
  {
    class ObNewScannerHelper
    {
      public:
        static int convert(ObScanner &scanner, const ObRowDesc *row_desc, ObNewScanner &new_scanner);
        static int get_row_desc(const ObScanParam &scan_param, ObRowDesc &row_desc);
        static int get_row_desc(const ObGetParam &get_param, bool has_flag_column, ObRowDesc &row_desc);

        static int add_cell(ObRow &row, const ObCellInfo &cell, bool is_ups_row);
        static int add_ups_cell(ObUpsRow &ups_row, const ObCellInfo &cell, ObStringBuf *str_buf = NULL);

        static int print_new_scanner(ObNewScanner &new_scanner, ObRow &row, bool has_rowkey = false);
      private:
        static int put_rowkey_to_row(ObRow &row, const ObCellInfo &cell);
    };
  }
}

#endif /* _OB_NEW_SCANNER_HELPER_H */

