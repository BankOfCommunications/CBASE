/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_TEST_OB_SCAN_GEN_H_
#define OCEANBASE_TEST_OB_SCAN_GEN_H_

#include "common/ob_string_buf.h"
#include "common/ob_scan_param.h"
#include "ob_test_bomb.h"
#include "ob_generator.h"
#include "ob_random.h"

namespace oceanbase
{
  namespace test
  {
    class ObScanGen : public ObGenerator
    {
      public:
        ObScanGen(int64_t max_line_no);
        virtual ~ObScanGen();
        virtual int gen(ObTestBomb &bomb);
      private:
        int64_t max_line_no_;
        common::ObScanParam sp_;
        ObRandom ran_;
        common::ObStringBuf string_buf_;
    };
  } // end namespace test
} // end namespace oceanbase

#endif // OCEANBASE_TEST_OB_SCAN_GEN_H_
