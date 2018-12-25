/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_ROW_FUSE_H
#define _OB_ROW_FUSE_H 1

#include "common/ob_row.h"
#include "ob_ups_row.h"
#include "utility.h"

namespace test
{
  class ObRowFuseTest_apply_row_test_Test;
}

namespace oceanbase
{
  namespace common
  {
    class ObRowFuse
    {
      friend class test::ObRowFuseTest_apply_row_test_Test;

      public:
        /*
         * function assign and fuse_row
         * used in ObTabletFuse, ObTabletGet, ObTabletJoin
         */
        static int assign(const ObUpsRow &incr_row, ObRow &result);
        static int join_row(const ObUpsRow *incr_row, const ObRow *sstable_row, ObRow *result);
        static int fuse_row(const ObUpsRow *incr_row, const ObRow *sstable_row, ObRow *result);

        /*
         * used in ObMutipleScanMerge and ObMultipleGetMerge
         * @param 
         * is_row_empty tell whether is result_row is empty after fuse, 
         * ATTENTION: is_row_empty should be set before this fun is called to tell 
         * whether result_row is empty before called
         * @param
         * is_ups_row tell whether result_row is ups row
         */
        static int fuse_row(const ObRow &row, ObRow &result_row, bool &is_row_empty, bool is_ups_row);
      private:
        static int fuse_sstable_row(const ObRow &row, ObRow &result_row, bool &is_row_empty);
        static int fuse_ups_row(const ObRow &row, ObRow &result_row, bool &is_row_empty);
        static int apply_row(const ObRow &row, ObRow &result_row, bool copy);
    };
  }
}

#endif /* _OB_ROW_FUSE_H */

