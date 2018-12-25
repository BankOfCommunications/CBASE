
/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_fuse.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_SCAN_FUSE_H
#define _OB_TABLET_SCAN_FUSE_H 1

#include "ob_rowkey_phy_operator.h"
#include "common/ob_ups_row.h"
#include "common/ob_range.h"
#include "ob_sstable_scan.h"
#include "ob_ups_scan.h"
#include "ob_tablet_fuse.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    // 用于CS从合并sstable中的静态数据和UPS中的增量数据
    class ObTabletScanFuse: public ObTabletFuse
    {
      public:
        ObTabletScanFuse();
        virtual ~ObTabletScanFuse();
        virtual void reset();
        virtual void reuse();
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        int set_sstable_scan(ObSSTableScan *sstable_scan);
        int set_incremental_scan(ObUpsScan *incremental_scan);

        int open();
        int close();
        virtual ObPhyOperatorType get_type() const { return PHY_TABLET_SCAN_FUSE; }
        int get_next_row(const ObRow *&row);
        int get_last_rowkey(const ObRowkey *&rowkey);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int get_row_desc(const common::ObRowDesc *&row_desc) const;

      private:
        // disallow copy
        ObTabletScanFuse(const ObTabletScanFuse &other);
        ObTabletScanFuse& operator=(const ObTabletScanFuse &other);

        int compare_rowkey(const ObRowkey &rowkey1, const ObRowkey &rowkey2);
        bool check_inner_stat();

      private:
        // data members
        ObSSTableScan *sstable_scan_; // 从sstable读取的静态数据
        ObUpsScan *incremental_scan_; // 从UPS读取的增量数据
        const ObRow *last_sstable_row_;
        const ObUpsRow *last_incr_row_;
        const ObRowkey *sstable_rowkey_;
        const ObRowkey *incremental_rowkey_;
        const ObRowkey *last_rowkey_;
        ObRow curr_row_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_SCAN_FUSE_H */
