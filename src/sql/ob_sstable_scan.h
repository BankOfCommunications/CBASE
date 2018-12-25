/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_scan.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SSTABLE_SCAN_H
#define _OB_SSTABLE_SCAN_H 1

#include "common/ob_row.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"
#include "sstable/ob_sstable_scan_param.h"
#include "compactsstablev2/ob_compact_sstable_scanner.h"
#include "ob_sstable_scanner.h"
#include "ob_phy_operator.h"
#include "ob_rowkey_phy_operator.h"
#include "ob_last_rowkey_interface.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObMultiVersionTabletImage;
    class ObTablet;
  };
  namespace sstable
  {
    class ObSSTableReader;
    class ObBlockIndexCache;
    class ObBlockCache;
  };
  namespace compactsstablev2
  {
    class ObCompactSSTableReader;
    class ObBlockIndexCache;
    class ObBlockCache;
  }

  namespace sql
  {
    struct ScanContext
    {
      sstable::ObBlockIndexCache* block_index_cache_;                  // iput param, block cache & index cache
      sstable::ObBlockCache* block_cache_;
      chunkserver::ObMultiVersionTabletImage *tablet_image_;           // input param, set by ObTabletScan

      chunkserver::ObTablet * tablet_;                                 // find in %tablet_image_ by call acquire_tablet
      sstable::ObSSTableReader * sstable_reader_;                      // find in %tablet_ by call find_sstable

      compactsstablev2::ObCompactSSTableScanner::ScanContext compact_context_;
      ScanContext()
        : block_index_cache_(NULL), block_cache_(NULL),
        tablet_image_(NULL), tablet_(NULL), sstable_reader_(NULL) {}
    };

    // 用于CS从磁盘或缓冲区扫描一个tablet
    class ObSSTableScan : public ObRowkeyPhyOperator, public ObLastRowkeyInterface
    {
      public:
        ObSSTableScan();
        virtual ~ObSSTableScan();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_SSTABLE_SCAN; }
        virtual int get_next_row(const common::ObRowkey* &row_key, const common::ObRow *&row_value);
        virtual int open_scan_context(const sstable::ObSSTableScanParam& param, const ScanContext& context);

        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        virtual int get_tablet_data_version(int64_t &version);
        virtual int get_tablet_range(ObNewRange &range);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int get_last_rowkey(const ObRowkey *&rowkey);
        //add wenghaixing [secondary index static_index_build.cs_scan]20150416
        int open_scan_context_local_idx(const sstable::ObSSTableScanParam& param, const ScanContext& context, ObNewRange &fake_range);
        int init_sstable_scanner_for_local_idx(ObNewRange &fake_range);
        int reset_scanner();//free thread local memory here
        //add e
      private:
        int init_sstable_scanner();
        // disallow copy
        ObSSTableScan(const ObSSTableScan &other);
        ObSSTableScan& operator=(const ObSSTableScan &other);
      private:
        sstable::ObSSTableScanParam scan_param_;
        ScanContext scan_context_;
        ObSSTableScanner scanner_;
        compactsstablev2::ObCompactSSTableScanner compact_scanner_;
        ObRowkeyIterator* iterator_;
        const ObRowkey *last_rowkey_;
        int64_t sstable_version_;
        int64_t row_counter_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SSTABLE_SCAN_H */
