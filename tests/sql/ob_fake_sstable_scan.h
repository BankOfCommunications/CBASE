/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_sstable_scan.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_FAKE_SSTABLE_SCAN_H
#define _OB_FAKE_SSTABLE_SCAN_H 1

#include "sql/ob_rowkey_phy_operator.h"
#include "ob_file_table.h"
#include "sql/ob_sstable_scan.h"

namespace oceanbase
{
  using namespace common;

  namespace sql 
  {
    namespace test
    {
      class ObFakeSSTableScan : public ObSSTableScan 
      {
        public:
          ObFakeSSTableScan(const char *file_name);
          virtual ~ObFakeSSTableScan() {};

          int set_child(int32_t child_idx, ObPhyOperator &child_operator);
          int open();
          int close();
          int get_next_row(const ObRowkey *&rowkey, const ObRow *&row);
          int64_t to_string(char* buf, const int64_t buf_len) const;
          int set_scan_param(const sstable::ObSSTableScanParam &param);
          int get_row_desc(const common::ObRowDesc *&row_desc) const;
        
          int get_tablet_data_version(int64_t &version)
          {
            version = 0;
            return OB_SUCCESS;
          }

        private:
          // disallow copy
          ObFakeSSTableScan(const ObFakeSSTableScan &other);
          ObFakeSSTableScan& operator=(const ObFakeSSTableScan &other);

        private:
          // data members
          ObFileTable file_table_;
          ObRowkey cur_rowkey_;
          ObObj rowkey_obj_;
          sstable::ObSSTableScanParam scan_param_;
          ObRow curr_row_;
          ObRowDesc row_desc_;
      };
    }
  }
}

#endif /* _OB_FAKE_SSTABLE_SCAN_H */

