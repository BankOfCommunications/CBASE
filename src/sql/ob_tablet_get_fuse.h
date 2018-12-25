/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get_fuse.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_TABLET_GET_FUSE_H
#define _OB_TABLET_GET_FUSE_H 1

#include "ob_phy_operator.h"
#include "ob_tablet_fuse.h"
#include "ob_ups_multi_get.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTabletGetFuse : public ObTabletFuse 
    {
      public:
        ObTabletGetFuse();
        virtual ~ObTabletGetFuse() {}
        virtual void reset();
        virtual void reuse();
        int open();
        int close();
        virtual ObPhyOperatorType get_type() const { return PHY_TABLET_GET_FUSE; }
        int get_next_row(const common::ObRow *&row);

        int set_sstable_get(ObRowkeyPhyOperator *sstable_get);
        int set_incremental_get(ObRowkeyPhyOperator *incremental_get);

        int64_t to_string(char* buf, const int64_t buf_len) const;
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);

        int get_last_rowkey(const common::ObRowkey *&rowkey);

      private:
        ObRowkeyPhyOperator *sstable_get_;
        ObRowkeyPhyOperator *incremental_get_;
        ObRow curr_row_;
        int64_t data_version_;
        const ObRowkey *last_rowkey_;
    };
  }
}

#endif /* _OB_TABLET_GET_FUSE_H */
  

