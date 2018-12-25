/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_get.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_SSTABLE_GET_H
#define _OB_SSTABLE_GET_H 1

#include "ob_rowkey_phy_operator.h"
#include "common/ob_iterator_adaptor.h"
#include "ob_last_rowkey_interface.h"
#include "chunkserver/ob_tablet_manager.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSSTableGet : public ObRowkeyPhyOperator, public ObLastRowkeyInterface
    {
      public:
        ObSSTableGet();
        virtual ~ObSSTableGet() {};

        int open();
        int close();
        virtual void reset();
        virtual void reuse();
        virtual ObPhyOperatorType get_type() const { return PHY_SSTABLE_GET; }
        int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        int64_t to_string(char* buf, const int64_t buf_len) const;

        int open_tablet_manager(chunkserver::ObTabletManager *tablet_manager,
                                const ObGetParam *get_param);

        int get_tablet_data_version(int64_t &data_version);
        int get_last_rowkey(const common::ObRowkey *&rowkey);

      private:
        chunkserver::ObTabletManager *tablet_manager_;
        const ObGetParam *get_param_;
        int64_t tablet_version_;
        common::ObRowIterAdaptor row_iter_;
        sstable::ObSSTableGetter sstable_getter_;
        ObRowDesc row_desc_;
        const ObRowkey *last_rowkey_;
    };
  }
}

#endif /* _OB_SSTABLE_GET_H */
