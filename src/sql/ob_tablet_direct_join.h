/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_direct_join.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_TABLET_DIRECT_JOIN_H
#define _OB_TABLET_DIRECT_JOIN_H 1

#include "ob_tablet_join.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTabletDirectJoin : public ObTabletJoin
    {
      public:
        ObTabletDirectJoin();
        virtual ~ObTabletDirectJoin();

        virtual int open();
        virtual void reset();
        virtual enum ObPhyOperatorType get_type() const
        {
          return PHY_TABLET_DIRECT_JOIN;
        }
      private:
        int get_ups_row(const ObRowkey &rowkey, ObUpsRow &ups_row, const ObGetParam &get_param);
        int gen_get_param(ObGetParam &get_param, const ObRow &fused_row);
        int fetch_fused_row_prepare();

      private:
        const ObRowkey *last_ups_rowkey_;
        const ObUpsRow *last_ups_row_;
    };
  }
}

#endif /* _OB_TABLET_DIRECT_JOIN_H */
