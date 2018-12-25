/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_empty_row_filter.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_EMPTY_ROW_FILTER_H
#define _OB_EMPTY_ROW_FILTER_H 1

#include "ob_single_child_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObEmptyRowFilter : public ObSingleChildPhyOperator
    {
      public:
        int open();
        virtual void reset();
        virtual void reuse();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const;

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        common::ObRowDesc cur_row_desc_;
        common::ObRow cur_row_;
    };
  }
}

#endif /* _OB_EMPTY_ROW_FILTER_H */
