/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert_dbsem_filter.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef _OB_UPDATE_DBSEM_FILTER_H
#define _OB_UPDATE_DBSEM_FILTER_H 1

#include "ob_single_child_phy_operator.h"
#include "common/ob_row_store.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    // update语义过滤器
    // check rowkey duplication when update rowkey
    // lbzhong [Update rowkey] 20160418
    class ObUpdateDBSemFilter: public ObSingleChildPhyOperator
    {
      public:
        ObUpdateDBSemFilter();
        ~ObUpdateDBSemFilter();
        virtual void reset();
        virtual void reuse();
      public:
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int get_duplicated_row(const ObRow *&row, ObRow &duplicated_row, ObRowDesc& row_desc) const;

        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const{return PHY_UPDATE_DB_SEM_FILTER;}
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        bool could_update_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPDATE_DBSEM_FILTER_H */
