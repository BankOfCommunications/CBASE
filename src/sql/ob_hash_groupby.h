/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_hash_groupby.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_HASH_GROUPBY_H
#define _OB_HASH_GROUPBY_H 1

#include "sql/ob_groupby.h"
#include "ob_sql_expression.h"

namespace oceanbase
{
  namespace sql
  {
    class ObHashGroupBy: public ObGroupBy
    {
      public:
        ObHashGroupBy();
        virtual ~ObHashGroupBy();

        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_HASH_GROUP_BY; }
        virtual int get_next_row(const common::ObRow *&row);

        virtual int add_group_column(const ObSqlExpression &expr);
        virtual int add_agg_column(const ObSqlExpression &expr);

        void set_backets_param(const int64_t backets_num, const int64_t backet_mem_size);
      private:
        // disallow copy
        ObHashGroupBy(const ObHashGroupBy &other);
        ObHashGroupBy& operator=(const ObHashGroupBy &other);
      private:
        // data members
        ObPhyOpearator *child_op_;
        ObArray<columnExpr> group_columns_;
        ObArray<columnExpr> other_columns_;
        ObArray<columnExpr> agg_columns_;
        common::ObRow curr_row_;
        common::ObRowDesc row_desc_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_HASH_GROUPBY_H */

