/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_groupby.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */

/*
Every unary aggregate function takes an arbitrary <value expression> as the argument; most unary aggregate
functions can optionally be qualified with either DISTINCT or ALL. Of the rows in the aggregation, the following
do not qualify:
— If DISTINCT is specified, then redundant duplicates.
— Every row in which the <value expression> evaluates to the null value.
If no row qualifies, then the result of COUNT is 0 (zero), and the result of any other aggregate function is the
null value.
Otherwise (i.e., at least one row qualifies), the result of the aggregate function is:
— If COUNT <value expression> is specified, then the number of rows that qualify.
— If SUM is specified, then the sum of <value expression> evaluated for each row that qualifies.
— If AVG is specified, then the average of <value expression> evaluated for each row that qualifies.
— If MAX is specified, then the maximum value of <value expression> evaluated for each row that qualifies.
— If MIN is specified, then the minimum value of <value expression> evaluated for each row that qualifies.
 */

#ifndef _OB_GROUPBY_H
#define _OB_GROUPBY_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_array.h"
namespace oceanbase
{
  namespace sql
  {
    struct ObGroupColumn
    {
      uint64_t table_id_;
      uint64_t column_id_;

      ObGroupColumn()
        :table_id_(common::OB_INVALID_ID), column_id_(common::OB_INVALID_ID)
      {
      }
    };

    class ObGroupBy: public ObSingleChildPhyOperator
    {
      public:
        ObGroupBy();
        virtual ~ObGroupBy();
        virtual void reset();
        virtual void reuse();
        virtual int get_next_row(const common::ObRow *&row) = 0;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        /// 添加一个group column
        /// if there is no group columns, this is a scalar aggragation operator
        virtual int add_group_column(const uint64_t tid, const uint64_t cid);
        /// 添加一个aggregate column
        virtual int add_aggr_column(const ObSqlExpression& expr);
        /// 添加一个partition_column, store as ObGroupColumn
        virtual int add_partition_column(const uint64_t tid, const uint64_t cid);//add liumz, [ROW_NUMBER]20150826
        /// 添加一个analytic column
        virtual int add_anal_column(const ObSqlExpression& expr);//add liumz, [ROW_NUMBER]20150824
        /// set memory limit
        void set_mem_size_limit(const int64_t limit);
        /// whether integer division as double
        virtual void set_int_div_as_double(bool did) = 0;
        virtual bool get_int_div_as_double() const = 0;

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObGroupBy(const ObGroupBy &other);
        ObGroupBy& operator=(const ObGroupBy &other);
      protected:
        common::ObArray<ObGroupColumn> group_columns_;
        ObExpressionArray aggr_columns_;
        common::ObArray<ObGroupColumn> partition_columns_;//add liumz, [ROW_NUMBER]20150826
        ObExpressionArray anal_columns_;//add liumz, [ROW_NUMBER]20150824, store analytic func expr
        int64_t mem_size_limit_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_GROUPBY_H */
