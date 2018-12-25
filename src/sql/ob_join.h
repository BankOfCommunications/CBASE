/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_join.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_JOIN_H
#define _OB_JOIN_H 1

#include "sql/ob_sql_expression.h"
#include "sql/ob_phy_operator.h"
#include "sql/ob_double_children_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObJoin: public ObDoubleChildrenPhyOperator
    {
      public:
        ObJoin();
        virtual ~ObJoin();
        virtual void reset();
        virtual void reuse();
        virtual int get_next_row(const ObRow *&row);
        enum JoinType{
          INNER_JOIN,
	  SEMI_JOIN,
          LEFT_OUTER_JOIN,
          RIGHT_OUTER_JOIN,
          FULL_OUTER_JOIN,
          LEFT_SEMI_JOIN,
          RIGHT_SEMI_JOIN,
          LEFT_ANTI_SEMI_JOIN,
          RIGHT_ANTI_SEMI_JOIN
        };
        virtual int set_join_type(const JoinType join_type);
        virtual int add_equijoin_condition(const ObSqlExpression& expr);
        virtual int add_other_join_condition(const ObSqlExpression& expr);
        JoinType get_join_type() { return join_type_; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // disallow copy
        ObJoin(const ObJoin &other);
        ObJoin& operator=(const ObJoin &other);
      protected:
        JoinType join_type_;
        ObExpressionArray equal_join_conds_;
        ObExpressionArray other_join_conds_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_JOIN_H */
