/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_limit.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_LIMIT_H
#define _OB_LIMIT_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"

namespace oceanbase
{
  namespace sql
  {
    class ObLimit: public ObSingleChildPhyOperator
    {
      public:
        ObLimit();
        virtual ~ObLimit();
        virtual void reset();
        virtual void reuse();
        /// @param limit -1 means no limit
        int set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset);
        int get_limit(int64_t &limit, int64_t &offset) const;
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const;

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObLimit(const ObLimit &other);
        ObLimit& operator=(const ObLimit &other);
        int get_int_value(const ObSqlExpression& in_val, int64_t& out_val) const;
      private:
        // data members
        ObSqlExpression org_limit_;
        ObSqlExpression org_offset_;
        bool is_instantiated_;
        int64_t limit_;         // -1 means no limit
        int64_t offset_;
        int64_t input_count_;
        int64_t output_count_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_LIMIT_H */
