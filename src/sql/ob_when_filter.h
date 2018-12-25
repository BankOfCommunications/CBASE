/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_when_filter.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_WHEN_FILTER_H_
#define OCEANBASE_SQL_OB_WHEN_FILTER_H_
#include "ob_multi_children_phy_operator.h"
#include "ob_phy_operator_type.h"
#include "ob_sql_expression.h"
#include "common/ob_se_array.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    class ObWhenFilter: public ObMultiChildrenPhyOperator
    {
      static const int64_t FILTER_PREALLOCATED_NUM = 64;
      public:
        ObWhenFilter();
        virtual ~ObWhenFilter();
        virtual void reset();
        virtual void reuse();
        void set_when_number(const int64_t when_num);
        int add_filter(const ObSqlExpression& expr);  
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int32_t get_child_num() const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const;

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObWhenFilter(const ObWhenFilter &other);
        ObWhenFilter& operator=(const ObWhenFilter &other);
      protected:
        // data members
        common::ObSEArray<ObSqlExpression, FILTER_PREALLOCATED_NUM, ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > filters_;
        bool could_do_next_;
        int32_t child_num_;
        int64_t when_number_;
        common::ObRow tmp_row;
    };

    inline void ObWhenFilter::set_when_number(const int64_t when_num)
    {
      when_number_ = when_num;
    }
    
    inline int32_t ObWhenFilter::get_child_num() const
    {
      int child_num = child_num_;
      if (child_num_ < ObMultiChildrenPhyOperator::get_child_num())
      {
        child_num = ObMultiChildrenPhyOperator::get_child_num();
      }
      return child_num;
    }
    
    inline ObPhyOperatorType ObWhenFilter::get_type() const
    {
      return PHY_WHEN_FILTER;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_WHEN_FILTER_H_ */


