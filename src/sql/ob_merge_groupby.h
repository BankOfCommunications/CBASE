/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_groupby.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MERGE_GROUPBY_H
#define _OB_MERGE_GROUPBY_H 1
#include "ob_groupby.h"
#include "ob_aggregate_function.h"
namespace oceanbase
{
  namespace sql
  {
    class ObScalarAggregate;

    // 输入数据已经按照groupby列排序
    class ObMergeGroupBy: public ObGroupBy
    {
      public:
        ObMergeGroupBy();
        virtual ~ObMergeGroupBy();
        virtual void reset();
        virtual void reuse();
        virtual void set_int_div_as_double(bool did);
        virtual bool get_int_div_as_double() const;

        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_MERGE_GROUP_BY; }
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        // used by ScalarAggregate operator when there's no input rows
        int get_row_for_empty_set(const ObRow *&row);
        //add liumz, [ROW_NUMBER]20150824
        void set_analytic_func(const bool &is_analytic);
        bool is_analytic_func();
        //add:e


        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        int is_same_group(const ObRow &row1, const ObRow &row2, bool &result);
        // disallow copy
        ObMergeGroupBy(const ObMergeGroupBy &other);
        ObMergeGroupBy& operator=(const ObMergeGroupBy &other);
        // friends
        friend class ObScalarAggregate;
      private:
        // data members
        ObAggregateFunction aggr_func_;
        const ObRow *last_input_row_;
        bool is_analytic_func_;//add liumz, [ROW_NUMBER]20150824
    };

    inline void ObMergeGroupBy::set_int_div_as_double(bool did)
    {
      aggr_func_.set_int_div_as_double(did);
    }

    inline int ObMergeGroupBy::get_row_for_empty_set(const ObRow *&row)
    {
      return aggr_func_.get_result_for_empty_set(row);
    }

    inline bool ObMergeGroupBy::get_int_div_as_double() const
    {
      return aggr_func_.get_int_div_as_double();
    }

    //add liumz, [ROW_NUMBER]20150824
    inline void ObMergeGroupBy::set_analytic_func(const bool &is_analytic)
    {
      is_analytic_func_ = is_analytic;
    }

    inline bool ObMergeGroupBy::is_analytic_func()
    {
      return is_analytic_func_;
    }
    //add:e
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MERGE_GROUPBY_H */
