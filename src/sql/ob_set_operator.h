/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_set_operator.h
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_SET_OPERATOR_H_
#define OCEANBASE_SQL_OB_SET_OPERATOR_H_

#include "sql/ob_phy_operator.h"
#include "sql/ob_double_children_phy_operator.h"
#include "common/ob_row.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSetOperator: public ObDoubleChildrenPhyOperator
    {
      public:
        ObSetOperator();
        ~ObSetOperator();
        virtual void reset();
        virtual void reuse();
        /**
         * 设置集合操作是否为DISTINCT
         *
         * @param is_distinct [in] 集合操作为DISTINCT/ALL类型
         *
         * @return OB_SUCCESS或错误码
         */
        virtual int set_distinct(bool is_distinct);
        bool is_distinct() const;
        /**
         * 获得下一行的引用
         * @note 在下次调用get_next或者close前，返回的row有效
         * @pre 调用open()
         * @param row [out]
         *
         * @return OB_SUCCESS或OB_ITER_END或错误码
         */
        virtual int get_next_row(const common::ObRow *&row);

        DECLARE_PHY_OPERATOR_ASSIGN;
      //add peiouya [IN_TYPEBUG_FIX] 20151225:b
      public:
        int get_output_columns_dsttype(common::ObArray<common::ObObjType> &output_columns_dsttype);
        int add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype);
      //add 20151225:e
      private:
        DISALLOW_COPY_AND_ASSIGN(ObSetOperator);
    
      protected:
        // 结果的RowDesc为第一个操作符的RowDesc
        const common::ObRowDesc *row_desc_;
        // Union ALL or Union DISTINCT
        bool distinct_;
        //add peiouya [IN_TYPEBUG_FIX] 20151225:b
        common::ObArray<common::ObObjType> output_columns_dsttype_;
        //add 20151225:e
    };
    inline bool ObSetOperator::is_distinct() const
    {
      return distinct_;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SET_OPERATOR_H_ */

