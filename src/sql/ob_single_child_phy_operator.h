/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_single_child_phy_operator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SINGLE_CHILD_PHY_OPERATOR_H
#define _OB_SINGLE_CHILD_PHY_OPERATOR_H 1
#include "ob_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSingleChildPhyOperator: public ObPhyOperator
    {
      public:
        ObSingleChildPhyOperator();
        virtual ~ObSingleChildPhyOperator();
        /// set the only one child
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        /// get the only one child
        virtual ObPhyOperator *get_child(int32_t child_idx) const;
        virtual int32_t get_child_num() const;
        /// open child_op_
        virtual int open();
        /// close child_op_
        virtual int close();
        virtual void reset() = 0;
        virtual void reuse() = 0;
        //add wenghaixing [secondary index secondary index static_index_build.consistency]20150424
        virtual int64_t get_index_num()const ;
        //add e
      private:
        // disallow copy
        ObSingleChildPhyOperator(const ObSingleChildPhyOperator &other);
        ObSingleChildPhyOperator& operator=(const ObSingleChildPhyOperator &other);
      protected:
        // data members
        ObPhyOperator *child_op_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SINGLE_CHILD_PHY_OPERATOR_H */

