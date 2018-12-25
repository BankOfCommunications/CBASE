/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multi_children_phy_operator.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_MULTI_CHILDREN_PHY_OPERATOR_H_
#define OCEANBASE_SQL_OB_MULTI_CHILDREN_PHY_OPERATOR_H_
#include "ob_phy_operator.h"
#include "common/ob_se_array.h"
#include "common/ob_define.h"
namespace oceanbase
{
  namespace sql
  {
    class ObMultiChildrenPhyOperator: public ObPhyOperator
    {
      public:
        ObMultiChildrenPhyOperator();
        virtual ~ObMultiChildrenPhyOperator();
        /// multi children
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
      private:
        // disallow copy
        ObMultiChildrenPhyOperator(const ObMultiChildrenPhyOperator &other);
        ObMultiChildrenPhyOperator& operator=(const ObMultiChildrenPhyOperator &other);
      protected:
        // data members
        common::ObSEArray<ObPhyOperator*, common::OB_PREALLOCATED_NUM> children_ops_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_MULTI_CHILDREN_PHY_OPERATOR_H_ */


