/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_no_children_phy_operator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_NO_CHILDREN_PHY_OPERATOR_H
#define _OB_NO_CHILDREN_PHY_OPERATOR_H 1
#include "sql/ob_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObNoChildrenPhyOperator: public ObPhyOperator
    {
      public:
        ObNoChildrenPhyOperator() {}
        virtual ~ObNoChildrenPhyOperator() {}
        virtual void reset() = 0;
        virtual void reuse() = 0;
        /// @note always return OB_NOT_SUPPORTED
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
      private:
        // types and constants
      private:
        // disallow copy
        ObNoChildrenPhyOperator(const ObNoChildrenPhyOperator &other);
        ObNoChildrenPhyOperator& operator=(const ObNoChildrenPhyOperator &other);
        // function members
      private:
        // data members
    };

    inline int ObNoChildrenPhyOperator::set_child(int32_t child_idx, ObPhyOperator &child_operator)
    {
      UNUSED(child_idx);
      UNUSED(child_operator);
      return common::OB_NOT_SUPPORTED;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_NO_CHILDREN_PHY_OPERATOR_H */
