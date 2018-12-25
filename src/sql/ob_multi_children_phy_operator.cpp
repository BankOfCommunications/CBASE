/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multi_children_phy_operator.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#include "ob_multi_children_phy_operator.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMultiChildrenPhyOperator::ObMultiChildrenPhyOperator()
{
}

ObMultiChildrenPhyOperator::~ObMultiChildrenPhyOperator()
{
}

void ObMultiChildrenPhyOperator::reset()
{
  children_ops_.clear();
}


void ObMultiChildrenPhyOperator::reuse()
{
  children_ops_.clear();
}

int ObMultiChildrenPhyOperator::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  if (child_idx < children_ops_.count())
  {
    ret = OB_INIT_TWICE;
    TBSYS_LOG(ERROR, "The %dth operator already init, ret=%d", child_idx, ret);
  }
  else if (child_idx > children_ops_.count())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "Child physical operator must be set in order, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = children_ops_.push_back(&child_operator)))
  {
    TBSYS_LOG(ERROR, "Add child physical operator failed, ret=%d", ret);
  }
  return ret;
}

ObPhyOperator *ObMultiChildrenPhyOperator::get_child(int32_t child_idx) const
{
  ObPhyOperator *op = NULL;
  if (child_idx >= 0 && child_idx < children_ops_.count())
  {
    op = children_ops_.at(child_idx);
  }
  else
  {
    TBSYS_LOG(WARN, "Invalid child index, index=%d, count=%ld", child_idx, children_ops_.count());
  }
  return op;
}

int ObMultiChildrenPhyOperator::open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(children_ops_.count() <= 0))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "No child op");
  }
  else
  {
    for (int32_t i = 0; ret == OB_SUCCESS && i < children_ops_.count(); i++)
    {
      if ((ret = children_ops_.at(i)->open()) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "failed to open %dth child_op, err=%d", i, ret);
        break;
      }
    }
  }
  return ret;
}

int ObMultiChildrenPhyOperator::close()
{
  int ret = OB_SUCCESS;
  //for (int32_t i = 0; ret == OB_SUCCESS && i < children_ops_.count(); i++)
  //mod dolphin children ops not close lead to leak bug fix@20160904
  for (int32_t i = 0; i < children_ops_.count(); i++)
  {
    if ((ret = children_ops_.at(i)->close()) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "failed to close %dth child_op, err=%d", i, ret);
    }
  }
  return ret;
}

int32_t ObMultiChildrenPhyOperator::get_child_num() const
{
  return static_cast<int32_t>(children_ops_.count());
}

