/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_double_children_phy_operator.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>

 *
 */
#include "ob_double_children_phy_operator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDoubleChildrenPhyOperator::ObDoubleChildrenPhyOperator()
  :left_op_(NULL), right_op_(NULL)
{
}

ObDoubleChildrenPhyOperator::~ObDoubleChildrenPhyOperator()
{
}

void ObDoubleChildrenPhyOperator::reset()
{
  left_op_ = NULL;
  right_op_ = NULL;
}

void ObDoubleChildrenPhyOperator::reuse()
{
  left_op_ = NULL;
  right_op_ = NULL;
}

int ObDoubleChildrenPhyOperator::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  if (0 == child_idx)
  {
    if (NULL != left_op_)
    {
      ret = OB_INIT_TWICE;
      TBSYS_LOG(ERROR, "left operator already init");
    }
    else
    {
      left_op_ = &child_operator;
    }
  }
  else if (1 == child_idx)
  {
    if (NULL != right_op_)
    {
      ret = OB_INIT_TWICE;
      TBSYS_LOG(ERROR, "right operator already init");
    }
    else
    {
      right_op_ = &child_operator;
    }
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "invalid child idx=%d", child_idx);
  }
  return ret;
}

int ObDoubleChildrenPhyOperator::open()
{
  int ret = OB_SUCCESS;
  if (NULL == left_op_ || NULL == right_op_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child(ren) operator(s) is/are NULL");
  }
  else
  {
    if (OB_SUCCESS != (ret = left_op_->open()) || OB_SUCCESS != (ret = right_op_->open()))
    {
      TBSYS_LOG(WARN, "failed to open child(ren) operator(s), err=%d", ret);
    }
  }
  return ret;
}

int ObDoubleChildrenPhyOperator::close()
{
  int ret = OB_SUCCESS;
  if (NULL != left_op_ && NULL != right_op_)
  {
    if (OB_SUCCESS != (ret = left_op_->close()) || OB_SUCCESS != (ret = right_op_->close()))
    {
      TBSYS_LOG(WARN, "failed to close child(ren) operator(s), err=%d", ret);
    }
  }
  return ret;
}

int32_t ObDoubleChildrenPhyOperator::get_child_num() const
{
  return 2;
}

ObPhyOperator *ObDoubleChildrenPhyOperator::get_child(int32_t child_idx) const
{
  ObPhyOperator *ret = NULL;
  if (0 == child_idx)
  {
    ret = left_op_;
  }
  else if (1 == child_idx)
  {
    ret = right_op_;
  }
  return ret;
}
