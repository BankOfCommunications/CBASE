/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_single_child_phy_operator.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_single_child_phy_operator.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSingleChildPhyOperator::ObSingleChildPhyOperator()
  :child_op_(NULL)
{
}

ObSingleChildPhyOperator::~ObSingleChildPhyOperator()
{
}

void ObSingleChildPhyOperator::reset()
{
  child_op_ = NULL;
}

void ObSingleChildPhyOperator::reuse()
{
  child_op_ = NULL;
}

int ObSingleChildPhyOperator::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  if (NULL != child_op_)
  {
    ret = OB_INIT_TWICE;
    TBSYS_LOG(ERROR, "child_op_ already init");
  }
  else if (0 != child_idx)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "invalid child idx=%d", child_idx);
  }
  else
  {
    child_op_ = &child_operator;
  }
  return ret;
}

ObPhyOperator *ObSingleChildPhyOperator::get_child(int32_t child_idx) const
{
  ObPhyOperator *ret = NULL;
  if (0 == child_idx)
  {
    ret = child_op_;
  }
  return ret;
}

int ObSingleChildPhyOperator::open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    if (OB_SUCCESS != (ret = child_op_->open()))
    {
      if (!IS_SQL_ERR(ret))
      {
        TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
      }
    }
  }
  return ret;
}

//add wenghaixing [secondary index secondary index static_index_build.consistency]20150424
int64_t ObSingleChildPhyOperator::get_index_num() const
{
  return 0;
}

//add e

int ObSingleChildPhyOperator::close()
{
  int ret = OB_SUCCESS;
  if (NULL != child_op_)
  {
    if (OB_SUCCESS != (ret = child_op_->close()))
    {
      TBSYS_LOG(WARN, "failed to close child_op, err=%d", ret);
    }
  }
  return ret;
}

int32_t ObSingleChildPhyOperator::get_child_num() const
{
  return 1;
}
