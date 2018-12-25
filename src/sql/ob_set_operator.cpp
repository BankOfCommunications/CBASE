/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_set_operator.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_set_operator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSetOperator::ObSetOperator()
  :row_desc_(NULL),distinct_(false)
{
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

 ObSetOperator::~ObSetOperator()
{
}

void ObSetOperator::reset()
{
  row_desc_ = NULL;
  distinct_ = false;
  ObDoubleChildrenPhyOperator::reset();
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

void ObSetOperator::reuse()
{
  row_desc_ = NULL;
  distinct_ = false;
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

int ObSetOperator::get_next_row(const ObRow *&row)
{
  UNUSED(row);
  return OB_ERROR;
}

int ObSetOperator::set_distinct(bool is_distinct)
{
  distinct_ = is_distinct;
  return OB_SUCCESS;
}

//add peiouya [IN_TYPEBUG_FIX] 20151225:b
int ObSetOperator::get_output_columns_dsttype(common::ObArray<common::ObObjType> &output_columns_dsttype)
{
  output_columns_dsttype = output_columns_dsttype_;
  return OB_SUCCESS ;
}
int ObSetOperator::add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype)
{
  output_columns_dsttype_ = columns_dsttype;
  return OB_SUCCESS;
}
//add 20151225:e

PHY_OPERATOR_ASSIGN(ObSetOperator)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObSetOperator);
  row_desc_ = NULL;
  distinct_ = o_ptr->distinct_;
  output_columns_dsttype_ = o_ptr->output_columns_dsttype_;  //add peiouya [IN_TYPEBUG_FIX] 20151225
  return ret;
}

