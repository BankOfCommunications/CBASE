/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multiple_merge.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_multiple_merge.h"

using namespace oceanbase;
using namespace sql;

ObMultipleMerge::ObMultipleMerge()
  :child_num_(0),
  is_ups_row_(true)
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  ,data_mark_param_()
  // add by maosy [Delete_Update_Function_isolation_RC] 20161228
//  ,data_mark_final_row_()
//  ,data_mark_final_row_desc_()
//add by maosy 
  // add by maosy [Delete_Update_Function_isolation_RC] 20161218
  ,stmt_start_time_(0)
  //add e
  //add duyr 20160531:e
{
  //memset(child_array_, 0, sizeof(ObPhyOperator *) * MAX_CHILD_OPERATOR_NUM);
}

ObMultipleMerge::~ObMultipleMerge()
{
}

void ObMultipleMerge::reset()
{
  allocator_.reuse();
  child_num_ = 0;
  is_ups_row_ = true;
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  data_mark_param_.reset();
//  cur_data_mark_.reset();// add by maosy [Delete_Update_Function_isolation_RC] 20161228
  // add by maosy [Delete_Update_Function_isolation_RC] 20161218
  stmt_start_time_ =0 ;
  //add by maosy
  //add duyr 20160531:e
}

void ObMultipleMerge::reuse()
{
  allocator_.reuse();
  child_num_ = 0;
  is_ups_row_ = true;
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  data_mark_param_.reset();
//  cur_data_mark_.reset();// add by maosy [Delete_Update_Function_isolation_RC] 20161228
  // add by maosy [Delete_Update_Function_isolation_RC] 20161218
  stmt_start_time_ =0 ;
  //add by maosy
  //add duyr 20160531:e
}

int ObMultipleMerge::get_row_desc(const ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (child_num_ <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not child setted");
  }
  else
  {
    ret = child_array_[0]->get_row_desc(row_desc);
  }
  return ret;
}

int ObMultipleMerge::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;

  if (child_idx >= MAX_CHILD_OPERATOR_NUM)
  {
    ret = OB_SIZE_OVERFLOW;
    TBSYS_LOG(WARN, "child_idx[%d] is overflow", child_idx);
  }
  else if (child_num_ < child_idx + 1)
  {
    child_num_ = child_idx + 1;
  }
  if (OB_SUCCESS == ret)
  {
    child_array_[child_idx] = &child_operator;
  }
  return ret;
}

ObPhyOperator *ObMultipleMerge::get_child(int32_t child_idx) const
{
  ObPhyOperator *ret = NULL;
  if (child_idx >= 0 && child_idx < child_num_)
  {
    ret = child_array_[child_idx];
  }
  return ret;
}

int32_t ObMultipleMerge::get_child_num() const
{
  return child_num_;
}

int ObMultipleMerge::copy_rowkey(const ObRow &row, ObRow &result_row, bool deep_copy)
{
  int ret = OB_SUCCESS;
  const ObRowDesc *row_desc = NULL;
  const ObObj *cell = NULL;
  ObObj value;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  row_desc = row.get_row_desc();
  if (NULL == row_desc)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "row_desc is null");
  }

  for (int i = 0; OB_SUCCESS == ret && i < row_desc->get_rowkey_cell_count(); i ++)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id) ))
    {
      TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
    }
    if (OB_SUCCESS == ret && deep_copy)
    {
      if (OB_SUCCESS != (ret = ob_write_obj(allocator_, *cell, value) ))
      {
        TBSYS_LOG(WARN, "fail to write obj:ret[%d]", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = result_row.raw_set_cell(i, deep_copy ? value : *cell)))
      {
        TBSYS_LOG(WARN, "fail to set cell:ret[%d]", ret);
      }
    }
  }
  return ret;
}

void ObMultipleMerge::set_is_ups_row(bool is_ups_row)
{
  is_ups_row_ = is_ups_row;
}

//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
int ObMultipleMerge::set_data_mark_param(const ObDataMarkParam &param)
{
    int ret = OB_SUCCESS;
    if (!param.is_valid())
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR,"invaid data mark param[%s]!ret=%d",
                  to_cstring(param),ret);
    }
    else
    {
        data_mark_param_ = param;
    }
    return ret;
}

const ObDataMarkParam& ObMultipleMerge::get_data_mark_param()const
{
    return data_mark_param_;
}
//add duyr 20160531:e

PHY_OPERATOR_ASSIGN(ObMultipleMerge)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObMultipleMerge);
  reset();
  is_ups_row_ = o_ptr->is_ups_row_;
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  data_mark_param_ = o_ptr->data_mark_param_;
  //add duyr 20160531:e
  return ret;
}

DEFINE_SERIALIZE(ObMultipleMerge)
{
 int ret = OB_SUCCESS;
 if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, child_num_)))
 {
   TBSYS_LOG(WARN, "fail to encode child_num_:ret[%d]", ret);
 }
 else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_ups_row_)))
 {
   TBSYS_LOG(WARN, "fail to encode is_ups_row_:ret[%d]", ret);
 }
 //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
 else if (OB_SUCCESS != (ret = data_mark_param_.serialize(buf,buf_len,pos)))
 {
   TBSYS_LOG(WARN,"fail to serialize data mark param!ret=%d",ret);
 }
 //add duyr 20160531:e
 return ret;
}

DEFINE_DESERIALIZE(ObMultipleMerge)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &child_num_)))
  {
    TBSYS_LOG(WARN, "fail to decode child_num_:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_ups_row_)))
  {
    TBSYS_LOG(WARN, "faio to decode is_ups_row_:ret[%d]", ret);
  }
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  else if (OB_SUCCESS != (ret = data_mark_param_.deserialize(buf,data_len,pos)))
  {
      TBSYS_LOG(WARN,"fail to deserialize data mark param!ret=%d",ret);
  }
  //add duyr 20160531:e
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObMultipleMerge)
{
  // TODO
  return 0;
}
