/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_multi_get.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_ups_multi_get.h"
#include "common/utility.h"
#include "common/ob_trace_log.h"

using namespace oceanbase;
using namespace sql;

ObUpsMultiGet::ObUpsMultiGet()
  :get_param_(NULL),
  rpc_proxy_(NULL),
  got_row_count_(0),
  row_desc_(NULL),
  ts_timeout_us_(0)
{
}

ObUpsMultiGet::~ObUpsMultiGet()
{
}

void ObUpsMultiGet::reset()
{
  get_param_ = NULL;
  cur_new_scanner_.clear();
  got_row_count_ = 0;
  row_desc_ = NULL;
}

void ObUpsMultiGet::reuse()
{
  get_param_ = NULL;
  cur_new_scanner_.reuse();
  got_row_count_ = 0;
  row_desc_ = NULL;
}

bool ObUpsMultiGet::check_inner_stat()
{
  bool ret = false;
  ret = NULL != get_param_ && NULL != rpc_proxy_ && NULL != row_desc_ && ts_timeout_us_ > 0;
  if(!ret)
  {
    TBSYS_LOG(WARN, "get_param_[%p], rpc_proxy_[%p], row_desc_[%p], ts_timeout_us_[%ld]",
      get_param_, rpc_proxy_, row_desc_, ts_timeout_us_);
  }
  return ret;
}

int ObUpsMultiGet::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObUpsMultiGet, PHY_UPS_MULTI_GET);
  }
}

int64_t ObUpsMultiGet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "UpsMultiGet()\n");
  return pos;
}

int ObUpsMultiGet::open()
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;

  got_row_count_ = 0;

  FILL_TRACE_LOG("begin open ups multi get.");

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if(OB_SUCCESS != (ret = next_get_param()))
  {
    TBSYS_LOG(WARN, "construct next get param fail:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    int64_t remain_us = 0;
    if (is_timeout(&remain_us))
    {
      TBSYS_LOG(WARN, "process ups scan timeout, remain_us[%ld]", remain_us);
      ret = OB_PROCESS_TIMEOUT;
    }
    else if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_get(cur_get_param_, cur_new_scanner_, remain_us)))
    {
      TBSYS_LOG(WARN, "ups get rpc fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
    {
      TBSYS_LOG(WARN, "get is req fillfulled fail:ret[%d]", ret);
    }
    else
    {
      if(fullfilled_row_num > 0)
      {
        got_row_count_ += fullfilled_row_num;
      }
      else if(!is_fullfilled)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get no item:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObUpsMultiGet::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObUpsMultiGet::get_next_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  while(OB_SUCCESS == ret)
  {
    ret = cur_new_scanner_.get_next_row(rowkey, cur_ups_row_);
    if(OB_ITER_END == ret)
    {
      if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
      {
        TBSYS_LOG(WARN, "get is fullfilled fail:ret[%d]", ret);
      }
      else if(is_fullfilled)
      {
        ret = OB_ITER_END;
      }
      else
      {
        if(OB_SUCCESS != (ret = next_get_param()))
        {
          TBSYS_LOG(WARN, "construct next get param fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_get(cur_get_param_, cur_new_scanner_, ts_timeout_us_)))
        {
          TBSYS_LOG(WARN, "ups get rpc fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
        {
          TBSYS_LOG(WARN, "get is req fillfulled fail:ret[%d]", ret);
        }
        else
        {
          if(fullfilled_row_num > 0)
          {
            got_row_count_ += fullfilled_row_num;
          }
          else if(!is_fullfilled)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "get no item:ret[%d]", ret);
          }
        }
      }
    }
    else if(OB_SUCCESS == ret)
    {
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    row = &cur_ups_row_;
  }
  return ret;
}


/*
 * 如果一次rpc调用不能返回所以结果数据，通过next_get_param构造下次请求的
 * get_param
 * 这个函数隐含一个假设，ObNewScanner返回的数据必然在get_param的前面，且
 * 个数一样，这个在服务器端保证
 */
int ObUpsMultiGet::next_get_param()
{
  int ret = OB_SUCCESS;
  cur_get_param_.reset();
  cur_get_param_.set_version_range(get_param_->get_version_range());
  cur_get_param_.set_is_read_consistency(get_param_->get_is_read_consistency());
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  cur_get_param_.set_data_mark_param(get_param_->get_data_mark_param());
  //add duyr 20160531:e

  ObGetParam::ObRowIndex row_idx;

  if(got_row_count_ >= get_param_->get_row_size())
  {
    ret = OB_SIZE_OVERFLOW;
    TBSYS_LOG(WARN, "get row count size overflow:[%ld]", got_row_count_);
  }
  else
  {
    row_idx = get_param_->get_row_index()[got_row_count_];
  }

  if(OB_SUCCESS == ret)
  {
    for(int64_t i=row_idx.offset_;OB_SUCCESS == ret && i<get_param_->get_cell_size();i++)
    {
      if(OB_SUCCESS != (ret = cur_get_param_.add_cell((*(*get_param_)[i]))))
      {
        TBSYS_LOG(WARN, "get param add cell fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

void ObUpsMultiGet::set_row_desc(const ObRowDesc &row_desc)
{
  row_desc_ = &row_desc;
  cur_ups_row_.set_row_desc(*row_desc_);
}

int ObUpsMultiGet::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = row_desc_;
  return ret;
}
