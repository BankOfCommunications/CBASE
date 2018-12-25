/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_operator.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_merger_operator.h"
#include "common/ob_scan_param.h"
using namespace oceanbase;
using namespace common;
using namespace mergeserver;
ObMergerOperator::ObMergerOperator()
{
  scan_param_ = NULL;
  res_iterator_ = NULL;
  status_ = NOT_INIT;
  sealed_ = false;
  last_sharding_res_ = NULL;
  sharding_res_count_ = 0;
  max_sharding_res_size_ = -1;
  total_sharding_res_size_ = 0;
}

ObMergerOperator::~ObMergerOperator()
{
  reset();
  status_ = NOT_INIT;
}

void ObMergerOperator::set_max_res_size(const int64_t size)
{
  max_sharding_res_size_ = size;
}

int64_t ObMergerOperator::get_max_res_size(void) const
{
  return max_sharding_res_size_;
}

void ObMergerOperator::reset()
{
  scan_param_ = NULL;
  res_iterator_ = NULL;
  cells_.clear();
  return_operator_.clear();
  sorted_operator_.reset();
  reversed_operator_.reset();
  groupby_operator_.reset();
  sealed_ = false;
  last_sharding_res_ = NULL;
  sharding_res_count_ = 0;
  total_sharding_res_size_ = 0;
}

int ObMergerOperator::set_param(const int64_t max_memory_size, const ObScanParam & scan_param)
{
  int err = OB_SUCCESS;
  reset();
  scan_param_ = &scan_param;
  if ((scan_param_->get_group_by_param().get_aggregate_row_width() == 0)
    && (scan_param_->get_orderby_column_size() == 0))
  {
    if (scan_param_->get_scan_direction() == ScanFlag::FORWARD)
    {
      status_ = USE_SORTED_OPERATOR;
      if (OB_SUCCESS != (err = sorted_operator_.set_param(*scan_param_)))
      {
        TBSYS_LOG(WARN,"fail to set scan param to sorted_operator_ [err:%d]", err);
      }
    }
    else
    {
      status_ = USE_REVERSE_OPERATOR;
      if (OB_SUCCESS != (err = reversed_operator_.set_param(*scan_param_,cells_)))
      {
        TBSYS_LOG(WARN,"fail to set scan param to sorted_operator_ [err:%d]", err);
      }
    }
  }
  else
  {
    status_ = USE_GROUPBY_OPERATOR;
    if (OB_SUCCESS != (err = groupby_operator_.set_param(max_memory_size, *scan_param_)))
    {
      TBSYS_LOG(WARN,"fail to set scan param to groupby_operator_ [err:%d]", err);
    }
  }
  if (OB_SUCCESS != err)
  {
    reset();
  }
  return err;
}

int ObMergerOperator::add_sharding_result(ObScanner & sharding_res, 
  const ObNewRange & query_range, const int64_t limit_offset, bool &is_finish, bool &can_free_res)
{
  int err = OB_SUCCESS;
  can_free_res = false;
  if (OB_NOT_INIT == status_)
  {
    TBSYS_LOG(WARN,"please set param first");
    err = OB_INVALID_ARGUMENT;
  }
  else if (sealed_)
  {
    TBSYS_LOG(WARN,"seal operator has already sealed");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    switch (status_)
    {
    case USE_SORTED_OPERATOR:
      err = sorted_operator_.add_sharding_result(sharding_res,query_range,is_finish);
      break;
    case USE_REVERSE_OPERATOR:
      err = reversed_operator_.add_sharding_result(sharding_res,query_range,is_finish);
      break;
    case USE_GROUPBY_OPERATOR:
      err = groupby_operator_.add_sharding_result(sharding_res,query_range,limit_offset, is_finish);
      can_free_res = true;
      break;
    default:
      TBSYS_LOG(ERROR, "status error [status_:%d]", status_);
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to add sharding result [err:%d,status_:%d]", err, status_);
    }
    else
    {
      total_sharding_res_size_ += sharding_res.get_size();
    }
  }
  if (OB_SUCCESS == err)
  {
    last_sharding_res_ = &sharding_res;
    sharding_res_count_ ++;
  }
  /// for anti some max query attack
  if ((OB_SUCCESS == err) && (max_sharding_res_size_ > 0)
      && (total_sharding_res_size_ > max_sharding_res_size_))
  {
    TBSYS_LOG(WARN, "find sharding res size exceed the max size:total[%ld], max[%ld]",
        total_sharding_res_size_, max_sharding_res_size_);
    err = OB_MEM_OVERFLOW;
  }
  return err;
}

int ObMergerOperator::seal()
{
  int err = OB_SUCCESS;
  if (OB_NOT_INIT == status_)
  {
    TBSYS_LOG(WARN,"please set param first");
    err = OB_INVALID_ARGUMENT;
  }
  else if (sealed_)
  {
    TBSYS_LOG(WARN,"seal operator twice");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    switch (status_)
    {
    case USE_SORTED_OPERATOR:
      err = sorted_operator_.seal();
      res_iterator_ = &sorted_operator_;
      break;
    case USE_REVERSE_OPERATOR:
      err = reversed_operator_.seal();
      res_iterator_ = &reversed_operator_;
      break;
    case USE_GROUPBY_OPERATOR:
      err = groupby_operator_.seal();
      res_iterator_ = &groupby_operator_;
      break;
    default:
      TBSYS_LOG(ERROR, "status error [status_:%d]", status_);
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to seal result [err:%d,status_:%d]", err, status_);
    }
  }
  int64_t limit_offset = 0;
  int64_t limit_count = 0;
  if (OB_SUCCESS == err)
  {
    scan_param_->get_limit_info(limit_offset,limit_count);
  }

  const ObArrayHelpers<bool> & return_info = (scan_param_->get_group_by_param().get_aggregate_row_width() > 0)?
    (scan_param_->get_group_by_param().get_return_infos()):
    (scan_param_->get_return_infos());
  int64_t row_width = (scan_param_->get_group_by_param().get_aggregate_row_width() > 0) ?
    (scan_param_->get_group_by_param().get_aggregate_row_width()):
    (scan_param_->get_column_id_size() + scan_param_->get_composite_columns_size());
  if ((OB_SUCCESS == err) && (res_iterator_ == &reversed_operator_))
  {
    limit_offset = 0;
    limit_count = 0;
  }
  if ((OB_SUCCESS == err) &&
    (OB_SUCCESS != (err = return_operator_.start_return(*res_iterator_,return_info,row_width,limit_offset,limit_count))))
  {
    TBSYS_LOG(WARN,"fail to start return [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    res_iterator_ = &return_operator_;
    sealed_ = true;
  }
  if (OB_SUCCESS != err)
  {
    res_iterator_ = NULL;
  }
  return err;
} 

int ObMergerOperator::get_mem_size_used()const
{
  int64_t res = 0;
  switch (status_)
  {
  case USE_SORTED_OPERATOR:
    res = sorted_operator_.get_mem_size_used();
    break;
  case USE_REVERSE_OPERATOR:
    res = reversed_operator_.get_mem_size_used();
    break;
  case USE_GROUPBY_OPERATOR:
    res = groupby_operator_.get_mem_size_used();
    break;
  default:
    TBSYS_LOG(WARN, "status error [status_:%d]", status_);
  }
  return static_cast<int32_t>(res);
}


int ObMergerOperator::next_cell()
{
  int err = OB_SUCCESS;
  if (NULL == scan_param_)
  {
    TBSYS_LOG(WARN,"please set request param first");
    err = OB_INVALID_ARGUMENT;
  }
  else if (!sealed_)
  {
    TBSYS_LOG(WARN,"operator has not sealed yet, please call seal first");
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (NULL == res_iterator_))
  {
    TBSYS_LOG(WARN,"please seal this operator first");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS == (err = res_iterator_->next_cell()))
    {
    }
    else if (OB_ITER_END == err)
    {
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get next_cell from result [err:%d,status_:%d]", err, status_);
    }
  }
  return err;
}

int ObMergerOperator::get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed)
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (NULL == cell))
  {
    TBSYS_LOG(WARN,"agument error [cell:%p]", cell);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (NULL == scan_param_))
  {
    TBSYS_LOG(WARN,"please set request param first");
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (NULL == res_iterator_))
  {
    TBSYS_LOG(WARN,"please seal this operator first");
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && !sealed_)
  {
    TBSYS_LOG(WARN,"operator has not sealed yet, please call seal first");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = res_iterator_->get_cell(cell,is_row_changed)))
    {
      TBSYS_LOG(WARN,"fail to get cell from result [err:%d,status_:%d]", err, status_);
    }
  }
  return err;
}

int ObMergerOperator::get_cell(common::ObInnerCellInfo** cell)
{
  return get_cell(cell,NULL);
}

int64_t ObMergerOperator::get_result_row_width()const
{
  int64_t res = 0;
  if (NULL != scan_param_)
  {
    res = (scan_param_->get_group_by_param().get_aggregate_row_width() > 0) ?
      (scan_param_->get_group_by_param().get_aggregate_row_width() ) : 
      (scan_param_->get_column_id_size() + scan_param_->get_composite_columns_size());
  }
  return res;
}

int64_t ObMergerOperator::get_whole_result_row_count()const
{
  int64_t res = 0;
  if(sealed_ && (USE_GROUPBY_OPERATOR == status_))
  {
    res = groupby_operator_.get_whole_result_row_count();
  }
  return static_cast<int32_t>(res);
}

