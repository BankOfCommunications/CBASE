/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_sub_get_request.cpp for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */

#include "ob_ms_sub_get_request.h"
#include "common/utility.h"
#include <algorithm>
using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerSubGetRequest::ObMergerSubGetRequest()
  : rowkey_buf_(ObModIds::OB_MS_GET_EVENT)
{
  reset();
}

void ObMergerSubGetRequest::init(PageArena<int64_t, oceanbase::common::ModulePageAllocator> & mem_allocator)
{
  cell_idx_in_org_param_.set_allocator(mem_allocator);
  res_vec_.set_allocator(mem_allocator);
  rowkey_buf_.clear();
  last_rowkey_.assign(NULL, 0);
}

void ObMergerSubGetRequest::reset()
{
  last_rpc_event_ = 0;
  cell_idx_in_org_param_.clear();
  res_vec_.clear();
  pget_param_ = NULL;
  received_cell_count_ = 0;
  poped_cell_count_ = 0;
  res_iterator_idx_  = 0;
  retry_times_ = 0;
  fail_svr_ip_ = 0;
  cur_get_param_.reset();
  rowkey_buf_.reset();
  last_rowkey_.assign(NULL, 0);
}

void ObMergerSubGetRequest::set_param(ObGetParam & get_param)
{
  reset();
  pget_param_ = &get_param;
}


int ObMergerSubGetRequest::add_cell(const int64_t cell_idx)
{
  int err = OB_SUCCESS;
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && ((cell_idx < 0) || (cell_idx >= pget_param_->get_cell_size())))
  {
    TBSYS_LOG(WARN,"cell_idx out of range [cell_idx:%ld,pget_param_->get_cell_size():%ld]",
      cell_idx, pget_param_->get_cell_size());
    err = OB_INVALID_ARGUMENT;
  }

  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = cell_idx_in_org_param_.push_back(cell_idx))))
  {
    TBSYS_LOG(WARN,"fail to append cell index [err:%d]", err);
  }

  return err;
}


int ObMergerSubGetRequest::add_result(ObScanner &res)
{
  int err = OB_SUCCESS;
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = res_vec_.push_back(reinterpret_cast<int64_t>(&res)))))
  {
    TBSYS_LOG(WARN,"fail to appedn result [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    received_cell_count_ += res.get_cell_num();
    retry_times_ = 0;
  }
  return err;
}


int ObMergerSubGetRequest::get_next_param_(ObGetParam & get_param)const
{
  int err = OB_SUCCESS;
  get_param.reset();
  ObReadParam & read_param = get_param;
  read_param = *dynamic_cast<ObReadParam*>(pget_param_);
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (received_cell_count_ >= cell_idx_in_org_param_.size()))
  {
    err = OB_ITER_END;
  }
  for (int64_t cell_idx = received_cell_count_; (OB_SUCCESS == err) && (cell_idx < cell_idx_in_org_param_.size()); cell_idx ++)
  {
    if ((OB_SUCCESS != (err = get_param.add_cell(*pget_param_->operator [](cell_idx_in_org_param_[static_cast<int32_t>(cell_idx)]))))
      && (OB_SIZE_OVERFLOW != err))
    {
      TBSYS_LOG(WARN,"fail to add cell to ObGetParam [err:%d]", err);
    }
    else if (OB_SIZE_OVERFLOW == err)
    {
      err = OB_SUCCESS;
      break;
    }
  }
  return err;
}

int ObMergerSubGetRequest::reset_iterator()
{
  int err = OB_SUCCESS;
  poped_cell_count_ = 0;
  res_iterator_idx_  = 0;
  for (int32_t res_idx = 0; (OB_SUCCESS == err) && (res_idx < res_vec_.size()); res_idx++)
  {
    if(OB_SUCCESS != (err = reinterpret_cast<ObScanner*>(res_vec_[res_idx])->reset_iter()))
    {
      TBSYS_LOG(WARN,"fail to reset ObScanner's iterator [err:%d]", err);
    }
  }
  return err;
}

int ObMergerSubGetRequest::has_next()
{
  int err = OB_SUCCESS;
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (received_cell_count_ != cell_idx_in_org_param_.size()))
  {
    TBSYS_LOG(ERROR, "cell count received not coincident with expected cell count [received_cell_count_:%ld,expect:%d]",
      received_cell_count_, cell_idx_in_org_param_.size());
    err = OB_ERR_UNEXPECTED;
  }
  while ((res_iterator_idx_ < res_vec_.size()) && (OB_SUCCESS == err))
  {
    if (OB_SUCCESS == (err = reinterpret_cast<ObScanner*>(res_vec_[static_cast<int32_t>(res_iterator_idx_)])->next_cell()))
    {
      break;
    }
    else if (OB_ITER_END == err)
    {
      res_iterator_idx_ ++;
      err = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN,"fail to call next_cell [err:%d]", err);
    }
  }
  if ((OB_SUCCESS == err) && (res_iterator_idx_ >= res_vec_.size()))
  {
    err = OB_ITER_END;
  }
  if (OB_SUCCESS == err)
  {
    poped_cell_count_ ++;
  }
  return err;
}

int  ObMergerSubGetRequest::next(oceanbase::common::ObInnerCellInfo *&cell, int64_t & org_cell_idx)
{
  int err = OB_SUCCESS;
  bool is_row_changed = false;
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (res_iterator_idx_ < res_vec_.size()))
  {
    if (OB_SUCCESS != (err = reinterpret_cast<ObScanner*>(res_vec_[static_cast<int32_t>(res_iterator_idx_)])->get_cell(&cell, &is_row_changed)))
    {
      TBSYS_LOG(WARN,"fail to get cell from ObScanner [err:%d,res_iterator_idx_:%ld]", err, res_iterator_idx_);
    }

    if (OB_SUCCESS == err && NULL != cell)
    {
      if (is_row_changed)
      {
        // deep copy current rowkey;
        err = rowkey_buf_.write_string(cell->row_key_, &last_rowkey_);
      }

      if (OB_SUCCESS == err)
      {
        cell->row_key_ = last_rowkey_;
      }
      else
      {
        TBSYS_LOG(WARN, "cannot copy rowkey(%s) to string buffer.", to_cstring(cell->row_key_));
      }

    }

    if ((OB_SUCCESS == err) && (poped_cell_count_ > cell_idx_in_org_param_.size()))
    {
      TBSYS_LOG(ERROR, "not all cells poped, but all indexes are poped [poped_cell_count_:%ld,res_iterator_idx_:%ld]",
        poped_cell_count_, res_iterator_idx_);
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS == err)
    {
      org_cell_idx = cell_idx_in_org_param_[static_cast<int32_t>(poped_cell_count_ - 1)];
    }
  }
  if (OB_SUCCESS == err)
  {
    if (((*pget_param_)[org_cell_idx]->table_id_ != cell->table_id_)
      || ((cell->value_.get_type() != ObExtendType) && ((*pget_param_)[org_cell_idx]->column_id_ != cell->column_id_))
      || ((*pget_param_)[org_cell_idx]->row_key_ != cell->row_key_)
      )
    {
      TBSYS_LOG(ERROR, "result not correct [param->table_id_:%lu, param->column_id_:%lu, param->row_key_:%s,"
        "result->table_id_:%lu, result->column_id_:%lu, result->row_key_:%s]",
        (*pget_param_)[org_cell_idx]->table_id_,
        (*pget_param_)[org_cell_idx]->column_id_,
        to_cstring((*pget_param_)[org_cell_idx]->row_key_),
        cell->table_id_, cell->column_id_, to_cstring(cell->row_key_));
      err = OB_ERR_UNEXPECTED;
    }
  }
  return err;
}

int ObGetMerger::init(ObMergerSubGetRequest * results, const int64_t res_count,
  const ObGetParam & get_param)
{
  int err = OB_SUCCESS;
  if ((NULL == results) || (res_count <= 0))
  {
    TBSYS_LOG(WARN,"invalid argument [results:%p,res_count:%ld]", results, res_count);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (res_count > MAX_RES_SIZE))
  {
    TBSYS_LOG(WARN,"result size too large [res_count:%ld,MAX_RES_SIZE:%ld]", res_count, MAX_RES_SIZE);
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  if (OB_SUCCESS == err)
  {
    res_size_ = res_count;
    res_vec_ = results;
    heap_size_ = 0;
    cur_cell_idx_ = 0;
    get_param_ = &get_param;
  }
  for (int64_t res_idx = 0; (OB_SUCCESS == err) && (res_idx < res_size_); res_idx ++)
  {
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = res_vec_[res_idx].has_next())) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to call has_next of result [err:%d,res_idx:%ld]", err, res_idx);
    }
    else if (OB_ITER_END == err)
    {
      err = OB_SUCCESS;
    }
    else if (OB_SUCCESS == err)
    {
      if (OB_SUCCESS == (err = res_vec_[res_idx].next(info_heap_[heap_size_].cur_cell_,info_heap_[heap_size_].org_cell_idx_)))
      {
        info_heap_[heap_size_].res_idx_ = res_idx;
        heap_size_ ++;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get next cell from result [err:%d,res_idx:%ld]", err, res_idx);
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    make_heap(info_heap_,info_heap_ + heap_size_);
  }
  return err;
}

int ObGetMerger::next_cell()
{
  int err = OB_SUCCESS;
  if (NULL == res_vec_)
  {
    TBSYS_LOG(WARN,"ObGetMerger not init yet");
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (heap_size_ <= 0))
  {
    err = OB_ITER_END;
  }
  if (OB_SUCCESS == err)
  {
    cur_cell_ = *info_heap_[0].cur_cell_;
    if (cur_cell_idx_ != info_heap_[0].org_cell_idx_)
    {
      TBSYS_LOG(ERROR, "unexpected error [cur_cell_idx_:%ld,got_cell_idx:%ld,res_idx:%ld]",
        cur_cell_idx_, info_heap_[0].org_cell_idx_, info_heap_[0].res_idx_);
      err = OB_ERR_UNEXPECTED;
    }
    else
    {
      cur_cell_idx_ ++;
    }
  }
  if (OB_SUCCESS == err)
  {
    if ((OB_SUCCESS != (err = res_vec_[info_heap_[0].res_idx_].has_next())) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to call has_next [err:%d,res_idx:%ld]", err, info_heap_[0].res_idx_);
    }
    else if (OB_ITER_END == err)
    {
      pop_heap(info_heap_, info_heap_ + heap_size_);
      heap_size_ --;
      err = OB_SUCCESS;
    }
    else if (OB_SUCCESS == err)
    {
      int64_t res_idx = info_heap_[0].res_idx_;
      ResInfo next_info;
      if (OB_SUCCESS != (err = res_vec_[res_idx].next(next_info.cur_cell_,next_info.org_cell_idx_)))
      {
        TBSYS_LOG(WARN,"fail to get next cell from result [res_idx:%ld,err:%d]", res_idx, err);
      }
      if (OB_SUCCESS == err)
      {
        next_info.res_idx_ = res_idx;
        pop_heap(info_heap_, info_heap_ + heap_size_);
        info_heap_[heap_size_ - 1] = next_info;
        push_heap(info_heap_, info_heap_ + heap_size_);
      }
    }
  }
  return err;
}


int ObGetMerger::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
{
  int err = OB_SUCCESS;
  if (NULL == cell)
  {
    TBSYS_LOG(WARN,"invalid agument [cell:%p]", cell);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    *cell = &cur_cell_;
    if (NULL != is_row_changed)
    {
      *is_row_changed = ((cur_cell_idx_ == 1)
        || ((*get_param_)[cur_cell_idx_ - 1]->table_id_ != (*get_param_)[cur_cell_idx_ - 2]->table_id_)
        || ((*get_param_)[cur_cell_idx_ - 1]->row_key_ != (*get_param_)[cur_cell_idx_ - 2]->row_key_));
    }
  }
  return err;
}


int ObGetMerger::get_cell(ObInnerCellInfo** cell)
{
  return get_cell(cell, NULL);
}
