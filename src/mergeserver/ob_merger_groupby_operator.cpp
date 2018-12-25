/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_merger_groupby_operator.cpp for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */

#include "common/ob_scan_param.h"
#include "common/ob_compose_operator.h"
#include "common/hash/ob_hashutils.h"
#include "ob_merger_groupby_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerGroupByOperator::ObMergerGroupByOperator()
{
  scan_param_ = NULL;
  sealed_ = false;
}

ObMergerGroupByOperator::~ObMergerGroupByOperator()
{
  scan_param_ = NULL;
  sealed_ = false;
}

void ObMergerGroupByOperator::reset()
{
  scan_param_ = NULL;
  sealed_ = false;
  operator_.clear();
  compose_operator_.clear();
}

int64_t ObMergerGroupByOperator::get_result_row_width()const
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

int64_t ObMergerGroupByOperator::get_whole_result_row_count()const
{
  int64_t res = 0;
  if (NULL != scan_param_)
  {
    res = (scan_param_->get_group_by_param().get_aggregate_row_width() > 0) ?
      (operator_.get_group_count()):cs_whole_row_count_;
  }
  return res;
}

int ObMergerGroupByOperator::set_param(const int64_t max_memory_size, const ObScanParam & param)
{
  int err = OB_SUCCESS;
  reset();
  if (OB_SUCCESS != (err = operator_.init(param.get_group_by_param(), max_memory_size, max_memory_size)))
  {
    TBSYS_LOG(WARN, "fail to init ObGroupByOperator [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    scan_param_ = &param;
    cs_whole_row_count_ = 0;
  }
  return err;
}

int ObMergerGroupByOperator::add_sharding_result(ObScanner & sharding_res,
  const ObNewRange & query_range, const int64_t limit_offset, bool &is_finish)
{
  int err = OB_SUCCESS;
  UNUSED(query_range);
  is_finish = false;
  if (NULL == scan_param_)
  {
    TBSYS_LOG(WARN,"operator was not initialized yet [scan_param_:%p]", scan_param_);
    err = OB_INVALID_ARGUMENT;
  }
  int64_t expect_row_width = 0;
  int64_t res_cell_count = 0;
  int64_t res_row_count = 0;
  if (OB_SUCCESS == err)
  {
    expect_row_width = (scan_param_->get_group_by_param().get_aggregate_row_width() > 0) ?
      (scan_param_->get_group_by_param().get_aggregate_row_width()) :
      (scan_param_->get_column_id_size() + scan_param_->get_composite_columns_size());
    res_cell_count = sharding_res.get_cell_num();
    res_row_count = sharding_res.get_row_num();
    if ((res_cell_count > 0)
      && ((expect_row_width != res_cell_count/res_row_count) || (res_cell_count % res_row_count != 0)))
    {
      TBSYS_LOG(ERROR, "unexpected error [expect_row_width:%ld,res_cell_count:%ld,res_row_count:%ld]",
        expect_row_width, res_cell_count, res_row_count);
      err = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_SUCCESS == err)
  {
    if (limit_offset == 0)
    {
      cs_whole_row_count_ += sharding_res.get_whole_result_row_num();
    }
    if (OB_SUCCESS == (err = sharding_res.next_cell()))
    {
    }
    else if (OB_ITER_END == err)
    {
    }
    else
    {
      TBSYS_LOG(WARN,"fail to call next cell on sharding_res [err:%d]", err);
    }
  }

  int64_t got_cell_count = 0;
  bool sharding_res_it_end = false;
  ObCellInfo *cur_cell = NULL;
  ObInnerCellInfo *cell_out = NULL;
  while ((OB_SUCCESS == err) && (!sharding_res_it_end))
  {
    int64_t cur_row_width = 0;
    row_cells_.reset();
    for (cur_row_width = 0;
      (cur_row_width < expect_row_width) && (OB_SUCCESS == err);
      cur_row_width++, got_cell_count ++)
    {
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sharding_res.get_cell(&cur_cell))))
      {
        TBSYS_LOG(WARN,"fail to got cell from ObScanner [err:%d,got_cell_count:%ld]", err, got_cell_count);
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = row_cells_.append(*cur_cell, cell_out))))
      {
        TBSYS_LOG(WARN,"fail to append cell to temp row cell array [err:%d]", err);
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS == (err = sharding_res.next_cell()))
        {
        }
        else if (OB_ITER_END == err)
        {
          if (cur_row_width == expect_row_width - 1)
          {
            err = OB_SUCCESS;
            sharding_res_it_end = true;
          }
          else
          {
            TBSYS_LOG(ERROR,"sharding result cell number error [cur_row_width:%ld,expected_row_width:%ld]",
              cur_row_width, expect_row_width);
            err  = OB_ERR_UNEXPECTED;
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to get next_cell from sharding result [err:%d]", err);
        }
      }
    }
    if ((OB_ITER_END == err) && (cur_row_width != expect_row_width))
    {
      TBSYS_LOG(ERROR,"unexpected error, current row not end while ObScanner is end [cur_row_width:%ld,expect_row_width:%ld]",
        cur_row_width, expect_row_width);
      err = OB_ERR_UNEXPECTED;
    }
    if ((cur_row_width == expect_row_width) && ((OB_SUCCESS == err) || (OB_ITER_END == err)))
    {
      err = OB_SUCCESS;
      if (OB_SUCCESS != (err = operator_.add_row(row_cells_,0,expect_row_width - 1)))
      {
        TBSYS_LOG(WARN,"fail to add row to ObGroupByOperator [err:%d]", err);
      }
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  if ((OB_SUCCESS == err) && (got_cell_count != res_cell_count))
  {
    TBSYS_LOG(ERROR, "unexpected error, number of cell got is less than ObScanner::get_cell_num [got_cell_count:%ld,"
      "ObScanner::get_cell_num():%ld]", got_cell_count, res_cell_count);
    err = OB_ERR_UNEXPECTED;
  }
  return err;
}


int ObMergerGroupByOperator::compose(ObCellArray & result_array,
    const ObArrayHelper<ObCompositeColumn>& composite_columns, const int64_t row_width)
{
  int err = OB_SUCCESS;
  compose_operator_.clear();
  if (OB_SUCCESS != (err = compose_operator_.start_compose(result_array, composite_columns, row_width)))
  {
    TBSYS_LOG(WARN,"fail to compose result [err:%d]", err);
  }
  return err;
}


int ObMergerGroupByOperator::having_condition_filter(ObGroupByOperator & result,
  const int64_t row_width, const ObGroupByParam &param)
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (row_width == 0))
  {
    TBSYS_LOG(WARN,"unexpected row_width:%ld", row_width);
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err) && (result.get_cell_size()%row_width != 0))
  {
    TBSYS_LOG(WARN,"argument error [result.get_cell_size():%ld,row_width:%ld]",
      result.get_cell_size(), row_width);
  }
  ObGroupKey g_key;
  const ObSimpleFilter & filter = param.get_having_condition();
  for (int64_t cell_idx = 0; (OB_SUCCESS == err) && (cell_idx < result.get_cell_size()); cell_idx += row_width)
  {
    bool check_pass = false;
    if (OB_SUCCESS != (err = filter.check(result,cell_idx, cell_idx + row_width - 1,check_pass)))
    {
      TBSYS_LOG(WARN,"fail to filter row [row_idx:%ld, err:%d]", cell_idx/row_width, err);
    }
    if ((OB_SUCCESS == err)&&(!check_pass))
    {
      if (OB_SUCCESS != (err = g_key.init(result,param,cell_idx, cell_idx + row_width - 1,ObGroupKey::AGG_KEY)))
      {
        TBSYS_LOG(WARN,"fail to init ObGroupKey [cell_idx:%ld,err:%d]", cell_idx, err);
      }
      if (OB_SUCCESS == err)
      {
        int hash_err = 0;
        if (common::hash::HASH_EXIST == (hash_err = result.remove_group(g_key)))
        {
          /// do nothing
        }
        else if (common::hash::HASH_NOT_EXIST == hash_err)
        {
          TBSYS_LOG(ERROR, "unexpected error, row is in ObGroupOperator, but remove fail [row_idx:%ld]", cell_idx/row_width);
          err = OB_ERR_UNEXPECTED;
        }
        else
        {
          TBSYS_LOG(WARN,"remove key from hash table err [hash_err:%d]", hash_err);
          err = OB_ERROR;
        }
      }
    }
  }

  return err;
}

int ObMergerGroupByOperator::seal()
{
  int err = OB_SUCCESS;
  if (NULL == scan_param_)
  {
    TBSYS_LOG(WARN,"operator was not initialized yet");
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    int64_t agg_row_width = scan_param_->get_group_by_param().get_aggregate_row_width();
    int64_t select_row_width = scan_param_->get_column_id_size() + scan_param_->get_composite_columns_size();
    /// compose
    bool need_compose = ((agg_row_width > 0) && (scan_param_->get_group_by_param().get_composite_columns().get_array_index()> 0));
    if ((OB_SUCCESS == err) && need_compose)
    {
      if (OB_SUCCESS != (err = compose(operator_,scan_param_->get_group_by_param().get_composite_columns(),agg_row_width)))
      {
        TBSYS_LOG(WARN,"fail to compose composite columns [err:%d]", err);
      }
    }
    /// having condition filter
    bool need_filter_having_condition = ((agg_row_width > 0)
        && (scan_param_->get_group_by_param().get_having_condition().get_count()> 0));
    if ((OB_SUCCESS == err) && (need_filter_having_condition))
    {
      if (OB_SUCCESS != (err = having_condition_filter(operator_,agg_row_width,scan_param_->get_group_by_param())))
      {
        TBSYS_LOG(WARN,"fail to filter having conditions [err:%d]", err);
      }
    }

    /// orderby
    const int64_t *orderby_idx = NULL;
    const uint8_t *orders = NULL;
    int64_t orderby_column_count = 0;
    scan_param_->get_orderby_column(orderby_idx,orders,orderby_column_count);
    if (static_cast<uint64_t>(orderby_column_count) > sizeof(orderby_desc_)/sizeof(orderby_desc_[0]))
    {
      TBSYS_LOG(WARN,"orderby_column_count too large [orderby_column_count:%ld]", orderby_column_count);
      err = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = 0; (i < orderby_column_count) && (OB_SUCCESS == err); i++)
    {
      orderby_desc_[i].cell_idx_ = static_cast<int32_t>(orderby_idx[i]);
      orderby_desc_[i].order_ = orders[i];
    }
    bool need_orderby = (orderby_column_count > 0);
    if ((OB_SUCCESS == err) && need_orderby)
    {
      if (agg_row_width > 0)
      {
        if (OB_SUCCESS != (err = operator_.orderby(agg_row_width,orderby_desc_, orderby_column_count)))
        {
          TBSYS_LOG(WARN,"fail to orderby grouped result [err:%d]", err);
        }
      }
      else
      {
        if (OB_SUCCESS != (err = operator_.orderby(select_row_width,orderby_desc_, orderby_column_count)))
        {
          TBSYS_LOG(WARN,"fail to orderby select result [err:%d]", err);
        }
      }
    }
    if (OB_SUCCESS == err)
    {
      sealed_ = true;
    }
  }
  return err;
}


int ObMergerGroupByOperator::get_mem_size_used()const
{
  int64_t res = 0;
  res += row_cells_.get_memory_size_used();
  res += operator_.get_memory_size_used();
  return static_cast<int32_t>(res);
}



int ObMergerGroupByOperator::next_cell()
{
  int err = OB_SUCCESS ;
  if (!sealed_)
  {
    TBSYS_LOG(WARN,"operator was not yet sealed, seal it first");
    err = OB_INVALID_ARGUMENT;
  }
  int64_t cur_cell_offset = 0;
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS == (err = operator_.next_cell(cur_cell_offset)))
    {
    }
    else if (OB_ITER_END == err)
    {
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get next_cell from ObGroupByOperator [err:%d]", err);
    }
  }
  bool check_pass = false;
  int64_t agg_row_width = scan_param_->get_group_by_param().get_aggregate_row_width();
  ObGroupKey g_key;

  if ((OB_SUCCESS == err)
    && (agg_row_width > 0 )
    && (scan_param_->get_group_by_param().get_having_condition().get_count() > 0)
    && (cur_cell_offset%agg_row_width == 0))
  {
    if (cur_cell_offset + agg_row_width - 1 >= operator_.get_cell_size())
    {
      TBSYS_LOG(ERROR, "unexpected error [cur_cell_offset:%ld,agg_row_width:%ld,ObGroupByOperator::get_cell_size():%ld]",
        cur_cell_offset, agg_row_width, operator_.get_cell_size());
      err  = OB_ERR_UNEXPECTED;
    }
    while ((OB_SUCCESS == err)  && !check_pass)
    {
      if (OB_SUCCESS != (err = g_key.init(operator_, scan_param_->get_group_by_param(),
              cur_cell_offset, cur_cell_offset + agg_row_width - 1, ObGroupKey::AGG_KEY)))
      {
        TBSYS_LOG(WARN,"fail to init ObGroupKey [cell_idx:%ld,err:%d]", cur_cell_offset, err);
      }
      else
      {
        check_pass = operator_.has_group(g_key);
      }

      if ((OB_SUCCESS == err) && !check_pass)
      {
        int64_t jumped_cell_count = 0;
        for (; (jumped_cell_count < agg_row_width) && (OB_SUCCESS == err); jumped_cell_count ++)
        {
          if (OB_SUCCESS == (err = operator_.next_cell(cur_cell_offset)))
          {
          }
          else if (OB_ITER_END == err)
          {
          }
          else
          {
            TBSYS_LOG(WARN,"fail to get next_cell from ObGroupByOperator [err:%d]", err);
          }
        }
        if ((jumped_cell_count < agg_row_width) && ((OB_SUCCESS == err) ||(OB_ITER_END == err)))
        {
          TBSYS_LOG(ERROR, "unexpected error, jumped cell count not enough [jumped_cell_count:%ld,agg_row_width:%ld]",
            jumped_cell_count , agg_row_width);
          err = OB_ERR_UNEXPECTED;
        }
      }
    }
  }
  return err;
}


int ObMergerGroupByOperator::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
{
  int err = OB_SUCCESS;
  if (!sealed_)
  {
    TBSYS_LOG(WARN,"operator was not yet sealed, seal it first");
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (NULL == cell))
  {
    TBSYS_LOG(WARN,"agument error [cell:%p]", cell);
    err = OB_INVALID_ARGUMENT;
  }

  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = operator_.get_cell(cell,is_row_changed))))
  {
    TBSYS_LOG(WARN,"fail to get cell from ObGroupByIterator [err:%d,ObGroupByOperator.get_cell_size():%ld]",
      err,  operator_.get_cell_size());
  }
  return err;
}


int ObMergerGroupByOperator::get_cell(ObInnerCellInfo** cell)
{
  return get_cell(cell,NULL);
}
