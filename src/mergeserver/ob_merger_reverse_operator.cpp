/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_reverse_operator.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */

#include "ob_merger_reverse_operator.h"
#include "ob_merger_sorted_operator.h"
#include "common/ob_cell_array.h"
#include "common/ob_scan_param.h"
using namespace oceanbase;
using namespace common;
using namespace mergeserver;

mergeserver::ObMergerReverseOperator::ObMergerReverseOperator()
{
  reset();
}

mergeserver::ObMergerReverseOperator::~ObMergerReverseOperator()
{
}

int  mergeserver::ObMergerReverseOperator::set_param(const ObScanParam & scan_param, ObCellArray &cells)
{
  int err = OB_SUCCESS;
  reset();
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sorted_operator_.set_param(scan_param))))
  {
    TBSYS_LOG(WARN,"set ObMergerSortedOperator fail [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    scan_param_ = &scan_param;
    reversed_cells_ = &cells;
    reversed_cells_->reset();
  }
  return err;
}

int mergeserver::ObMergerReverseOperator::add_sharding_result(ObScanner & sharding_res, const ObNewRange &query_range, 
  bool &is_finish)
{
  int err = OB_SUCCESS;
  if (NULL == reversed_cells_)
  {
    TBSYS_LOG(WARN,"operator not initalized yet [reversed_cells_:%p]", reversed_cells_);
  }
  else
  {
    err = sorted_operator_.add_sharding_result(sharding_res,query_range,is_finish);
  }
  return err;
}

int mergeserver::ObMergerReverseOperator::seal()
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (NULL == reversed_cells_))
  {
    TBSYS_LOG(WARN,"operator was not initialized yet [reversed_cells_:%p]", reversed_cells_);
    err = OB_INVALID_ARGUMENT;
  }

  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sorted_operator_.seal())))
  {
    TBSYS_LOG(WARN,"fail to seal ObMergerSortedOperator [err:%d]", err);
  }
  /// jump limit offset
  int64_t row_width = scan_param_->get_column_id_size() + scan_param_->get_composite_columns_size();
  int64_t limit_offset = 0;
  int64_t limit_count = 0;
  scan_param_->get_limit_info(limit_offset, limit_count);
  if (limit_offset > 0)
  {
    int64_t jumped_cell_count = limit_offset * row_width;
    for (int64_t i = 0; (i < jumped_cell_count) && (OB_SUCCESS == err); i ++)
    {
      if ((OB_SUCCESS != (err = sorted_operator_.next_cell())) && (OB_ITER_END != err))
      {
        TBSYS_LOG(WARN,"fail to get next cell from ObMergerSortedOperator [err:%d]", err);
      }
    }
  }

  ObInnerCellInfo * cur_cell = NULL;
  ObInnerCellInfo * cell_out = NULL;
  uint64_t cell_count_need_got = static_cast<uint64_t>(-1);
  uint64_t cell_count_got = 0;
  if ((OB_SUCCESS == err) && (row_width == 0))
  {
    TBSYS_LOG(WARN, "unexpected row_width:%ld", row_width);
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err) && (limit_count > 0) && (static_cast<uint64_t>(limit_count) < cell_count_need_got/row_width))
  {
    cell_count_need_got = 1ull * limit_count * row_width;
  }
  ObCellInfo temp_cell;
  while ((OB_SUCCESS == err) && (cell_count_got < cell_count_need_got))
  {
    if (OB_SUCCESS == (err = sorted_operator_.next_cell()))
    {
      if (OB_SUCCESS != (err = sorted_operator_.get_cell(&cur_cell)))
      {
        TBSYS_LOG(WARN,"fail to get cell from sorted operator [err:%d]", err);
      }
      else
      {
        // TODO soon
        temp_cell.table_id_ = cur_cell->table_id_;
        temp_cell.column_id_ = cur_cell->column_id_;
        temp_cell.row_key_ = cur_cell->row_key_;
        temp_cell.value_ = cur_cell->value_;
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = reversed_cells_->append(temp_cell, cell_out))))
      {
        TBSYS_LOG(WARN,"fail to append cell to reversed cell [err:%d]", err);
      }
      if (OB_SUCCESS == err)
      {
        cell_count_got ++;
      }
    }
    else if (OB_ITER_END == err)
    {
      /// do nothing
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get next_cell from SortedOperator [err:%d]", err);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  if (OB_SUCCESS == err)
  {
    if ((reversed_cells_->get_cell_size() > 0) && (OB_SUCCESS != (err = reversed_cells_->reverse_rows(row_width))))
    {
      TBSYS_LOG(WARN,"fail to reverse result [err:%d]", err);
    }
  }
  return err;
}


int64_t mergeserver::ObMergerReverseOperator::get_mem_size_used()const
{
  int64_t res = 0;
  res += sorted_operator_.get_mem_size_used();
  if(NULL != reversed_cells_)
  {
    res += reversed_cells_->get_memory_size_used();
  }
  return res;
}


int mergeserver::ObMergerReverseOperator::next_cell()
{
  int err = OB_SUCCESS;
  if (NULL == reversed_cells_)
  {
    TBSYS_LOG(WARN,"operator was not initialized yet");
    err = OB_ITER_END;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS == (err = reversed_cells_->next_cell())))
  {
    ///
  }
  else if (OB_ITER_END == err)
  {
    ///
  }
  else
  {
    TBSYS_LOG(WARN,"fail to get next cell from cell array [err:%d]", err);
  }
  return err;
}

int mergeserver::ObMergerReverseOperator::get_cell(ObInnerCellInfo ** cell, bool *is_row_changed)
{
  int err = OB_SUCCESS;
  if (NULL == cell)
  {
    TBSYS_LOG(WARN,"invalid argument [cell:%p]", cell);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (NULL == reversed_cells_))
  {
    TBSYS_LOG(WARN,"operator not initialized yet [reversed_cells_:%p]", reversed_cells_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = reversed_cells_->get_cell(cell,is_row_changed))))
  {
    TBSYS_LOG(WARN,"fail to get cell from sorted operator [err:%d]", err);
  }
  return err;
}


int mergeserver::ObMergerReverseOperator::get_cell(ObInnerCellInfo ** cell)
{
  return get_cell(cell,NULL);
}
