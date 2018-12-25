/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_cell_array.cpp is for what ...
 *
 * Version: $id: ob_cell_array.cpp,v 0.1 9/16/2010 4:29p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_cell_array.h"
#include <algorithm>
#include <iostream>
#include "ob_define.h"
#include "ob_malloc.h"
#include "page_arena.h"
#include "ob_read_common_data.h"

using namespace oceanbase::common;

namespace
{
  ModulePageAllocator g_page_arena_allocator(ObModIds::OB_MS_CELL_ARRAY);
}

void ObCellArray::initialize_()
{
  cell_num_ = 0;
  current_block_ = NULL;
  current_block_cell_num_ = 0;
  consumed_row_num_ = 0;
  cur_row_consumed_cell_num_ = -1;
  orderby_column_num_ = 0;
  heap_ = NULL;
  heap_size_ = 0;
  row_width_ = 0;
  row_num_ = 1;
  limit_cell_num_ = 0;
  prev_key_.assign(NULL,0);
  prev_tableid_ = OB_INVALID_ID;
  allocated_memory_size_ = 0;
}

ObCellArray::ObCellArray()
: page_arena_(PageArena<char,ModulePageAllocator>::DEFAULT_PAGE_SIZE, g_page_arena_allocator),
  heap_buf_(DEFAULT_HEAP_BUF_SIZE)
{
  next_block_array_slot_ = 0;
  initialize_();
  memset(cell_block_array_,0,sizeof(cell_block_array_));
}

ObCellArray::~ObCellArray()
{
  clear();
}

void ObCellArray::clear()
{
  ObDLink *cell_block_it = cell_block_list_.next();
  CellBlock *block = NULL;
  while (cell_block_it != &cell_block_list_)
  {
    block = CONTAINING_RECORD(cell_block_it, CellBlock, cell_block_link_);
    cell_block_it = cell_block_it->next();
    block->cell_block_link_.remove();
    ob_free(block);
  }
  initialize_();
  page_arena_.free();
  sorted_row_offsets_.clear();
  memset(cell_block_array_,0,sizeof(CellBlock*) * next_block_array_slot_);
  next_block_array_slot_ = 0;
}

void ObCellArray::reset()
{
  initialize_();
  page_arena_.reuse();
  if (NULL != cell_block_array_[0])
  {
    current_block_ = cell_block_array_[0];
  }
  sorted_row_offsets_.reset();
}

inline int ObCellArray::copy_obj_(ObObj &dst, const ObObj &src)
{
  int err = OB_SUCCESS;
  char *tmp_buf = NULL;
  /// allocate value
  if (src.get_type() == ObVarcharType)
  {
    ObString src_value;
    ObString dst_value;
    err = src.get_varchar(src_value);
    if (OB_SUCCESS == err)
    {
      // must assign %src to %dst first, for copy meta data to %dst object.
      // dst's meta_ members(especially meta_.reserved_) could be random value
      // if only call dst.set_varchar();
      dst = src;

      if (src_value.length() > 0)
      {
        tmp_buf =reinterpret_cast<char*>( page_arena_.alloc(src_value.length()));
        if (NULL == tmp_buf)
        {
          TBSYS_LOG(WARN, "%s", "fail to malloc buffer for varchar value");
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          allocated_memory_size_ += src_value.length();
          memcpy(tmp_buf, src_value.ptr(), src_value.length());
          dst_value.assign(tmp_buf,src_value.length());
          dst.set_varchar(dst_value);
        }
      }
      else
      {
        dst.set_varchar(dst_value);
      }
    }
  }
  //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
     else if(src.get_type() == ObDecimalType){
      ObString src_value;
      ObString dst_value;
      err = src.get_decimal(src_value);
      if (OB_SUCCESS == err)
      {
         dst = src;
         if (src_value.length() > 0)
         {
             tmp_buf =reinterpret_cast<char*>( page_arena_.alloc(src_value.length()));
             if (NULL == tmp_buf)
             {
               TBSYS_LOG(WARN, "%s", "fail to malloc buffer for decimal value");
               err = OB_ALLOCATE_MEMORY_FAILED;
             }
             else
             {
               allocated_memory_size_ += src_value.length();
               memcpy(tmp_buf, src_value.ptr(), src_value.length());
               dst_value.assign(tmp_buf,src_value.length());
               dst.set_decimal(dst_value,src.get_precision(),src.get_scale(),src.get_vscale());
             }

         }
         else
         {
           dst.set_decimal(dst_value,src.get_precision(),src.get_scale(),src.get_vscale());
           TBSYS_LOG(WARN,"WARNING,str buf length is smaller than zero!");
         }
      }

  }
    //add e
  return err;
}

int ObCellArray::copy_cell_(ObInnerCellInfo &dst, const ObCellInfo &src, const int64_t prev_cell_idx)
{
  int err = OB_SUCCESS;
  ObInnerCellInfo *same_row_cell = &empty_inner_cell_;

  dst.table_id_ = src.table_id_;
  dst.column_id_ = src.column_id_;

  if (prev_cell_idx >= 0 && prev_cell_idx < cell_num_)
  {
    err = get_cell(prev_cell_idx, same_row_cell);
  }
  /// allocate rowkey
  if (OB_SUCCESS == err)
  {
    if (NULL != same_row_cell && same_row_cell->row_key_ == src.row_key_)
    {
      dst.row_key_ = same_row_cell->row_key_;
    }
    else if (src.row_key_.length() > 0)
    {
      err = src.row_key_.deep_copy(dst.row_key_, page_arena_);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "deep copy new rowkey failed:err[%d]", err);
      }
      else
      {
        allocated_memory_size_ += src.row_key_.get_deep_copy_size();
      }
    }
    else
    {
      dst.row_key_.assign(NULL,0);
    }
  }
  if (OB_SUCCESS == err)
  {
    //if (src.value_.get_type() != ObVarcharType) old code
    //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
    if (src.value_.get_type() != ObVarcharType&&src.value_.get_type() != ObDecimalType)
    //modify :e
    {
      dst.value_ = src.value_;
    }
    else
    {
      err = copy_obj_(dst.value_,src.value_);
    }
  }

  return err;
}

inline int ObCellArray::copy_cell_fast(ObInnerCellInfo &dst, const ObCellInfo &src,
  const ObRowkey& row_key, const bool is_first_cell_of_row)
{
  int err = OB_SUCCESS;

  dst.table_id_ = src.table_id_;
  dst.column_id_ = src.column_id_;

  if (!is_first_cell_of_row)
  {
    dst.row_key_ = row_key;
  }
  else
  {
    // allocate rowkey
    if (src.row_key_.length() > 0)
    {
      err = src.row_key_.deep_copy(dst.row_key_, page_arena_);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "deep copy new rowkey failed:err[%d]", err);
      }
      else
      {
        allocated_memory_size_ += src.row_key_.get_deep_copy_size();
      }
    }
    else
    {
      dst.row_key_.assign(NULL,0);
    }
  }

   //if (src.value_.get_type() != ObVarcharType) old code
    //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
    if (src.value_.get_type() != ObVarcharType&&src.value_.get_type() != ObDecimalType)
    //modify :e
  {
    dst.value_ = src.value_;
  }
  else
  {
    err = copy_obj_(dst.value_,src.value_);
  }

  return err;
}

int ObCellArray::ensure_current_block_space()
{
  int err = OB_SUCCESS;

  if (NULL != current_block_ && current_block_cell_num_ >= CELL_BLOCK_CAPACITY)
  {
    if (current_block_->cell_block_link_.next() != &cell_block_list_)
    {
      current_block_ = CONTAINING_RECORD(current_block_->cell_block_link_.next(),
        CellBlock, cell_block_link_);
      current_block_cell_num_ = 0;
    }
    else
    {
      current_block_ = NULL;
      current_block_cell_num_ = 0;
    }
  }

  if (NULL == current_block_)
  {
    void * block_buf = ob_malloc(sizeof(CellBlock), ObModIds::OB_MS_CELL_ARRAY);
    if (OB_SUCCESS == err && NULL != block_buf)
    {
      /// current_block_ = new(block_buf)CellBlock;
      current_block_ = reinterpret_cast<CellBlock*>(block_buf);
      new(&(current_block_->cell_block_link_))ObDLink;
      cell_block_list_.insert_prev(current_block_->cell_block_link_);
      current_block_cell_num_ = 0;
      if (next_block_array_slot_ < CELL_BLOCK_ARRAY_SIZE)
      {
        cell_block_array_[next_block_array_slot_] = current_block_;
        next_block_array_slot_ ++;
      }
    }
    else
    {
      err = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "%s", "fail to allocate memory for cellinfo array");
    }
  }

  return err;
}

int ObCellArray::append(const ObCellInfo &cell, ObInnerCellInfo *& cell_out)
{
  int err = OB_SUCCESS;
  if (0 == cell_num_ && sorted_row_offsets_.size() <= 0)
  {
    err = sorted_row_offsets_.push_back(0);
  }
  if (OB_SUCCESS == err && OB_SUCCESS == (err = ensure_current_block_space()))
  {
    err = copy_cell_(current_block_->cell_array_[current_block_cell_num_], cell,
      cell_num_ - 1);
  }
  if (OB_SUCCESS == err)
  {
    cell_out = current_block_->cell_array_  + current_block_cell_num_;
    current_block_cell_num_ ++;
    cell_num_ ++;
    row_width_ ++;
    limit_cell_num_ ++;
  }
  return err;
}

int ObCellArray::append(const ObCellInfo &cell, ObInnerCellInfo *& cell_out,
    const ObRowkey &row_key, const bool is_first_cell_of_row)
{
  int err = OB_SUCCESS;
  if (0 == cell_num_ && sorted_row_offsets_.size() <= 0)
  {
    err = sorted_row_offsets_.push_back(0);
  }
  if (OB_SUCCESS == err && OB_SUCCESS == (err = ensure_current_block_space()))
  {
    err = copy_cell_fast(current_block_->cell_array_[current_block_cell_num_],
      cell, row_key, is_first_cell_of_row);
  }
  if (OB_SUCCESS == err)
  {
    cell_out = current_block_->cell_array_  + current_block_cell_num_;
    ++current_block_cell_num_;
    ++cell_num_;
    ++row_width_;
    ++limit_cell_num_;
  }
  return err;
}

int ObCellArray::batch_append(
    ObCellInfo **cells_in, ObInnerCellInfo **cells_out, const int64_t cell_size)
{
  int err = OB_SUCCESS;

  if (NULL == cells_in || NULL == cells_out || cell_size <= 0)
  {
    TBSYS_LOG(WARN, "invalid param, cells_in=%p cells_out=%p, cell_size=%ld",
      cells_in, cells_out, cell_size);
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 == cell_num_ && sorted_row_offsets_.size() <= 0)
  {
    err = sorted_row_offsets_.push_back(0);
  }

  for (int64_t i = 0; i < cell_size && OB_SUCCESS == err; i++)
  {
    err = ensure_current_block_space();
    if (OB_SUCCESS == err && NULL != cells_in[i])
    {
      if (0 == i)
      {
        err = copy_cell_(current_block_->cell_array_[current_block_cell_num_], *cells_in[i],
          cell_num_ - 1);
      }
      else
      {
        err = copy_cell_fast(current_block_->cell_array_[current_block_cell_num_], *cells_in[i],
          cells_out[i - 1]->row_key_);
      }
      if (OB_SUCCESS == err)
      {
        cells_out[i] = current_block_->cell_array_  + current_block_cell_num_;
        current_block_cell_num_ ++;
        cell_num_ ++;
        row_width_ ++;
        limit_cell_num_ ++;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to copy cell, cell_size=%ld, index=%ld",
          cell_size, i);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "input cells to append is NULL or can't allocate memory "
                      "for cell array, index=%ld", i);
      err = OB_ERROR;
    }
  }

  return err;
}

int ObCellArray::apply(const ObCellInfo &src_cell, ObInnerCellInfo *& affected_cell)
{
  int err = OB_SUCCESS;
  if (NULL == affected_cell && OB_SUCCESS == err)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (NULL == affected_cell->row_key_.ptr() && OB_INVALID_ID == affected_cell->column_id_)
  {
    TBSYS_LOG(ERROR, "should not goto this path:rowkey[%p], table[%lu], column[%lu]",
        affected_cell->row_key_.ptr(), affected_cell->table_id_, affected_cell->column_id_);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObObj real_obj;
    /// copy vchar value
      //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
    //if (src_cell.value_.get_type() != ObVarcharType)
    if (src_cell.value_.get_type() != ObVarcharType&&src_cell.value_.get_type() != ObDecimalType)
    //modify:e
    {
      real_obj = src_cell.value_;
    }
    else
    {
      err = copy_obj_(real_obj,src_cell.value_);
    }
    if (OB_SUCCESS == err)
    {
      err = affected_cell->value_.apply(real_obj);
    }
  }
  return err;
}

int ObCellArray::apply(const ObCellInfo & src, const int64_t offset, ObInnerCellInfo *& cell_out)
{
  int err = OB_SUCCESS;
  ObInnerCellInfo *target = NULL;
  err = get_cell(offset, target);
  if (OB_SUCCESS == err)
  {
    err = apply(src, target);
  }
  if (OB_SUCCESS == err && NULL != target)
  {
    cell_out = target;
  }
  return err;
}

ObInnerCellInfo & ObCellArray::operator[](int64_t offset)
{
  ObInnerCellInfo *result = NULL;
  int err = get_cell(offset, result);
  if (OB_SUCCESS != err)
  {
    result = &empty_inner_cell_;
    TBSYS_LOG(ERROR, "fail to get cell [offset:%ld,cell_num_:%ld,err:%d]", offset, cell_num_,
      err);
  }
  return *result;
}

const ObCellInfo & ObCellArray::operator[] (int64_t offset) const
{
  ObCellInfo * result = const_cast<ObCellInfo*>(&empty_cell_);
  ObInnerCellInfo * inner_cell = NULL;
  int err = get_cell(offset, inner_cell);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "fail to get cell [offset:%ld,cell_num_:%ld,err:%d]", offset, cell_num_,
      err);
  }
  else
  {
    result->table_id_ = inner_cell->table_id_;
    result->column_id_ = inner_cell->column_id_;
    result->row_key_ = inner_cell->row_key_;
    result->value_ = inner_cell->value_;
  }
  return  *result;
}

int ObCellArray::get_cell(const int64_t offset, ObInnerCellInfo *&cell)const
{
  int err = OB_SUCCESS;

  if (offset >= cell_num_ || offset < 0)
  {
    err = OB_ARRAY_OUT_OF_RANGE;
    TBSYS_LOG(WARN,"try to access cell out of range [offset:%ld,cell_num_:%ld]",
      offset, cell_num_);
  }
  else
  {
    if (offset < O1_ACCESS_CELL_NUM)
    {
      ///int64_t block_offset = offset/CELL_BLOCK_CAPACITY;
      int64_t block_offset = (offset>>CELL_BLOCK_SHIF_BITS);
      int64_t in_block_offset = (offset & CELL_IN_BLOCK_OFFSET_AND_VAL);
      if (NULL != cell_block_array_[block_offset])
      {
        cell = cell_block_array_[block_offset]->cell_array_ + in_block_offset;
      }
      else
      {
        TBSYS_LOG(ERROR, "unexpected error, offset less than cell_num_, but out of range "
          "[offset:%ld,cell_num_:%ld]", offset, cell_num_);
        err = OB_ERR_UNEXPECTED;
      }
    }
    else
    {
      int64_t tmp_offset = 0;
      CellBlock * mother_block = NULL;
      ObDLink *block_it = NULL;
      tmp_offset = offset - O1_ACCESS_CELL_NUM;
      block_it = cell_block_array_[CELL_BLOCK_ARRAY_SIZE - 1]->cell_block_link_.next();
      while (block_it != &cell_block_list_ && tmp_offset >= CELL_BLOCK_CAPACITY)
      {
        block_it = block_it->next();
        tmp_offset -= CELL_BLOCK_CAPACITY;
      }
      if (block_it == &cell_block_list_)
      {
        TBSYS_LOG(ERROR, "unexpected error, offset less than cell_num_, but out of range "
          "[offset:%ld,cell_num_:%ld]", offset, cell_num_);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        mother_block = CONTAINING_RECORD(block_it, CellBlock, cell_block_link_);
        cell = mother_block->cell_array_ + tmp_offset;
      }
    }
  }
  return err;
}

void ObCellArray::reset_iterator()
{
  consumed_row_num_ = 0;
  cur_row_consumed_cell_num_ = -1;
  prev_key_.assign(NULL,0);
  prev_tableid_ = OB_INVALID_ID;
}


void ObCellArray::consume_all_cell()
{
  consumed_row_num_  = 0;
  cur_row_consumed_cell_num_ = static_cast<int32_t>(limit_cell_num_ - 1);
}


int  ObCellArray::unget_cell()
{
  int err = OB_SUCCESS;
  if (cur_row_consumed_cell_num_ > 0)
  {
    cur_row_consumed_cell_num_ --;
  }
  else if (consumed_row_num_ > 0)
  {
    consumed_row_num_ --;
    cur_row_consumed_cell_num_ = static_cast<int32_t>(row_width_ - 1);
  }
  else if (0 == cur_row_consumed_cell_num_ && 0 == consumed_row_num_)
  {
    cur_row_consumed_cell_num_ --;
  }
  else
  {
    TBSYS_LOG(WARN,"cannot unget cell, param error [cur_row_consumed_cell_num_:%d,consumed_row_num_:%ld]",
      cur_row_consumed_cell_num_, consumed_row_num_);
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}


int ObCellArray::next_cell(int64_t & cur_cell_offset)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS == (err = next_cell()))
  {
    cur_cell_offset = sorted_row_offsets_[static_cast<int32_t>(consumed_row_num_)] + cur_row_consumed_cell_num_;
  }
  else if (OB_ITER_END == err)
  {
  }
  else
  {
    TBSYS_LOG(WARN,"fail to call next cell [err:%d]", err);
  }
  return err;
}

int ObCellArray::next_cell()
{
  int err = OB_SUCCESS;
  int64_t org_cur_row_consumed_cell_num = cur_row_consumed_cell_num_;
  int64_t org_consumed_row_num = consumed_row_num_;
  cur_row_consumed_cell_num_ ++;
  if (consumed_row_num_*row_width_ + cur_row_consumed_cell_num_ >= limit_cell_num_)
  {
    err = OB_ITER_END;
  }
  if (OB_SUCCESS == err && cur_row_consumed_cell_num_ >= row_width_)
  {
    consumed_row_num_ ++;
    cur_row_consumed_cell_num_ = 0;
  }
  if (OB_SUCCESS == err)
  {
    cur_cell_row_changed_ = false;
  }
  if (OB_ITER_END == err)
  {
    cur_row_consumed_cell_num_ = static_cast<int32_t>(org_cur_row_consumed_cell_num);
    consumed_row_num_ = org_consumed_row_num;
  }
  return err;
}

int ObCellArray::get_cell(ObInnerCellInfo **cell)
{
  return get_cell(cell, NULL);
}

int ObCellArray::get_cell(ObInnerCellInfo **cell, bool * is_row_changed)
{
  int err = OB_SUCCESS;
  if (NULL == cell)
  {
    TBSYS_LOG(WARN,"param error [cell:%p]", cell);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err && cur_row_consumed_cell_num_ < 0)
  {
    cur_row_consumed_cell_num_ = 0;
  }
  if (OB_SUCCESS == err
    && consumed_row_num_*row_width_ + cur_row_consumed_cell_num_ >= limit_cell_num_)
  {
    err = OB_ITER_END;
  }
  if (OB_SUCCESS == err)
  {
    if (NULL != heap_ && heap_size_ > 0)
    {
      err = get_cell(heap_[consumed_row_num_] + cur_row_consumed_cell_num_, *cell);
    }
    else
    {
      err = get_cell(sorted_row_offsets_[static_cast<int32_t>(consumed_row_num_)] + cur_row_consumed_cell_num_,
        *cell);
    }
  }
  if (OB_SUCCESS == err
    && ((*cell)->row_key_ != prev_key_ || (*cell)->table_id_ != prev_tableid_))
  {
    cur_cell_row_changed_ = true;
    prev_key_ = (*cell)->row_key_;
    prev_tableid_ = (*cell)->table_id_;
  }
  if (NULL != is_row_changed)
  {
    *is_row_changed = cur_cell_row_changed_;
  }
  return err;
}


int ObCellArray::orderby(int64_t row_width, OrderDesc *order_desc, int64_t desc_size)
{
  return topk(row_width, order_desc, desc_size, 0);
}

int oceanbase::common::ObCellArray::topk(
  const int64_t row_width, OrderDesc *order_desc,
  const int64_t desc_size, const int64_t sharding_row_cnt)
{
  int err = OB_SUCCESS;

  if (row_width < 0 || cell_num_ % row_width != 0)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN,"param error [row_width:%ld,cell_num_:%ld]", row_width, cell_num_);
  }
  else if (row_width_ != cell_num_ && row_width_ != row_width)
  {
    TBSYS_LOG(WARN,"param error, row_width not coincident [row_width:%ld,row_width_:%ld,"
      "cell_num_:%ld]", row_width, row_width_, cell_num_);
    err  = OB_INVALID_ARGUMENT;
  }
  else if ((NULL == order_desc && 0 != desc_size)
    || (NULL != order_desc && 0 == desc_size)
    || desc_size > OB_MAX_COLUMN_NUMBER
    || sharding_row_cnt < 0)
  {
    TBSYS_LOG(WARN,"param error [order_desc:%p,desc_size:%ld,max_desc_size:%ld,"
                   "sharding_row_cnt:%ld]",
      order_desc, desc_size, OB_MAX_COLUMN_NUMBER, sharding_row_cnt);
    err = OB_INVALID_ARGUMENT;
  }
  else if (desc_size > row_width)
  {
    TBSYS_LOG(WARN,"param error, desc_size should less or equal to row_width"
      "[desc_size:%ld,row_width:%ld]", desc_size, row_width);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    for (int32_t i = 0; i < desc_size && OB_SUCCESS == err; i++)
    {
      if (order_desc[i].cell_idx_ >= row_width)
      {
        TBSYS_LOG(WARN,"cell idx should less than row_width [order_desc_idx:%d,cell_idx:%d,"
          "row_width:%ld]", i, order_desc[i].cell_idx_, row_width);
        err = OB_INVALID_ARGUMENT;
      }
    }
  }

  if (get_cell_size() > 0)
  {
    if (1 != sorted_row_offsets_.size())
    {
      TBSYS_LOG(WARN,"topk, orderby and reverse_rows can be only called once "
                     "[sorted_row_offsets_.size():%u]",
        sorted_row_offsets_.size());
      err = OB_INVALID_ARGUMENT;
    }

    if (OB_SUCCESS == err)
    {
      row_width_ = row_width;
      row_num_ = cell_num_ / row_width_;
      memcpy(orderby_columns_,order_desc, desc_size*sizeof(OrderDesc));
      orderby_column_num_ = static_cast<int32_t>(desc_size);
      if (0 == sharding_row_cnt)
      {
        for (int64_t row_idx = 1;
          row_idx < row_num_ && OB_SUCCESS == err;
          row_idx ++)
        {
          err = sorted_row_offsets_.push_back(row_width_*row_idx);
        }
        RowComp row_cmp(orderby_columns_,orderby_column_num_,*this);
        std::sort(sorted_row_offsets_.begin(), sorted_row_offsets_.end(),row_cmp);
      }
      else
      {
        err = heap_topk(sharding_row_cnt);
      }
    }
  }

  return err;
}

int oceanbase::common::ObCellArray::heap_topk(const int64_t sharding_row_cnt)
{
  int err =  OB_SUCCESS;

  if (sharding_row_cnt >= row_num_)
  {
    TBSYS_LOG(WARN, "heap topk is only used for case that "
                    "row_num_ > sharding_row_cnt, row_num_=%ld, "
                    "sharding_row_cnt=%ld",
      row_num_, sharding_row_cnt);
    err = OB_ERROR;
  }
  else
  {
    RowComp row_cmp(orderby_columns_, orderby_column_num_, *this);
    err = heap_buf_.ensure_space(sharding_row_cnt * sizeof(int64_t),
      ObModIds::OB_MS_CELL_ARRAY);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to allocate memory for heap buffer");
    }
    else
    {
      heap_ = reinterpret_cast<int64_t*>(heap_buf_.get_buffer());
      heap_size_ = sharding_row_cnt;
      for (int64_t i = 0; i < heap_size_; ++i)
      {
        heap_[i] = row_width_ * i;
      }
      std::make_heap(&heap_[0], &heap_[heap_size_], row_cmp);
    }

    if (OB_SUCCESS == err)
    {
      for (int64_t i = heap_size_; i < row_num_; ++i)
      {
        if (row_cmp(i * row_width_, heap_[0]))
        {
          std::pop_heap(&heap_[0], &heap_[heap_size_], row_cmp);
          heap_[heap_size_ - 1] = i * row_width_;
          std::push_heap(&heap_[0], &heap_[heap_size_], row_cmp);
        }
      }

      std::sort_heap(&heap_[0], &heap_[heap_size_], row_cmp);
      limit_cell_num_ = sharding_row_cnt * row_width_;
    }
  }

  return err;
}

void oceanbase::common::ObCellArray::get_topk_heap(
  int64_t*& heap, int64_t& heap_size) const
{
  heap = heap_;
  heap_size = heap_size_;
}

int oceanbase::common::ObCellArray::reverse_rows(const int64_t row_width_in)
{
  int err = 0;
  if (row_width_in < 0 || cell_num_ % row_width_in != 0)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN,"param error [row_width_in:%ld,cell_num_:%ld]", row_width_in, cell_num_);
  }
  if (OB_SUCCESS == err && row_width_ != cell_num_ && row_width_ != row_width_in)
  {
    TBSYS_LOG(WARN,"param error, row_width not coincident [row_width_in:%ld,row_width_:%ld,"
      "cell_num_:%ld]", row_width_in, row_width_, cell_num_);
    err  = OB_INVALID_ARGUMENT;
  }
  if (get_cell_size()>0)
  {
    if (1 != sorted_row_offsets_.size())
    {
      TBSYS_LOG(WARN,"orderby and reverse_rows can be only called once");
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err)
    {
      row_width_ = row_width_in;
      row_num_ = cell_num_ / row_width_;
      for (int64_t row_idx = 1;
        row_idx < row_num_ && OB_SUCCESS == err;
        row_idx ++)
      {
        err = sorted_row_offsets_.push_back(row_width_*row_idx);
      }
    }
    if (OB_SUCCESS == err)
    {
      std::reverse(sorted_row_offsets_.begin(), sorted_row_offsets_.end());
    }
  }
  return err;
}

int ObCellArray::limit(int64_t offset, int64_t count, int32_t row_width)
{
  int err = 0;
  if (offset < 0 || count < 0 || row_width <= 0)
  {
    TBSYS_LOG(WARN,"param error [offset:%ld,count:%ld,row_width:%d]", offset, count, row_width);
    err  = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err && row_width_ != cell_num_ && row_width_ != row_width)
  {
    TBSYS_LOG(WARN,"param error, row_width not coincident [row_width:%d,row_width_:%ld,"
      "cell_num_:%ld]", row_width, row_width_, cell_num_);
    err  = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err && cell_num_ % row_width != 0)
  {
    TBSYS_LOG(WARN,"param error, cells in array not compose rows [cell_num_:%ld,row_width:%d]",
      cell_num_, row_width);
    err  = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err && 0 == count)
  {
    /// unlimited
    count = cell_num_ / row_width;
  }
  if (OB_SUCCESS == err && 0 < count)
  {
    limit_cell_num_ = std::min<int64_t>(row_width * (count+offset), cell_num_);
    if (1 == sorted_row_offsets_.size())
    {
      /// do not sorted
      row_width_ = limit_cell_num_;
      cur_row_consumed_cell_num_ = static_cast<int32_t>(cur_row_consumed_cell_num_ + offset *row_width);
    }
    else
    {
      row_width_ = row_width;
      consumed_row_num_ += offset;
    }
  }
  return err;
}

ObCellArray::RowComp::RowComp(OrderDesc * order_desc, int32_t order_column_num, const ObCellArray & cell_array)
{
  desc_ = order_desc;
  desc_size_ = order_column_num;
  cell_array_ = &cell_array;
}


ObCellArray::RowComp::~RowComp()
{
  desc_ = NULL;
  desc_size_ = 0;
  cell_array_ = NULL;
}


bool ObCellArray::RowComp::operator()(int64_t off1, int64_t off2)
{
  int result = 0;
  int err = 0;
  ObInnerCellInfo *cell1 = NULL;
  ObInnerCellInfo *cell2 = NULL;
  int64_t cell1_off = -1;
  int64_t cell2_off = -1;
  int32_t cur_column_idx = 0;
  while (0 == result && OB_SUCCESS == err && cur_column_idx < desc_size_)
  {
    cell1_off = off1 + desc_[cur_column_idx].cell_idx_;
    cell2_off = off2 + desc_[cur_column_idx].cell_idx_;
    err = cell_array_->get_cell(cell1_off, cell1);
    if (OB_SUCCESS == err)
    {
      err = cell_array_->get_cell(cell2_off, cell2);
    }
    else
    {
      TBSYS_LOG(ERROR, "unexpected error [cell_off:%ld,err:%d]", cell1_off, err);
      result = -1;
    }
    if (OB_SUCCESS == err)
    {
      if (cell1->value_ < cell2->value_)
      {
        result = -1;
      }
      else if (cell1->value_  > cell2->value_)
      {
        result = 1;
      }
      else
      {
        result = 0;
      }
      if ((result != 0) && (ObScanParam::DESC == desc_[cur_column_idx].order_))
      {
        result *= -1;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "unexpected error [cell_off:%ld,err:%d]", cell2_off, err);
      result = -1;
    }
    cur_column_idx ++;
  }
  if (0 == result)
  {
    result = (off1 > off2) ? 1 : -1;
  }
  return(result < 0);
}

ObCellArray::iterator::iterator()
{
  array_ = NULL;
  cur_offset_ = -1;
}

ObCellArray::iterator::~iterator()
{
  array_ = NULL;
  cur_offset_ = -1;
}

void ObCellArray::iterator::set_args(ObCellArray & cell_array, int64_t offset)
{
  array_ = &cell_array;
  cur_offset_ = offset;
}

ObCellArray::iterator &ObCellArray::iterator::operator ++()
{
  if (NULL != array_)
  {
    cur_offset_ ++;
  }
  return *this;
}

ObCellArray::iterator ObCellArray::iterator::operator ++(int)
{
  ObCellArray::iterator tmp_it = *this;
  if (NULL != array_)
  {
    cur_offset_ ++;
  }
  return tmp_it;
}

ObInnerCellInfo & ObCellArray::iterator::operator*()
{
  ObInnerCellInfo *result = &cell_ugly_used_for_empty_iterator_;
  int err = OB_SUCCESS;
  if (NULL == array_ || cur_offset_ < 0 || cur_offset_  >= array_->get_cell_size())
  {
    TBSYS_LOG(ERROR, "logic error, try to access cell out of range "
      "[array_:%p, cur_offset_:%ld, cell_num_:%ld]", array_,
      cur_offset_, (array_ == NULL) ? 0 : array_->get_cell_size());
  }
  else
  {
    err = array_->get_cell(cur_offset_, result);
    if (OB_SUCCESS != err)
    {
      result = &cell_ugly_used_for_empty_iterator_;
      TBSYS_LOG(ERROR, "unexpected error, fail to get cell [offset:%ld,cell_num_:%ld,err:%d]",
        cur_offset_, array_->get_cell_size(), err);
    }
  }
  return *result;
}

ObInnerCellInfo * ObCellArray::iterator::operator->()
{
  return &(*(*this));
}

ObCellArray::iterator ObCellArray::iterator::operator +(int64_t inc_num)
{
  ObCellArray::iterator it = *this;
  if (NULL != array_)
  {
    it.cur_offset_ += inc_num;
  }
  return it;
}

bool ObCellArray::iterator::operator !=(const ObCellArray::iterator &other)
{
  return(array_ != other.array_ || cur_offset_ != other.cur_offset_);
}

ObCellArray::iterator ObCellArray::begin()
{
  iterator it;
  it.set_args(*this,0);
  return it;
}

ObCellArray::iterator ObCellArray::end()
{
  iterator it;
  it.set_args(*this,cell_num_);
  return it;
}
