/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multiple_scan_merge.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_multiple_scan_merge.h"
#include "common/ob_row_fuse.h"

using namespace oceanbase;
using namespace sql;

ObMultipleScanMerge::ObMultipleScanMerge()
  :child_context_num_(0),
  is_cur_row_valid_(false)
{
}

void ObMultipleScanMerge::reset()
{
  child_context_num_ = 0;
  //cur_row_.reset(false, ObRow::DEFAULT_NULL);
  last_child_context_.child_ = NULL;
  last_child_context_.row_ = NULL;
  last_child_context_.seq_ = 0;
  is_cur_row_valid_ = false;
  ObMultipleMerge::reset();
}

void ObMultipleScanMerge::reuse()
{
  child_context_num_ = 0;
  //cur_row_.reset(false, ObRow::DEFAULT_NULL);
  last_child_context_.child_ = NULL;
  last_child_context_.row_ = NULL;
  last_child_context_.seq_ = 0;
  is_cur_row_valid_ = false;
  ObMultipleMerge::reuse();
}

int ObMultipleScanMerge::write_row(ObRow &row)
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

  for (int64_t i = row_desc->get_rowkey_cell_count(); OB_SUCCESS == ret && i < row.get_column_num(); i ++)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id) ))
    {
      TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = ob_write_obj(allocator_, *cell, value) ))
    {
      TBSYS_LOG(WARN, "fail to write obj:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = row.raw_set_cell(i, value) ))
    {
      TBSYS_LOG(WARN, "fail to set cell:ret[%d]", ret);
    }
  }
  return ret;
}

bool ObMultipleScanMerge::CmpFunc::operator() (ObMultipleScanMerge::ChildContext a, ObMultipleScanMerge::ChildContext b) const
{
  bool bret = false;
  const ObRowkey *r1 = NULL;
  const ObRowkey *r2 = NULL;

  if (OB_SUCCESS != a.row_->get_rowkey(r1))
  {
    TBSYS_LOG(WARN, "fail to get rowkey");
  }
  else if (OB_SUCCESS != b.row_->get_rowkey(r2))
  {
    TBSYS_LOG(WARN, "fail to get rowkey");
  }

  int32_t r = r1->compare(*r2);
  if (r > 0)
  {
    bret = true;
  }
  else if (0 == r)
  {
    bret = (a.seq_ > b.seq_);
  }
  return bret;
}

int ObMultipleScanMerge::open()
{
  int ret = OB_SUCCESS;
  const ObRow *row = NULL;
  CmpFunc cmp_func;
  const ObRowDesc *row_desc = NULL;

  child_context_num_ = 0;

  for (int32_t i=0;OB_SUCCESS == ret && i<child_num_;i++)
  {
    if (NULL != child_array_[i])
    {
      if (OB_SUCCESS != (ret = child_array_[i]->open()))
      {
        TBSYS_LOG(WARN, "fail to open child operator:ret[%d], i[%d]", ret, i);
      }
      if (OB_SUCCESS == ret)
      {
        ret = child_array_[i]->get_next_row(row);
        if (OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "fail to get next row:ret[%d], i[%d]", ret, i);
        }
        else if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
        }
        else if (OB_SUCCESS == ret)
        {
          child_context_array_[child_context_num_].child_ = child_array_[i];
          child_context_array_[child_context_num_].row_ = row;
          child_context_array_[child_context_num_].seq_ = i;
          child_context_num_ ++;
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
    }
    else
    {
      cur_row_.set_row_desc(*row_desc);
    }
  }

  if (OB_SUCCESS == ret && child_context_num_ > 0)
  {
    std::make_heap(&child_context_array_[0], &child_context_array_[child_context_num_], cmp_func);
  }

  return ret;
}

int ObMultipleScanMerge::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRowkey *cur_rowkey = NULL;
  const ObRowkey *rowkey = NULL;
  bool is_row_empty = false;

  CmpFunc cmp_func;
  ChildContext min_rowkey_child;

  is_cur_row_valid_ = false;
  cur_row_.reset(true, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
  allocator_.reuse();

  if (OB_SUCCESS == ret && 0 == child_context_num_ && NULL == last_child_context_.child_)
  {
    ret = OB_ITER_END;
  }

  while(OB_SUCCESS == ret && (child_context_num_ > 0 || NULL != last_child_context_.child_))
  {
    if (NULL == last_child_context_.child_)
    {
      std::pop_heap(&child_context_array_[0], &child_context_array_[child_context_num_], cmp_func);
      min_rowkey_child = child_context_array_[child_context_num_-1];
      child_context_num_ --;
    }
    else
    {
      min_rowkey_child = last_child_context_;
      last_child_context_.child_ = NULL;
    }

    if (is_cur_row_valid_)
    {
      if (OB_SUCCESS != (ret = cur_row_.get_rowkey(cur_rowkey)))
      {
        TBSYS_LOG(WARN, "fail to get rowkey:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = min_rowkey_child.row_->get_rowkey(rowkey)))
      {
        TBSYS_LOG(WARN, "fail to get rowkey:ret[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (!is_cur_row_valid_ || *cur_rowkey == *rowkey)
      {
        is_row_empty = !is_cur_row_valid_;
        if (OB_SUCCESS != (ret = common::ObRowFuse::fuse_row(*(min_rowkey_child.row_), cur_row_, is_row_empty, is_ups_row_)))
        {
          TBSYS_LOG(WARN, "fail to fuse ups row:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = copy_rowkey(*(min_rowkey_child.row_), cur_row_, true) ))
        {
          TBSYS_LOG(WARN, "fail to copy rowkey:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = write_row(cur_row_) ))
        {
          TBSYS_LOG(WARN, "fail to write row:ret[%d]", ret);
        }

        if (OB_SUCCESS == ret)
        {
          if (!is_ups_row_ && is_row_empty)
          {
            is_cur_row_valid_ = false;
            cur_row_.reset(true, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
            allocator_.reuse();
          }
          else
          {
            is_cur_row_valid_ = true;
          }
        }
      }
      else
      {
        last_child_context_ = min_rowkey_child;
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = min_rowkey_child.child_->get_next_row(min_rowkey_child.row_);
      if (OB_SUCCESS != ret && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "fail to get next row:ret[%d]", ret);
      }
      else if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      else if (OB_SUCCESS == ret)
      {
        child_context_array_[child_context_num_] = min_rowkey_child;
        child_context_num_ ++;
        std::push_heap(&child_context_array_[0], &child_context_array_[child_context_num_], cmp_func);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (is_cur_row_valid_)
    {
      row = &cur_row_;
    }
    else
    {
      ret = OB_ITER_END;
    }
  }
  return ret;
}



int ObMultipleScanMerge::close()
{
  int ret = OB_SUCCESS;
  for (int32_t i=0;i<child_num_;i++)
  {
    ret = child_array_[i]->close();
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMultipleScanMerge, PHY_MULTIPLE_SCAN_MERGE);
  }
}

int64_t ObMultipleScanMerge::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MuitipleScanMerge(children_num=%d is_ups_row=%c)\n",
                  child_num_, is_ups_row_?'Y':'N');
  for (int32_t i = 0; i < child_num_; ++i)
  {
    databuff_printf(buf, buf_len, pos, "Child%d:\n", i);
    if (NULL != child_array_[i])
    {
      pos += child_array_[i]->to_string(buf+pos, buf_len-pos);
    }
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObMultipleScanMerge)
{
  int ret = OB_SUCCESS;
  reset();
  ret = ObMultipleMerge::assign(other);
  return ret;
}

