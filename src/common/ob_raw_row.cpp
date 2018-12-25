/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_raw_row.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_raw_row.h"
//add lbzhong[Update rowkey] 20160112:b
#include "utility.h"
//add:e
using namespace oceanbase::common;
ObRawRow::ObRawRow()
  : cells_((ObObj*)cells_buffer_), cells_count_(0), reserved1_(0), reserved2_(0)
{
  memset(cells_buffer_, 0, sizeof(cells_buffer_));
}

ObRawRow::~ObRawRow()
{
}

void ObRawRow::assign(const ObRawRow &other)
{
  if (this != &other)
  {
    for (int16_t i = 0; i < other.cells_count_; ++i)
    {
      this->cells_[i] = other.cells_[i];
    }
    this->cells_count_ = other.cells_count_;
  }
}

int ObRawRow::add_cell(const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (cells_count_ >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "array overflow, cells_count=%hd", cells_count_);
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    cells_[cells_count_++] = cell;
  }
  return ret;
}
//add lbzhong [Update rowkey] 20160112:b
int ObRawRow::deep_copy(const ObRawRow& src, ModuleArena &row_alloc)
{
  int ret = OB_SUCCESS;
  if (this != &src)
  {
    for (int16_t i = 0; i < src.cells_count_; ++i)
    {
      if(OB_SUCCESS != (ret = ob_write_obj(row_alloc, src.cells_[i], this->cells_[i])))
      {
        TBSYS_LOG(WARN, "fail to write obj to cell[%d], cell=%s, ret=%d", i, to_cstring(src.cells_[i]), ret);
        break;
      }
    }
    this->cells_count_ = src.cells_count_;
  }
  return ret;
}
//add:e
