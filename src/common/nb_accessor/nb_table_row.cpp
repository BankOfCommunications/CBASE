/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * nb_table_row.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "nb_table_row.h"
#include "common/utility.h"

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

TableRow::TableRow()
  :cell_map_(NULL),
  cells_(NULL),
  cell_count_(0)
{
}

TableRow::~TableRow()
{
  if(NULL != cells_)
  {
    ob_free(cells_, ObModIds::OB_NB_ACCESSOR);
    cells_ = NULL;
    cell_count_ = 0;
  }
}

int64_t TableRow::count() const
{
  return cell_count_;
}

void TableRow::dump() const
{
  for (int64_t i = 0 ; i < cell_count_; ++i)
  {
    TBSYS_LOG(DEBUG, "cell:%ld, %s", i, print_cellinfo(&cells_[i]));
  }
}

int TableRow::init(hash::ObHashMap<const char*, int64_t>* cell_map, ObCellInfo* cells, int64_t cell_count)
{
  int ret = OB_SUCCESS;
  if(NULL == cell_map || NULL == cells || cell_count <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "cell_map[%p], cells[%p], cell_count[%ld]", cell_map, cells, cell_count);
  }
  else
  {
    this->cell_map_ = cell_map;
    this->cells_ = cells;
    this->cell_count_ = cell_count;
  }

  return ret;
}

ObCellInfo* TableRow::get_cell_info(const char* column_name) const
{
  ObCellInfo* ret = NULL;
  int err = OB_SUCCESS;

  if(NULL == column_name)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "column_name is null");
  }

  if(OB_SUCCESS == err && !check_inner_stat())
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "cells_[%p], cell_map_[%p], cell_count_[%ld]", cells_, cell_map_, cell_count_);
  }

  int64_t index = 0;
  if(OB_SUCCESS == err)
  {
    int hash_rc = 0;
    hash_rc = cell_map_->get(column_name, index);
    if(-1 == hash_rc)
    {
      ret = NULL;
      err = OB_ERROR;
      TBSYS_LOG(WARN, "cell_map_ get fail:column_name[%s]", column_name);
    }
    else if(hash::HASH_NOT_EXIST == hash_rc)
    {
      ret = NULL;
      err = OB_ERROR;
      TBSYS_LOG(WARN, "hash not exist:column_name[%s]", column_name);
    }
  }

  if(OB_SUCCESS == err)
  {
    ret = get_cell_info(index);
  }

  return ret;
}

ObCellInfo* TableRow::get_cell_info(int64_t index) const
{
  ObCellInfo* ret = NULL;
  int err = OB_SUCCESS;

  if(index < 0 || index >= cell_count_)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "index is out of range:index[%ld], cell_count_[%ld]", index, cell_count_);
  }

  if(OB_SUCCESS == err && !check_inner_stat())
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "cells_[%p], cell_map_[%p], cell_count_[%ld]", cells_, cell_map_, cell_count_);
  }

  if(OB_SUCCESS == err)
  {
    ret = &(cells_[index]);
  }

  return ret;
}


bool TableRow::check_inner_stat() const
{
  return cells_ != NULL && cell_map_ != NULL && cell_count_ > 0;
}

int TableRow::set_cell(ObCellInfo* cell, int64_t index)
{
  int ret = OB_SUCCESS;
  if(NULL == cell || index < 0 || index >= cell_count_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "cell[%p], index[%ld], cell_count_[%ld]", cell, index, cell_count_);
  }

  if(OB_SUCCESS == ret && !check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "cells_[%p], cell_map_[%p], cell_count_[%ld]", cells_, cell_map_, cell_count_);
  }


  if(OB_SUCCESS == ret)
  {
    cells_[index] = *cell;
  }

  return ret;
}

