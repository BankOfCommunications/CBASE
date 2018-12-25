/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         ob_multi_cg_scanner.cpp is for what ...
 *
 *  Version: $Id: ob_multi_cg_scanner.cpp 09/13/2012 02:46:21 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "common/ob_rowkey.h"
#include "common/utility.h"
#include "ob_multi_cg_scanner.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObMultiColumnGroupScanner::ObMultiColumnGroupScanner()
{
  reset();
}

ObMultiColumnGroupScanner::~ObMultiColumnGroupScanner()
{
  reset();
}

void ObMultiColumnGroupScanner::reset()
{
  memset(iters_, 0, sizeof(ObRowkeyIterator*) * MAX_ITER_NUM);
  iter_num_ = 0;
  row_desc_.reset();
}

int ObMultiColumnGroupScanner::add_row_iterator(ObRowkeyIterator* iterator)
{
  int ret = OB_SUCCESS;
  if (NULL == iterator)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (iter_num_ >= MAX_ITER_NUM)
  {
    TBSYS_LOG(WARN, "current iter num=%ld >= MAX_ITER_NUM=%ld", iter_num_, MAX_ITER_NUM);
    ret = OB_SIZE_OVERFLOW;
  }
  else if (OB_SUCCESS != (ret = cons_row_desc(iterator)))
  {
    TBSYS_LOG(WARN, "cons iterator's row desc error.");
  }
  else
  {
    iters_[iter_num_++] = iterator;
  }
  return ret;
}

int ObMultiColumnGroupScanner::cons_row_desc(ObRowkeyIterator* iterator)
{
  int ret = OB_SUCCESS;
  const ObRowDesc* row_desc = NULL;
  uint64_t table_id = 0;
  uint64_t column_id = 0;

  if (NULL == iterator)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = iterator->get_row_desc(row_desc) || NULL == row_desc))
  {
    TBSYS_LOG(WARN, "get_row_desc ret=%d, row_desc=%p error.", ret, row_desc);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    for (int64_t i = 0; i < row_desc->get_column_num() && OB_SUCCESS == ret; ++i)
    {
      if (OB_SUCCESS != (ret = row_desc->get_tid_cid(i, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "cannot get [%ld]th tid cid, row desc=%s", i, to_cstring(*row_desc));
      }
      else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id, column_id)))
      {
        TBSYS_LOG(WARN, "cannot add [%ld]th [%lu,%lu] to column desc.", i, table_id, column_id);
      }
    }
  }
  return ret;
}

int ObMultiColumnGroupScanner::cons_row(int64_t& cur_cell_num, const common::ObRow& row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t table_id = 0;
  uint64_t column_id = 0;

  for (int64_t i = 0; i < row.get_column_num() && OB_SUCCESS == ret; ++i, ++cur_cell_num)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "cannot get [%ld]th cell from row, ret=%d", i, ret);
    }
    else if (OB_SUCCESS != (ret = current_row_.raw_set_cell(cur_cell_num, *cell)))
    {
      TBSYS_LOG(WARN, "cannot set [%ld]th cell from row, cel_num=%ld, ret=%d", i, cur_cell_num, ret);
    }
  }

  return ret;
}

int ObMultiColumnGroupScanner::get_row_desc(const common::ObRowDesc *&row_desc) const 
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}

int ObMultiColumnGroupScanner::get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row) 
{
  int ret = OB_SUCCESS;
  const ObRowkey * first_rowkey = NULL;
  const ObRow* first_row = NULL;
  int first_result = OB_SUCCESS;
  int64_t cur_cell_num = 0;

  if (0 >= iter_num_)
  {
    ret = OB_ITER_END;
  }
  else
  {
    current_row_.set_row_desc(row_desc_);
    for (int64_t i = 0; i < iter_num_; ++i)
    {
      if (0 == i) 
      {
        first_result = iters_[i]->get_next_row(first_rowkey, first_row);
        if (OB_SUCCESS == first_result 
            && OB_SUCCESS != (ret = cons_row(cur_cell_num, *first_row)))
        {
          TBSYS_LOG(WARN, "cons first row error=%d, cur_cell_num=%ld", ret, cur_cell_num);
        }
      }
      else
      {
        rowkey = NULL;
        row = NULL;
        ret = iters_[i]->get_next_row(rowkey, row);

        if (OB_ITER_END == ret)
        {
          if (first_result != OB_ITER_END)
          {
            TBSYS_LOG(WARN, "iterator[%ld] got result[%d] <> [%d]", i, ret, first_result);
            ret = OB_ERROR;
            break;
          }
          continue;
        }
        else if (OB_SUCCESS != ret || NULL == rowkey || NULL == row)
        {
          TBSYS_LOG(WARN, "iterator[%ld] get next row ret=%d ", i, ret);
          break;
        }
        else if (ret != first_result || rowkey->compare(*first_rowkey) != 0)
        {
          TBSYS_LOG(WARN, "iterator[%ld] got result[%d] <> [%d], rowkey[%s] <> [%s]", 
              i, ret, first_result, to_cstring(*rowkey), to_cstring(*first_rowkey));
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = cons_row(cur_cell_num, *row)))
        {
          TBSYS_LOG(WARN, "const iterator[%ld] row ret=%d ", i, ret);
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      row = &current_row_;
    }
  }

  return ret;
}

