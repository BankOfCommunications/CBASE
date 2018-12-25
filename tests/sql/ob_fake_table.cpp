/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_table.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_fake_table.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

ObFakeTable::ObFakeTable()
  :table_id_(TABLE_ID), row_count_(0), get_count_(0)
{
  buff_[0] = '\0';
}

ObFakeTable::~ObFakeTable()
{
}

void ObFakeTable::set_row_count(const int64_t count)
{
  row_count_ = count;
}

int ObFakeTable::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  UNUSED(child_idx);
  UNUSED(child_operator);
  return ret;
}

int ObFakeTable::open()
{
  int ret = OB_SUCCESS;
  get_count_ = 0;
  curr_row_.set_row_desc(row_desc_);
  for (int64_t i = 0; i < COLUMN_COUNT; ++i)
  {
    if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id_, OB_APP_MIN_COLUMN_ID+i)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  } // end for
  return ret;
}

int ObFakeTable::close()
{
  int ret = OB_SUCCESS;
  row_desc_.reset();
  return ret;
}

int ObFakeTable::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (0 >= row_desc_.get_column_num())
  {
    TBSYS_LOG(ERROR, "not opened");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObFakeTable::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (get_count_ < row_count_)
  {
    if (OB_SUCCESS != (ret = cons_curr_row(get_count_)))
    {
      TBSYS_LOG(WARN, "failed to cons current row, err=%d", ret);
    }
    else
    {
      row = &curr_row_;
      ++get_count_;
    }
  }
  else
  {
    TBSYS_LOG(INFO, "end of table");
    ret = OB_ITER_END;
  }
  return ret;
}

/***************************************************************************************************
  c0       | c1      | c2        | c3        | c4        | c5         | c6    | c7   |
-----------------------------------------------------------------------------------------------------
  rand str | row_idx | row_idx%2 | row_idx%3 | row_idx/2 | row_idx/3  | c1+c2 |c3+c4 |
***************************************************************************************************/
int ObFakeTable::cons_curr_row(const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  int64_t c2_val = 0, c3_val = 0, c4_val = 0, c5_val = 0, c6_val = 0;
  ObObj cell;
  // column 0: varchar
  if (OB_SUCCESS != (ret = cons_varchar_cell(cell)))
  {
    TBSYS_LOG(WARN, "failed to cons varchar cell");
  }
  else if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID, cell)))
  {
    TBSYS_LOG(WARN, "failed to set cell");
  }
  // column 1: int, row_idx
  cell.set_int(row_idx);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+1, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 2: int, row_idx % 2
  cell.set_int(c2_val = row_idx % 2);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+2, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 3: int, row_idx % 3
  cell.set_int(c3_val = row_idx % 3);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+3, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 4 int, row_idx / 2, e.g. 0,0,1,1,2,2,3,3,...
  cell.set_int(c4_val = row_idx / 2);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+4, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 5 int, row_idx / 3, e.g. 0,0,0,1,1,1,2,2,2,...
  cell.set_int(c5_val = row_idx / 3);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+5, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 6 int, c2+c3
  cell.set_int(c6_val = c2_val + c3_val);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+6, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 7 int, c4+c5
  cell.set_int(c6_val = c4_val + c5_val);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+7, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }

  // column 8: column with null values, value is null for each even row
  if (row_idx % 2 == 0)
  {
    cell.set_null();
  }
  else
  {
    cell.set_int(row_idx);
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+8, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 9 int, row_idx/2*2, e.g. 0,0,2,2,4,4,6,6,...
  cell.set_int(row_idx / 2 * 2);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+9, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 10 int, row_idx/3*3, e.g. 0,0,0,3,3,3,6,6,6,...
  cell.set_int(row_idx / 3 * 3);
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+10, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell");
    }
  }
  // column 11-15: int, random data
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 11; i < COLUMN_COUNT; ++i)
    {
      if (OB_SUCCESS != (ret = cons_random_int_cell(cell)))
      {
        break;
      }
      else if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID+i, cell)))
      {
        break;
      }
    } // end for
  }
  return ret;
}

int ObFakeTable::cons_varchar_cell(common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  int charnum = rand_int(VARCHAR_CELL_BUFF_SIZE);
  for (int i = 0; i < charnum; ++i)
  {
    buff_[i] = (char)('A'+rand_int(26));
  }
  ObString varchar;
  varchar.assign_ptr(buff_, charnum);
  cell.set_varchar(varchar);
  return ret;
}

int ObFakeTable::cons_random_int_cell(common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  cell.set_int(rand());
  return ret;
}

inline int ObFakeTable::rand_int(int max)
{
  double fmax = max;
  int j = (int) (fmax * (rand() / (RAND_MAX + 1.0)));
  return j;
}

int64_t ObFakeTable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "FakeTableForTesting\n");
  return pos;
}
