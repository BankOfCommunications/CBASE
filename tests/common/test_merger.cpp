/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_ups_merger.cpp,v 0.1 2010/09/21 10:43:53 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <gtest/gtest.h>
#include "test_helper.h"
#include "tblog.h"
#include "ob_merger.h"
#include "ob_malloc.h"
#include "ob_action_flag.h"
#include "mock_mem_iterator.h"
#include "test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;

static CharArena  allocator_;

void check_cell_with_op(const ObCellInfo& ori_cell_info, const ObCellInfo& new_cell_info,
    const int64_t op_type)
{
  // op_type = 0, update op
  // op_type = 1, add op
  // op_type = 2, row not exist op
  if (0 == op_type)
  {
    // update op
    check_cell(ori_cell_info, new_cell_info);
  }
  else if (1 == op_type)
  {
    // add op
    check_cell(ori_cell_info, new_cell_info);
    int64_t tmp_int_val = 0;
    bool tmp_is_add = 0;
    new_cell_info.value_.get_int(tmp_int_val, tmp_is_add);
    EXPECT_TRUE(tmp_is_add);
  }
  else if (2 == op_type)
  {
    // row not exist
    int64_t tmp_ext = 0;
    new_cell_info.value_.get_ext(tmp_ext);
    EXPECT_EQ((int64_t) ObActionFlag::OP_ROW_DOES_NOT_EXIST, tmp_ext);
  }
}

namespace oceanbase
{
namespace tests
{
namespace common
{

class TestObMerger : public ::testing::Test
{
public:
  virtual void SetUp()
  {

  }

  virtual void TearDown()
  {

  }
};

TEST_F(TestObMerger, test_one_iterator)
{
  MockMemIterator iterator;

  static const int64_t COL_NUM = 10;
  uint64_t table_id = 10;
  ObRowkey row_key = make_rowkey("row_key", &allocator_);

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[COL_NUM];
  // init cell infos
  for (int64_t i = 0; i < COL_NUM; ++i)
  {
    cell_infos[i].column_id_ = i + 1;
    cell_infos[i].table_id_ = table_id;
    cell_infos[i].row_key_ = row_key;
    cell_infos[i].value_.set_int(1000 + i);
  }

  for (int64_t i = 0; i < COL_NUM; ++i)
  {
    err = iterator.add_cell(cell_infos[i]);
    ASSERT_EQ(0, err);
  }

  // check iterator
  int64_t count = 0;
  ObCellInfo* cur_cell = NULL;

  while (OB_SUCCESS == iterator.next_cell())
  {
    err = iterator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    check_cell(cell_infos[count], *cur_cell);
    ++count;
  }
  EXPECT_EQ(COL_NUM, count);

  // check result
  iterator.reset();
  ObMerger merger;
  merger.add_iterator(&iterator);

  count = 0;
  cur_cell = NULL;
  bool is_row_changed = false;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    check_cell(cell_infos[count], *cur_cell);
    ++count;
  }
  EXPECT_EQ(COL_NUM, count);
}

TEST_F(TestObMerger, test_two_iterator)
{
  MockMemIterator iter[2];

  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = iter[j%2].add_cell(cell_infos[i][j]);
      ASSERT_EQ(0, err);
    }
  }

  // check iterator
  int64_t count = 0;
  ObCellInfo* cur_cell = NULL;
  ObMerger merger;
  merger.add_iterator(&iter[0]);
  merger.add_iterator(&iter[1]);

  bool is_row_changed = false;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    //fprintf(stderr, "AAA column_id=%ld\n", cur_cell->column_id_);
    if ((count % COL_NUM) < (COL_NUM + 1) / 2)
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM) * 2], *cur_cell);
    }
    else
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM - (COL_NUM + 1) / 2) * 2 + 1], *cur_cell);
    }

    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);
}

TEST_F(TestObMerger, test_three_iterator)
{
  MockMemIterator iter[3];

  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 10;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = iter[j%3].add_cell(cell_infos[i][j]);
      ASSERT_EQ(0, err);
    }
  }

  // check iterator
  int64_t count = 0;
  ObCellInfo* cur_cell = NULL;
  ObMerger merger;
  merger.add_iterator(&iter[0]);
  merger.add_iterator(&iter[1]);
  merger.add_iterator(&iter[2]);

  bool is_row_changed = false;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    //fprintf(stderr, "AAA column_id=%ld\n", cur_cell->column_id_);
    int64_t sum1 = (COL_NUM + 2) / 3;
    int64_t sum2 = (COL_NUM + 1) / 3;
    if ((count % COL_NUM) < sum1)
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM) * 3], *cur_cell);
    }
    else if ((count % COL_NUM) < sum1 + sum2)
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM - sum1) * 3 + 1], *cur_cell);
    }
    else
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM - sum1 - sum2) * 3 + 2], *cur_cell);
    }

    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);
}


TEST_F(TestObMerger, test_del_row)
{
  MockMemIterator iter[2];

  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  // 0: does nothing;
  // 1: add del row to first iter;
  // 2: add del row to second iter
  int del_row[ROW_NUM];
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    del_row[i] = rand() % 3;
  }

  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    ObCellInfo del_row_cell;
    del_row_cell.table_id_ = cell_infos[i][0].table_id_;
    del_row_cell.row_key_ = cell_infos[i][0].row_key_;
    del_row_cell.value_.set_ext(ObActionFlag::OP_DEL_ROW);
    int64_t tmp_ext = 0;
    del_row_cell.value_.get_ext(tmp_ext);
    EXPECT_EQ((int64_t) ObActionFlag::OP_DEL_ROW, tmp_ext);
    if (1 == del_row[i])
    {
      iter[0].add_cell(del_row_cell);
    }
    else if (2 == del_row[i])
    {
      iter[1].add_cell(del_row_cell);
    }

    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = iter[j%2].add_cell(cell_infos[i][j]);
      ASSERT_EQ(0, err);
    }
  }

  // check iterator
  ObMerger merger;
  merger.add_iterator(&iter[0]);
  merger.add_iterator(&iter[1]);

  ObCellInfo* cur_cell = NULL;
  bool is_row_changed = false;
  int64_t row_idx = -1;
  int64_t col_idx = 0;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    if (true == is_row_changed)
    {
      ++row_idx;
      col_idx = 0;
    }
    else
    {
      ++col_idx;
    }

    int64_t middle = (COL_NUM + 1) / 2;
    //fprintf(stderr, "AAA column_id=%ld\n", cur_cell->column_id_);
    if (2 == del_row[row_idx])
    {
      if (0 == col_idx)
      {
        int64_t ext_val = 0;
        cur_cell->value_.get_ext(ext_val);
        EXPECT_EQ((int64_t) ObActionFlag::OP_DEL_ROW, ext_val);
      }
      else
      {
        check_cell(cell_infos[row_idx][(col_idx - 1) * 2 + 1], *cur_cell);
      }
    }
    else if (1 == del_row[row_idx])
    {
      if (0 == col_idx)
      {
        int64_t ext_val = 0;
        cur_cell->value_.get_ext(ext_val);
        EXPECT_EQ((int64_t) ObActionFlag::OP_DEL_ROW, ext_val);
      }
      else if (col_idx < middle + 1)
      {
        check_cell(cell_infos[row_idx][(col_idx - 1) * 2], *cur_cell);
      }
      else
      {
        check_cell(cell_infos[row_idx][(col_idx - 1 - middle) * 2 + 1], *cur_cell);
      }
    }
    else
    {
      if (col_idx < middle)
      {
        check_cell(cell_infos[row_idx][col_idx * 2], *cur_cell);
      }
      else
      {
        check_cell(cell_infos[row_idx][(col_idx - middle) * 2 + 1], *cur_cell);
      }
    }
  }
  //EXPECT_EQ(ROW_NUM * COL_NUM, count);
  EXPECT_EQ(ROW_NUM, row_idx + 1);
}

TEST_F(TestObMerger, test_all_op)
{
  MockMemIterator iter[2];

  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  // 0: does nothing;
  // 1: add del row to first iter;
  // 2: add del row to second iter
  int del_row[ROW_NUM];
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    del_row[i] = rand() % 3;
  }

  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      //cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
      int op_type = static_cast<int32_t>(j % 3);
      if (0 == op_type)
      {
        // update op
        cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
      }
      else if (1 == op_type)
      {
        // add op
        cell_infos[i][j].value_.set_int(100 + i * COL_NUM + j, true);
      }
      else
      {
        // row not exist
        cell_infos[i][j].value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
      }
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    ObCellInfo del_row_cell;
    del_row_cell.table_id_ = cell_infos[i][0].table_id_;
    del_row_cell.row_key_ = cell_infos[i][0].row_key_;
    del_row_cell.value_.set_ext(ObActionFlag::OP_DEL_ROW);
    int64_t tmp_ext = 0;
    del_row_cell.value_.get_ext(tmp_ext);
    EXPECT_EQ((int64_t) ObActionFlag::OP_DEL_ROW, tmp_ext);
    if (1 == del_row[i])
    {
      iter[0].add_cell(del_row_cell);
    }
    else if (2 == del_row[i])
    {
      iter[1].add_cell(del_row_cell);
    }

    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = iter[j%2].add_cell(cell_infos[i][j]);
      ASSERT_EQ(0, err);
    }
  }

  // check iterator
  ObMerger merger;
  merger.add_iterator(&iter[0]);
  merger.add_iterator(&iter[1]);

  ObCellInfo* cur_cell = NULL;
  bool is_row_changed = false;
  int64_t row_idx = -1;
  int64_t col_idx = 0;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    if (true == is_row_changed)
    {
      ++row_idx;
      col_idx = 0;
    }
    else
    {
      ++col_idx;
    }

    int64_t middle = (COL_NUM + 1) / 2;
    //fprintf(stderr, "AAA column_id=%ld\n", cur_cell->column_id_);
    if (2 == del_row[row_idx])
    {
      if (0 == col_idx)
      {
        int64_t ext_val = 0;
        cur_cell->value_.get_ext(ext_val);
        EXPECT_EQ((int64_t) ObActionFlag::OP_DEL_ROW, ext_val);
      }
      else
      {
        int64_t tmp_col_idx = (col_idx - 1) * 2 + 1;
        check_cell_with_op(cell_infos[row_idx][tmp_col_idx], *cur_cell, tmp_col_idx % 3);
      }
    }
    else if (1 == del_row[row_idx])
    {
      if (0 == col_idx)
      {
        int64_t ext_val = 0;
        cur_cell->value_.get_ext(ext_val);
        EXPECT_EQ((int64_t) ObActionFlag::OP_DEL_ROW, ext_val);
      }
      else if (col_idx < middle + 1)
      {
        int64_t tmp_col_idx = (col_idx - 1) * 2;
        check_cell_with_op(cell_infos[row_idx][tmp_col_idx], *cur_cell, tmp_col_idx % 3);
      }
      else
      {
        int64_t tmp_col_idx = (col_idx - 1 - middle) * 2 + 1;
        check_cell_with_op(cell_infos[row_idx][tmp_col_idx], *cur_cell, tmp_col_idx % 3);
      }
    }
    else
    {
      if (col_idx < middle)
      {
        int64_t tmp_col_idx = col_idx * 2;
        check_cell_with_op(cell_infos[row_idx][tmp_col_idx], *cur_cell, tmp_col_idx % 3);
      }
      else
      {
        int64_t tmp_col_idx = (col_idx - middle) * 2 + 1;
        check_cell_with_op(cell_infos[row_idx][tmp_col_idx], *cur_cell, tmp_col_idx % 3);
      }
    }
  }
  //EXPECT_EQ(ROW_NUM * COL_NUM, count);
  EXPECT_EQ(ROW_NUM, row_idx + 1);
}

TEST_F(TestObMerger, test_empty_iterator)
{
  MockMemIterator iterator;

  static const int64_t COL_NUM = 10;
  uint64_t table_id = 10;
  ObRowkey row_key = make_rowkey("row_key", &allocator_);

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[COL_NUM];
  // init cell infos
  for (int64_t i = 0; i < COL_NUM; ++i)
  {
    cell_infos[i].column_id_ = i + 1;
    cell_infos[i].table_id_ = table_id;
    cell_infos[i].row_key_ = row_key;
    cell_infos[i].value_.set_int(1000 + i);
  }

  for (int64_t i = 0; i < COL_NUM; ++i)
  {
    err = iterator.add_cell(cell_infos[i]);
    ASSERT_EQ(0, err);
  }

  // check iterator
  int64_t count = 0;
  ObCellInfo* cur_cell = NULL;

  while (OB_SUCCESS == iterator.next_cell())
  {
    err = iterator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    check_cell(cell_infos[count], *cur_cell);
    ++count;
  }
  EXPECT_EQ(COL_NUM, count);

  // check result
  iterator.reset();
  ObMerger merger;
  merger.add_iterator(&iterator);
  MockMemIterator empty_iterator;
  merger.add_iterator(&empty_iterator);

  count = 0;
  cur_cell = NULL;
  bool is_row_changed = false;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    check_cell(cell_infos[count], *cur_cell);
    ++count;
  }
  EXPECT_EQ(COL_NUM, count);

  iterator.reset();
  merger.reset();
  merger.add_iterator(&empty_iterator);
  merger.add_iterator(&iterator);

  count = 0;
  cur_cell = NULL;
  is_row_changed = false;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    check_cell(cell_infos[count], *cur_cell);
    ++count;
  }
  EXPECT_EQ(COL_NUM, count);
}

TEST_F(TestObMerger, test_empty_row)
{
  MockMemIterator iter[2];

  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  int64_t row_not_exist[ROW_NUM];
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    row_not_exist[i] = rand() % 3;
  }

  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      if (0 == row_not_exist[i])
      {
        // all row exist
        err = iter[j%2].add_cell(cell_infos[i][j]);
        ASSERT_EQ(0, err);
      }
      else if (1 == row_not_exist[i])
      {
        // iter1 row not exist
        if (j % 2 == 1)
        {
          err = iter[j%2].add_cell(cell_infos[i][j]);
          ASSERT_EQ(0, err);
        }
      }
      else
      {
        // iter2 row not exist
        if (j % 2 == 0)
        {
          err = iter[j%2].add_cell(cell_infos[i][j]);
          ASSERT_EQ(0, err);
        }
      }
    }
  }

  // check iterator
  int64_t count = 0;
  ObCellInfo* cur_cell = NULL;
  ObMerger merger;
  merger.add_iterator(&iter[0]);
  merger.add_iterator(&iter[1]);

  bool is_row_changed = false;
  int64_t row_idx = -1;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    if (is_row_changed)
    {
      ++row_idx;
      count = 0;
    }
    //fprintf(stderr, "AAA column_id=%ld\n", cur_cell->column_id_);
    if (row_not_exist[row_idx] == 0)
    {
      if (count < (COL_NUM + 1) / 2)
      {
        check_cell(cell_infos[row_idx][count * 2], *cur_cell);
      }
      else
      {
        check_cell(cell_infos[row_idx][(count - (COL_NUM + 1) / 2) * 2 + 1], *cur_cell);
      }
    }
    else if (row_not_exist[row_idx] == 1)
    {
      // iter1 row not exist
      check_cell(cell_infos[row_idx][count * 2 + 1], *cur_cell);
    }
    else
    {
      // iter2 row not exist
      check_cell(cell_infos[row_idx][count * 2], *cur_cell);
    }

    ++count;
  }
  EXPECT_EQ(ROW_NUM - 1, row_idx);
}

TEST_F(TestObMerger, test_two_iterator_with_error)
{
  MockMemIterator iter[2];

  static const int64_t ROW_NUM = 10;
  static const int64_t COL_NUM = 5;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = iter[j%2].add_cell(cell_infos[i][j]);
      ASSERT_EQ(0, err);
    }
  }

  ObMerger merger;
  bool is_row_changed = false;

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      iter[0].reset();
      iter[1].reset();
      merger.reset();
      merger.add_iterator(&iter[0]);
      merger.add_iterator(&iter[1]);

      if (j % 2 == 0)
      {
        iter[0].set_err(i * ((COL_NUM + 1) / 2) + j / 2, OB_ERROR);
      }
      else
      {
        iter[1].set_err(i * (COL_NUM / 2) + j / 2, OB_ERROR);
      }
      int err_code = OB_ITER_END;
      ObCellInfo* cur_cell = NULL;
      while (OB_SUCCESS == (err_code=merger.next_cell()))
      {
        err_code = merger.get_cell(&cur_cell, &is_row_changed);
        if (0 != err_code)
        {
          break;
        }
      }

      fprintf(stderr, "err_code=%d\n", err_code);
      EXPECT_NE(OB_ITER_END, err_code); // should not finish normally
      EXPECT_EQ(OB_ERROR, err_code); // end with OB_ERROR
    }
  }
}

TEST_F(TestObMerger, test_three_iterator_unordered)
{
  MockMemIterator iter[3];

  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 10;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", ROW_NUM - i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = iter[j%3].add_cell(cell_infos[i][j]);
      ASSERT_EQ(0, err);
    }
  }

  // check iterator
  int64_t count = 0;
  ObCellInfo* cur_cell = NULL;
  ObMerger merger(false);
  merger.add_iterator(&iter[0]);
  merger.add_iterator(&iter[1]);
  merger.add_iterator(&iter[2]);

  bool is_row_changed = false;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    //fprintf(stderr, "AAA column_id=%ld\n", cur_cell->column_id_);
    int64_t sum1 = (COL_NUM + 2) / 3;
    int64_t sum2 = (COL_NUM + 1) / 3;
    if ((count % COL_NUM) < sum1)
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM) * 3], *cur_cell);
    }
    else if ((count % COL_NUM) < sum1 + sum2)
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM - sum1) * 3 + 1], *cur_cell);
    }
    else
    {
      check_cell(cell_infos[count / COL_NUM][(count % COL_NUM - sum1 - sum2) * 3 + 2], *cur_cell);
    }

    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);
}

TEST_F(TestObMerger, test_empty_row_unordered)
{
  MockMemIterator iter[2];

  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;
  uint64_t table_id = 10;

  int err = OB_SUCCESS;
  char row_key_buf[ROW_NUM][50];
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  int64_t row_not_exist[ROW_NUM];
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    row_not_exist[i] = rand() % 3;
  }

  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_buf[i], "row_key_%08ld", ROW_NUM - i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].column_id_ = j + 1;
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_ = make_rowkey(row_key_buf[i], &allocator_);
      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      if (0 == row_not_exist[i])
      {
        // all row exist
        err = iter[j%2].add_cell(cell_infos[i][j]);
        ASSERT_EQ(0, err);
      }
      else if (1 == row_not_exist[i])
      {
        // iter1 row not exist
        if (j % 2 == 1)
        {
          err = iter[j%2].add_cell(cell_infos[i][j]);
          ASSERT_EQ(0, err);
        }
      }
      else
      {
        // iter2 row not exist
        if (j % 2 == 0)
        {
          err = iter[j%2].add_cell(cell_infos[i][j]);
          ASSERT_EQ(0, err);
        }
      }
    }
  }

  // check iterator
  int64_t count = 0;
  ObCellInfo* cur_cell = NULL;
  ObMerger merger(false);
  merger.add_iterator(&iter[0]);
  merger.add_iterator(&iter[1]);

  bool is_row_changed = false;
  int64_t row_idx = -1;
  while (OB_SUCCESS == merger.next_cell())
  {
    err = merger.get_cell(&cur_cell, &is_row_changed);
    EXPECT_EQ(0, err);
    ASSERT_NE((ObCellInfo*) NULL, cur_cell);
    if (is_row_changed)
    {
      ++row_idx;
      count = 0;
    }
    //fprintf(stderr, "AAA column_id=%ld\n", cur_cell->column_id_);
    if (row_not_exist[row_idx] == 0)
    {
      if (count < (COL_NUM + 1) / 2)
      {
        check_cell(cell_infos[row_idx][count * 2], *cur_cell);
      }
      else
      {
        check_cell(cell_infos[row_idx][(count - (COL_NUM + 1) / 2) * 2 + 1], *cur_cell);
      }
    }
    else if (row_not_exist[row_idx] == 1)
    {
      // iter1 row not exist
      check_cell(cell_infos[row_idx][count * 2 + 1], *cur_cell);
    }
    else
    {
      // iter2 row not exist
      check_cell(cell_infos[row_idx][count * 2], *cur_cell);
    }

    ++count;
  }
  EXPECT_EQ(ROW_NUM - 1, row_idx);
}


} // end namespace common
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

