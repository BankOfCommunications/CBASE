/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_merge_join.h"
#include "ob_fake_table.h"
#include <gtest/gtest.h>
using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObMergeJoinTest: public ::testing::Test
{
  public:
    ObMergeJoinTest();
    virtual ~ObMergeJoinTest();
    virtual void SetUp();
    virtual void TearDown();
  protected:
    void test_inner_join(int64_t left_row_count, int64_t right_row_count,
                         int64_t left_cid, int64_t right_cid,
                         int64_t left_factor, int64_t right_factor,
                         bool has_other_cond);
    void test_left_outer_join(int64_t left_row_count, int64_t right_row_count,
                              bool has_other_cond);
    void test_right_outer_join(int64_t left_row_count, int64_t right_row_count,
                               bool has_other_cond);
    void test_full_outer_join(int64_t left_row_count, int64_t right_row_count,
                               bool has_other_cond);
    // disallow copy
    ObMergeJoinTest(const ObMergeJoinTest &other);
    ObMergeJoinTest& operator=(const ObMergeJoinTest &other);
  private:
    // data members
};

ObMergeJoinTest::ObMergeJoinTest()
{
}

ObMergeJoinTest::~ObMergeJoinTest()
{
}

void ObMergeJoinTest::SetUp()
{
}

void ObMergeJoinTest::TearDown()
{
}

void ObMergeJoinTest::test_inner_join(int64_t left_row_count, int64_t right_row_count,
                                      int64_t left_cid, int64_t right_cid,
                                      int64_t left_factor, int64_t right_factor,
                                      bool has_other_cond)
{
  const int64_t LEFT_ROW_COUNT = left_row_count;
  const int64_t RIGHT_ROW_COUNT = right_row_count;
  static const uint64_t LEFT_TID = 1001;
  static const uint64_t RIGHT_TID = 2001;
  printf("left_row_count=%ld right_row_count=%ld\n", left_row_count, right_row_count);
  test::ObFakeTable left_input;
  left_input.set_row_count(LEFT_ROW_COUNT);
  left_input.set_table_id(LEFT_TID);
  test::ObFakeTable right_input;
  right_input.set_row_count(RIGHT_ROW_COUNT);
  right_input.set_table_id(RIGHT_TID);
  ObMergeJoin merge_join;
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(0, left_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(1, right_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_join_type(ObJoin::INNER_JOIN));
  // equijoin: left.left_col = right.right_col
  const uint64_t LEFT_COL = OB_APP_MIN_COLUMN_ID + left_cid;
  const uint64_t RIGHT_COL = OB_APP_MIN_COLUMN_ID + right_cid;
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = LEFT_TID;
    item.value_.cell_.cid = LEFT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = RIGHT_TID;
    item.value_.cell_.cid = RIGHT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    ASSERT_EQ(OB_SUCCESS, merge_join.add_equijoin_condition(expr));
  }
  // other cond: left.left_col % 2 = 0
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = LEFT_TID;
    item.value_.cell_.cid = LEFT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_MOD;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 0;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    if (has_other_cond)
    {
      ASSERT_EQ(OB_SUCCESS, merge_join.add_other_join_condition(expr));
    }
  }
  // print
  char buff[1024];
  merge_join.to_string(buff, 1024);
  printf("%s\n", buff);
  // begin
  ASSERT_EQ(OB_SUCCESS, merge_join.open());
  int64_t g = 0;
  int64_t r = 0;
  int64_t f = 0;
  if (RIGHT_ROW_COUNT/right_factor < LEFT_ROW_COUNT/left_factor)
  {
    assert(LEFT_ROW_COUNT + left_factor > RIGHT_ROW_COUNT/right_factor*left_factor);
    g = RIGHT_ROW_COUNT/right_factor;
    r = RIGHT_ROW_COUNT%right_factor;
    f = left_factor;
  }
  else
  {
    assert(RIGHT_ROW_COUNT + right_factor > LEFT_ROW_COUNT/left_factor*right_factor);
    g = LEFT_ROW_COUNT/left_factor;
    r = LEFT_ROW_COUNT%left_factor;
    f = right_factor;
  }
  printf("g=%ld r=%ld f=%ld\n", g, r, f);
  int64_t result_row_count = 0;
  int64_t vg = g;
  if (has_other_cond)
  {
    if (g % 2 == 1) // whether qualified rows
    {
      vg = g/2+1;
    }
    else
    {
      vg = g/2;
    }
  }
  result_row_count = left_factor*right_factor*vg;
  printf("vg=%ld result_row_count=%ld\n", vg, result_row_count);
  if (r != 0)
  {
    if (has_other_cond)
    {
      if (g % 2 == 0) // whether qualified rows
      {
        result_row_count += f * r;
      }
    }
    else
    {
      result_row_count += f * r;
    }
  }
  printf("result_row_count=%ld\n", result_row_count);
  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  int64_t val = 0;
  for (int64_t i = 0; i < result_row_count; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, merge_join.get_next_row(row));
    ASSERT_EQ(32, row->get_column_num());
    ASSERT_EQ(OB_SUCCESS, row->get_cell(LEFT_TID, LEFT_COL, cell));
    ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
    if (has_other_cond)
    {
      ASSERT_EQ(i/6*2, val);
    }
    else
    {
      ASSERT_EQ(i/6, val);
    }
    ASSERT_EQ(OB_SUCCESS, row->get_cell(RIGHT_TID, RIGHT_COL, cell));
    ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
    if (has_other_cond)
    {
      ASSERT_EQ(i/6*2, val);
    }
    else
    {
      ASSERT_EQ(i/6, val);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, merge_join.close());
}

TEST_F(ObMergeJoinTest, inner_join)
{
  bool has_other_cond = false;
  for (int64_t i = 0; i < 2; ++i)
  {
    if (i == 0)
    {
      has_other_cond = false;
    }
    else if (i == 1)
    {
      has_other_cond = true;
    }
    else
    {
      ASSERT_TRUE(false);
    }
    // left.c4 = right.c5
    for (int64_t row_count = 99; row_count < 99+6; ++row_count)
    {
      test_inner_join(row_count, row_count, 4, 5, 2, 3, has_other_cond);
    }
    test_inner_join(100, 0, 4, 5, 2, 3, has_other_cond);
    test_inner_join(0, 100, 4, 5, 2, 3, has_other_cond);
    test_inner_join(0, 0, 4, 5, 2, 3, has_other_cond);
    // left.c5 = right.c4
    for (int64_t row_count = 99; row_count < 99+6; ++row_count)
    {
      test_inner_join(row_count, row_count, 5, 4, 3, 2, has_other_cond);
    }
    test_inner_join(100, 0, 5, 4, 3, 2, has_other_cond);
    test_inner_join(0, 100, 5, 4, 3, 2, has_other_cond);
    test_inner_join(0, 0, 5, 4, 3, 2, has_other_cond);
  }
}

void ObMergeJoinTest::test_left_outer_join(int64_t left_row_count, int64_t right_row_count,
                          bool has_other_cond)
{
  assert(left_row_count % 6 == 0);
  assert(right_row_count % 6 == 0);
  assert(left_row_count >= right_row_count * 3);
  const int64_t LEFT_ROW_COUNT = left_row_count;
  const int64_t RIGHT_ROW_COUNT = right_row_count;
  static const uint64_t LEFT_TID = 1001;
  static const uint64_t RIGHT_TID = 2001;
  printf("left_row_count=%ld right_row_count=%ld\n", left_row_count, right_row_count);
  test::ObFakeTable left_input;
  left_input.set_row_count(LEFT_ROW_COUNT);
  left_input.set_table_id(LEFT_TID);
  test::ObFakeTable right_input;
  right_input.set_row_count(RIGHT_ROW_COUNT);
  right_input.set_table_id(RIGHT_TID);
  ObMergeJoin merge_join;
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(0, left_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(1, right_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_join_type(ObJoin::LEFT_OUTER_JOIN));
  // equijoin: left.c5 = right.c9
  // column 5 int64_t, row_idx / 3, e.g. 0,0,0,1,1,1,2,2,2,...
  // column 9 int64_t, row_idx/2*2, e.g. 0,0,2,2,4,4,6,6,...
  const uint64_t LEFT_COL = OB_APP_MIN_COLUMN_ID + 5;
  const uint64_t RIGHT_COL = OB_APP_MIN_COLUMN_ID + 9;
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = LEFT_TID;
    item.value_.cell_.cid = LEFT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = RIGHT_TID;
    item.value_.cell_.cid = RIGHT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    ASSERT_EQ(OB_SUCCESS, merge_join.add_equijoin_condition(expr));
  }
  // other cond: left.c1 % 2 = 0
  // column 1: int64_t, row_idx
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = LEFT_TID;
    item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 1; // C1
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_MOD;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 0;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    if (has_other_cond)
    {
      ASSERT_EQ(OB_SUCCESS, merge_join.add_other_join_condition(expr));
    }
  }
  // print
  char buff[1024];
  merge_join.to_string(buff, 1024);
  printf("%s\n", buff);
  // begin
  ASSERT_EQ(OB_SUCCESS, merge_join.open());
  int64_t result_row_count = 0;
  int64_t n1 = (RIGHT_ROW_COUNT / 2 * 9);
  int64_t n2 = (LEFT_ROW_COUNT - (RIGHT_ROW_COUNT * 3));
  if (has_other_cond)
  {
    n1 = n1 * 5 / 9;            // 5/9 normal rows
    n2 = n2 * 3 / 6;                // 3/6 extra rows
  }
  result_row_count = n1 + n2;
  printf("result_row_count=%ld n1=%ld n2=%ld\n", result_row_count, n1, n2);
  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  int64_t val = 0;
  for (int64_t i = 0; i < result_row_count; ++i)
  {
    printf("row=%ld\n", i);
    ASSERT_EQ(OB_SUCCESS, merge_join.get_next_row(row));
    ASSERT_EQ(32, row->get_column_num());
    ASSERT_EQ(OB_SUCCESS, row->get_cell(LEFT_TID, LEFT_COL, cell));
    // verify LEFT_COL
    ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
    int64_t mod = 0;
    if (!has_other_cond)
    {
      mod = i % 9;
      if (i < n1)
      {
        if (mod < 6)
        {
          ASSERT_EQ(i/9*2, val);
        }
        else
        {
          ASSERT_EQ((i/9)*2+1, val);
        }
      }
      else
      {
        ASSERT_EQ(RIGHT_ROW_COUNT + (i-n1)/3, val);
      }
    }
    else
    {
      mod = i % 5;
      if (i < n1)
      {
        if (mod < 4)
        {
          ASSERT_EQ(i/5*2, val);
        }
        else
        {
          ASSERT_EQ((i/5)*2+1, val);
        }
      }
      else
      {
        printf("val=%ld r=%ld i=%ld n1=%ld\n", val, RIGHT_ROW_COUNT, i, n1);
        int64_t m = (i-n1) % 3;
        if (0 == m || 1 == m)
        {
          m = (i-n1)/3*2;
        }
        else
        {
          m = (i-n1)/3*2+1;
        }
        ASSERT_EQ(RIGHT_ROW_COUNT + m, val);
      }
    }
    // verify RIGHT_COL
    ASSERT_EQ(OB_SUCCESS, row->get_cell(RIGHT_TID, RIGHT_COL, cell));
    if (!has_other_cond)
    {
      if (i < n1)
      {
        if (mod < 6)
        {
          ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
          ASSERT_EQ(i/9*2, val);
        }
        else
        {
          ASSERT_TRUE(cell->is_null());
        }
      }
      else
      {
        ASSERT_TRUE(cell->is_null());
      }
    }
    else
    {
      if (i < n1)
      {
        if (mod < 4)
        {
          ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
          ASSERT_EQ(i/5*2, val);
        }
        else
        {
          ASSERT_TRUE(cell->is_null());
        }
      }
      else
      {
        ASSERT_TRUE(cell->is_null());
      }
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, merge_join.close());
}

TEST_F(ObMergeJoinTest, left_outer_join)
{
  bool has_other_cond = false;
  for (int64_t i = 0; i < 2; ++i)
  {
    if (i == 0)
    {
      has_other_cond = false;
    }
    else if (i == 1)
    {
      has_other_cond = true;
    }
    else
    {
      ASSERT_TRUE(false);
    }
    test_left_outer_join(360, 120, has_other_cond);
    test_left_outer_join(372, 120, has_other_cond);
    test_left_outer_join(372, 0, has_other_cond);
    test_left_outer_join(0, 0, has_other_cond);
  }
}

void ObMergeJoinTest::test_right_outer_join(int64_t left_row_count, int64_t right_row_count,
                          bool has_other_cond)
{
  assert(left_row_count % 6 == 0);
  assert(right_row_count % 6 == 0);
  assert(right_row_count >= left_row_count * 3);
  const int64_t LEFT_ROW_COUNT = left_row_count;
  const int64_t RIGHT_ROW_COUNT = right_row_count;
  static const uint64_t LEFT_TID = 1001;
  static const uint64_t RIGHT_TID = 2001;
  printf("left_row_count=%ld right_row_count=%ld\n", left_row_count, right_row_count);
  test::ObFakeTable left_input;
  left_input.set_row_count(LEFT_ROW_COUNT);
  left_input.set_table_id(LEFT_TID);
  test::ObFakeTable right_input;
  right_input.set_row_count(RIGHT_ROW_COUNT);
  right_input.set_table_id(RIGHT_TID);
  ObMergeJoin merge_join;
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(0, left_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(1, right_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_join_type(ObJoin::RIGHT_OUTER_JOIN));
  // equijoin: left.c9 = right.c5
  // column 5 int64_t, row_idx / 3, e.g. 0,0,0,1,1,1,2,2,2,...
  // column 9 int64_t, row_idx/2*2, e.g. 0,0,2,2,4,4,6,6,...
  const uint64_t LEFT_COL = OB_APP_MIN_COLUMN_ID + 9;
  const uint64_t RIGHT_COL = OB_APP_MIN_COLUMN_ID + 5;
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = LEFT_TID;
    item.value_.cell_.cid = LEFT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = RIGHT_TID;
    item.value_.cell_.cid = RIGHT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    ASSERT_EQ(OB_SUCCESS, merge_join.add_equijoin_condition(expr));
  }
  // other cond: right.c1 % 2 = 0
  // column 1: int64_t, row_idx
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = RIGHT_TID;
    item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 1; // C1
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_MOD;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 0;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    if (has_other_cond)
    {
      ASSERT_EQ(OB_SUCCESS, merge_join.add_other_join_condition(expr));
    }
  }
  // print
  char buff[1024];
  merge_join.to_string(buff, 1024);
  printf("%s\n", buff);
  // begin
  ASSERT_EQ(OB_SUCCESS, merge_join.open());
  int64_t result_row_count = 0;
  int64_t n1 = (LEFT_ROW_COUNT / 2 * 9);
  int64_t n2 = (RIGHT_ROW_COUNT - (LEFT_ROW_COUNT * 3));
  if (has_other_cond)
  {
    n1 = n1 * 5 / 9;            // 5/9 normal rows
    n2 = n2 * 3 / 6;                // 3/6 extra rows
  }
  result_row_count = n1 + n2;
  printf("result_row_count=%ld n1=%ld n2=%ld\n", result_row_count, n1, n2);
  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  int64_t val = 0;
  for (int64_t i = 0; i < result_row_count; ++i)
  {
    printf("row=%ld\n", i);
    ASSERT_EQ(OB_SUCCESS, merge_join.get_next_row(row));
    ASSERT_EQ(32, row->get_column_num());
    ASSERT_EQ(OB_SUCCESS, row->get_cell(RIGHT_TID, RIGHT_COL, cell));
    // verify LEFT_COL
    ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
    int64_t mod = 0;
    if (!has_other_cond)
    {
      mod = i % 9;
      if (i < n1)
      {
        if (mod < 6)
        {
          ASSERT_EQ(i/9*2, val);
        }
        else
        {
          ASSERT_EQ((i/9)*2+1, val);
        }
      }
      else
      {
        ASSERT_EQ(LEFT_ROW_COUNT + (i-n1)/3, val);
      }
    }
    else
    {
      mod = i % 5;
      if (i < n1)
      {
        if (mod < 4)
        {
          ASSERT_EQ(i/5*2, val);
        }
        else
        {
          ASSERT_EQ((i/5)*2+1, val);
        }
      }
      else
      {
        printf("val=%ld r=%ld i=%ld n1=%ld\n", val, LEFT_ROW_COUNT, i, n1);
        int64_t m = (i-n1) % 3;
        if (0 == m || 1 == m)
        {
          m = (i-n1)/3*2;
        }
        else
        {
          m = (i-n1)/3*2+1;
        }
        ASSERT_EQ(LEFT_ROW_COUNT + m, val);
      }
    }
    // verify LEFT_COL
    ASSERT_EQ(OB_SUCCESS, row->get_cell(LEFT_TID, LEFT_COL, cell));
    if (!has_other_cond)
    {
      if (i < n1)
      {
        if (mod < 6)
        {
          ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
          ASSERT_EQ(i/9*2, val);
        }
        else
        {
          ASSERT_TRUE(cell->is_null());
        }
      }
      else
      {
        ASSERT_TRUE(cell->is_null());
      }
    }
    else
    {
      if (i < n1)
      {
        if (mod < 4)
        {
          ASSERT_EQ(OB_SUCCESS, cell->get_int(val));
          ASSERT_EQ(i/5*2, val);
        }
        else
        {
          ASSERT_TRUE(cell->is_null());
        }
      }
      else
      {
        ASSERT_TRUE(cell->is_null());
      }
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, merge_join.close());
}

TEST_F(ObMergeJoinTest, right_outer_join)
{
  bool has_other_cond = false;
  for (int64_t i = 0; i < 2; ++i)
  {
    if (i == 0)
    {
      has_other_cond = false;
    }
    else if (i == 1)
    {
      has_other_cond = true;
    }
    else
    {
      ASSERT_TRUE(false);
    }
    test_right_outer_join(120, 360, has_other_cond);
    test_right_outer_join(120, 372, has_other_cond);
    test_right_outer_join(0, 372, has_other_cond);
    test_right_outer_join(0, 0, has_other_cond);
  }
}

void ObMergeJoinTest::test_full_outer_join(int64_t left_row_count, int64_t right_row_count,
                          bool has_other_cond)
{
  assert(left_row_count % 6 == 0);
  assert(right_row_count % 6 == 0);
  const int64_t LEFT_ROW_COUNT = left_row_count;
  const int64_t RIGHT_ROW_COUNT = right_row_count;
  static const uint64_t LEFT_TID = 1001;
  static const uint64_t RIGHT_TID = 2001;
  printf("left_row_count=%ld right_row_count=%ld\n", left_row_count, right_row_count);
  test::ObFakeTable left_input;
  left_input.set_row_count(LEFT_ROW_COUNT);
  left_input.set_table_id(LEFT_TID);
  test::ObFakeTable right_input;
  right_input.set_row_count(RIGHT_ROW_COUNT);
  right_input.set_table_id(RIGHT_TID);
  ObMergeJoin merge_join;
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(0, left_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_child(1, right_input));
  ASSERT_EQ(OB_SUCCESS, merge_join.set_join_type(ObJoin::FULL_OUTER_JOIN));
  // equijoin: left.c5 = right.c9
  // column 9 int64_t, row_idx/2*2, e.g. 0,0,2,2,4,4,6,6,...
  // column 10 int64_t, row_idx/3*3, e.g. 0,0,0,3,3,3,6,6,6,...
  const uint64_t LEFT_COL = OB_APP_MIN_COLUMN_ID + 9;
  const uint64_t RIGHT_COL = OB_APP_MIN_COLUMN_ID + 10;
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = LEFT_TID;
    item.value_.cell_.cid = LEFT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = RIGHT_TID;
    item.value_.cell_.cid = RIGHT_COL;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    ASSERT_EQ(OB_SUCCESS, merge_join.add_equijoin_condition(expr));
  }
  // other cond: left.c1 % 2 = 0
  // column 1: int64_t, row_idx
  {
    ObSqlExpression expr;
    ExprItem item;
    item.type_ = T_REF_COLUMN;
    item.value_.cell_.tid = LEFT_TID;
    item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 1; // C1
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_MOD;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_INT;
    item.value_.int_ = 0;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    item.type_ = T_OP_EQ;
    item.value_.int_ = 2;
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item(item));
    ASSERT_EQ(OB_SUCCESS, expr.add_expr_item_end());
    if (has_other_cond)
    {
      ASSERT_EQ(OB_SUCCESS, merge_join.add_other_join_condition(expr));
    }
  }
  // print
  char buff[1024];
  merge_join.to_string(buff, 1024);
  printf("%s\n", buff);
  // begin
  ASSERT_EQ(OB_SUCCESS, merge_join.open());
  int64_t normal_row_count = 0;
  int64_t extra_row_count = 0;
  if (LEFT_ROW_COUNT < RIGHT_ROW_COUNT)
  {
    normal_row_count = LEFT_ROW_COUNT / 6 * 13;
    extra_row_count = (RIGHT_ROW_COUNT - LEFT_ROW_COUNT);
    if (has_other_cond)
    {
      normal_row_count = normal_row_count * 5 / 13;
      extra_row_count = 0;
    }
  }
  else
  {
    normal_row_count = RIGHT_ROW_COUNT / 6 * 13;
    extra_row_count = (LEFT_ROW_COUNT - RIGHT_ROW_COUNT);
    if (has_other_cond)
    {
      normal_row_count = normal_row_count * 5 / 13;
      extra_row_count = extra_row_count / 2;
    }
  }
  int64_t result_row_count = normal_row_count + extra_row_count;
  printf("result_row_count=%ld n1=%ld n2=%ld\n", result_row_count, normal_row_count, extra_row_count);
  const ObRow *row = NULL;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  int64_t val1 = 0;
  int64_t val2 = 0;
  for (int64_t i = 0; i < result_row_count; ++i)
  {
    printf("row=%ld\n", i);
    ASSERT_EQ(OB_SUCCESS, merge_join.get_next_row(row));
    ASSERT_EQ(32, row->get_column_num());
    // verify LEFT_COL
    ASSERT_EQ(OB_SUCCESS, row->get_cell(LEFT_TID, LEFT_COL, cell1));
    if (OB_SUCCESS == cell1->get_int(val1))
    {
      printf("left.c9=%ld ", val1);
    }
    else
    {
      printf("left.c9=NULL ");
    }
    // verify RIGHT_COL
    ASSERT_EQ(OB_SUCCESS, row->get_cell(RIGHT_TID, RIGHT_COL, cell2));
    if (OB_SUCCESS == cell2->get_int(val2))
    {
      printf("left.c10=%ld\n", val2);
    }
    else
    {
      printf("left.c10=NULL\n");
    }

    if (i < normal_row_count && !has_other_cond)
    {
      if (!has_other_cond)
      {
        int64_t mod = i % 13;

        if (mod < 6)
        {
          ASSERT_EQ(i/13*6, val1);
          ASSERT_EQ(i/13*6, val2);
        }
        else if (mod < 8)
        {
          ASSERT_EQ(2 + i/13*6, val1);
          ASSERT_TRUE(cell2->is_null());
        }
        else if (mod < 11)
        {
          ASSERT_TRUE(cell1->is_null());
          ASSERT_EQ(3 + i/13*6, val2);
        }
        else
        {
          ASSERT_EQ(4 + i/13*6, val1);
          ASSERT_TRUE(cell2->is_null());
        }
      }
      else
      {
        int64_t mod = i % 5;
        if (mod < 3)
        {
          ASSERT_EQ(i/5*6, val1);
          ASSERT_EQ(i/5*6, val2);
        }
        else if (mod < 4)
        {
          ASSERT_EQ(2 + i/5*6, val1);
          ASSERT_TRUE(cell2->is_null());
        }
        else
        {
          ASSERT_EQ(4 + i/5*6, val1);
          ASSERT_TRUE(cell2->is_null());
        }
      }
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, merge_join.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, merge_join.close());
}

TEST_F(ObMergeJoinTest, full_outer_join)
{
  bool has_other_cond = false;
  for (int64_t i = 0; i < 2; ++i)
  {
    if (i == 0)
    {
      has_other_cond = false;
    }
    else if (i == 1)
    {
      has_other_cond = true;
    }
    else
    {
      ASSERT_TRUE(false);
    }
    test_full_outer_join(120, 120, has_other_cond);
    test_full_outer_join(120, 132, has_other_cond);
    test_full_outer_join(132, 120, has_other_cond);
    test_full_outer_join(0, 120, has_other_cond);
    test_full_outer_join(120, 0, has_other_cond);
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
