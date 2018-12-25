/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_aggregate_function_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_aggregate_function.h"
#include "ob_fake_table.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObAggregateFunctionTest: public ::testing::Test
{
  public:
    ObAggregateFunctionTest();
    virtual ~ObAggregateFunctionTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObAggregateFunctionTest(const ObAggregateFunctionTest &other);
    ObAggregateFunctionTest& operator=(const ObAggregateFunctionTest &other);
  protected:
    void test(ObItemType aggr_func, int64_t expect_res, bool is_distinct, int64_t expect_res2, bool is_distinct2);
};

ObAggregateFunctionTest::ObAggregateFunctionTest()
{
}

ObAggregateFunctionTest::~ObAggregateFunctionTest()
{
}

void ObAggregateFunctionTest::SetUp()
{
}

void ObAggregateFunctionTest::TearDown()
{
}

void ObAggregateFunctionTest::test(ObItemType func, int64_t expect_res, bool is_distinct, int64_t expect_res2, bool is_distinct2)
{
  //ObArray<ObSqlExpression> exprs;
  ObExpressionArray exprs; 
  static const int64_t AGGR1_CID = 9999;
  static const int64_t AGGR2_CID = 9998;
  {
    ObSqlExpression sexpr1;
    sexpr1.set_aggr_func(func, is_distinct);
    sexpr1.set_tid_cid(OB_INVALID_ID, AGGR1_CID);
    // aggr(c1+c2)
    // c1+c2
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
    expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+1;
    sexpr1.add_expr_item(expr_item); // c1
    expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+2;
    sexpr1.add_expr_item(expr_item); // c2
    expr_item.type_ = T_OP_ADD;
    sexpr1.add_expr_item(expr_item); // + op
    sexpr1.add_expr_item_end();
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(sexpr1));
  }
  {
    ObSqlExpression sexpr1;
    sexpr1.set_aggr_func(func, is_distinct2);
    sexpr1.set_tid_cid(OB_INVALID_ID, AGGR2_CID);
    // aggr(c2+c3)
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
    expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+2;
    sexpr1.add_expr_item(expr_item); // c2
    expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+3;
    sexpr1.add_expr_item(expr_item); // c3
    expr_item.type_ = T_OP_ADD;
    sexpr1.add_expr_item(expr_item); // + op
    sexpr1.add_expr_item_end();
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(sexpr1));
  }
  // init
  static const int64_t ROW_COUNT = 100;
  test::ObFakeTable input;
  input.set_row_count(ROW_COUNT);
  ASSERT_EQ(OB_SUCCESS, input.open());
  ObAggregateFunction aggr_func;
  ASSERT_EQ(OB_SUCCESS, aggr_func.init(input.get_row_desc(), exprs));
  const ObRow *row = NULL;
  for (int64_t i = 0; i < ROW_COUNT; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, input.get_next_row(row));
    if (0 == i)
    {
      ASSERT_EQ(OB_SUCCESS, aggr_func.prepare(*row));
    }
    else
    {
      ASSERT_EQ(OB_SUCCESS, aggr_func.process(*row));
    }
  }
  ASSERT_EQ(OB_ITER_END, input.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, aggr_func.get_result(row));
  // verify
  const ObObj *cell = NULL;
  ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR1_CID, cell));
  if (T_FUN_AVG == func)
  {
    double result = 0.0;
    ASSERT_EQ(OB_SUCCESS, cell->get_double(result));
    char buff[ObNumber::MAX_PRINTABLE_SIZE];
    snprintf(buff, ObNumber::MAX_PRINTABLE_SIZE, "%ld", static_cast<int64_t>(result));// truncate to int64
    char buff2[ObNumber::MAX_PRINTABLE_SIZE];
    snprintf(buff2, ObNumber::MAX_PRINTABLE_SIZE, "%ld", expect_res);
    ASSERT_STREQ(buff, buff2);
  }
  else
  {
    int64_t result = 0;
    ASSERT_EQ(OB_SUCCESS, cell->get_int(result));
    ASSERT_EQ(expect_res, result);
  }
  ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR2_CID, cell));
  if (T_FUN_AVG == func)
  {
    double result = 0.0;
    ASSERT_EQ(OB_SUCCESS, cell->get_double(result));
    char buff[ObNumber::MAX_PRINTABLE_SIZE];
    snprintf(buff, ObNumber::MAX_PRINTABLE_SIZE, "%ld", static_cast<int64_t>(result)); // truncate to int64
    char buff2[ObNumber::MAX_PRINTABLE_SIZE];
    snprintf(buff2, ObNumber::MAX_PRINTABLE_SIZE, "%ld", expect_res2);
    ASSERT_STREQ(buff, buff2);
  }
  else
  {
    int64_t result = 0;
    ASSERT_EQ(OB_SUCCESS, cell->get_int(result));
    ASSERT_EQ(expect_res2, result);
  }
  // destroy
  ASSERT_EQ(OB_SUCCESS, input.close());
}

TEST_F(ObAggregateFunctionTest, basic_test)
{
  test(T_FUN_SUM, 5000, false, 149, false);
  test(T_FUN_MIN, 0, false, 0, false);
  test(T_FUN_MAX, 100, false, 3, false);
  test(T_FUN_COUNT, 100, false, 100, false);
  test(T_FUN_AVG, 50, false, 1, false);
  // one distinct column
  test(T_FUN_SUM, 2550, true, 149, false);
  test(T_FUN_MIN, 0, true, 0, false);
  test(T_FUN_MAX, 100, true, 3, false);
  test(T_FUN_COUNT, 51, true, 100, false);
  test(T_FUN_AVG, 50, true, 1, false);
  // both distinct
  test(T_FUN_SUM, 2550, true, 6, true);
  test(T_FUN_MIN, 0, true, 0, true);
  test(T_FUN_MAX, 100, true, 3, true);
  test(T_FUN_COUNT, 51, true, 4, true);
  test(T_FUN_AVG, 50, true, 1, true);
}

TEST_F(ObAggregateFunctionTest, count_star)
{
  // count(*) and count(c8)
  //ObArray<ObSqlExpression> exprs;
  ObExpressionArray exprs;
  static const int64_t AGGR1_CID = 9999;
  static const int64_t AGGR2_CID = 9998;
  {
    ObSqlExpression sexpr1;
    sexpr1.set_aggr_func(T_FUN_COUNT, false);
    sexpr1.set_tid_cid(OB_INVALID_ID, AGGR1_CID);
    // count(*)
    sexpr1.add_expr_item_end();
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(sexpr1));
  }
  {
    ObSqlExpression sexpr1;
    sexpr1.set_aggr_func(T_FUN_COUNT, false);
    sexpr1.set_tid_cid(OB_INVALID_ID, AGGR2_CID);
    // aggr(c8)
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
    expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+8;
    sexpr1.add_expr_item(expr_item); // c8
    sexpr1.add_expr_item_end();
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(sexpr1));
  }
  // init
  static const int64_t ROW_COUNT = 100;
  test::ObFakeTable input;
  input.set_row_count(ROW_COUNT);
  ASSERT_EQ(OB_SUCCESS, input.open());
  ObAggregateFunction aggr_func;
  ASSERT_EQ(OB_SUCCESS, aggr_func.init(input.get_row_desc(), exprs));
  const ObRow *row = NULL;
  for (int64_t i = 0; i < ROW_COUNT; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, input.get_next_row(row));
    if (0 == i)
    {
      ASSERT_EQ(OB_SUCCESS, aggr_func.prepare(*row));
    }
    else
    {
      ASSERT_EQ(OB_SUCCESS, aggr_func.process(*row));
    }
  }
  ASSERT_EQ(OB_ITER_END, input.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, aggr_func.get_result(row));
  // verify
  const ObObj *cell = NULL;
  ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR1_CID, cell));
  int64_t result = 0;
  ASSERT_EQ(OB_SUCCESS, cell->get_int(result));
  ASSERT_EQ(ROW_COUNT, result); // count(*) == 100
  ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR2_CID, cell));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(result));
  ASSERT_EQ(ROW_COUNT/2, result); // count(c8) == 50
  // destroy
  ASSERT_EQ(OB_SUCCESS, input.close());
}

TEST_F(ObAggregateFunctionTest, empty_set)
{
  // count(*) and count(c8) and avg(c1)
  //ObArray<ObSqlExpression> exprs;
  ObExpressionArray exprs;
  static const int64_t AGGR1_CID = 9999;
  static const int64_t AGGR2_CID = 9998;
  static const int64_t AGGR3_CID = 9997;
  {
    ObSqlExpression sexpr1;
    sexpr1.set_aggr_func(T_FUN_COUNT, false);
    sexpr1.set_tid_cid(OB_INVALID_ID, AGGR1_CID);
    // count(*)
    sexpr1.add_expr_item_end();
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(sexpr1));
  }
  {
    ObSqlExpression sexpr1;
    sexpr1.set_aggr_func(T_FUN_COUNT, false);
    sexpr1.set_tid_cid(OB_INVALID_ID, AGGR2_CID);
    // aggr(c8)
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
    expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+8;
    sexpr1.add_expr_item(expr_item); // c8
    sexpr1.add_expr_item_end();
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(sexpr1));
  }
  {
    ObSqlExpression sexpr1;
    sexpr1.set_aggr_func(T_FUN_AVG, false);
    sexpr1.set_tid_cid(OB_INVALID_ID, AGGR3_CID);
    // aggr(c8)
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
    expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+1;
    sexpr1.add_expr_item(expr_item); // c1
    sexpr1.add_expr_item_end();
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(sexpr1));
  }
  // init
  static const int64_t ROW_COUNT = 1;
  test::ObFakeTable input;
  input.set_row_count(ROW_COUNT);
  ASSERT_EQ(OB_SUCCESS, input.open());
  ObAggregateFunction aggr_func;
  ASSERT_EQ(OB_SUCCESS, aggr_func.init(input.get_row_desc(), exprs));
  // verify
  const ObRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, aggr_func.get_result_for_empty_set(row));
  ASSERT_EQ(input.get_row_desc().get_column_num() + 3, row->get_column_num());
  const ObObj *cell = NULL;
  ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR1_CID, cell));
  int64_t i64 = 0;
  ASSERT_EQ(OB_SUCCESS, cell->get_int(i64)); // COUNT(*)
  ASSERT_EQ(0, i64);
  ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR2_CID, cell));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(i64)); // COUNT(c8)
  ASSERT_EQ(0, i64);
  ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR3_CID, cell));
  ASSERT_TRUE(cell->is_null()); // AVG(c1)
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; i < row->get_column_num()-3; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, row->raw_get_cell(i, cell, tid, cid));
    ASSERT_TRUE(cell->is_null());
  } // end for
  // destroy
  ASSERT_EQ(OB_SUCCESS, input.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
