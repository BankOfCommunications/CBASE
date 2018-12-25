/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_limit_test.cpp,  06/04/2012 05:47:34 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   Test ObLimit class
 *
 */
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_string_buf.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_row.h"
#include "sql/ob_limit.h"
#include "ob_fake_table.h"


using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

class ObLimitTest: public ::testing::Test
{
  public:
    ObLimitTest();
    virtual ~ObLimitTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObLimitTest(const ObLimitTest &other);
    ObLimitTest& operator=(const ObLimitTest &other);
  private:
    // data members
};

ObLimitTest::ObLimitTest()
{
}

ObLimitTest::~ObLimitTest()
{
}

void ObLimitTest::SetUp()
{
}

void ObLimitTest::TearDown()
{
}

TEST_F(ObLimitTest, limiter_1k_10_2_basic_test)
{
  ObObj obj_a, obj_b;
  ObLimit limiter;
  ObSingleChildPhyOperator &single_op = limiter;
  ObFakeTable phy_op;
  const ObRow *next_row_p = NULL;
  int count = 0;

  phy_op.set_row_count(1000);
  single_op.set_child(0, phy_op);
  ObSqlExpression limit;
  ObSqlExpression offset;
  ExprItem item;
  item.type_ = T_INT;
  item.data_type_ = ObIntType;
  item.value_.int_ = 10;
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item_end());
  item.value_.int_ = 2;
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item_end());
  ASSERT_EQ(OB_SUCCESS, limiter.set_limit(limit, offset));
  int ret = limiter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  while(OB_ITER_END != limiter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    next_row_p = NULL;
    count++;
  }
  ASSERT_EQ(count, 10);
  limiter.close();
}

TEST_F(ObLimitTest, limiter_10_10_2_basic_test)
{
  ObObj obj_a, obj_b;
  ObLimit limiter;
  ObSingleChildPhyOperator &single_op = limiter;
  ObFakeTable phy_op;
  const ObRow *next_row_p = NULL;
  int count = 0;

  phy_op.set_row_count(10);
  single_op.set_child(0, phy_op);
  ObSqlExpression limit;
  ObSqlExpression offset;
  ExprItem item;
  item.type_ = T_INT;
  item.data_type_ = ObIntType;
  item.value_.int_ = 10;
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item_end());
  item.value_.int_ = 2;
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item_end());
  limiter.set_limit(limit, offset);
  limiter.open();
  while(OB_ITER_END != limiter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    next_row_p = NULL;
    count++;
  }
  ASSERT_EQ(count, 8);
  limiter.close();
}

TEST_F(ObLimitTest, limiter_manylines_test)
{
  ObObj obj_a, obj_b;
  ObLimit limiter;
  ObSingleChildPhyOperator &single_op = limiter;
  ObFakeTable phy_op;
  const ObRow *next_row_p = NULL;
  int count = 0;

  phy_op.set_row_count(10000);
  single_op.set_child(0, phy_op);
  ObSqlExpression limit;
  ObSqlExpression offset;
  ExprItem item;
  item.type_ = T_INT;
  item.data_type_ = ObIntType;
  item.value_.int_ = 8000;
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item_end());
  item.value_.int_ = 1111;
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item_end());
  limiter.set_limit(limit, offset);
  limiter.open();
  while(OB_ITER_END != limiter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    next_row_p = NULL;
    count++;
  }
  ASSERT_EQ(count, 8000);
  limiter.close();
}

TEST_F(ObLimitTest, serialize_test)
{
  ObLimit limiter, limiter_deserialize;
  int64_t limit1 = 9000, offset1= 10;
  int64_t limit2 = 0, offset2= 0;

  ObSqlExpression limit;
  ObSqlExpression offset;
  ExprItem item;
  item.type_ = T_INT;
  item.data_type_ = ObIntType;
  item.value_.int_ = 9000;
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, limit.add_expr_item_end());
  item.value_.int_ = 10;
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, offset.add_expr_item_end());
  limiter.set_limit(limit, offset);
  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  int64_t data_len = 0;
  limiter.serialize(buf, buf_len, pos);
  data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, limiter_deserialize.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  limiter_deserialize.get_limit(limit2, offset2);
  ASSERT_EQ(limit1, limit2);
  ASSERT_EQ(offset1, offset2);
}



int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  //ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
