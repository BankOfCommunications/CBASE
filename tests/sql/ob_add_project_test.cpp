/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_add_project_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_add_project.h"
#include "ob_fake_table.h"
#include <gtest/gtest.h>
using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObAddProjectTest: public ::testing::Test
{
  public:
    ObAddProjectTest();
    virtual ~ObAddProjectTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObAddProjectTest(const ObAddProjectTest &other);
    ObAddProjectTest& operator=(const ObAddProjectTest &other);
  private:
    // data members
};

ObAddProjectTest::ObAddProjectTest()
{
}

ObAddProjectTest::~ObAddProjectTest()
{
}

void ObAddProjectTest::SetUp()
{
}

void ObAddProjectTest::TearDown()
{
}

TEST_F(ObAddProjectTest, basic_test)
{
  static const int64_t MY_CID = 9999;
  static const int64_t ROW_COUNT = 100;
  ObAddProject add_project;
  ObSqlExpression sexpr1;
  sexpr1.set_tid_cid(OB_INVALID_ID, MY_CID);
  // c1 as MY_CID
  static const uint64_t C1_CID = OB_APP_MIN_COLUMN_ID+1;
  ExprItem expr_item;
  expr_item.type_ = T_REF_COLUMN;
  expr_item.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  expr_item.value_.cell_.cid = C1_CID;
  sexpr1.add_expr_item(expr_item); // c1
  sexpr1.add_expr_item_end();
  ASSERT_EQ(OB_SUCCESS, add_project.add_output_column(sexpr1));
  test::ObFakeTable input;
  input.set_row_count(ROW_COUNT);
  ASSERT_EQ(OB_SUCCESS, add_project.set_child(0, input));
  ASSERT_EQ(OB_SUCCESS, add_project.open());
  const ObRow *row = NULL;
  for (int64_t i = 0; i < ROW_COUNT; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, add_project.get_next_row(row));
    ASSERT_EQ(input.get_row_desc().get_column_num() + 1, row->get_column_num());
    const ObObj *c1 = NULL;
    int64_t c1_val = 0;
    ASSERT_EQ(OB_SUCCESS, row->get_cell(test::ObFakeTable::TABLE_ID, C1_CID, c1));
    ASSERT_EQ(OB_SUCCESS, c1->get_int(c1_val));
    ASSERT_EQ(i, c1_val);
    const ObObj *my_cell = NULL;
    int64_t my_val = 0;
    ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, MY_CID, my_cell));
    ASSERT_EQ(OB_SUCCESS, my_cell->get_int(my_val));
    ASSERT_EQ(i, my_val);
  }
  ASSERT_EQ(OB_ITER_END, add_project.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, add_project.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, add_project.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
