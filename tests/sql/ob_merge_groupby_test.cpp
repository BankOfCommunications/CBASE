/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_groupby_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_merge_groupby.h"
#include "ob_fake_table.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObMergeGroupByTest: public ::testing::Test
{
  public:
    ObMergeGroupByTest();
    virtual ~ObMergeGroupByTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObMergeGroupByTest(const ObMergeGroupByTest &other);
    ObMergeGroupByTest& operator=(const ObMergeGroupByTest &other);
  private:
    // data members
};

ObMergeGroupByTest::ObMergeGroupByTest()
{
}

ObMergeGroupByTest::~ObMergeGroupByTest()
{
}

void ObMergeGroupByTest::SetUp()
{
}

void ObMergeGroupByTest::TearDown()
{
}

TEST_F(ObMergeGroupByTest, basic_test)
{
  static const int64_t ROW_COUNT = 100;
  ObMergeGroupBy groupby;
  test::ObFakeTable input;
  input.set_row_count(ROW_COUNT);
  // sum(c1+c2) group by c5
  // note that the input is already sorted by c5
  ASSERT_EQ(OB_SUCCESS, groupby.add_group_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+5));
  // sum(c1+c2)
  ObSqlExpression sexpr2;
  static const int64_t AGGR_CID = 9999;
  sexpr2.set_aggr_func(T_FUN_SUM, false);
  sexpr2.set_tid_cid(OB_INVALID_ID, AGGR_CID);
  ExprItem expr_item;
  expr_item.type_ = T_REF_COLUMN;
  expr_item.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+1;
  sexpr2.add_expr_item(expr_item); // c1
  expr_item.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+2;
  sexpr2.add_expr_item(expr_item); // c2
  expr_item.type_ = T_OP_ADD;
  sexpr2.add_expr_item(expr_item); // + op
  sexpr2.add_expr_item_end();
  ASSERT_EQ(OB_SUCCESS, groupby.add_aggr_column(sexpr2));
  groupby.set_child(0, input);
  // test
  char strbuff[1024];
  groupby.to_string(strbuff, 1024);
  TBSYS_LOG(INFO, "groupby=%s", strbuff);
  ASSERT_EQ(OB_SUCCESS, groupby.open());
  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  int64_t i64 = 0;
  for (int i = 0; i < ROW_COUNT/3+1; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, groupby.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, row->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+5, cell));
    ASSERT_EQ(OB_SUCCESS, cell->get_int(i64));
    ASSERT_EQ(i, i64);
    // verify c5
    ASSERT_EQ(OB_SUCCESS, row->get_cell(OB_INVALID_ID, AGGR_CID, cell));
    ASSERT_EQ(OB_SUCCESS, cell->get_int(i64));
    if (i == ROW_COUNT/3)
    {
      // last row
      ASSERT_EQ(100, i64);
    }
    else
    {
      ASSERT_EQ((i*9)+3+i%2+1, i64);
    }
  }
  ASSERT_EQ(OB_ITER_END, groupby.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, groupby.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, groupby.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
