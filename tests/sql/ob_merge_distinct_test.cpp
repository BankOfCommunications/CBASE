/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_merge_distinct_test.cpp,  06/04/2012 05:47:34 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   Test ObMergeDistinct class
 * 
 */
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_string_buf.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_row.h"
#include "sql/ob_sort.h"
#include "sql/ob_merge_distinct.h"
#include "ob_fake_table.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::test;

class ObSortedDistinctTest: public ::testing::Test
{
  public:
    ObSortedDistinctTest();
    virtual ~ObSortedDistinctTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSortedDistinctTest(const ObSortedDistinctTest &other);
    ObSortedDistinctTest& operator=(const ObSortedDistinctTest &other);
  protected:
    void test(const int64_t row_count, int64_t expected_val);
    // data members
    ObSort sort_;
    ObMergeDistinct distinct_;
    test::ObFakeTable input_table_;
};

ObSortedDistinctTest::ObSortedDistinctTest()
{
}

ObSortedDistinctTest::~ObSortedDistinctTest()
{
}

void ObSortedDistinctTest::SetUp()
{
  char *filename = (char*)"ob_sort_test.run";
  ObString run_filename;
  run_filename.assign_ptr(filename, (int32_t)strlen(filename));
  sort_.set_run_filename(run_filename);

  ASSERT_EQ(OB_SUCCESS, sort_.add_sort_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, true));
  ASSERT_EQ(OB_SUCCESS, sort_.add_sort_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3, true));
  ASSERT_EQ(OB_SUCCESS, sort_.set_child(0, input_table_));

  sort_.set_mem_size_limit(200*1024*1024LL);

  ASSERT_EQ(OB_SUCCESS, distinct_.set_child(0, sort_));
  ASSERT_EQ(OB_SUCCESS, distinct_.add_distinct_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2));
  ASSERT_EQ(OB_SUCCESS, distinct_.add_distinct_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3));

}

void ObSortedDistinctTest::TearDown()
{
}


void ObSortedDistinctTest::test(const int64_t row_count, int64_t expected_val)
{ 
  input_table_.set_row_count(row_count);
  ASSERT_EQ(OB_SUCCESS, distinct_.open());
  const ObRow *row = NULL;
  int64_t cnt = 0;
    while(OB_SUCCESS ==  distinct_.get_next_row(row))
    {
     cnt++;
    }
  ASSERT_EQ(cnt, expected_val);
}

TEST_F(ObSortedDistinctTest, test1)
{
  test(1, 1);
}
TEST_F(ObSortedDistinctTest, test2)
{
  test(2, 2);
}

TEST_F(ObSortedDistinctTest, test3)
{
  test(3, 3);
}

TEST_F(ObSortedDistinctTest, test4)
{
  test(4, 4);
}

TEST_F(ObSortedDistinctTest, test5)
{
  test(5, 5);
}

TEST_F(ObSortedDistinctTest, test6)
{
  test(6, 6);
}

TEST_F(ObSortedDistinctTest, test7)
{
  test(100, 6);
}

TEST_F(ObSortedDistinctTest, test8)
{
  test(10000, 6);
}


class ObMergeDistinctTest: public ::testing::Test
{
  public:
    ObMergeDistinctTest();
    virtual ~ObMergeDistinctTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObMergeDistinctTest(const ObMergeDistinctTest &other);
    ObMergeDistinctTest& operator=(const ObMergeDistinctTest &other);
  private:
    // data members
};

ObMergeDistinctTest::ObMergeDistinctTest()
{
}

ObMergeDistinctTest::~ObMergeDistinctTest()
{
}

void ObMergeDistinctTest::SetUp()
{
}

void ObMergeDistinctTest::TearDown()
{
}


/***************************************************************************************************
 *   c0       | c1      | c2        | c3        | c4        | c5
 *   -----------------------------------------------------------------------------------------------------
 *   rand str | row_idx | row_idx%2 | row_idx%3 | row_idx/2 | row_idx/3
 ****************************************************************************************************/

TEST_F(ObMergeDistinctTest, distinct_row1_row2_div_2)
{
  ObObj obj_a, obj_b;
  ObMergeDistinct  distinct;
  ObSingleChildPhyOperator &single_op = distinct;
  ObFakeTable phy_op;
  const ObRow *next_row_p = NULL;
  int count = 0;
  const int total_row_count = 10000;

  phy_op.set_row_count(total_row_count);
  ASSERT_EQ(OB_SUCCESS, distinct.add_distinct_column(ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+4));
  ASSERT_EQ(OB_SUCCESS, single_op.set_child(0, phy_op));
  ASSERT_EQ(OB_SUCCESS, distinct.open());
  while(OB_ITER_END != distinct.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    next_row_p = NULL;
    //TBSYS_LOG(INFO, "find a distinct row");
    count++;
  }
  ASSERT_EQ(count, total_row_count / 2);
  ASSERT_EQ(OB_SUCCESS, distinct.close());
}

TEST_F(ObMergeDistinctTest, distinct_row1_row2_div_3)
{
  ObObj obj_a, obj_b;
  ObMergeDistinct  distinct;
  ObSingleChildPhyOperator &single_op = distinct;
  ObFakeTable phy_op;
  const ObRow *next_row_p = NULL;
  int count = 0;
  const int total_row_count = 10000;

  phy_op.set_row_count(total_row_count);
  ASSERT_EQ(OB_SUCCESS, distinct.add_distinct_column(ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+5));
  ASSERT_EQ(OB_SUCCESS, single_op.set_child(0, phy_op));
  ASSERT_EQ(OB_SUCCESS, distinct.open());
  while(OB_ITER_END != distinct.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    next_row_p = NULL;
    //TBSYS_LOG(INFO, "find a distinct row");
    count++;
  }
  ASSERT_EQ(count, total_row_count / 3 + 1);
  ASSERT_EQ(OB_SUCCESS, distinct.close());
}


TEST_F(ObMergeDistinctTest, distinct_row1_row2_diff_1)
{
  ObObj obj_a, obj_b;
  ObMergeDistinct  distinct;
  ObSingleChildPhyOperator &single_op = distinct;
  ObFakeTable phy_op;
  const ObRow *next_row_p = NULL;
  int count = 0;
  const int total_row_count = 10000;

  phy_op.set_row_count(total_row_count);
  ASSERT_EQ(OB_SUCCESS, distinct.add_distinct_column(ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3));
  ASSERT_EQ(OB_SUCCESS, single_op.set_child(0, phy_op));
  ASSERT_EQ(OB_SUCCESS, distinct.open());
  while(OB_ITER_END != distinct.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    next_row_p = NULL;
    //TBSYS_LOG(INFO, "find a distinct row");
    count++;
  }
  ASSERT_EQ(count, total_row_count);
  ASSERT_EQ(OB_SUCCESS, distinct.close());

  char buf[1000];
  distinct.to_string(buf, 1000);
}

TEST_F(ObMergeDistinctTest, distinct_row1_row2_col2_diff_1)
{
  ObObj obj_a, obj_b;
  ObMergeDistinct  distinct;
  ObSingleChildPhyOperator &single_op = distinct;
  ObFakeTable phy_op;
  const ObRow *next_row_p = NULL;
  int count = 0;
  const int total_row_count = 10000;

  phy_op.set_row_count(total_row_count);
  /* (idx%2, idx%3) */
  ASSERT_EQ(OB_SUCCESS, distinct.add_distinct_column(ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2));
  ASSERT_EQ(OB_SUCCESS, distinct.add_distinct_column(ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3));
  single_op.set_child(0, phy_op);
  ASSERT_EQ(OB_SUCCESS, distinct.open());
  while(OB_ITER_END != distinct.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    next_row_p = NULL;
    //TBSYS_LOG(INFO, "find a distinct row");
    count++;
  }
  /* if sorted. count != total_row_count. not sorted here */
  ASSERT_EQ(count, total_row_count);
  ASSERT_EQ(OB_SUCCESS, distinct.close());

  char buf[1000];
  distinct.to_string(buf, 1000);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
