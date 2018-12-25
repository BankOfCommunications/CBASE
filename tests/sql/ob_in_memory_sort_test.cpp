/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_in_memory_sort_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_in_memory_sort.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObInMemorySortTest: public ::testing::Test
{
  public:
    ObInMemorySortTest();
    virtual ~ObInMemorySortTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObInMemorySortTest(const ObInMemorySortTest &other);
    ObInMemorySortTest& operator=(const ObInMemorySortTest &other);
  protected:
    void test(const uint64_t orderby_col1, const uint64_t orderby_col2);
};

ObInMemorySortTest::ObInMemorySortTest()
{
}

ObInMemorySortTest::~ObInMemorySortTest()
{
}

void ObInMemorySortTest::SetUp()
{
}

void ObInMemorySortTest::TearDown()
{
}

void ObInMemorySortTest::test(const uint64_t orderby_col1, const uint64_t orderby_col2)
{
  static const int64_t ROW_COUNT = 128*1024;
  ObInMemorySort in_mem_sort;
  ObArray<ObSortColumn> sort_columns;
  ObSortColumn sort_column;
  sort_column.table_id_ = test::ObFakeTable::TABLE_ID;
  sort_column.column_id_ = orderby_col1;
  sort_column.is_ascending_ = false;
  ASSERT_EQ(OB_SUCCESS, sort_columns.push_back(sort_column));
  sort_column.table_id_ = test::ObFakeTable::TABLE_ID;
  sort_column.column_id_ = orderby_col2;
  sort_column.is_ascending_ = true;
  ASSERT_EQ(OB_SUCCESS, sort_columns.push_back(sort_column));
  in_mem_sort.set_sort_columns(sort_columns);

  test::ObFakeTable input_table;
  input_table.set_row_count(ROW_COUNT);
  ASSERT_EQ(OB_SUCCESS, input_table.open());

  const ObRow *row = NULL;
  for (int i = 0; i < ROW_COUNT; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, input_table.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, in_mem_sort.add_row(*row));
  }
  ASSERT_EQ(OB_ITER_END, input_table.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, in_mem_sort.sort_rows());

  // read and check
  ASSERT_EQ(ROW_COUNT, in_mem_sort.get_row_count());
  const ObRow *curr_row = NULL;
  ObRow last_row;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  for (int i = 0; i < ROW_COUNT; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, in_mem_sort.get_next_row(curr_row));
    if (0 < i)
    {
      // check order
      ASSERT_EQ(OB_SUCCESS, last_row.get_cell(test::ObFakeTable::TABLE_ID, orderby_col1, cell1));
      ASSERT_EQ(OB_SUCCESS, curr_row->get_cell(test::ObFakeTable::TABLE_ID, orderby_col1, cell2));
      if (*cell1 == *cell2)
      {
        ASSERT_EQ(OB_SUCCESS, last_row.get_cell(test::ObFakeTable::TABLE_ID, orderby_col2, cell1));
        ASSERT_EQ(OB_SUCCESS, curr_row->get_cell(test::ObFakeTable::TABLE_ID, orderby_col2, cell2));
        ASSERT_TRUE(*cell1 <= *cell2);
      }
      else
      {
        // cell1->dump();
        // cell2->dump();
        ASSERT_TRUE(*cell1 >= *cell2);
      }
    }
    last_row = *curr_row;
  } // end for
  ASSERT_EQ(OB_ITER_END, in_mem_sort.get_next_row(curr_row));
  ASSERT_EQ(OB_SUCCESS, input_table.close());
}

TEST_F(ObInMemorySortTest, sort_with_varchar_item)
{
  static const uint64_t orderby_col1 = OB_APP_MIN_COLUMN_ID;
  static const uint64_t orderby_col2 = OB_APP_MIN_COLUMN_ID+1;
  test(orderby_col1, orderby_col2);
}

TEST_F(ObInMemorySortTest, sort_with_equal_items)
{
  static const uint64_t orderby_col1 = OB_APP_MIN_COLUMN_ID+2;
  static const uint64_t orderby_col2 = OB_APP_MIN_COLUMN_ID+3;
  test(orderby_col1, orderby_col2);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

