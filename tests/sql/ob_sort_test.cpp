/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sort_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_sort.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObSortTest: public ::testing::Test
{
  public:
    ObSortTest();
    virtual ~ObSortTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSortTest(const ObSortTest &other);
    ObSortTest& operator=(const ObSortTest &other);
  protected:
    void test(const int64_t row_count, bool did_verify);
    // data members
    ObSort sort_;
    test::ObFakeTable input_table_;
};

ObSortTest::ObSortTest()
{
}

ObSortTest::~ObSortTest()
{
}

void ObSortTest::SetUp()
{
  char *filename = (char*)"ob_sort_test.run";
  ObString run_filename;
  run_filename.assign_ptr(filename, (int32_t)strlen(filename));
  sort_.set_run_filename(run_filename);

  ASSERT_EQ(OB_SUCCESS, sort_.add_sort_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID, false));
  ASSERT_EQ(OB_SUCCESS, sort_.add_sort_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+1, true));
  ASSERT_EQ(OB_SUCCESS, sort_.set_child(0, input_table_));

  sort_.set_mem_size_limit(200*1024*1024LL);
}

void ObSortTest::TearDown()
{
}

void ObSortTest::test(const int64_t row_count, bool did_verify)
{
  input_table_.set_row_count(row_count);
  ASSERT_EQ(OB_SUCCESS, sort_.open());
  const ObRow *row = NULL;
  char buff[1024];
  ObString str_cell1;
  ObObj last_cell1;
  ObObj last_cell2;
  int64_t int_cell2 = 0;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  for (int i = 0; i < row_count; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, sort_.get_next_row(row));
    if (did_verify)
    {
      ASSERT_EQ(OB_SUCCESS, row->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID, cell1));
      ASSERT_EQ(OB_SUCCESS, row->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+1, cell2));
      if (0 < i)
      {
        // check order
        if (last_cell1 == *cell1)
        {
          ASSERT_TRUE(last_cell2 <= *cell2);
        }
        else
        {
          ASSERT_TRUE(last_cell1 > *cell1);
        }
      }
      ASSERT_EQ(OB_SUCCESS, cell1->get_varchar(str_cell1));
      ASSERT_TRUE(1024 > str_cell1.length());
      memcpy(buff, str_cell1.ptr(), str_cell1.length());
      str_cell1.assign_ptr(buff, str_cell1.length());
      last_cell1.set_varchar(str_cell1);
      ASSERT_EQ(OB_SUCCESS, cell2->get_int(int_cell2));
      last_cell2.set_int(int_cell2);

    }
  } // end for
  ASSERT_EQ(OB_ITER_END, sort_.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, sort_.close());
}

TEST_F(ObSortTest, in_mem_test)
{
  test(1024*1024, true);
}

TEST_F(ObSortTest, merge_sort_test)
{
  test(1024*1024*10, true);
}

TEST_F(ObSortTest, in_mem_perf_test)
{
  test(1024*1024, false);
}

TEST_F(ObSortTest, merge_sort_perf_test)
{
  test(1024*1024*10, false);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
