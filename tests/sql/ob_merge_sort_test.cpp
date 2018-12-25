/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_sort_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_merge_sort.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObMergeSortTest: public ::testing::Test
{
  public:
    ObMergeSortTest();
    virtual ~ObMergeSortTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObMergeSortTest(const ObMergeSortTest &other);
    ObMergeSortTest& operator=(const ObMergeSortTest &other);
  protected:
    // data members
    test::ObFakeTable input_table_;
    ObMergeSort merge_sort_;
    ObArray<ObSortColumn> sort_columns_;
    ObInMemorySort in_mem_sort_;
};

ObMergeSortTest::ObMergeSortTest()
{
}

ObMergeSortTest::~ObMergeSortTest()
{
}

void ObMergeSortTest::SetUp()
{
  sort_columns_.clear();
  ObSortColumn sort_column;
  sort_column.table_id_ = test::ObFakeTable::TABLE_ID;
  sort_column.column_id_ = OB_APP_MIN_COLUMN_ID;
  sort_column.is_ascending_ = false;
  ASSERT_EQ(OB_SUCCESS, sort_columns_.push_back(sort_column));
  sort_column.table_id_ = test::ObFakeTable::TABLE_ID;
  sort_column.column_id_ = OB_APP_MIN_COLUMN_ID+1;
  sort_column.is_ascending_ = true;
  ASSERT_EQ(OB_SUCCESS, sort_columns_.push_back(sort_column));
  ObString filename;
  char* filename_buf = (char *)"ob_merge_sort_test.run";
  filename.assign_ptr(filename_buf, (int32_t)strlen(filename_buf));
  merge_sort_.set_run_filename(filename);
  merge_sort_.set_sort_columns(sort_columns_);
  in_mem_sort_.set_sort_columns(sort_columns_);
}

void ObMergeSortTest::TearDown()
{
  merge_sort_.reset();
  in_mem_sort_.reset();
}

TEST_F(ObMergeSortTest, basic_test)
{
  static const int dump_run_count = 8;
  static const int64_t row_count_per_run = 4096;

  input_table_.set_row_count(row_count_per_run);
  for (int i = 0; i < dump_run_count + 1; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, input_table_.open());
    in_mem_sort_.reset();
    const ObRow *row = NULL;
    for (int j = 0; j < row_count_per_run; ++j)
    {
      ASSERT_EQ(OB_SUCCESS, input_table_.get_next_row(row));
      ASSERT_EQ(OB_SUCCESS, in_mem_sort_.add_row(*row));
    }
    ASSERT_EQ(OB_SUCCESS, in_mem_sort_.sort_rows());
    if (i < dump_run_count)
    {
      ASSERT_EQ(OB_SUCCESS, merge_sort_.dump_run(in_mem_sort_));
    }
    else
    {
      merge_sort_.set_final_run(in_mem_sort_);
    }
    ASSERT_EQ(OB_SUCCESS, input_table_.close());
  } // end for

  // verify
  ASSERT_EQ(OB_SUCCESS, input_table_.open());
  ASSERT_EQ(OB_SUCCESS, merge_sort_.build_merge_heap());
  const ObRow *curr_row = NULL;
  char buff[1024];
  ObString str_cell1;
  ObObj last_cell1;
  ObObj last_cell2;
  int64_t int_cell2 = 0;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  for (int64_t i = 0; i < row_count_per_run * (dump_run_count + 1); ++i)
  {
    ASSERT_EQ(OB_SUCCESS, merge_sort_.get_next_row(curr_row));
    ASSERT_EQ(OB_SUCCESS, curr_row->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID, cell1));
    ASSERT_EQ(OB_SUCCESS, curr_row->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+1, cell2));
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
  ASSERT_EQ(OB_ITER_END, merge_sort_.get_next_row(curr_row));
  ASSERT_EQ(OB_ITER_END, merge_sort_.get_next_row(curr_row));
  ASSERT_EQ(OB_SUCCESS, input_table_.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
