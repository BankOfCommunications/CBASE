/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_run_file_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include "sql/ob_run_file.h"
#include "ob_fake_table.h"
#include "common/ob_row_util.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObRunFileTest: public ::testing::Test
{
  public:
    ObRunFileTest();
    virtual ~ObRunFileTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRunFileTest(const ObRunFileTest &other);
    ObRunFileTest& operator=(const ObRunFileTest &other);
  protected:
    static const int64_t ROW_COUNT = 1024*1024;
    // data members
    test::ObFakeTable input_data_;
};

ObRunFileTest::ObRunFileTest()
{
}

ObRunFileTest::~ObRunFileTest()
{
}

void ObRunFileTest::SetUp()
{
  input_data_.set_row_count(ROW_COUNT);
}

void ObRunFileTest::TearDown()
{
}

TEST_F(ObRunFileTest, test_all)
{
  static const int64_t BUFFER_SIZE = 1024;
  char compact_row_buff_[BUFFER_SIZE];
  ObRunFile run_file;
  char* fname = (char*)"ob_run_file_test.run";
  ObString filename;
  ASSERT_FALSE(run_file.is_opened());
  ASSERT_NE(OB_SUCCESS, run_file.open(filename));
  filename.assign(fname, (int32_t)strlen(fname));
  ASSERT_EQ(OB_SUCCESS, run_file.open(filename));
  ASSERT_EQ(true, run_file.is_opened());
  const int64_t bucket_count = 4;
  const int64_t run_count_per_bucket = 8;
  for (int64_t j = 0; j < run_count_per_bucket; ++j)
  {
    for (int64_t i = 0; i < bucket_count; ++i)
    {
      ASSERT_EQ(OB_SUCCESS, run_file.begin_append_run(i));
      const ObRow *row = NULL;
      ObString compact_row;
      compact_row.assign_buffer(compact_row_buff_, BUFFER_SIZE);
      ASSERT_EQ(OB_SUCCESS, input_data_.open());
      while (OB_SUCCESS == input_data_.get_next_row(row))
      {
        compact_row.assign_buffer(compact_row_buff_, BUFFER_SIZE);
        ASSERT_EQ(OB_SUCCESS, ObRowUtil::convert(*row, compact_row));
        ASSERT_EQ(OB_SUCCESS, run_file.append_row(compact_row));
      } // end for
      ASSERT_EQ(OB_SUCCESS, input_data_.close());
      ASSERT_EQ(OB_SUCCESS, run_file.end_append_run());
    } // end for
  }
  // read and check
  ObRow row;
  const ObObj *cell = NULL;
  uint64_t table_id;
  uint64_t column_id;
  ASSERT_EQ(OB_SUCCESS, input_data_.open());
  row.set_row_desc(input_data_.get_row_desc());
  for (int64_t i = 0; i < bucket_count; ++i)
  {
    int64_t run_count = 0;
    ASSERT_EQ(OB_SUCCESS, run_file.begin_read_bucket(i, run_count));
    ASSERT_EQ(run_count, run_count_per_bucket);
    for (int64_t j = 0; j < ROW_COUNT; ++j)
    {
      for (int64_t k = 0; k < run_count; ++k)
      {
        ASSERT_EQ(OB_SUCCESS, run_file.get_next_row(k, row));
        ASSERT_EQ(OB_SUCCESS, row.raw_get_cell(1, cell, table_id, column_id));
        int64_t int_value;
        ASSERT_EQ(OB_SUCCESS, cell->get_int(int_value));
        ASSERT_EQ(j, int_value);
      }
    } // end for
    for (int64_t k = 0; k < run_count; ++k)
    {
      ASSERT_EQ(OB_ITER_END, run_file.get_next_row(k, row));
    }
    ASSERT_EQ(OB_SUCCESS, run_file.end_read_bucket());
  }
  ASSERT_EQ(OB_SUCCESS, run_file.close());
  ASSERT_FALSE(run_file.is_opened());
  ASSERT_EQ(OB_SUCCESS, input_data_.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

