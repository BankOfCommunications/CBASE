/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_row_util_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_ups_row_util.h"
#include "common/utility.h"
#include "common/ob_compact_cell_writer.h"

using namespace oceanbase;
using namespace common;

#define TABLE_ID 1000
#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObUpsRowUtilTest: public ::testing::Test
{
  public:
    ObUpsRowUtilTest();
    virtual ~ObUpsRowUtilTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObUpsRowUtilTest(const ObUpsRowUtilTest &other);
    ObUpsRowUtilTest& operator=(const ObUpsRowUtilTest &other);
  protected:
    // data members
};

ObUpsRowUtilTest::ObUpsRowUtilTest()
{
}

ObUpsRowUtilTest::~ObUpsRowUtilTest()
{
}

void ObUpsRowUtilTest::SetUp()
{
}

void ObUpsRowUtilTest::TearDown()
{
}

TEST_F(ObUpsRowUtilTest, basic_test2)
{
  char buf[1024];
  ObCompactCellWriter cell_writer;
  OK(cell_writer.init(buf, 1024, DENSE_SPARSE));

  ObRowkey rowkey;
  ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];
  rowkey_obj[0].set_int(3);
  rowkey_obj[1].set_int(30);

  rowkey.assign(rowkey_obj, 2);
  
  OK(cell_writer.append_rowkey(rowkey));

  ObObj value;
  ObRowDesc row_desc;
  for(int64_t i=1;i<10;i++)
  {
    value.set_int(i);
    row_desc.add_column_desc(TABLE_ID, i);
    OK(cell_writer.append(i, value));
  }
  OK(cell_writer.row_finish());

  ObString compact_row;
  compact_row.assign_ptr(cell_writer.get_buf(), (int32_t)cell_writer.size());

  ObUpsRow ups_row;
  ObRowkey rk;
  ObObj rk_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];

  ups_row.set_row_desc(row_desc);
  OK(ObUpsRowUtil::convert(TABLE_ID, compact_row, ups_row, &rk, rk_obj));

  ASSERT_TRUE( rowkey == rk );
  

  const ObObj *cell = NULL;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  int64_t int_value = 0;
  for(int64_t i=1;i<10;i++)
  {
    OK(ups_row.raw_get_cell(i-1, cell, table_id, column_id));
    cell->get_int(int_value);
    ASSERT_EQ(i, int_value);
  }
}

TEST_F(ObUpsRowUtilTest, basic_test)
{
  ObRowDesc ups_row_desc;
  for(int i=0;i<8;i++)
  {
    ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_TABLE_ID + i);
  }

  ObUpsRow ups_row;
  ups_row.set_row_desc(ups_row_desc);
  ups_row.set_is_delete_row(true);

  ObObj value;

  for(int i=0;i<8;i++)
  {
    value.set_int(i);
    ups_row.raw_set_cell(i, value);
  }

  char buf[1024];
  ObString str;

  ObUpsRow result_row;
  result_row.set_row_desc(ups_row_desc);
  const ObObj *cell = NULL;
  const ObObj *result_cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t result_table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t result_column_id = OB_INVALID_ID;


  str.assign_buffer(buf, sizeof(buf));
  OK(ObUpsRowUtil::convert(ups_row, str));
  OK(ObUpsRowUtil::convert(TABLE_ID, str, result_row));
  
  for(int i=0;i<8;i++)
  {
    OK(ups_row.raw_get_cell(i, cell, table_id, column_id));
    OK(result_row.raw_get_cell(i, result_cell, result_table_id, result_column_id));
    printf("cell:%s\n", print_obj(*cell));
    printf("result_cell:%s\n", print_obj(*result_cell));
    ASSERT_TRUE( *cell == *result_cell );
  }

  ASSERT_TRUE(result_row.get_is_delete_row());

  ups_row.set_is_delete_row(false);

  str.assign_buffer(buf, sizeof(buf));
  OK(ObUpsRowUtil::convert(ups_row, str));
  OK(ObUpsRowUtil::convert(TABLE_ID, str, result_row));
  
  for(int i=0;i<8;i++)
  {
    OK(ups_row.raw_get_cell(i, cell, table_id, column_id));
    OK(result_row.raw_get_cell(i, result_cell, result_table_id, result_column_id));
    printf("cell:%s\n", print_obj(*cell));
    printf("result_cell:%s\n", print_obj(*result_cell));
    ASSERT_TRUE( *cell == *result_cell );
  }

  ASSERT_FALSE(result_row.get_is_delete_row());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


