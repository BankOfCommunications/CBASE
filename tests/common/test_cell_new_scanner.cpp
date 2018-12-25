/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_cell_new_scanner.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_new_scanner.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObCellNewScannerTest: public ::testing::Test
{
  public:
    ObCellNewScannerTest();
    virtual ~ObCellNewScannerTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObCellNewScannerTest(const ObCellNewScannerTest &other);
    ObCellNewScannerTest& operator=(const ObCellNewScannerTest &other);
  protected:
    // data members
};

ObCellNewScannerTest::ObCellNewScannerTest()
{
}

ObCellNewScannerTest::~ObCellNewScannerTest()
{
}

void ObCellNewScannerTest::SetUp()
{
}

void ObCellNewScannerTest::TearDown()
{
}

ObRowkey get_rowkey(int row_seq, CharArena &rowkey_allocer)
{
  ObRowkey tmp_rowkey;
  ObObj obj;
  obj.set_int(row_seq);
  tmp_rowkey.assign(&obj, 1);

  ObRowkey rowkey;
  tmp_rowkey.deep_copy(rowkey, rowkey_allocer);
  return rowkey;
}

TEST_F(ObCellNewScannerTest, basic_test)
{
  int ret = OB_SUCCESS;

  ObCellNewScanner new_scanner;
  const int TABLE_ID = 1001;
  const int COLUMN_ID_NUM = 8;
  const int ROW_NUM = 47;
  ObRowDesc row_desc;
  for(int i=0;i<COLUMN_ID_NUM;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }

  new_scanner.set_row_desc(row_desc);
  new_scanner.set_mem_size_limit(2 * 1024);

  ObCellInfo cell_info;

  CharArena rowkey_allocer;

  int count = 0;
  for(int i=0;OB_SUCCESS == ret && i<ROW_NUM;i++)
  {
    ObRowkey rowkey = get_rowkey(i, rowkey_allocer);
    for(int j=0;OB_SUCCESS == ret && j<COLUMN_ID_NUM;j++)
    {
      cell_info.table_id_ = TABLE_ID;
      cell_info.column_id_ = j + OB_APP_MIN_COLUMN_ID;
      cell_info.row_key_ = rowkey;
      cell_info.value_.set_int(j);
      ret = new_scanner.add_cell(cell_info, true);
    }
    if(OB_SUCCESS == ret)
    {
      count ++;
    }
  }
  count = count - 1;
  ret = new_scanner.finish();

  ASSERT_EQ(0, ret);

  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num);
  ASSERT_FALSE(is_fullfilled);
  ASSERT_EQ(count, fullfilled_row_num);

  ObRowkey last_rowkey;
  new_scanner.get_last_row_key(last_rowkey);
  ASSERT_EQ(last_rowkey, get_rowkey(count - 1, rowkey_allocer));
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

