/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_multi_get_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "ob_fake_ups_multi_get.h"
#include "common/utility.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace test;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1000

class ObFakeUpsMultiGetTest: public ::testing::Test
{
  public:
    ObFakeUpsMultiGetTest();
    virtual ~ObFakeUpsMultiGetTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObFakeUpsMultiGetTest(const ObFakeUpsMultiGetTest &other);
    ObFakeUpsMultiGetTest& operator=(const ObFakeUpsMultiGetTest &other);
  protected:
    // data members
};

ObFakeUpsMultiGetTest::ObFakeUpsMultiGetTest()
{
}

ObFakeUpsMultiGetTest::~ObFakeUpsMultiGetTest()
{
}

void ObFakeUpsMultiGetTest::SetUp()
{
}

void ObFakeUpsMultiGetTest::TearDown()
{
}

TEST_F(ObFakeUpsMultiGetTest, basic_test)
{
  ObRowDesc row_desc;
  OK(row_desc.add_column_desc(TABLE_ID, 3));
  OK(row_desc.add_column_desc(TABLE_ID, 4));

  ObFakeUpsMultiGet fake_ups_multi_get("test_cases/ob_fake_ups_multi_get.ini");

  char str_buf[100];
  ObRowkey rowkey;
  ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];

  const ObRowkey *get_rowkey = NULL;
  ObGetParam get_param(true);
  ObCellInfo cell_info;

  for(int i=0;i<3;i++)
  {
    rowkey_obj[0].set_int(1);
    rowkey_obj[1].set_int(i + 1);
    rowkey.assign(rowkey_obj, 2);

    cell_info.table_id_ = TABLE_ID;
    cell_info.row_key_ = rowkey;
    cell_info.column_id_ = 3;
    get_param.add_cell(cell_info);
    cell_info.column_id_ = 4;
    get_param.add_cell(cell_info);
  }

  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObString str;
  int64_t int_value = 0;


  fake_ups_multi_get.set_get_param(get_param);
  fake_ups_multi_get.set_row_desc(row_desc);

  OK(fake_ups_multi_get.open());
  
  for(int i=0;i<3;i++)
  {
    OK(fake_ups_multi_get.get_next_row(get_rowkey, row));
    OK(row->raw_get_cell(0, cell, table_id, column_id));
    cell->get_varchar(str);

    sprintf(str_buf, "chen%d", i+1);
    printf("rowkey:%s\n", to_cstring(*get_rowkey)); 
    printf("str:%s\n", str_buf);
    printf("str:%.*s\n", str.length(), str.ptr());
    ASSERT_EQ(0, strncmp(str.ptr(), str_buf, str.length()));

    (get_rowkey->get_obj_ptr()[0]).get_int(int_value);
    ASSERT_EQ(1, int_value);
    (get_rowkey->get_obj_ptr()[1]).get_int(int_value);
    ASSERT_EQ(i+1, int_value);
  }

  OK(fake_ups_multi_get.close());

  fake_ups_multi_get.reset();

  fake_ups_multi_get.set_get_param(get_param);
  OK(fake_ups_multi_get.open());
  
  get_param.reset(true);
  for(int i=3;i<6;i++)
  {
    rowkey_obj[0].set_int(1);
    rowkey_obj[1].set_int(i + 1);
    rowkey.assign(rowkey_obj, 2);

    cell_info.table_id_ = TABLE_ID;
    cell_info.row_key_ = rowkey;
    cell_info.column_id_ = 3;
    get_param.add_cell(cell_info);
    cell_info.column_id_ = 4;
    get_param.add_cell(cell_info);
  }

  for(int i=3;i<6;i++)
  {
    OK(fake_ups_multi_get.get_next_row(get_rowkey, row));
    OK(row->raw_get_cell(0, cell, table_id, column_id));
    cell->get_varchar(str);

    sprintf(str_buf, "chen%d", i+1);
    printf("rowkey:%s\n", to_cstring(*get_rowkey)); 
    printf("str:%s\n", str_buf);
    printf("str:%.*s\n", str.length(), str.ptr());
    ASSERT_EQ(0, strncmp(str.ptr(), str_buf, str.length()));

    (get_rowkey->get_obj_ptr()[0]).get_int(int_value);
    ASSERT_EQ(1, int_value);
    (get_rowkey->get_obj_ptr()[1]).get_int(int_value);
    ASSERT_EQ(i+1, int_value);
  }

  OK(fake_ups_multi_get.close());

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


