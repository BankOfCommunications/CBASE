/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_new_scanner_helper.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */


#include "common/ob_new_scanner_helper.h"
#include "common/ob_malloc.h"
#include "common/ob_string_buf.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1000LU
#define COLUMN_NUM 8
#define ROW_NUM 10

bool rowkey_equal(const ObRowkey &rowkey, const char *str)
{
  if(rowkey.length() != 1)
  {
    return false;
  }
  if(ObVarcharType != rowkey.ptr()[0].get_type())
  {
    return false;
  }
  ObString rowkey_str;
  rowkey.ptr()[0].get_varchar(rowkey_str);

  bool ret = 0 == strncmp(str, rowkey_str.ptr(), rowkey_str.length());
  if(!ret)
  {
    printf("rowkey: %.*s\n", rowkey_str.length(), rowkey_str.ptr());
  }
  return ret;
}

class ObNewScannerHelperTest: public ::testing::Test
{
  public:
    ObNewScannerHelperTest();
    virtual ~ObNewScannerHelperTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObNewScannerHelperTest(const ObNewScannerHelperTest &other);
    ObNewScannerHelperTest& operator=(const ObNewScannerHelperTest &other);

    ObStringBuf str_buf_;
    CharArena arena_;
  protected:
    // data members
    ObCellInfo gen_cell_info(const char *rowkey, uint64_t table_id, uint64_t column_id, int64_t int_value, bool is_add = false);
    int gen_row(ObScanner &scanner, int64_t rowkey);
    int gen_row2(ObScanner &scanner, int64_t rowkey);
    int64_t total(int64_t num);

    int gen_rowkey(const char *str, CharArena &arena, ObRowkey &rowkey);
};

int ObNewScannerHelperTest::gen_rowkey(const char *str, CharArena &arena, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;

  char* key_buf = arena.alloc(strlen(str));
  memcpy(key_buf, str, strlen(str));

  ObString rowkey_str;
  rowkey_str.assign_ptr(key_buf, (int32_t)strlen(str));

  ObObj *rowkey_obj = (ObObj *)arena.alloc(sizeof(ObObj));
  rowkey_obj->set_varchar(rowkey_str);

  rowkey.assign(rowkey_obj, 1);
  return ret;
}

ObNewScannerHelperTest::ObNewScannerHelperTest()
{
}

ObNewScannerHelperTest::~ObNewScannerHelperTest()
{
}

void ObNewScannerHelperTest::SetUp()
{
}

void ObNewScannerHelperTest::TearDown()
{
}

int64_t ObNewScannerHelperTest::total(int64_t num)
{
  int64_t ret = 0;
  for(int64_t i=0;i<num;i++)
  {
    ret += i;
  }
  return ret;
}

ObCellInfo ObNewScannerHelperTest::gen_cell_info(const char *rowkey, uint64_t table_id, uint64_t column_id, int64_t int_value, bool is_add)
{
  ObCellInfo cell_info;
  
  gen_rowkey(rowkey, arena_, cell_info.row_key_);
  cell_info.table_id_ = table_id;
  cell_info.column_id_ = column_id;
  cell_info.value_.set_int(int_value, is_add);
  return cell_info;
}

int ObNewScannerHelperTest::gen_row2(ObScanner &scanner,  int64_t rowkey)
{
  int ret = OB_SUCCESS;
  char rowkey_buf[100];
  sprintf(rowkey_buf, "rowkey_%05ld", rowkey);
  for(uint64_t i=OB_APP_MIN_COLUMN_ID; OB_SUCCESS == ret && i<OB_APP_MIN_COLUMN_ID + COLUMN_NUM;i++)
  {
    for(int64_t j=0;OB_SUCCESS == ret && j<(int64_t)i;j++)
    {
      ObCellInfo cell = gen_cell_info(rowkey_buf, TABLE_ID, i, j, true); 
      if(OB_SUCCESS != (ret = scanner.add_cell(cell)))
      {
        TBSYS_LOG(WARN, "add cell fail:ret[%d]", ret);
      }
    }
  }

  return ret;
}

int ObNewScannerHelperTest::gen_row(ObScanner &scanner,  int64_t rowkey)
{
  int ret = OB_SUCCESS;
  char rowkey_buf[100];
  sprintf(rowkey_buf, "rowkey_%05ld", rowkey);
  for(uint64_t i=OB_APP_MIN_COLUMN_ID; OB_SUCCESS == ret && i<OB_APP_MIN_COLUMN_ID + COLUMN_NUM;i++)
  {
    ObCellInfo cell = gen_cell_info(rowkey_buf, TABLE_ID, i, i + rowkey); 
    if(OB_SUCCESS != (ret = scanner.add_cell(cell)))
    {
      TBSYS_LOG(WARN, "add cell fail:ret[%d]", ret);
    }
  }
  return ret;
}

TEST_F(ObNewScannerHelperTest, basic_test1)
{
  ObScanner scanner;
  ObNewScanner new_scanner;

  ObCellInfo cell_info;
  ObRow row;

  ObRowDesc row_desc;
  for(uint64_t i=OB_APP_MIN_COLUMN_ID; i < OB_APP_MIN_COLUMN_ID + COLUMN_NUM;i++)
  {
    OK(row_desc.add_column_desc(TABLE_ID, i));
  }

  row.set_row_desc(row_desc);

  for(int64_t i=0;i<ROW_NUM;i++)
  {
    OK(gen_row2(scanner, i));
  }

  scanner.set_is_req_fullfilled(true, ROW_NUM * COLUMN_NUM);

  OK(ObNewScannerHelper::convert(scanner, &row_desc, new_scanner));

  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *value = NULL;
  int64_t int_value = 0;
  const ObRowkey *rowkey = NULL;

  for(int i=0;i<ROW_NUM;i++)
  {
    OK(new_scanner.get_next_row(rowkey, row));
    for(int64_t j=0;j<row.get_column_num();j++)
    {
      OK(row.raw_get_cell(j, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ(total(column_id), int_value);
    }
  }

  ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, row));

}


//test ObNewScannerHelper.add_cell
TEST_F(ObNewScannerHelperTest, add_cell_test)
{
  ObRow row;
  ObRowDesc row_desc;
  ObObj *cell = NULL;
  ObCellInfo cell_info;

  cell_info.table_id_ = TABLE_ID;

  row.get_cell(TABLE_ID, 88, cell);

  for (int i = 0; i < COLUMN_NUM; i ++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }
  row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  row.set_row_desc(row_desc);

  row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
  cell->set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);

  cell_info.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_NOP);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_VALID == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_int(3);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_VALID == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_DEL_ROW == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_NOP);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_NEW_ADD == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_NOP);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_NEW_ADD == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_DEL_ROW == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_DEL_ROW == cell->get_ext()) << cell->get_ext();

}

TEST_F(ObNewScannerHelperTest, basic_test2)
{
  ObScanner scanner;
  ObNewScanner new_scanner;

  ObCellInfo cell_info;
  ObRow row;

  ObRowDesc row_desc;
  for(uint64_t i=OB_APP_MIN_COLUMN_ID; i < OB_APP_MIN_COLUMN_ID + COLUMN_NUM;i++)
  {
    OK(row_desc.add_column_desc(TABLE_ID, i));
  }

  row.set_row_desc(row_desc);

  for(int64_t i=0;i<ROW_NUM;i++)
  {
    OK(gen_row(scanner, i));
  }

  scanner.set_is_req_fullfilled(false, ROW_NUM * COLUMN_NUM);

  OK(ObNewScannerHelper::convert(scanner, &row_desc, new_scanner));

  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *value = NULL;
  int64_t int_value = 0;
  const ObRowkey *rowkey = NULL;

  for(int i=0;i<ROW_NUM;i++)
  {
    OK(new_scanner.get_next_row(rowkey, row));
    for(int64_t j=0;j<row.get_column_num();j++)
    {
      OK(row.raw_get_cell(j, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ((int64_t)(i+column_id), int_value);
    }
  }

  ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, row));

  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num);
  ASSERT_FALSE(is_fullfilled);
  ASSERT_EQ(ROW_NUM, fullfilled_row_num);

}


TEST_F(ObNewScannerHelperTest, new_scanner_size_overflow)
{
  ObScanner scanner;
  ObNewScanner new_scanner;

  new_scanner.set_mem_size_limit(1024);

  int64_t row_num = 100;

  ObCellInfo cell_info;
  ObRow row;

  ObRowDesc row_desc;
  for(uint64_t i=OB_APP_MIN_COLUMN_ID; i < OB_APP_MIN_COLUMN_ID + COLUMN_NUM;i++)
  {
    OK(row_desc.add_column_desc(TABLE_ID, i));
  }

  row.set_row_desc(row_desc);

  for(int64_t i=0;i<row_num;i++)
  {
    OK(gen_row(scanner, i));
  }

  scanner.set_is_req_fullfilled(true, row_num * COLUMN_NUM);

  OK(ObNewScannerHelper::convert(scanner, &row_desc, new_scanner));

  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;

  new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num);

  ASSERT_FALSE(is_fullfilled);

  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *value = NULL;
  int64_t int_value = 0;
  const ObRowkey *rowkey = NULL;
  ObString rowkey_str;
  char buf[100];


  sprintf(buf, "rowkey_%05ld", fullfilled_row_num - 1);
  ObRowkey last_rowkey;

  new_scanner.get_last_row_key(last_rowkey);
  ASSERT_TRUE(rowkey_equal(last_rowkey, buf));

  for(int i=0;i<fullfilled_row_num;i++)
  {
    OK(new_scanner.get_next_row(rowkey, row));
    for(int64_t j=0;j<row.get_column_num();j++)
    {
      OK(row.raw_get_cell(j, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ((int64_t)(i+column_id), int_value);
    }
    int cmp = strncmp(buf, rowkey_str.ptr(), rowkey_str.length());

    if(0 != cmp)
    {
      rowkey->ptr()[0].get_varchar(rowkey_str);
      sprintf(buf, "rowkey_%05d", i);
      printf("%s\n", buf);
      printf("%.*s\n", rowkey_str.length(), rowkey_str.ptr());
      ASSERT_FALSE(true);
    }
  }
  ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, row));
  printf("fullfilled_row_num %ld\n", fullfilled_row_num);
}

TEST_F(ObNewScannerHelperTest, basic_test)
{
  ObScanner scanner;
  ObNewScanner new_scanner;

  ObCellInfo cell_info;
  ObRow row;

  ObRowDesc row_desc;
  for(uint64_t i=OB_APP_MIN_COLUMN_ID; i < OB_APP_MIN_COLUMN_ID + COLUMN_NUM;i++)
  {
    OK(row_desc.add_column_desc(TABLE_ID, i));
  }

  row.set_row_desc(row_desc);

  for(int64_t i=0;i<ROW_NUM;i++)
  {
    OK(gen_row(scanner, i));
  }

  scanner.set_is_req_fullfilled(true, ROW_NUM * COLUMN_NUM);

  OK(ObNewScannerHelper::convert(scanner, &row_desc, new_scanner));

  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *value = NULL;
  int64_t int_value = 0;
  const ObRowkey *rowkey = NULL;
  ObString rowkey_str;
  char buf[100];

  for(int i=0;i<ROW_NUM;i++)
  {
    OK(new_scanner.get_next_row(rowkey, row));
    for(int64_t j=0;j<row.get_column_num();j++)
    {
      OK(row.raw_get_cell(j, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ((int64_t)(i+column_id), int_value);
    }
    int cmp = strncmp(buf, rowkey_str.ptr(), rowkey_str.length());

    if(0 != cmp)
    {
      rowkey->ptr()[0].get_varchar(rowkey_str);
      sprintf(buf, "rowkey_%05d", i);
      printf("%s\n", buf);
      printf("%.*s\n", rowkey_str.length(), rowkey_str.ptr());
      ASSERT_FALSE(true);
    }
  }

  ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, row));

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

