/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ob_new_scanner.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */


#include "common/ob_new_scanner.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;
using namespace std;

#define TABLE_ID 1000LU

template <typename T>
void CLEAN(T & e)
{
  memset(&e, 0, sizeof(T));
}


ObObj gen_int_obj(int64_t int_value, bool is_add = false)
{
  ObObj value;
  value.set_int(int_value, is_add);
  return value;
}

  #define ADD_ROW(scanner, num1, num2, num3, num4, num5) ({ \
    int ret = OB_SUCCESS; \
    row.set_cell(TABLE_ID, 1, gen_int_obj(num1, false)); \
    row.set_cell(TABLE_ID, 2, gen_int_obj(num2, false)); \
    row.set_cell(TABLE_ID, 3, gen_int_obj(num3, false)); \
    row.set_cell(TABLE_ID, 4, gen_int_obj(num4, false)); \
    row.set_cell(TABLE_ID, 5, gen_int_obj(num5, false)); \
    ret = scanner.add_row(row); })

// get_row MUST be global
  #define CHECK_CELL(column_id, num) \
  { \
    const ObObj *cell = NULL; \
    int64_t int_value = 0; \
    get_row.get_cell(TABLE_ID, column_id, cell); \
    cell->get_int(int_value); \
    ASSERT_EQ(num, int_value); \
  }

  #define CHECK_ROW(scanner, num1, num2, num3, num4, num5) \
  EXPECT_EQ(OB_SUCCESS, scanner.get_next_row(get_row)); \
  CHECK_CELL(1, num1); \
  CHECK_CELL(2, num2); \
  CHECK_CELL(3, num3); \
  CHECK_CELL(4, num4); \
  CHECK_CELL(5, num5);


TEST(TestObNewScanner, add_row_serialize)
{
  char buffer[2048];
  int64_t pos = 0;
  int64_t pos_dest = 0;
  ObNewScanner sc, sc_dest;
  ObNewRange range, sc_range, sc_dest_range;
  ObObj start_obj, end_obj;
  start_obj.set_int(0);
  end_obj.set_int(0xffff);
  range.start_key_.assign(&start_obj, 1);
  range.end_key_.assign(&end_obj, 1);
  EXPECT_EQ(OB_SUCCESS, sc.set_range(range));
  
  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 1);
  row_desc.add_column_desc(TABLE_ID, 2);
  row_desc.add_column_desc(TABLE_ID, 3);
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);

  ObRow row;
  ObRow get_row;
  row.set_row_desc(row_desc);
  get_row.set_row_desc(row_desc);

  ADD_ROW(sc, 1, 2, 4, 5, 3);
  ADD_ROW(sc, 1, 2, 4, 5, 3);
  ADD_ROW(sc, 1, 2, 4, 5, 3);
  ADD_ROW(sc, 1, 2, 4, 5, 3);
  ADD_ROW(sc, 1, 2, 4, 5, 3);

  CHECK_ROW(sc, 1, 2, 4, 5, 3);
  CHECK_ROW(sc, 1, 2, 4, 5, 3);
  CHECK_ROW(sc, 1, 2, 4, 5, 3);
  CHECK_ROW(sc, 1, 2, 4, 5, 3);
  CHECK_ROW(sc, 1, 2, 4, 5, 3);


  bool tmp_is_fulfilled = true;
  EXPECT_EQ(OB_SUCCESS, sc.set_is_req_fullfilled(tmp_is_fulfilled, 0));
  EXPECT_EQ(OB_SUCCESS, sc.serialize(buffer, 2048, pos));
  EXPECT_EQ(sc.get_serialize_size(), pos);
  EXPECT_EQ(OB_SUCCESS, sc_dest.deserialize(buffer, 2048, pos_dest));
  EXPECT_EQ(pos, pos_dest);
  EXPECT_EQ(OB_SUCCESS, sc.get_range(sc_range));
  EXPECT_EQ(OB_SUCCESS, sc.get_range(sc_dest_range));
  EXPECT_EQ(0, sc_range.compare_with_startkey2(sc_dest_range));
  EXPECT_EQ(0, sc_range.compare_with_endkey2(sc_dest_range));

  CHECK_ROW(sc_dest, 1, 2, 4, 5, 3);
  CHECK_ROW(sc_dest, 1, 2, 4, 5, 3);
  CHECK_ROW(sc_dest, 1, 2, 4, 5, 3);
  CHECK_ROW(sc_dest, 1, 2, 4, 5, 3);
  CHECK_ROW(sc_dest, 1, 2, 4, 5, 3);

  bool is_fulfilled = false;
  int64_t fulfilled_item_num = -1;
  EXPECT_EQ(OB_SUCCESS, sc.get_is_req_fullfilled(is_fulfilled, fulfilled_item_num));
  EXPECT_TRUE(true == is_fulfilled);
  EXPECT_TRUE(0 == fulfilled_item_num);

  EXPECT_EQ(OB_SUCCESS, sc.get_is_req_fullfilled(is_fulfilled, fulfilled_item_num));
  EXPECT_TRUE(true == is_fulfilled);
  EXPECT_TRUE(0 == fulfilled_item_num);
  
  CLEAN(sc);
  CLEAN(sc_dest);
}

TEST(TestObNewScanner, set_mem_limit)
{
  ObNewScanner sc;
  ObNewRange range, sc_range;
  ObObj start_obj, end_obj;
  start_obj.set_int(0);
  end_obj.set_int(0xffff);
  range.start_key_.assign(&start_obj, 1);
  range.end_key_.assign(&end_obj, 1);
  EXPECT_EQ(OB_SUCCESS, sc.set_range(range));
  
  sc.set_mem_size_limit(40);
  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 1);
  row_desc.add_column_desc(TABLE_ID, 2);
  row_desc.add_column_desc(TABLE_ID, 3);
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);

  ObRow row;
  ObRow get_row;
  row.set_row_desc(row_desc);
  get_row.set_row_desc(row_desc);


  int ret;
  ret =  ADD_ROW(sc, 1, 2, 4, 5, 3);
  EXPECT_EQ(OB_SUCCESS,ret);
  
  ret =  ADD_ROW(sc, 1, 2, 4, 5, 3);
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  
  ret =  ADD_ROW(sc, 1, 2, 4, 5, 3);
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);


  CLEAN(sc);
}


TEST(TestObNewScanner, try_all_the_best_to_add_row_to_scanner_as_much_as_possible)
{
  ObNewScanner sc;
  ObNewRange range, sc_range;
  ObObj start_obj, end_obj;
  start_obj.set_int(0);
  end_obj.set_int(0xffff);
  range.start_key_.assign(&start_obj, 1);
  range.end_key_.assign(&end_obj, 1);
  EXPECT_EQ(OB_SUCCESS, sc.set_range(range));
  
  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 1);
  row_desc.add_column_desc(TABLE_ID, 2);
  row_desc.add_column_desc(TABLE_ID, 3);
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);

  ObRow row;
  ObRow get_row;
  row.set_row_desc(row_desc);
  get_row.set_row_desc(row_desc);


  int ret;
  int n = 1024*1024;
  while(n-- > 0)
  {
    ret =  ADD_ROW(sc, 1, 2, 4, 5, 3);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_SIZE_OVERFLOW == ret);
    if (ret == OB_SIZE_OVERFLOW)
    {
      TBSYS_LOG(INFO, "total %d rows inserted", n);
      break;
    }
  }
 
  CLEAN(sc);
}


#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1000LU

class ObNewScannerTest: public ::testing::Test
{
  public:
    ObNewScannerTest();
    virtual ~ObNewScannerTest();
    virtual void SetUp();
    virtual void TearDown();

    int gen_rowkey(const char *str, CharArena &arena, ObRowkey &rowkey);

  private:
    // disallow copy
    ObNewScannerTest(const ObNewScannerTest &other);
    ObNewScannerTest& operator=(const ObNewScannerTest &other);

  protected:
    // data members
    CharArena arena_;
};

ObNewScannerTest::ObNewScannerTest()
{
}

ObNewScannerTest::~ObNewScannerTest()
{
}

int ObNewScannerTest::gen_rowkey(const char *str, CharArena &arena, ObRowkey &rowkey)
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

void ObNewScannerTest::SetUp()
{
}

void ObNewScannerTest::TearDown()
{
}

TEST_F(ObNewScannerTest, serialize)
{
  ObNewScanner new_scanner;

  char rowkey_buf[100];
  ObRowkey rowkey;
  bool is_fulfilled = false;
  int64_t fullfilled_row_num = 0;

  ObRowDesc row_desc;
  for(int64_t i=0;i<10;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }

  ObRow row;
  row.set_row_desc(row_desc);

  ObObj cell;
  const ObObj *value = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = 0;

  for(int64_t j=0;j<9;j++)
  {
    sprintf(rowkey_buf, "rowkey_%05ld", j);
    gen_rowkey(rowkey_buf, arena_, rowkey);
    for(int64_t i=0;i<10;i++)
    {
      cell.set_int(j * 1000 + i);
      OK(row.raw_set_cell(i, cell));
    }
    OK(new_scanner.add_row(rowkey, row));
  }
  OK(new_scanner.set_is_req_fullfilled(true, 1000));
  
  char buf[1024];
  int64_t pos = 0;
  OK(new_scanner.serialize(buf, sizeof(buf), pos));
  int64_t size = new_scanner.get_serialize_size();
  ASSERT_EQ(size, pos);

  printf("serialize len [%ld]\n", pos);

  ObNewScanner scanner2;

  int64_t data_len = pos;
  pos = 0;
  OK(scanner2.deserialize(buf, data_len, pos));

  const ObRowkey *rk = NULL;

  
  for(int64_t j=0;j<9;j++)
  {
    OK(scanner2.get_next_row(rk, row));

    sprintf(rowkey_buf, "rowkey_%05ld", j);
    gen_rowkey(rowkey_buf, arena_, rowkey);
    ASSERT_TRUE(rowkey == *rk);

    for(int64_t i=0;i<10;i++)
    {
      OK(row.raw_get_cell(i, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ( j * 1000 + i, int_value);
    }
  }

  OK(scanner2.get_is_req_fullfilled(is_fulfilled, fullfilled_row_num));

  ASSERT_TRUE(true == is_fulfilled);
  ASSERT_EQ(1000, fullfilled_row_num);


}

TEST_F(ObNewScannerTest, op_nop)
{
  ObNewScanner new_scanner;
  ObUpsRow ups_row;

  ObRowDesc row_desc;
  for(uint64_t i=1;i<=8;i++)
  {
    OK(row_desc.add_column_desc(TABLE_ID, i));
  }

  ups_row.set_row_desc(row_desc);
  OK(ups_row.reset());

  OK(new_scanner.add_row(ups_row));

  ObUpsRow got_ups_row;
  got_ups_row.set_row_desc(row_desc);

  ObObj value;
  value.set_int(1);

  for(int64_t i=0;i<8;i++)
  {
    got_ups_row.raw_set_cell(0, value);
  }

  new_scanner.get_next_row(got_ups_row);

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  for(int64_t i=0;i<8;i++)
  {
    got_ups_row.raw_get_cell(i, cell, table_id, column_id);
    ASSERT_EQ(ObExtendType, cell->get_type());
    ASSERT_TRUE( ObActionFlag::OP_NOP == cell->get_ext() );
  }


}

TEST_F(ObNewScannerTest, basic_test)
{
  ObNewScanner new_scanner;

  char rowkey_buf[100];
  ObString rowkey_str;
  ObObj rowkey_obj;
  ObRowkey rowkey;

  ObRowDesc row_desc;
  for(int64_t i=0;i<10;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }

  ObRow row;
  row.set_row_desc(row_desc);

  ObObj cell;
  const ObObj *value = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = 0;

  for(int64_t j=0;j<9;j++)
  {
    sprintf(rowkey_buf, "rowkey_%05ld", j);
    rowkey_str.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));
    rowkey_obj.set_varchar(rowkey_str);
    rowkey.assign(&rowkey_obj, 1);
    for(int64_t i=0;i<10;i++)
    {
      cell.set_int(j * 1000 + i);
      OK(row.raw_set_cell(i, cell));
    }
    OK(new_scanner.add_row(rowkey, row));
  }

  const ObRowkey *rk = NULL;
  
  for(int64_t j=0;j<9;j++)
  {
    OK(new_scanner.get_next_row(rk, row));

    sprintf(rowkey_buf, "rowkey_%05ld", j);
    rowkey_str.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));
    rowkey_obj.set_varchar(rowkey_str);
    rowkey.assign(&rowkey_obj, 1);

    ASSERT_TRUE(rowkey == *rk);

    for(int64_t i=0;i<10;i++)
    {
      OK(row.raw_get_cell(i, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ( j * 1000 + i, int_value);
    }
  }

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
