/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_new_scanner_test.cpp,  08/02/2012 01:50:32 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   test ob_new_scanner
 * 
 */

#include "common/ob_new_scanner.h"
#include "gtest/gtest.h"


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




int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


