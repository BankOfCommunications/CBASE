/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ob_row_store.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */



#include "common/ob_compact_cell_iterator.h"
#include "common/ob_row_store.h"
#include "common/ob_malloc.h"
#include "common/ob_action_flag.h"
#include "common/ob_kv_storecache.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1000LU

class ObRowStoreTest: public ::testing::Test
{
  public:
    ObRowStoreTest();
    virtual ~ObRowStoreTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowStoreTest(const ObRowStoreTest &other);
    ObRowStoreTest& operator=(const ObRowStoreTest &other);
  protected:
    // data members
};

ObRowStoreTest::ObRowStoreTest()
{
}

ObRowStoreTest::~ObRowStoreTest()
{
}

void ObRowStoreTest::SetUp()
{
}

void ObRowStoreTest::TearDown()
{
}

ObObj gen_int_obj(int64_t int_value, bool is_add = false)
{
  ObObj value;
  value.set_int(int_value, is_add);
  return value;
}

TEST_F(ObRowStoreTest, rowkey)
{
  ObRowStore row_store;

  ObRowkey rowkey;
  ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];

  ObRowkey rk;
  ObObj rk_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];

  ObRowDesc row_desc;
  for(int64_t i=0;i<10;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }

  ObRow row;
  row.set_row_desc(row_desc);

  ObObj cell;
  const ObRowStore::StoredRow *stored_row = NULL;
  const ObObj *value = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = 0;

  ObCompactCellIterator cell_reader;


  for(int64_t j=0;j<9;j++)
  {
    rowkey_obj[0].set_int(j);
    rowkey_obj[1].set_int(j * 10);
    rowkey.assign(rowkey_obj, 2);

    for(int64_t i=0;i<10;i++)
    {
      cell.set_int(j * 1000 + i);
      OK(row.raw_set_cell(i, cell));
    }
    OK(row_store.add_row(rowkey, row, stored_row));

    OK(cell_reader.init(stored_row->get_compact_row(), DENSE_SPARSE));

    for(int64_t i=0;i<2;i++)
    {
      OK(cell_reader.next_cell());
      OK(cell_reader.get_cell(value));

      rk_obj[i] = *value;
    }

    rk.assign(rk_obj, 2);
    ASSERT_TRUE( rowkey == rk );

    OK(cell_reader.next_cell());

    for(int64_t i=0;i<10;i++)
    {
      OK(cell_reader.next_cell());
      OK(cell_reader.get_cell(column_id, value));
      value->get_int(int_value);
      ASSERT_EQ(j * 1000 + i, int_value);
    }
  }

  
  const ObRowkey *rk_ptr = NULL;
  for(int64_t j=0;j<9;j++)
  {
    OK(row_store.get_next_row(rk_ptr, row));

    rowkey_obj[0].set_int(j);
    rowkey_obj[1].set_int(j * 10);
    rowkey.assign(rowkey_obj, 2);

    ASSERT_TRUE( rowkey == *rk_ptr );

    for(int64_t i=0;i<10;i++)
    {
      OK(row.raw_get_cell(i, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ( j * 1000 + i, int_value);
    }
  }
}

TEST_F(ObRowStoreTest, op_nop)
{
  ObRowStore row_store;

  ObUpsRow ups_row;

  ObRowDesc row_desc;
  for(uint64_t i=1;i<=8;i++)
  {
    OK(row_desc.add_column_desc(TABLE_ID, i));
  }

  ups_row.set_row_desc(row_desc);
  OK(ups_row.reset());

  const ObRowStore::StoredRow *stored_row = NULL;
  OK(row_store.add_ups_row(ups_row, stored_row));

  ObUpsRow got_ups_row;
  got_ups_row.set_row_desc(row_desc);

  ObObj value;
  value.set_int(1);

  for(int64_t i=0;i<8;i++)
  {
    got_ups_row.raw_set_cell(0, value);
  }

  row_store.get_next_ups_row(got_ups_row);

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

TEST_F(ObRowStoreTest, ups_row_test)
{
  ObRowStore row_store;
  int64_t cur_size_counter = 0;

  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 1);
  row_desc.add_column_desc(TABLE_ID, 2);
  row_desc.add_column_desc(TABLE_ID, 3);
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);

  ObUpsRow row;
  row.set_row_desc(row_desc);

  #define ADD_ROW(is_delete, num1, num2, num3, num4, num5) \
  row.set_is_delete_row(is_delete); \
  row.set_cell(TABLE_ID, 1, gen_int_obj(num1, false)); \
  row.set_cell(TABLE_ID, 2, gen_int_obj(num2, false)); \
  row.set_cell(TABLE_ID, 3, gen_int_obj(num3, false)); \
  row.set_cell(TABLE_ID, 4, gen_int_obj(num4, false)); \
  row.set_cell(TABLE_ID, 5, gen_int_obj(num5, false)); \
  row_store.add_row(row, cur_size_counter);


  ObUpsRow get_row;
  get_row.set_row_desc(row_desc);

  #define CHECK_CELL(column_id, num) \
  { \
    const ObObj *cell = NULL; \
    int64_t int_value = 0; \
    get_row.get_cell(TABLE_ID, column_id, cell); \
    cell->get_int(int_value); \
    ASSERT_EQ(num, int_value); \
  }

  #define CHECK_ROW(is_delete, num1, num2, num3, num4, num5) \
  row_store.get_next_row(get_row); \
  ASSERT_TRUE( get_row.get_is_delete_row() == is_delete ); \
  CHECK_CELL(1, num1); \
  CHECK_CELL(2, num2); \
  CHECK_CELL(3, num3); \
  CHECK_CELL(4, num4); \
  CHECK_CELL(5, num5);

  ADD_ROW(true, 1, 2, 4, 5, 3);
  ADD_ROW(false, 1, 2, 4, 5, 3);
  ADD_ROW(true, 1, 2, 4, 5, 3);
  ADD_ROW(false, 1, 2, 4, 5, 3);
  ADD_ROW(false, 1, 2, 4, 5, 3);

  CHECK_ROW(true, 1, 2, 4, 5, 3);
  CHECK_ROW(false, 1, 2, 4, 5, 3);
  CHECK_ROW(true, 1, 2, 4, 5, 3);
  CHECK_ROW(false, 1, 2, 4, 5, 3);
  CHECK_ROW(false, 1, 2, 4, 5, 3);

  #undef ADD_ROW
  #undef CHECK_ROW
}

TEST_F(ObRowStoreTest, basic_test)
{
  ObRowStore row_store;
  int64_t cur_size_counter = 0;

  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 1);
  row_desc.add_column_desc(TABLE_ID, 2);
  row_desc.add_column_desc(TABLE_ID, 3);
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);

  ObRow row;
  row.set_row_desc(row_desc);

  #define ADD_ROW(num1, num2, num3, num4, num5) \
  row.set_cell(TABLE_ID, 1, gen_int_obj(num1, false)); \
  row.set_cell(TABLE_ID, 2, gen_int_obj(num2, false)); \
  row.set_cell(TABLE_ID, 3, gen_int_obj(num3, false)); \
  row.set_cell(TABLE_ID, 4, gen_int_obj(num4, false)); \
  row.set_cell(TABLE_ID, 5, gen_int_obj(num5, false)); \
  row_store.add_row(row, cur_size_counter);


  ObRow get_row;
  get_row.set_row_desc(row_desc);

  #define CHECK_CELL(column_id, num) \
  { \
    const ObObj *cell = NULL; \
    int64_t int_value = 0; \
    get_row.get_cell(TABLE_ID, column_id, cell); \
    cell->get_int(int_value); \
    ASSERT_EQ(num, int_value); \
  }

  #define CHECK_ROW(num1, num2, num3, num4, num5) \
  row_store.get_next_row(get_row); \
  CHECK_CELL(1, num1); \
  CHECK_CELL(2, num2); \
  CHECK_CELL(3, num3); \
  CHECK_CELL(4, num4); \
  CHECK_CELL(5, num5);

  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);

  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
  KeyValueCache<ObString, ObRowStore, 8 * 1024, 2 * 1024 * 1024> cache;
  ASSERT_TRUE(cache.init(100 * 1024 * 1024) == OB_SUCCESS);
  ObString key1 = ObString::make_string("item1");
  ASSERT_TRUE(cache.put(key1, row_store) == OB_SUCCESS);
  ObRowStore *row_store2;
  common::CacheHandle handle;
  ASSERT_TRUE(cache.get(key1, row_store2, handle) == OB_SUCCESS);
  cache.revert(handle);
  #define CHECK_ROW2(num1, num2, num3, num4, num5) \
  row_store2->get_next_row(get_row); \
  CHECK_CELL(1, num1); \
  CHECK_CELL(2, num2); \
  CHECK_CELL(3, num3); \
  CHECK_CELL(4, num4); \
  CHECK_CELL(5, num5);
  CHECK_ROW2(1, 2, 4, 5, 3);
  CHECK_ROW2(1, 2, 4, 5, 3);
  CHECK_ROW2(1, 2, 4, 5, 3);
  CHECK_ROW2(1, 2, 4, 5, 3);
  CHECK_ROW2(1, 2, 4, 5, 3);
  TBSYS_LOG(INFO, "SUCCESS");
}


TEST_F(ObRowStoreTest, row_store_reset_test)
{
  ObRowStore row_store;
  int ret = OB_SUCCESS;

  const int64_t COLUMN_NUM = 8;
  int64_t row_num = 1000;

  ObRow row;
  ObRow got_row;
  ObRowDesc row_desc;
  ObObj cell;
  const ObObj *got_cell = NULL;
  int64_t int_value = 0;
  const ObRowStore::StoredRow *stored_row = NULL;
  int64_t used_mem_size = 0;
  int64_t got_row_count = 0;

  for (int i=0;i<COLUMN_NUM;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }
  row.set_row_desc(row_desc);

  
  for (int i=0;i<COLUMN_NUM;i++)
  {
    cell.set_int(i);
    row.set_cell(TABLE_ID, i + OB_APP_MIN_COLUMN_ID, cell);
  }

  for (int k=0; k<3; k++)
  {
    TBSYS_LOG(INFO, "k=%d", k);
    for (int i=0; i<row_num; i++)
    {
      row_store.add_row(row, stored_row);
    }

    got_row.set_row_desc(row_desc);
    got_row_count = 0;
    while (true)
    {
      ret = row_store.get_next_row(got_row);
      if (OB_ITER_END == ret)
      {
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret) << "time " << k << std::endl;
      for (int i=0; i<COLUMN_NUM; i++)
      {
        ret = got_row.get_cell(TABLE_ID, i + OB_APP_MIN_COLUMN_ID, got_cell);
        got_cell->get_int(int_value);
        ASSERT_EQ(i, int_value);
      }
      got_row_count ++;
    }
    ASSERT_EQ(row_num, got_row_count);

    if (0 == used_mem_size)
    {
      used_mem_size = row_store.get_used_mem_size();
    }
    row_store.reuse();
    ASSERT_EQ(used_mem_size, row_store.get_used_mem_size());
  }
}


TEST_F(ObRowStoreTest, test_page)
{
  ObRowStore row_store;
  ObUpsRow ups_row;
  ObRowDesc row_desc;
  uint64_t table_id = 3001;
  ObRowkey rowkey;
  const int column_num = 16;
  ObObj obj_buf[column_num];
  row_desc.add_column_desc(table_id, 16);

  ups_row.set_row_desc(row_desc);
  ups_row.set_is_delete_row(true);

  rowkey.assign(obj_buf, column_num);

  ObObj obj;
  obj.set_varchar(ObString::make_string("oceanbase is a powerful database. I love oceanbaseoceanbase is a powerful database. I love oceanbase"));

  for (int i = 0; i < column_num; i ++)
  {
    obj_buf[i] = obj;
  }

  obj.set_int(3);
  ups_row.raw_set_cell(0, obj);

  int64_t cur_size_counter = 0;
  for (int i = 0; i < 1000; i ++)
  {
    row_store.add_ups_row(rowkey, ups_row, cur_size_counter);
  }

  ObUpsRow result_ups_row;
  const ObRowkey *result_rowkey = NULL;
  result_ups_row.set_row_desc(row_desc);
  for (int i = 0; i < 1000; i ++)
  {
    ASSERT_EQ(OB_SUCCESS, row_store.get_next_ups_row(result_rowkey, result_ups_row));
  }
  ASSERT_EQ(OB_ITER_END, row_store.get_next_ups_row(result_rowkey, result_ups_row));
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

