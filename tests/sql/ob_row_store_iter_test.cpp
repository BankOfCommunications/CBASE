/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_store_iter_test.cpp
 *
 *
 * Authors:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 *   - test ObRowStore::Iterator function
 */

#include <gtest/gtest.h>
#include "ob_fake_table.h"
#include "common/ob_row_store.h"
#include "common/ob_row.h"
#include "common/ob_object.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObRowStoreIteratorTest: public ::testing::Test
{
  public:
    ObRowStoreIteratorTest();
    virtual ~ObRowStoreIteratorTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowStoreIteratorTest(const ObRowStoreIteratorTest &other);
    ObRowStoreIteratorTest& operator=(const ObRowStoreIteratorTest &other);
  protected:
    // data members
    ObRowStore store_;
    ObRow row_;
    ObRowDesc row_desc_;
};

ObRowStoreIteratorTest::ObRowStoreIteratorTest()
{
}

ObRowStoreIteratorTest::~ObRowStoreIteratorTest()
{
}

void ObRowStoreIteratorTest::SetUp()
{
  //  char *filename = "ob_sort_test.run";
  //  ObString run_filename;
  //  run_filename.assign_ptr(filename, strlen(filename));
  //  sort_.set_run_filename(run_filename);
  //
  //  ASSERT_EQ(OB_SUCCESS, sort_.add_sort_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID, false));
  //  ASSERT_EQ(OB_SUCCESS, sort_.add_sort_column(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+1, true));
  //  ASSERT_EQ(OB_SUCCESS, sort_.set_child(0, input_table_));
  //
  //  sort_.set_mem_size_limit(200*1024*1024LL);
  //
  //setup store here;
  ObObj obj_a, obj_b, obj_d;
  ObObj str_c;
  ObString var_str;
  var_str.assign("hello", 5);
  row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0);
  row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1);
  row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2);
  row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3);
  obj_a.set_int(19);
  obj_b.set_int(2);
  str_c.set_varchar(var_str);
  obj_d.set_int(3);
  row_.set_row_desc(row_desc_);
  row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, obj_a);
  row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj_b);
  row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, str_c);
  row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3, obj_d);

}

void ObRowStoreIteratorTest::TearDown()
{
}


TEST_F(ObRowStoreIteratorTest, iter_test)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_TRUE(NULL != stored_row);

  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  //ObRowStore::Iterator iter = store_.begin();
  //ASSERT_EQ(true, store_.has_next_row());
  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_SUCCESS, store_.get_row(my_row));
  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_SUCCESS, store_.get_row(my_row));
  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_SUCCESS, store_.get_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.next_row());
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
}



TEST_F(ObRowStoreIteratorTest, iter_2_test)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_TRUE(NULL != stored_row);

  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  //ObRowStore::Iterator iter = store_.begin();
  //ASSERT_EQ(true, store_.has_next_row());
  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_ITER_END, store_.next_row());
  ASSERT_EQ(OB_ITER_END, store_.next_row());
  ASSERT_EQ(OB_ITER_END, store_.next_row());
  ASSERT_EQ(OB_ITER_END, store_.next_row());
}

TEST_F(ObRowStoreIteratorTest, iter_3_test)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_TRUE(NULL != stored_row);

  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  //ObRowStore::Iterator iter = store_.begin();
  //ASSERT_EQ(true, store_.has_next_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
}

TEST_F(ObRowStoreIteratorTest, iter_4_test)
{
  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  //ObRowStore::Iterator iter = store_.begin();
  //ASSERT_EQ(true, store_.has_next_row(my_row));
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
}

TEST_F(ObRowStoreIteratorTest, iter_5_test)
{
  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  ASSERT_EQ(OB_ITER_END, store_.next_row());
}

TEST_F(ObRowStoreIteratorTest, iter_6_test)
{
  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  //ASSERT_EQ(false, store_.has_next_row());
}

/* test get data from store */
TEST_F(ObRowStoreIteratorTest, iter_7_test)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
  ASSERT_TRUE(NULL != stored_row);

  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  //ASSERT_EQ(true, store_.has_next_row());
  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_SUCCESS, store_.get_row(my_row));

  const ObObj *a, *b, *c;
  int64_t i_a, i_b;
  ObString s_c;
  ObString tmp;
  tmp.assign("hello", 5);

  ASSERT_NE(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID + 1, OB_APP_MIN_COLUMN_ID + 0, a));
  ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, a));
  ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, b));
  ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, c));
  ASSERT_EQ(OB_SUCCESS, a->get_int(i_a));
  ASSERT_TRUE(19 == i_a);
  ASSERT_EQ(OB_SUCCESS, b->get_int(i_b));
  ASSERT_TRUE(2 == i_b);
  ASSERT_EQ(OB_SUCCESS, c->get_varchar(s_c));
  ASSERT_TRUE(tmp == s_c);
  a->dump();
  b->dump();
  c->dump();

  ASSERT_EQ(OB_SUCCESS, store_.next_row());
  ASSERT_EQ(OB_SUCCESS, store_.get_row(my_row));
  ASSERT_NE(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID + 1, OB_APP_MIN_COLUMN_ID + 0, a));
  ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, a));
  ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, b));
  ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, c));
  ASSERT_EQ(OB_SUCCESS, a->get_int(i_a));
  ASSERT_TRUE(19 == i_a);
  ASSERT_EQ(OB_SUCCESS, b->get_int(i_b));
  ASSERT_TRUE(2 == i_b);
  ASSERT_EQ(OB_SUCCESS, c->get_varchar(s_c));
  tmp.assign("hello", 5);
  ASSERT_TRUE(tmp == s_c);
  a->dump();
  b->dump();
  c->dump();
}

/* test get data from store when store cross multi blocks */
TEST_F(ObRowStoreIteratorTest, iter_8_test)
{
  const int row_count = 1000 * 1000;
  int i = 0;
  const ObRowStore::StoredRow *stored_row = NULL;
  for (i = 0; i < row_count ; i++)
  {
    ObObj obj_a;
    obj_a.set_int(i);
    row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, obj_a);

    ASSERT_EQ(OB_SUCCESS, store_.add_row(row_, stored_row));
    ASSERT_TRUE(NULL != stored_row);
  }
  TBSYS_LOG(INFO, "per line size=%ld", stored_row->compact_row_size_ + sizeof(ObRowStore::StoredRow));
  TBSYS_LOG(INFO, "total size=%ld", row_count * (stored_row->compact_row_size_ + sizeof(ObRowStore::StoredRow)));

  ObRow my_row;
  my_row.set_row_desc(row_desc_);
  int ret = OB_SUCCESS;

  for (i = 0; i < row_count; i++)
  {
    if(OB_SUCCESS !=( ret = store_.next_row()))
    {
    //ASSERT_EQ(true, store_.has_next_row());
      TBSYS_LOG(INFO, "i=%d", i);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    //ASSERT_EQ(OB_SUCCESS, store_.next_row());
    ASSERT_EQ(OB_SUCCESS, store_.get_row(my_row));

    const ObObj *a, *b, *c;
    int64_t i_a, i_b;
    ObString s_c;
    ObString tmp;
    tmp.assign("hello", 5);

    ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, a));
    ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, b));
    ASSERT_EQ(OB_SUCCESS, my_row.get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, c));
    ASSERT_EQ(OB_SUCCESS, a->get_int(i_a));
    ASSERT_TRUE(i == i_a);
    ASSERT_EQ(OB_SUCCESS, b->get_int(i_b));
    ASSERT_TRUE(2 == i_b);
    ASSERT_EQ(OB_SUCCESS, c->get_varchar(s_c));
    ASSERT_TRUE(tmp == s_c);
    a->dump();
    b->dump();
    c->dump();
  }
  
  ASSERT_EQ(OB_ITER_END, store_.next_row());
  ASSERT_EQ(OB_ITER_END, store_.get_row(my_row));
}







int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

