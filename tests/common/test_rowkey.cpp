/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_rowkey.cpp
 *
 * Authors:
 *   Huang Yu <xiaochu.yh@alipay.com>
 *
 */

#include "common/ob_malloc.h"
#include "common/ob_rowkey.h"
#include "common/ob_schema.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;

#define TABLE_ID 1000
#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObRowkeyTest: public ::testing::Test
{
  public:
    ObRowkeyTest();
    virtual ~ObRowkeyTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowkeyTest(const ObRowkeyTest &other);
    ObRowkeyTest& operator=(const ObRowkeyTest &other);
  protected:
    // data members
};

ObRowkeyTest::ObRowkeyTest()
{
}

ObRowkeyTest::~ObRowkeyTest()
{
}

void ObRowkeyTest::SetUp()
{
}

void ObRowkeyTest::TearDown()
{
}


TEST_F(ObRowkeyTest, basic_test1)
{
  char buf[1024];
  int64_t pos = 0;
  ObRowkey rowkey;
  ObRowkey result_rowkey;
  ObObj foo_obj;
  result_rowkey.assign(&foo_obj, 1);
  rowkey.set_max_row();
  EXPECT_TRUE(OB_SUCCESS == rowkey.serialize(buf, 1024, pos));
  TBSYS_LOG(INFO, "after serialize, pos=%ld", pos);
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == result_rowkey.deserialize((const char*)buf, 1024L, pos));
  TBSYS_LOG(INFO, "after deserialize, pos=%ld", pos);
  EXPECT_TRUE(result_rowkey.is_max_row());
}

TEST_F(ObRowkeyTest, cast)
{
  // (1, '2013-1-7', 'yes', true) ==> (varchar, datetime, bool, varchar)
  ObRowkeyInfo rowkey_info;
  ObRowkeyColumn rowkey_col;
  rowkey_col.length_ = 0;
  rowkey_col.column_id_ = 0;
  rowkey_col.type_ = ObVarcharType;
  ASSERT_EQ(OB_SUCCESS, rowkey_info.add_column(rowkey_col));
  rowkey_col.type_ = ObPreciseDateTimeType;
  ASSERT_EQ(OB_SUCCESS, rowkey_info.add_column(rowkey_col));
  rowkey_col.type_ = ObBoolType;
  ASSERT_EQ(OB_SUCCESS, rowkey_info.add_column(rowkey_col));
  rowkey_col.type_ = ObVarcharType;
  ASSERT_EQ(OB_SUCCESS, rowkey_info.add_column(rowkey_col));
  ASSERT_EQ(4, rowkey_info.get_size());
  // cons rowkey
  ObObj rowkey_objs[4];
  rowkey_objs[0].set_int(1234);
  rowkey_objs[1].set_varchar(ObString::make_string("2013-1-7"));
  rowkey_objs[2].set_varchar(ObString::make_string("yes"));
  rowkey_objs[3].set_bool(true);
  ObRowkey rowkey;
  rowkey.assign(rowkey_objs, 4);
  // cast
  char buff[1024];
  memset(buff, 0, 1024);
  int64_t used_len = 0;
  ASSERT_EQ(OB_SUCCESS, ob_cast_rowkey(rowkey_info, rowkey, buff, 1024, used_len));
  // verify
  ObString varchar;
  ObPreciseDateTime dt;
  bool bool_val = false;
  ASSERT_EQ(OB_SUCCESS, rowkey.get_obj_ptr()[0].get_varchar(varchar));
  ASSERT_STREQ("1234", varchar.ptr());
  ASSERT_EQ(OB_SUCCESS, rowkey.get_obj_ptr()[1].get_precise_datetime(dt));
  ASSERT_EQ(OB_SUCCESS, rowkey.get_obj_ptr()[2].get_bool(bool_val));
  ASSERT_EQ(true, bool_val);
  ASSERT_EQ(OB_SUCCESS, rowkey.get_obj_ptr()[3].get_varchar(varchar));
  ASSERT_STREQ("true", varchar.ptr());
  ASSERT_EQ(8, used_len);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
