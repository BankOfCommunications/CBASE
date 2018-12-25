/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_test.cpp 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */


#include <gtest/gtest.h>
#include "common/ob_define.h"
#include "sql/ob_sql_get_param.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObSqlGetParamTest: public ::testing::Test
{
  public:
    ObSqlGetParamTest();
    virtual ~ObSqlGetParamTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSqlGetParamTest(const ObSqlGetParamTest &other);
    ObSqlGetParamTest& operator=(const ObSqlGetParamTest &other);
  protected:
    // data members
};

ObSqlGetParamTest::ObSqlGetParamTest()
{
}

ObSqlGetParamTest::~ObSqlGetParamTest()
{
}

void ObSqlGetParamTest::SetUp()
{
}

void ObSqlGetParamTest::TearDown()
{
}


TEST_F(ObSqlGetParamTest, serialize_test)
{
  ObSqlGetParam get;
  ObSqlGetParam get2;
  char buf[1024];
  int64_t buf_len = 1024;
  int64_t pos = 0;
  int64_t pos2 = 0;

  uint64_t tid = 123;
  ASSERT_EQ(OB_SUCCESS, get.set_table_id(tid, tid));
  ASSERT_EQ(123UL, get.get_table_id());


  ObRowkey rowkey[3];
  ObObj obj[3][3];

  for (int i = 0; i < 3; i++)
  {
    for (int j = 0; j < 3; j++)
    {
      obj[i][j].set_int(i * 3 + j);
    }
    rowkey[i].assign(obj[i], 3);
    ASSERT_EQ(OB_SUCCESS, get.add_rowkey(rowkey[i]));
  }

  ASSERT_EQ(OB_SUCCESS, get.serialize(buf, buf_len, pos));
  ASSERT_EQ(OB_SUCCESS, get2.deserialize(buf, buf_len, pos2));
  ASSERT_EQ(pos, pos2);
  
  for (int i = 0; i < 3; i++)
  {
    TBSYS_LOG(INFO, "%s vs %s", to_cstring(*get[i]), to_cstring(*get2[i]));
    ASSERT_TRUE((*get[i])== (*get2[i]));
  }

  ASSERT_EQ(123UL, get2.get_table_id());
}

TEST_F(ObSqlGetParamTest, capacity_test)
{

}

TEST_F(ObSqlGetParamTest, interface_test)
{
  ObSqlGetParam get;

  // test table id
  uint64_t tid = 123;
  ASSERT_EQ(OB_SUCCESS, get.set_table_id(tid, tid));
  ASSERT_EQ(123UL, get.get_table_id());


  // test add rowkey (not deep copy)
  ObRowkey rowkey[3];
  ObObj obj[3][3];
  for (int i = 0; i < 3; i++)
  {
    for (int j = 0; j < 3; j++)
    {
      obj[i][j].set_int(i * 3 + j);
    }
    rowkey[i].assign(obj[i], 3);
    ASSERT_EQ(OB_SUCCESS, get.add_rowkey(rowkey[i]));
  }

  // test read data
  for (int i = 0; i < 3; i++)
  {
    for (int j = 0; j < 3; j++)
    {
      int64_t val = 0;
      ASSERT_EQ(OB_SUCCESS, get[i]->ptr()[j].get_int(val));
      ASSERT_TRUE(i * 3 + j == val);
    }
  }

  // test add rowkey (deep copy)
  for (int i = 0; i < 3; i++)
  {
    ASSERT_EQ(OB_SUCCESS, get.add_rowkey(rowkey[i], true));
  }

  // test read data (mix)
  for (int i = 0; i < 6; i++)
  {
    for (int j = 0; j < 3; j++)
    {
      int64_t val = 0;
      ASSERT_EQ(OB_SUCCESS, get[i]->ptr()[j].get_int(val));
      ASSERT_TRUE((i % 3) * 3 + j == val);
    }
  }

}


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

