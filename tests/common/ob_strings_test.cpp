/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_strings_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_strings.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class ObStringsTest: public ::testing::Test
{
  public:
    ObStringsTest();
    virtual ~ObStringsTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObStringsTest(const ObStringsTest &other);
    ObStringsTest& operator=(const ObStringsTest &other);
  private:
    // data members
};

ObStringsTest::ObStringsTest()
{
}

ObStringsTest::~ObStringsTest()
{
}

void ObStringsTest::SetUp()
{
}

void ObStringsTest::TearDown()
{
}

TEST_F(ObStringsTest, basic_test)
{
  ObStrings strings;
  char buf[1024];
  int64_t idx = 0;
  ObString str;
  for (int i = 0; i < 100; ++i)
  {
    snprintf(buf, 1024, "hello%d", i);
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
    ASSERT_EQ(OB_SUCCESS, strings.add_string(str, &idx));
    ASSERT_EQ(i, idx);
  }
  ASSERT_EQ(100, strings.count());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, strings.serialize(buf, 1024, pos));
  strings.clear();
  ASSERT_EQ(0, strings.count());
  // deserialize and verify
  ObStrings strings2;
  int64_t size = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, strings2.deserialize(buf, size, pos));
  ObString str2;
  for (int i = 0; i < 100; ++i)
  {
    snprintf(buf, 1024, "hello%d", i);
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
    ASSERT_EQ(OB_SUCCESS, strings2.get_string(i, str2));
    ASSERT_TRUE(str == str2);
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
