/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         test_rowkey_helper.cpp is for what ...
 *
 *  Version: $Id: test_rowkey_helper.cpp 08/09/2012 02:33:00 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "test_rowkey_helper.h"
#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/utility.h"

using namespace oceanbase;
using namespace common;

TEST(TestRowkeyHelper, basic_test)
{
  const char *sz = "hello";
  int64_t len = strlen(sz);
  ObString str(0, static_cast<int32_t>(len), (char*)sz);

  TestRowkeyHelper rh(str);

  ObObj obj;
  obj.set_varchar(str);
  ObRowkey rk(&obj, 1);
  EXPECT_EQ(0, rk.compare(rh));

  TestRowkeyHelper nk = str;
  EXPECT_EQ(0, rk.compare(nk));

  TestRowkeyHelper ni;
  ni = rk;
  EXPECT_EQ(0, nk.compare(nk));

  ObRowkey rk1 = ni;
  EXPECT_EQ(0, rk1.compare(rk));

  // segment fault, cause temp object TestRowkeyHelper(rk) will
  // be destruct after assignment.
  //ObString str1 = TestRowkeyHelper(rk);
  //EXPECT_EQ(0, str1.compare(str));

  
}

TEST(TestRowkeyHelper, test_with_common_allocator)
{
  const char *sz = "hello";
  int64_t len = strlen(sz);
  ObString str(0, static_cast<int32_t>(len), sz);

  CharArena arena;

  TestRowkeyHelper rh(&arena);
  rh = str;

  ObObj obj;
  obj.set_varchar(str);
  ObRowkey rk(&obj, 1);
  EXPECT_EQ(0, rk.compare(rh));

  TestRowkeyHelper nk(&arena); 
  nk = rh;
  EXPECT_EQ(0, rk.compare(nk));

  TestRowkeyHelper ni(&arena);
  ni = rk;
  EXPECT_EQ(0, nk.compare(nk));

  ObRowkey rk1 = ni;
  EXPECT_EQ(0, rk1.compare(rk));

  ObString str1 = TestRowkeyHelper(rk, &arena);
  EXPECT_EQ(0, str1.compare(str));

  fprintf(stderr, "size=%ld,%ld,%ld\n", arena.pages(), arena.used(), arena.total());

  
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
