/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test_ob_range.cpp is for what ...
 *
 *  Version: $Id: test_ob_range.cpp 2010年12月21日 14时53分04秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "gtest/gtest.h"
#include "common/ob_range.h"
#include "common/ob_malloc.h"

#define FUNC_BLOCK() if (true) \

using namespace oceanbase;
using namespace common;

TEST(TestObRange, trim)
{
  int ret = OB_SUCCESS;
  ObStringBuf string_buf;

  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.table_id_ = 1;
    r2.table_id_ = 2;

    ret = r1.trim(r2, string_buf);
    EXPECT_EQ(ret, OB_ERROR);
  }

  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.table_id_ = 1;
    r2.table_id_ = 1;

    r1.border_flag_.unset_min_value();
    r1.border_flag_.unset_max_value();
    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * r1_start = (char*)"abc";
    char * r1_end = (char*)"abd";

    r1.start_key_.assign_ptr(r1_start, static_cast<int32_t>(strlen(r1_start)));
    r1.end_key_.assign_ptr(r1_end, static_cast<int32_t>(strlen(r1_end)));

    r2.border_flag_.unset_min_value();
    r2.border_flag_.unset_max_value();
    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();

    char * r2_start = (char*)"abc";
    char * r2_end = (char*)"abd";

    r2.start_key_.assign_ptr(r2_start, static_cast<int32_t>(strlen(r2_start)));
    r2.end_key_.assign_ptr(r2_end, static_cast<int32_t>(strlen(r2_end)));

    ret = r1.trim(r2, string_buf);
    EXPECT_EQ(ret, OB_EMPTY_RANGE);
  }

  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.table_id_ = 1;
    r2.table_id_ = 1;

    r1.border_flag_.unset_min_value();
    r1.border_flag_.unset_max_value();
    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * r1_start = (char*)"abc";
    char * r1_end = (char*)"abd";

    r1.start_key_.assign_ptr(r1_start, static_cast<int32_t>(strlen(r1_start)));
    r1.end_key_.assign_ptr(r1_end, static_cast<int32_t>(strlen(r1_end)));

    r2.border_flag_.unset_min_value();
    r2.border_flag_.unset_max_value();
    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();

    char * r2_start = (char*)"abc";
    char * r2_end = (char*)"abf";

    r2.start_key_.assign_ptr(r2_start, static_cast<int32_t>(strlen(r2_start)));
    r2.end_key_.assign_ptr(r2_end, static_cast<int32_t>(strlen(r2_end)));

    ret = r1.trim(r2, string_buf);
    EXPECT_EQ(ret, OB_ERROR);
  }

  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.table_id_ = 1;
    r2.table_id_ = 1;

    r1.border_flag_.unset_min_value();
    r1.border_flag_.set_max_value();
    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * r1_start = (char*)"abc";
    char * r1_end = (char*)"abd";

    r1.start_key_.assign_ptr(r1_start, static_cast<int32_t>(strlen(r1_start)));
    r1.end_key_.assign_ptr(r1_end, static_cast<int32_t>(strlen(r1_end)));

    r2.border_flag_.unset_min_value();
    r2.border_flag_.unset_max_value();
    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();

    char * r2_start = (char*)"abc";
    char * r2_end = (char*)"abf";

    r2.start_key_.assign_ptr(r2_start, static_cast<int32_t>(strlen(r2_start)));
    r2.end_key_.assign_ptr(r2_end, static_cast<int32_t>(strlen(r2_end)));

    ret = r1.trim(r2, string_buf);
    EXPECT_EQ(ret, OB_SUCCESS);
    EXPECT_TRUE(r1.start_key_ == ObString(0, static_cast<int32_t>(strlen(r2_end)), r2_end));
    EXPECT_TRUE(!r1.border_flag_.inclusive_start());
  }

  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.table_id_ = 1;
    r2.table_id_ = 1;

    r1.border_flag_.set_min_value();
    r1.border_flag_.unset_max_value();
    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * r1_start = (char*)"abc";
    char * r1_end = (char*)"abf";

    r1.start_key_.assign_ptr(r1_start, static_cast<int32_t>(strlen(r1_start)));
    r1.end_key_.assign_ptr(r1_end, static_cast<int32_t>(strlen(r1_end)));

    r2.border_flag_.unset_min_value();
    r2.border_flag_.unset_max_value();
    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();

    char * r2_start = (char*)"abc";
    char * r2_end = (char*)"abf";

    r2.start_key_.assign_ptr(r2_start, static_cast<int32_t>(strlen(r2_start)));
    r2.end_key_.assign_ptr(r2_end, static_cast<int32_t>(strlen(r2_end)));

    ret = r1.trim(r2, string_buf);
    EXPECT_EQ(ret, OB_SUCCESS);
    EXPECT_TRUE(r1.end_key_ == ObString(0, static_cast<int32_t>(strlen(r2_start)), r2_start));
    EXPECT_TRUE(!r1.border_flag_.inclusive_end());
  }

  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.table_id_ = 1;
    r2.table_id_ = 1;

    r1.border_flag_.set_min_value();
    r1.border_flag_.set_max_value();
    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * r1_start = (char*)"abc";
    char * r1_end = (char*)"abf";

    r1.start_key_.assign_ptr(r1_start, static_cast<int32_t>(strlen(r1_start)));
    r1.end_key_.assign_ptr(r1_end, static_cast<int32_t>(strlen(r1_end)));

    r2.border_flag_.unset_min_value();
    r2.border_flag_.set_max_value();
    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();

    char * r2_start = (char*)"abc";
    char * r2_end = (char*)"abf";

    r2.start_key_.assign_ptr(r2_start, static_cast<int32_t>(strlen(r2_start)));
    r2.end_key_.assign_ptr(r2_end, static_cast<int32_t>(strlen(r2_end)));

    ret = r1.trim(r2, string_buf);
    EXPECT_EQ(ret, OB_SUCCESS);
    EXPECT_TRUE(r1.end_key_ == ObString(0, static_cast<int32_t>(strlen(r2_start)), r2_start));
    EXPECT_TRUE(!r1.border_flag_.inclusive_end());
  }

}

TEST(TestObRange, compare_with_startkey2)
{
  const int EQ = 0;
  const int LT = -1;
  const int GT = 1;
  
  // min value
  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.border_flag_.set_min_value();
    r2.border_flag_.set_min_value();

    EXPECT_TRUE(r1.compare_with_startkey2(r2) == EQ);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == EQ);

    r1.border_flag_.unset_min_value();
    EXPECT_TRUE(r1.compare_with_startkey2(r2) == GT);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == LT);
  }
  
  // rowkey eq
  FUNC_BLOCK()
  {
    char *key1 = (char*)"cdef";
    ObString sk1(0, 4, key1);

    ObRange r1, r2;
    r1.border_flag_.set_inclusive_start();
    r1.start_key_ = sk1;

    r2.border_flag_.set_inclusive_start();
    r2.start_key_ = sk1;

    EXPECT_TRUE(r1.compare_with_startkey2(r2) == EQ);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == EQ);

    // unset inclusive start
    r2.border_flag_.unset_inclusive_start();
    EXPECT_TRUE(r1.compare_with_startkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == GT);

    r1.border_flag_.unset_inclusive_start();
    EXPECT_TRUE(r1.compare_with_startkey2(r2) == EQ);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == EQ);
  }
  
  // row key not eq 
  FUNC_BLOCK()
  {
    char *key1 = (char*)"cdef";
    char *key2 = (char*)"ddef";
    ObString sk1(0, 4, key1);
    ObString sk2(0, 4, key2);
    
    ObRange r1, r2;
    r1.start_key_ = sk1;
    r2.start_key_ = sk2;
    
    EXPECT_TRUE(r1.compare_with_startkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == GT);
    
    r1.border_flag_.set_inclusive_start();
    EXPECT_TRUE(r1.compare_with_startkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == GT);
    
    r2.border_flag_.set_inclusive_start();
    EXPECT_TRUE(r1.compare_with_startkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_startkey2(r1) == GT);
  }
}

TEST(TestObRange, compare_with_endkey2)
{
  const int EQ = 0;
  const int LT = -1;
  const int GT = 1;
  
  // max value
  FUNC_BLOCK()
  {
    ObRange r1, r2;
    r1.border_flag_.set_max_value();
    r2.border_flag_.set_max_value();

    EXPECT_TRUE(r1.compare_with_endkey2(r2) == EQ);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == EQ);

    r1.border_flag_.unset_max_value();
    EXPECT_TRUE(r1.compare_with_endkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == GT);
  }
  
  // rowkey eq
  FUNC_BLOCK()
  {
    char *key1 = (char*)"cdef";
    ObString sk1(0, 4, key1);

    ObRange r1, r2;
    r1.border_flag_.set_inclusive_end();
    r1.end_key_ = sk1;

    r2.border_flag_.set_inclusive_end();
    r2.end_key_ = sk1;

    EXPECT_TRUE(r1.compare_with_endkey2(r2) == EQ);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == EQ);

    // unset inclusive start
    r2.border_flag_.unset_inclusive_end();
    EXPECT_TRUE(r1.compare_with_endkey2(r2) == GT);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == LT);

    r1.border_flag_.unset_inclusive_end();
    EXPECT_TRUE(r1.compare_with_endkey2(r2) == EQ);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == EQ);
  }
  
  // row key not eq 
  FUNC_BLOCK()
  {
    char *key1 = (char*)"cdef";
    char *key2 = (char*)"ddef";
    ObString sk1(0, 4, key1);
    ObString sk2(0, 4, key2);
    
    ObRange r1, r2;
    r1.end_key_ = sk1;
    r2.end_key_ = sk2;
    
    EXPECT_TRUE(r1.compare_with_endkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == GT);
    
    r1.border_flag_.set_inclusive_end();
    EXPECT_TRUE(r1.compare_with_endkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == GT);
    
    r2.border_flag_.set_inclusive_end();
    EXPECT_TRUE(r1.compare_with_endkey2(r2) == LT);
    EXPECT_TRUE(r2.compare_with_endkey2(r1) == GT);
  }
}

TEST(TestObRange, check_range)
{
  // table name
  ObRange scan_range;

  ObString key1;
  scan_range.table_id_ = 23455;
  char * start_key = (char*)"start_row_key_start";
  char * end_key = (char*)"tend_row_key_end";
  key1.assign(start_key, static_cast<int32_t>(strlen(start_key)));
  scan_range.start_key_ = key1;
  
  ObString key2;
  key2.assign(end_key, static_cast<int32_t>(strlen(end_key)));
  scan_range.end_key_ = key2;
  EXPECT_TRUE(scan_range.check() == true);
  
  scan_range.start_key_ = key2;
  scan_range.end_key_ = key1;
  EXPECT_TRUE(scan_range.check() == false);

  ObString key3;
  scan_range.start_key_ = key3;
  scan_range.end_key_ = key3;
  scan_range.border_flag_.set_min_value();
  scan_range.border_flag_.set_max_value();
  EXPECT_TRUE(scan_range.check() == true);
  
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.unset_inclusive_end();
  EXPECT_TRUE(scan_range.check() == true);
  
  scan_range.start_key_ = key2;
  scan_range.border_flag_.set_max_value();
  EXPECT_TRUE(scan_range.check() == true);

  scan_range.border_flag_.unset_min_value();
  scan_range.border_flag_.unset_max_value();
  
  scan_range.start_key_ = key2;
  scan_range.end_key_ = key2;
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  EXPECT_TRUE(scan_range.check() == true);

  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.unset_inclusive_end();
  EXPECT_TRUE(scan_range.check() == false);
  
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.unset_inclusive_end();
  EXPECT_TRUE(scan_range.check() == false);
  
  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  EXPECT_TRUE(scan_range.check() == false);
}


TEST(TestObRange, intersect)
{
  char *key1 = (char*)"cdef";
  char *key2 = (char*)"cdef";
  //char key1[9];
  //char key2[9];
  //memset(key1, 0xff, 9);
  //memset(key2, 0xff, 9);
  ObString sk1(0, 4, key1);
  ObString sk2(0, 4, key2);

  ObRange r1, r2;
  r1.table_id_ = 1002;
  //r1.border_flag_.set_inclusive_start();
  r1.border_flag_.set_inclusive_end();
  r1.start_key_ = sk1;
  r1.end_key_ = sk2;

  //r1.hex_dump();
   
  r2.table_id_ = 1002;
  //r2.border_flag_.set_inclusive_start();
  r2.border_flag_.set_inclusive_end();
  r2.start_key_ = sk1;
  r2.end_key_ = sk2;
  //r2.hex_dump();

  EXPECT_EQ(0, r1.intersect(r2));

}

TEST(TestObRange, to_string)
{
  //char *key1 = "cdef";
  //char *key2 = "cdef";
  char key1[9];
  char key2[9];
  memset(key1, 0xff, 9);
  memset(key2, 0xff, 9);
  ObString sk1(0, 4, key1);
  ObString sk2(0, 4, key2);

  ObRange r1;
  r1.table_id_ = 1002;
  r1.border_flag_.set_inclusive_end();
  r1.start_key_ = sk1;
  r1.end_key_ = sk2;

  char buffer[128];
  r1.to_string(buffer, 128);
  printf("r1:%s\n", buffer);

  r1.border_flag_.set_inclusive_start();
  r1.border_flag_.unset_inclusive_end();
  r1.to_string(buffer, 128);
  printf("r1:%s\n", buffer);

  r1.border_flag_.set_inclusive_start();
  r1.border_flag_.set_inclusive_end();
  r1.to_string(buffer, 128);
  printf("r1:%s\n", buffer);

  r1.border_flag_.set_min_value();
  r1.to_string(buffer, 128);
  printf("r1:%s\n", buffer);


  r1.border_flag_.unset_min_value();
  r1.border_flag_.set_max_value();
  r1.to_string(buffer, 128);
  printf("r1:%s\n", buffer);

  r1.border_flag_.set_min_value();
  r1.border_flag_.set_max_value();
  r1.to_string(buffer, 128);
  printf("r1:%s\n", buffer);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
