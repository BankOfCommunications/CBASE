/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_ob_string_buf.cpp,v 0.1 2010/08/20 14:24:46 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_string_buf.h"
#include "ob_string.h"
#include "ob_cache.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

//using namespace oceanbase::chunkserver;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tests
{
namespace chunkserver
{

class TestObStringBuf : public ::testing::Test
{
public:
  virtual void SetUp()
  {

  }

  virtual void TearDown()
  {

  }
};

TEST_F(TestObStringBuf, lifecycle)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf;
  const int64_t NUM = 10;
  char str_buf[NUM][50];
  char compare_buf[NUM][50];
  ObString str[NUM];
  ObString stored_str[NUM];

  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    sprintf(str_buf[i], "row_key_%ld", i);
    str[i].assign(str_buf[i], static_cast<int32_t>(strlen(str_buf[i])));
    memcpy(compare_buf[i], str_buf[i], strlen(str_buf[i]) + 1);
  }
  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_string(str[i], &stored_str[i]);
    EXPECT_EQ(0, err);
  }

  // clear ori str buf
  memset(str_buf, 0x00, sizeof(str_buf));

  // check result
  for (int64_t i = 0; i < NUM; ++i)
  {
    EXPECT_TRUE((int64_t) strlen(compare_buf[i]) == (int64_t) stored_str[i].length());
    EXPECT_NE(compare_buf[i], stored_str[i].ptr());
    EXPECT_NE(str_buf[i], stored_str[i].ptr());
    EXPECT_EQ(0, memcmp(compare_buf[i], stored_str[i].ptr(), stored_str[i].length()));
  }
}

TEST_F(TestObStringBuf, test_write_str_obj)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf;
  const int64_t NUM = 10;
  char str_buf[NUM][50];
  char compare_buf[NUM][50];
  ObString str[NUM];
  ObObj obj[NUM];
  ObObj stored_obj[NUM];

  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    sprintf(str_buf[i], "row_key_%ld", i);
    str[i].assign(str_buf[i], static_cast<int32_t>(strlen(str_buf[i])));
    memcpy(compare_buf[i], str_buf[i], strlen(str_buf[i]) + 1);
    obj[i].set_varchar(str[i]);
  }

  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_obj(obj[i], &stored_obj[i]);
    EXPECT_EQ(0, err);
  }

  // clear ori str buf
  memset(str_buf, 0x00, sizeof(str_buf));

  // check result
  for (int64_t i = 0; i < NUM; ++i)
  {
    ObString tmp;
    err = stored_obj[i].get_varchar(tmp);
    EXPECT_EQ(0, err);
    EXPECT_TRUE((int64_t) strlen(compare_buf[i]) == (int64_t) tmp.length());
    EXPECT_NE(compare_buf[i], tmp.ptr());
    EXPECT_NE(str_buf[i], tmp.ptr());
    EXPECT_EQ(0, memcmp(compare_buf[i], tmp.ptr(), tmp.length()));
  }
}

TEST_F(TestObStringBuf, test_write_int_obj)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf;
  const int64_t NUM = 10;
  ObObj obj[NUM];
  ObObj stored_obj[NUM];

  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    obj[i].set_int(i + 1000);
  }

  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_obj(obj[i], &stored_obj[i]);
    EXPECT_EQ(0, err);
  }

  // check result
  int64_t val = 0;
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = stored_obj[i].get_int(val);
    EXPECT_EQ(0, err);
    EXPECT_EQ(i + 1000, val);
  }
}

TEST_F(TestObStringBuf, test_write_time_second_obj)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf;
  const int64_t NUM = 10;
  ObObj obj[NUM];
  ObObj stored_obj[NUM];

  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    obj[i].set_datetime(i + 1000);
  }

  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_obj(obj[i], &stored_obj[i]);
    EXPECT_EQ(0, err);
  }

  // check result
  ObDateTime val = 0;
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = stored_obj[i].get_datetime(val);
    EXPECT_EQ(0, err);
    EXPECT_EQ(i + 1000, val);
  }
}


TEST_F(TestObStringBuf, test_write_time_microsecond_obj)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf;
  const int64_t NUM = 10;
  ObObj obj[NUM];
  ObObj stored_obj[NUM];

  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    obj[i].set_precise_datetime(i + 1000);
  }

  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_obj(obj[i], &stored_obj[i]);
    EXPECT_EQ(0, err);
  }

  // check result
  ObPreciseDateTime val = 0;
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = stored_obj[i].get_precise_datetime(val);
    EXPECT_EQ(0, err);
    EXPECT_EQ(i + 1000, val);
  }
}

TEST_F(TestObStringBuf, test_set_string_buf_size)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf(0, 1211);
  const int64_t NUM = 1;
  ObObj obj[NUM];
  ObObj stored_obj[NUM];
  const int64_t cstr_len = 500;
  char cstr[cstr_len];
  ObString str;

  strcpy(cstr, "hello world");
  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    str.assign(cstr, cstr_len);
    obj[i].set_varchar(str);
  }

  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_obj(obj[i], &stored_obj[i]);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // check result
  ObString val;
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = stored_obj[i].get_varchar(val);
    EXPECT_EQ(OB_SUCCESS, err);
    EXPECT_EQ(val, str);
  }
  ASSERT_EQ(64 * 1024L  , string_buf.total());
}




TEST_F(TestObStringBuf, test_set_string_buf_size_overflow)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf(0, 200);
  const int64_t NUM = 1;
  ObObj obj[NUM];
  ObObj stored_obj[NUM];
  const int64_t cstr_len = 64 * 1024 + 100;
  char *cstr = (char*)ob_malloc(cstr_len, ObModIds::TEST);
  ObString str;

  ASSERT_TRUE(NULL != cstr);
  strcpy(cstr, "hello world");
  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    str.assign(cstr, cstr_len);
    obj[i].set_varchar(str);
  }

  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_obj(obj[i], &stored_obj[i]);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // check result
  ObString val;
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = stored_obj[i].get_varchar(val);
    EXPECT_EQ(OB_SUCCESS, err);
    EXPECT_EQ(val, str);
  }
  // 128KB
  EXPECT_EQ(24+cstr_len+64 * 1024L, string_buf.total());
  ob_free(cstr);
}


TEST_F(TestObStringBuf, test_set_string_buf_size_use_user_define)
{
  int err = OB_SUCCESS;
  ObStringBuf string_buf(0, 100 * 1024);
  const int64_t NUM = 1;
  ObObj obj[NUM];
  ObObj stored_obj[NUM];
  const int64_t cstr_len = 64 * 1024 + 100;
  char *cstr = (char*)ob_malloc(cstr_len, ObModIds::TEST);
  ObString str;

  ASSERT_TRUE(NULL != cstr);
  strcpy(cstr, "hello world");
  // init ObString obj
  for (int64_t i = 0; i < NUM; ++i)
  {
    str.assign(cstr, cstr_len);
    obj[i].set_varchar(str);
  }

  // add string to string buf
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = string_buf.write_obj(obj[i], &stored_obj[i]);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // check result
  ObString val;
  for (int64_t i = 0; i < NUM; ++i)
  {
    err = stored_obj[i].get_varchar(val);
    EXPECT_EQ(OB_SUCCESS, err);
    EXPECT_EQ(val, str);
  }
  EXPECT_EQ(100 * 1024L, string_buf.total());
  ob_free(cstr);
}








} // end namespace chunkserver
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
	ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
