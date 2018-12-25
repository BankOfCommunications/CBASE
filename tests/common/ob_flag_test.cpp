/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_flag_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include <stdint.h>
#include "common/ob_flag.h"
using namespace oceanbase::common;

TEST(ObFlagTest, int_flag)
{
  const char* section_str = "section1";
  ObFlag<int> flag1;
  ASSERT_TRUE( OB_SUCCESS != flag1.set_flag(NULL, "flag1name", -1));
  ASSERT_EQ(OB_SUCCESS, flag1.set_flag(section_str, "flag1name", -1));
  tbsys::CConfig config;
  ASSERT_EQ(0, config.load("ob_flag_test1.ini"));
  int ret = 0;
  ASSERT_TRUE(strcmp("section1", flag1.get_section_str()) == 0);
  ASSERT_TRUE(strcmp("flag1name", flag1.get_flag_str()) == 0);
  LOAD_FLAG_INT(config, flag1);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, flag1.get());

  char buf[1024];
  memset(buf, 0xff, 1024);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, flag1.serialize(buf, 1024, pos));

  ObFlag<int> flag11;
  int64_t pos2 = 0;
  flag11.deserialize(buf, pos, pos2);

  ASSERT_EQ(-1, flag11.get_default());
  ASSERT_EQ(0, flag11.get());
  ASSERT_TRUE(strcmp("section1", flag11.get_section_str()) == 0);
  ASSERT_TRUE(strcmp("flag1name", flag11.get_flag_str()) == 0);
  
  ObFlag<int> flag2;
  ASSERT_EQ(OB_SUCCESS, flag2.set_flag(section_str, "flag2name", -1));
  LOAD_FLAG_INT(config, flag2);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-100, flag2.get());

  ObFlag<int> flag3;
  ASSERT_EQ(OB_SUCCESS, flag3.set_flag(section_str, "flag3name", -1));
  LOAD_FLAG_INT(config, flag3);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(100, flag3.get());

  ObFlag<int> flag4;
  ASSERT_EQ(OB_SUCCESS, flag4.set_flag(section_str, "flag4max", -1));
  LOAD_FLAG_INT(config, flag4);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(2147483647, flag4.get());

  ObFlag<int> flag5;
  ASSERT_EQ(OB_SUCCESS, flag5.set_flag(section_str, "flag5min", -1));
  LOAD_FLAG_INT(config, flag5);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-2147483648, flag5.get());

  ObFlag<int> flag6;
  ASSERT_EQ(OB_SUCCESS, flag6.set_flag(section_str, "flag6inval", -1024));
  LOAD_FLAG_INT(config, flag6);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-1024, flag6.get());

  ObFlag<int> flag7;
  ASSERT_EQ(OB_SUCCESS, flag7.set_flag(section_str, "flag7inval", -10240));
  LOAD_FLAG_INT(config, flag7);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-10240, flag7.get());

  ObFlag<int> flag8;
  ASSERT_EQ(OB_SUCCESS, flag8.set_flag(section_str, "flag6notexist", -102400));
  LOAD_FLAG_INT(config, flag8);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-102400, flag8.get());
}

TEST(ObFlagTest, int64_flag)
{
  tbsys::CConfig config;
  ASSERT_EQ(0, config.load("ob_flag_test1.ini"));
  int ret = 0;

  const char* section_str = "section2";
  ObFlag<int64_t> flag1;
  ASSERT_EQ(OB_SUCCESS, flag1.set_flag(section_str, "flag1name", -1));
  LOAD_FLAG_INT64(config, flag1);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, flag1.get());

  char buf[1024];
  memset(buf, 0xff, 1024);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, flag1.serialize(buf, 1024, pos));

  ObFlag<int64_t> flag11;
  int64_t pos2 = 0;
  flag11.deserialize(buf, pos, pos2);

  ASSERT_EQ(-1, flag11.get_default());
  ASSERT_EQ(0, flag11.get());
  ASSERT_TRUE(strcmp("section2", flag11.get_section_str()) == 0);
  ASSERT_TRUE(strcmp("flag1name", flag11.get_flag_str()) == 0);

  ObFlag<int64_t> flag2;
  ASSERT_EQ(OB_SUCCESS, flag2.set_flag(section_str, "flag2biggerintmax", -1));
  LOAD_FLAG_INT64(config, flag2);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(2147483648L, flag2.get());

  ObFlag<int64_t> flag3;
  ASSERT_EQ(OB_SUCCESS, flag3.set_flag(section_str, "flag3lessintmin", -1));
  LOAD_FLAG_INT64(config, flag3);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-2147483649L, flag3.get());
  
  ObFlag<int64_t> flag4;
  ASSERT_EQ(OB_SUCCESS, flag4.set_flag(section_str, "flag4min", -1));
  LOAD_FLAG_INT64(config, flag4);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(INT64_MIN, flag4.get());
  
  ObFlag<int64_t> flag5;
  ASSERT_EQ(OB_SUCCESS, flag5.set_flag(section_str, "flag5max", -1));
  LOAD_FLAG_INT64(config, flag5);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(INT64_MAX, flag5.get());
  
  ObFlag<int64_t> flag6;
  ASSERT_EQ(OB_SUCCESS, flag6.set_flag(section_str, "flag6inval", -1024));
  LOAD_FLAG_INT64(config, flag6);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(-1024, flag6.get());
  
  ObFlag<int64_t> flag7;
  ASSERT_EQ(OB_SUCCESS, flag7.set_flag(section_str, "flag7inval", -10240));
  ret = 0;
  LOAD_FLAG_INT64(config, flag7);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-10240, flag7.get());
  
  ObFlag<int64_t> flag8;
  ASSERT_EQ(OB_SUCCESS, flag8.set_flag(section_str, "flag8notexist", -102400));
  LOAD_FLAG_INT64(config, flag8);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(-102400, flag8.get());
}

TEST(ObFlagTest, string_flag)
{
  tbsys::CConfig config;
  ASSERT_EQ(0, config.load("ob_flag_test1.ini"));
  int ret = 0;

  const char* section_str = "section3";

  ObFlag<const char*> flag1;
  ASSERT_EQ(OB_SUCCESS, flag1.set_flag(section_str, "flag1name", ""));
  LOAD_FLAG_STRING(config, flag1);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, strcmp(flag1.get(), "hello world"));
  
  char buf[1024];
  memset(buf, 0xff, 1024);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, flag1.serialize(buf, 1024, pos));

  ObFlag<const char*> flag11;
  int64_t pos2 = 0;
  flag11.deserialize(buf, pos, pos2);

  ASSERT_TRUE( 0 == strlen(flag11.get_default()));
  ASSERT_EQ(0, strcmp(flag11.get(), "hello world"));
  ASSERT_TRUE(strcmp("section3", flag11.get_section_str()) == 0);
  ASSERT_TRUE(strcmp("flag1name", flag11.get_flag_str()) == 0);


  ObFlag<const char*> flag2;
  ASSERT_EQ(OB_SUCCESS, flag2.set_flag(section_str, "flag2name", ""));
  LOAD_FLAG_STRING(config, flag2);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, strcmp(flag2.get(), "A"));

  ObFlag<const char*> flag3;
  ASSERT_EQ(OB_SUCCESS, flag3.set_flag(section_str, "flag3empty", ""));
  LOAD_FLAG_STRING(config, flag3);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, strcmp(flag3.get(), ""));

  ObFlag<const char*> flag4;
  ASSERT_EQ(OB_SUCCESS, flag4.set_flag(section_str, "flag4long", ""));
  LOAD_FLAG_STRING(config, flag4);
  ASSERT_EQ(0, ret);
  ASSERT_EQ((uint64_t)OB_MAX_FILE_NAME_LENGTH-1, strlen(flag4.get()));

  ObFlag<const char*> flag5;
  ASSERT_EQ(OB_SUCCESS, flag5.set_flag(section_str, "flag5toolong", "toolong"));
  LOAD_FLAG_STRING(config, flag5);
  ASSERT_TRUE(OB_SUCCESS != ret);
  ASSERT_EQ(0, strcmp(flag5.get(), "toolong"));

  ObFlag<const char*> flag6;
  ASSERT_EQ(OB_SUCCESS, flag6.set_flag(section_str, "flag6notexist", "default"));
  ret = 0;
  LOAD_FLAG_STRING(config, flag6);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, strcmp(flag6.get(), "default"));

  ObFlag<const char*> flag7;
  ASSERT_EQ(OB_SUCCESS, flag7.set_flag(section_str, "flag7notexist", ""));
  ASSERT_TRUE( 0 == strlen(flag7.get_default()));
  ret = 0;
  LOAD_FLAG_STRING(config, flag7);
  ASSERT_EQ(0, strcmp(flag7.get(), ""));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
