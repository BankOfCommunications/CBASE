/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   yiming.czw <yiming.czw@taobao.com>
*      - initial release
*
*/
#include "sql_ob_init.h"

TEST_F(ObWithoutInit, 01_init)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  mysql_close(&my_);

  mysql_library_end();
}

TEST_F(ObWithoutInit, 02_init_lib_with_data)
{
  int ret;

  char*a = "a";
  char*b = "b";
  char*c = "c";

  char**argv = new char*[3]{a, b, c};
  char**groups = new char*[3]{a, b, c};

  ret = mysql_library_init(-1, argv, groups);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  mysql_close(&my_);


  mysql_library_end();
}

TEST_F(ObWithoutInit, 03_init_double)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_init(&my_));

  mysql_close(&my_);

  mysql_library_end();
}

TEST_F(ObWithoutInit, 04_close_double)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  mysql_close(&my_);
  mysql_close(&my_);

  mysql_library_end();
}

TEST_F(ObWithoutInit, 05_init_without_lib)
{
  int ret;

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  mysql_close(&my_);
}

TEST_F(ObWithoutInit, 06_init_null)
{
  int ret;

  EXPECT_TRUE(NULL != mysql_init(NULL));
}

TEST_F(ObWithoutInit, 07_without_init_close_null)
{
  int ret;
  mysql_close(NULL);
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
