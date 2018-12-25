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

TEST_F(ObWithoutInit, 01_init_lib)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

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

  mysql_library_end();
}

TEST_F(ObWithoutInit, 03_end_lib)
{
  mysql_library_end();
}

TEST_F(ObWithoutInit, 04_end_lib_double)
{
  mysql_library_end();
  mysql_library_end();
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
