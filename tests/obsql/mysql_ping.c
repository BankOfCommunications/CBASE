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
*   admin <admin@taobao.com>
*      - initial release
*
*/
#include "sql_ob_init.h"

TEST_F(ObWithoutInit, 01_ping)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

  ret = mysql_ping(&my_);
  EXPECT_TRUE( ret == 0 );

  mysql_close(&my_);
  mysql_library_end();
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
