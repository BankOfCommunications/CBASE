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

TEST_F(ObWithoutInit, 01_get_host_info)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

  const char*host_info = NULL;
  host_info = mysql_get_host_info(&my_);
  printf("host info : %s\n",host_info);
  EXPECT_TRUE( host_info != NULL);

  mysql_close(&my_);
  mysql_library_end();
}

TEST_F(ObWithoutInit, 02_get_server_version)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

  unsigned long server_version = 0;
  server_version = mysql_get_server_version(&my_);
  printf("server version : %ld\n", server_version);
  EXPECT_TRUE( server_version != 0);

  mysql_close(&my_);
  mysql_library_end();
}

TEST_F(ObWithoutInit, 03_get_proto_info)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

  unsigned int proto_info = 0;
  proto_info = mysql_get_proto_info(&my_);
  printf("proto info : %d\n", proto_info);
  EXPECT_TRUE( proto_info != 0);

  mysql_close(&my_);
  mysql_library_end();
}



int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
