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

TEST_F(ObWithoutInit, 01_get_client_info)
{
  const char*info = NULL;
  info = mysql_get_client_info();
  EXPECT_TRUE( info != NULL );
  printf("client info : %s\n", info);
}

TEST_F(ObWithoutInit, 02_get_client_version)
{
  unsigned long version = 0;
  version = mysql_get_client_version();
  EXPECT_TRUE( version != 0);
  printf("client version : %ld\n", version);
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
