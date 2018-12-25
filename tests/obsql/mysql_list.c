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

TEST_F(ObWithoutInit, 01_mysql_list_tables)
{
  int ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));

  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

  MYSQL_RES* res;
  res = mysql_list_tables(&my_, NULL);

  unsigned int num_fields = mysql_num_fields(res);
  printf("field is %d\n", num_fields);
  //EXPECT_TRUE( num_fields != 0 );

  unsigned long *lengths = NULL;
  lengths = mysql_fetch_lengths(res);

  MYSQL_ROW row;
  //EXPECT_TRUE( NULL != row );
  int n = (int)mysql_num_rows(res);
  printf("row is %d\n", n);

  row = mysql_fetch_row(res);
  while( row != NULL )
  {
    printf("%s\n", row[0]);
    row = mysql_fetch_row(res);
  }

  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
