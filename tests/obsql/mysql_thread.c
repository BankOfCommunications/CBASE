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
#include <string.h>
#include <pthread.h>

#define  CREATE_TABLE  "create table if not exists t1 (c1 int primary key, c2 int, c3 int, c4 int, c5 int)"
#define  SELECT_SQL  "select * from t1"
#define  DROP_TABLE  "drop table if exists t1"

void * run (void * s)
{
  printf("start thread %d:\n", *(int *)s);
  int ret;
  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));
  ret = mysql_query(&my_, SELECT_SQL);
  EXPECT_EQ(ret, 0);
  mysql_close(&my_);
  mysql_thread_end();
  printf("end thread %d:\n", *(int *)s);
}

//void * run (void * s)
//{
//  int ret;
//
//  MYSQL* mysql = (MYSQL*)s;
//  mysql_thread_init();
//  //EXPECT_TRUE(NULL != mysql_real_connect(mysql, HOST, "admin", "admin", "test", PORT, NULL, 0));
//  EXPECT_TRUE(NULL != mysql_real_connect(mysql, "10.235.162.9", "fangji", "test", "test", 3306, NULL, 0));
//
//  ret = mysql_query(mysql, SELECT_SQL);
//  EXPECT_EQ(ret, 0);
//
//  mysql_close(mysql);
//  mysql_thread_end();
//}


TEST_F(ObWithoutInit, 01_mysql_thread)
{
  int ret;
  my_ulonglong my_long_ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));
  //EXPECT_TRUE(NULL != mysql_real_connect(&my_, "10.235.162.9", "fangji", "test", "test", 3306, NULL, 0));

  printf("query: %s\n", DROP_TABLE);
  ret = mysql_query(&my_, DROP_TABLE);
  EXPECT_TRUE( ret == 0 );

  printf("query: %s\n", CREATE_TABLE);
  ret = mysql_query(&my_, CREATE_TABLE);
  EXPECT_TRUE( ret == 0 );


  int len = 256;
  char sql[len];
  int i = 0;
  int n = 5;
  for( i=0; i<5; i++ )
  {
      memset(sql, '\0', len);
      sprintf(sql, "insert into t1 values ( %d, %d, %d, %d ,%d )", i*n, i*n+1, i*n+2, i*n+3, i*n+4);
      printf("query: %s\n", sql);

      ret = mysql_query(&my_, sql);
      EXPECT_TRUE( ret == 0 );
      my_long_ret = mysql_affected_rows(&my_);
      EXPECT_TRUE( my_long_ret == 1 );
  }


  ret = mysql_query(&my_, SELECT_SQL);
  EXPECT_TRUE( ret == 0 );

  MYSQL_RES* res;
  res = mysql_store_result(&my_);

  MYSQL_ROW row;
  row = mysql_fetch_row(res);
  i = 1;
  while( row != NULL )
  {
    printf("ROW %d: %2s %2s %2s %2s %2s\n", i, row[0], row[1], row[2], row[3], row[4]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );


  pthread_t id;
  int num[5];
  for( i=0; i<5; i++)
  {
    num[i]=i;
    pthread_create(&id, NULL, run, (void *)&(num[i]));
    //pthread_create(&id, NULL, run, (void *)&my_);
  }

  pthread_join(id, NULL);
  sleep(1);

  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
