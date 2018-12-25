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
#define  INSERT_PRE  "insert into t1 values (?, ?, ?, ?, ?)"

TEST_F(ObWithoutInit, 01_mysql_trx)
{
  int ret;
  my_ulonglong my_long_ret;
  my_bool bret;

  MYSQL_RES* res;
  MYSQL_ROW row;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

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

  bret = mysql_autocommit(&my_, 0);
  EXPECT_EQ(0, bret);

  for( i=0; i<5; i++ )
  {
      memset(sql, '\0', len);
      sprintf(sql, "insert into t1 values ( %d, %d, %d, %d ,%d )", i*n, i*n+1, i*n+2, i*n+3, i*n+4);
      printf("query: %s\n", sql);

      ret = mysql_query(&my_, sql);
      EXPECT_TRUE( ret == 0 );
      my_long_ret = mysql_affected_rows(&my_);
      EXPECT_EQ(1, my_long_ret);

      ret = mysql_query(&my_, SELECT_SQL);
      EXPECT_TRUE( ret == 0 );
      res = mysql_store_result(&my_);
      row = mysql_fetch_row(res);
      EXPECT_EQ(NULL, row);
  }

  bret = mysql_commit(&my_);
  EXPECT_EQ(0, bret);

  my_long_ret = mysql_affected_rows(&my_);
  EXPECT_EQ(0, my_long_ret);

  bret = mysql_autocommit(&my_, 1);
  EXPECT_EQ(0, bret);

  ret = mysql_query(&my_, SELECT_SQL);
  EXPECT_TRUE( ret == 0 );

  res = mysql_store_result(&my_);

  row = mysql_fetch_row(res);
  i = 1;
  while( row != NULL )
  {
    printf("ROW %d: %2s %2s %2s %2s %2s\n", i, row[0], row[1], row[2], row[3], row[4]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );


  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();
}

TEST_F(ObWithoutInit, 02_mysql_trx_rollback)
{
  int ret;
  my_ulonglong my_long_ret;
  my_bool bret;

  MYSQL_RES* res;
  MYSQL_ROW row;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

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

  bret = mysql_autocommit(&my_, 0);
  EXPECT_EQ(0, bret);

  for( i=0; i<5; i++ )
  {
      memset(sql, '\0', len);
      sprintf(sql, "insert into t1 values ( %d, %d, %d, %d ,%d )", i*n, i*n+1, i*n+2, i*n+3, i*n+4);
      printf("query: %s\n", sql);

      ret = mysql_query(&my_, sql);
      EXPECT_TRUE( ret == 0 );
      my_long_ret = mysql_affected_rows(&my_);
      EXPECT_EQ(1, my_long_ret);

      ret = mysql_query(&my_, SELECT_SQL);
      EXPECT_TRUE( ret == 0 );
      res = mysql_store_result(&my_);
      row = mysql_fetch_row(res);
      EXPECT_EQ(NULL, row);
  }

  bret = mysql_rollback(&my_);
  EXPECT_EQ(0, bret);

  my_long_ret = mysql_affected_rows(&my_);
  EXPECT_EQ(0, my_long_ret);

  bret = mysql_autocommit(&my_, 1);
  EXPECT_EQ(0, bret);

  ret = mysql_query(&my_, SELECT_SQL);
  EXPECT_TRUE( ret == 0 );

  res = mysql_store_result(&my_);

  row = mysql_fetch_row(res);
  EXPECT_EQ(NULL,row);

  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();
}

TEST_F(ObWithoutInit, 03_mysql_query_ps)
{
  int ret;
  my_bool bret;
  my_ulonglong lret;

  MYSQL_STMT *stmt;
  MYSQL_BIND bind_param[5];
  MYSQL_RES  *res;
  MYSQL_ROW row;


  long c1;
  long c2;
  long c3;
  long c4;
  long c5;

  int i;

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

  stmt = mysql_stmt_init(&my_);
  EXPECT_TRUE( NULL != stmt );

  ret = mysql_stmt_prepare(stmt,INSERT_PRE, strlen(INSERT_PRE));
  EXPECT_EQ(0, ret);

  memset(bind_param, 0, sizeof(bind_param));

  bind_param[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_param[0].buffer = (char *) &c1;
  bind_param[0].is_null = 0;
  bind_param[0].length = 0;

  bind_param[1].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_param[1].buffer = (char *) &c2;
  bind_param[1].is_null = 0;
  bind_param[1].length = 0;

  bind_param[2].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_param[2].buffer = (char *) &c3;
  bind_param[2].is_null = 0;
  bind_param[2].length = 0;

  bind_param[3].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_param[3].buffer = (char *) &c4;
  bind_param[3].is_null = 0;
  bind_param[3].length = 0;

  bind_param[4].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_param[4].buffer = (char *) &c5;
  bind_param[4].is_null = 0;
  bind_param[4].length = 0;

  bret = mysql_stmt_bind_param(stmt, bind_param);
  EXPECT_TRUE( bret == 0 );

  printf("insert data start! \n");

  bret = mysql_autocommit(&my_, 0);
  EXPECT_EQ(0, bret);

  for(i=0; i<5; i++)
  {
    c1 = 5*i;
    c2 = 5*i + 1;
    c3 = 5*i + 2;
    c4 = 5*i + 3;
    c5 = 5*i + 4;

    printf("%d %d %d %d %d\n",c1,c2, c3, c4,c5);
    ret = mysql_stmt_execute(stmt);
    EXPECT_EQ(0, ret);

    lret = mysql_stmt_affected_rows(stmt);
    EXPECT_EQ(1, lret);

    ret = mysql_query(&my_, SELECT_SQL);
    EXPECT_TRUE( ret == 0 );
    res = mysql_store_result(&my_);
    row = mysql_fetch_row(res);
    EXPECT_EQ(NULL, row);
  }

  bret = mysql_commit(&my_);
  EXPECT_EQ(0, bret);

  lret = mysql_affected_rows(&my_);
  EXPECT_EQ(0, lret);

  bret = mysql_autocommit(&my_, 1);
  EXPECT_EQ(0, bret);

  printf("insert data end! \n");

  /*check data*/


  ret = mysql_query(&my_, SELECT_SQL);
  EXPECT_TRUE( ret == 0 );

  res = mysql_store_result(&my_);

  row = mysql_fetch_row(res);
  i = 1;
  while( row != NULL )
  {
    printf("ROW %d: %2s %2s %2s %2s %2s\n", i, row[0], row[1], row[2], row[3], row[4]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );

  mysql_free_result(res);
  mysql_stmt_close(stmt);
  mysql_close(&my_);
  mysql_library_end();
}


int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
