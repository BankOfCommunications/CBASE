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

#define  CREATE_TABLE  "create table if not exists t1 (c1 int primary key, c2 int, c3 int, c4 int, c5 int)"
#define  SELECT_SQL  "select * from t1"
#define  DROP_TABLE  "drop table if exists t1"

TEST_F(ObWithoutInit, 01_mysql_query)
{
  int ret;
  my_ulonglong my_long_ret;
  unsigned int u_ret;

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

  my_long_ret = mysql_num_rows(res);
  EXPECT_TRUE( my_long_ret == 5 );

  my_long_ret = mysql_num_fields(res);
  EXPECT_TRUE( my_long_ret == 5 );

  u_ret = mysql_field_count(&my_);
  EXPECT_TRUE( u_ret == 5 );

  MYSQL_ROW row;
  row = mysql_fetch_row(res);
  i = 1;
  while( row != NULL )
  {
    printf("ROW %d: %s\n", i, row[0]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );

  MYSQL_FIELD * field;
  field = mysql_fetch_field(res);
  i = 1;
  while( field != NULL)
  {
    printf("FIELD %d: %s\n", i, field->name);
    ++i;
    field = mysql_fetch_field(res);
  }
  EXPECT_TRUE( i == 6 );

  my_ulonglong  offset = 3;
  mysql_data_seek(res, offset);

  row = mysql_fetch_row(res);
  i = 4;
  while( row != NULL )
  {
    printf("ROW %d: %s\n", i, row[0]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );

  unsigned int fieldnr = 3;
  field = mysql_fetch_field_direct(res,fieldnr);
  printf("FIELD %d: %s\n", 4, field->name);
  char*s_ret = "c4";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );

  mysql_data_seek(res, 0);
  mysql_row_seek(res, mysql_row_tell(res));
  row = mysql_fetch_row(res);
  printf("ROW %d: %s\n", 1, row[0]);
  s_ret = "0";
  EXPECT_TRUE( strcmp(s_ret, row[0]) == 0 );

  mysql_field_seek(res, 0);
  field = mysql_fetch_field(res);
  printf("FIELD %d: %s\n", 1, field->name);
  s_ret = "c1";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );


  unsigned long * lengths;
  lengths = mysql_fetch_lengths(res);
  for( i =0 ; i<5; i++)
  {
    printf("Column %u is %lu bytes in length.\n", i, lengths[i]);
    EXPECT_TRUE( 1 == lengths[i] );
  }

  printf("SQL STAT: %s\n",  mysql_stat(&my_));

  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();

}

TEST_F(ObWithoutInit, 02_mysql_real_query)
{
  int ret;
  my_ulonglong my_long_ret;
  unsigned int u_ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

  printf("query: %s\n", DROP_TABLE);
  ret = mysql_real_query(&my_, DROP_TABLE, strlen(DROP_TABLE));
  EXPECT_TRUE( ret == 0 );

  printf("query: %s\n", CREATE_TABLE);
  ret = mysql_real_query(&my_, CREATE_TABLE, strlen(CREATE_TABLE));
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

      ret = mysql_real_query(&my_, sql, strlen(sql));
      EXPECT_TRUE( ret == 0 );
      my_long_ret = mysql_affected_rows(&my_);
      EXPECT_TRUE( my_long_ret == 1 );
  }

  ret = mysql_real_query(&my_, SELECT_SQL, strlen(SELECT_SQL));
  EXPECT_TRUE( ret == 0 );

  MYSQL_RES* res;
  res = mysql_store_result(&my_);

  my_long_ret = mysql_num_rows(res);
  EXPECT_TRUE( my_long_ret == 5 );

  my_long_ret = mysql_num_fields(res);
  EXPECT_TRUE( my_long_ret == 5 );

  u_ret = mysql_field_count(&my_);
  EXPECT_TRUE( u_ret == 5 );

  MYSQL_ROW row;
  row = mysql_fetch_row(res);
  i = 1;
  while( row != NULL )
  {
    printf("ROW %d: %s\n", i, row[0]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );

  MYSQL_FIELD * field;
  field = mysql_fetch_field(res);
  i = 1;
  while( field != NULL)
  {
    printf("FIELD %d: %s\n", i, field->name);
    ++i;
    field = mysql_fetch_field(res);
  }
  EXPECT_TRUE( i == 6 );

  my_ulonglong  offset = 3;
  mysql_data_seek(res, offset);

  row = mysql_fetch_row(res);
  i = 4;
  while( row != NULL )
  {
    printf("ROW %d: %s\n", i, row[0]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );

  unsigned int fieldnr = 3;
  field = mysql_fetch_field_direct(res,fieldnr);
  printf("FIELD %d: %s\n", 4, field->name);
  char*s_ret = "c4";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );

  mysql_data_seek(res, 0);
  mysql_row_seek(res, mysql_row_tell(res));
  row = mysql_fetch_row(res);
  printf("ROW %d: %s\n", 1, row[0]);
  s_ret = "0";
  EXPECT_TRUE( strcmp(s_ret, row[0]) == 0 );



  mysql_field_seek(res, 0);
  field = mysql_fetch_field(res);
  printf("FIELD %d: %s\n", 1, field->name);
  s_ret = "c1";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );


  //mysql_data_seek(res,2);
  unsigned long * lengths;
  lengths = mysql_fetch_lengths(res);
  for( i =0 ; i<5; i++)
  {
    printf("Column %u is %lu bytes in length.\n", i, lengths[i]);
    EXPECT_TRUE( 1 == lengths[i] );
  }

  printf("SQL STAT: %s\n",  mysql_stat(&my_));

  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();

}

TEST_F(ObWithoutInit, 03_mysql_query_use_result)
{
  int ret;
  my_ulonglong my_long_ret;
  unsigned int u_ret;

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
  res = mysql_use_result(&my_);

  my_long_ret = mysql_num_fields(res);
  EXPECT_TRUE( my_long_ret == 5 );

  u_ret = mysql_field_count(&my_);
  EXPECT_TRUE( u_ret == 5 );

  MYSQL_ROW row;
  row = mysql_fetch_row(res);
  i = 1;
  while( row != NULL )
  {
    printf("ROW %d: %s\n", i, row[0]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );

  MYSQL_FIELD * field;
  field = mysql_fetch_field(res);
  i = 1;
  while( field != NULL)
  {
    printf("FIELD %d: %s\n", i, field->name);
    ++i;
    field = mysql_fetch_field(res);
  }
  EXPECT_TRUE( i == 6 );

  unsigned int fieldnr = 3;
  field = mysql_fetch_field_direct(res,fieldnr);
  printf("FIELD %d: %s\n", 4, field->name);
  char*s_ret = "c4";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );

  mysql_field_seek(res, 0);
  field = mysql_fetch_field(res);
  printf("FIELD %d: %s\n", 1, field->name);
  s_ret = "c1";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );



  unsigned long * lengths;
  lengths = mysql_fetch_lengths(res);
  for( i =0 ; i<5; i++)
  {
    printf("Column %u is %lu bytes in length.\n", i, lengths[i]);
    EXPECT_TRUE( 2 == lengths[i] );
  }

  printf("SQL STAT: %s\n",  mysql_stat(&my_));

  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();

}

TEST_F(ObWithoutInit, 04_mysql_real_query_use_result)
{
  int ret;
  my_ulonglong my_long_ret;
  unsigned int u_ret;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));

  printf("query: %s\n", DROP_TABLE);
  ret = mysql_real_query(&my_, DROP_TABLE, strlen(DROP_TABLE));
  EXPECT_TRUE( ret == 0 );

  printf("query: %s\n", CREATE_TABLE);
  ret = mysql_real_query(&my_, CREATE_TABLE, strlen(CREATE_TABLE));
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

      ret = mysql_real_query(&my_, sql, strlen(sql));
      EXPECT_TRUE( ret == 0 );
      my_long_ret = mysql_affected_rows(&my_);
      EXPECT_TRUE( my_long_ret == 1 );
  }

  ret = mysql_real_query(&my_, SELECT_SQL, strlen(SELECT_SQL));
  EXPECT_TRUE( ret == 0 );

  MYSQL_RES* res;
  res = mysql_use_result(&my_);

  my_long_ret = mysql_num_fields(res);
  EXPECT_TRUE( my_long_ret == 5 );

  u_ret = mysql_field_count(&my_);
  EXPECT_TRUE( u_ret == 5 );

  MYSQL_ROW row;
  row = mysql_fetch_row(res);
  i = 1;
  while( row != NULL )
  {
    printf("ROW %d: %s\n", i, row[0]);
    ++i;
    row = mysql_fetch_row(res);
  }
  EXPECT_TRUE( i == 6 );

  MYSQL_FIELD * field;
  field = mysql_fetch_field(res);
  i = 1;
  while( field != NULL)
  {
    printf("FIELD %d: %s\n", i, field->name);
    ++i;
    field = mysql_fetch_field(res);
  }
  EXPECT_TRUE( i == 6 );

  unsigned int fieldnr = 3;
  field = mysql_fetch_field_direct(res,fieldnr);
  printf("FIELD %d: %s\n", 4, field->name);
  char*s_ret = "c4";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );

  mysql_field_seek(res, 0);
  field = mysql_fetch_field(res);
  printf("FIELD %d: %s\n", 1, field->name);
  s_ret = "c1";
  EXPECT_TRUE( strcmp(s_ret, field->name) == 0 );



  unsigned long * lengths;
  lengths = mysql_fetch_lengths(res);
  for( i =0 ; i<5; i++)
  {
    printf("Column %u is %lu bytes in length.\n", i, lengths[i]);
    EXPECT_TRUE( 2 == lengths[i] );
  }

  printf("SQL STAT: %s\n",  mysql_stat(&my_));

  mysql_free_result(res);
  mysql_close(&my_);
  mysql_library_end();

}


int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
