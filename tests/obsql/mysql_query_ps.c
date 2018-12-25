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

#define  CREATE_TABLE  "create table if not exists t1 (c1 int primary key, c2 float, c3 double, c4 tinyint, c5 varchar(32), c6 datetime)"
#define  SELECT_SQL  "select * from t1"
#define  DROP_TABLE  "drop table if exists t1"
#define  INSERT_PRE  "insert into t1 values (?, ?, ?, ?, ?, ?)"
#define  STR_LEN 128



TEST_F(ObWithoutInit, 01_mysql_query_ps)
{
  int ret;
  my_bool bret;
  my_ulonglong lret;
  unsigned int uret;

  MYSQL_FIELD * field;
  MYSQL_STMT *stmt;
  MYSQL_BIND bind_param[6];
  MYSQL_BIND bind_ressult[6];
  MYSQL_TIME time;
  MYSQL_RES  *res;

  unsigned long length[6];
  my_bool  is_null[6];

  long c1;
  float c2;
  double c3;
  bool c4;
  char c5[STR_LEN];
  MYSQL_TIME c6;
  unsigned long  c5_len;

  memset(&c6, 0, sizeof(c6));
  int i;

  ret = mysql_library_init(0, NULL, NULL);
  EXPECT_EQ(ret,0);

  MYSQL my_;
  EXPECT_TRUE(NULL != mysql_init(&my_));
  EXPECT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));
  // EXPECT_TRUE(NULL != mysql_real_connect(&my_, "10.235.162.9", "fangji", "test", "test", 3306, NULL, 0));
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

  ret = mysql_stmt_param_count(stmt);
  EXPECT_EQ(6, ret);

  memset(bind_param, 0, sizeof(bind_param));

  bind_param[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_param[0].buffer = (char *) &c1;
  bind_param[0].is_null = 0;
  bind_param[0].length = 0;

  bind_param[1].buffer_type = MYSQL_TYPE_FLOAT;
  bind_param[1].buffer = (char *) &c2;
  bind_param[1].is_null = 0;
  bind_param[1].length = 0;

  bind_param[2].buffer_type = MYSQL_TYPE_DOUBLE;
  bind_param[2].buffer = (char *) &c3;
  bind_param[2].is_null = 0;
  bind_param[2].length = 0;

  bind_param[3].buffer_type = MYSQL_TYPE_TINY;
  bind_param[3].buffer = (char *) &c4;
  bind_param[3].is_null = 0;
  bind_param[3].length = 0;

  bind_param[4].buffer_type = MYSQL_TYPE_VAR_STRING;
  bind_param[4].buffer = (char *) &c5;
  bind_param[4].is_null = 0;
  bind_param[4].buffer_length = STR_LEN;
  bind_param[4].length = &c5_len;

  bind_param[5].buffer_type = MYSQL_TYPE_DATETIME;
  bind_param[5].buffer = (char *) &c6;
  bind_param[5].is_null = 0;
  bind_param[5].length = 0;

  bret = mysql_stmt_bind_param(stmt, bind_param);
  EXPECT_TRUE( bret == 0 );

  printf("insert data start! \n");

  for(i=1; i<=5; i++)
  {
    c1 = i;
    c2 = i + 0.1;
    c3 = i + 0.2;

    if( i%2 == 0 )
      c4 = false;
    else
      c4 = true;

    memset(c5, '\0', STR_LEN);
    sprintf(c5, "c5-%d", i);
    c5_len = strlen(c5);

    c6.year = 2013;
    c6.month = 7;
    c6.day = 8;
    c6.hour = 16;
    c6.minute = 4;
    c6.second = i;
    ret = mysql_stmt_execute(stmt);
    EXPECT_EQ(0, ret);
    lret = mysql_stmt_affected_rows(stmt);
    EXPECT_EQ(1, lret);
  }

  printf("insert data end! \n");

  /*check data*/

  printf("select data and check it: \n");

  ret = mysql_stmt_prepare(stmt, SELECT_SQL, strlen(SELECT_SQL));
  EXPECT_EQ(0, ret);

  ret = mysql_stmt_param_count(stmt);
  EXPECT_EQ(0, ret);

  res = mysql_stmt_result_metadata(stmt);
  EXPECT_TRUE( NULL != res );

  ret = mysql_num_fields(res);
  EXPECT_EQ(6, ret);


  field = mysql_fetch_field(res);
  i = 1;
  while( field != NULL)
  {
    printf("FIELD %d: %s\n", i, field->name);
    ++i;
    field = mysql_fetch_field(res);
  }

  mysql_free_result(res);

  ret = mysql_stmt_execute(stmt);
  EXPECT_EQ(0, ret);

  memset(bind_ressult, 0, sizeof(bind_ressult));

  bind_ressult[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_ressult[0].buffer = (char *) &c1;
  bind_ressult[0].is_null = &is_null[0];
  bind_ressult[0].length = &length[0];

  bind_ressult[1].buffer_type = MYSQL_TYPE_FLOAT;
  bind_ressult[1].buffer = (char *) &c2;
  bind_ressult[1].is_null = &is_null[1];
  bind_ressult[1].length = &length[1];;

  bind_ressult[2].buffer_type = MYSQL_TYPE_DOUBLE;
  bind_ressult[2].buffer = (char *) &c3;
  bind_ressult[2].is_null = &is_null[2];
  bind_ressult[2].length = &length[2];;

  bind_ressult[3].buffer_type = MYSQL_TYPE_TINY;
  bind_ressult[3].buffer = (char *) &c4;
  bind_ressult[3].is_null = &is_null[3];
  bind_ressult[3].length = &length[3];;

  bind_ressult[4].buffer_type = MYSQL_TYPE_VAR_STRING;
  bind_ressult[4].buffer = (char *) &c5;
  bind_ressult[4].is_null = &is_null[4];
  bind_ressult[4].buffer_length = STR_LEN;
  bind_ressult[4].length = &length[4];;

  bind_ressult[5].buffer_type = MYSQL_TYPE_DATETIME;
  bind_ressult[5].buffer = (char *) &c6;
  bind_ressult[5].is_null = &is_null[5];
  bind_ressult[5].length = &length[5];;

  bret = mysql_stmt_bind_result(stmt, bind_ressult);
  EXPECT_EQ(0, bret);

  ret = mysql_stmt_store_result(stmt);
  EXPECT_EQ(0, ret);

  lret =  mysql_stmt_num_rows(stmt);
  EXPECT_EQ(5, lret);

  uret = mysql_stmt_field_count(stmt);
  EXPECT_EQ(6, uret);

  char c5_ex [STR_LEN];
  i = 1;
  while (!mysql_stmt_fetch(stmt))
  {
    printf("ROW : %d:  %ld %f %f %d %s %04d-%02d-%02d %02d:%02d:%02d\n", i, c1, c2, c3, c4, c5, c6.year, c6.month, c6.day, c6.hour, c6.minute, c6.second);

    EXPECT_EQ(i, c1);
    ASSERT_FLOAT_EQ(i+0.1, c2);
    ASSERT_DOUBLE_EQ(i+0.2, c3);
    if( 0 == i%2 )
      EXPECT_EQ(0, c4);
    else
      EXPECT_EQ(1, c4);

    memset(c5_ex, '\0', STR_LEN);
    sprintf(c5_ex, "c5-%d", i);
    EXPECT_EQ(0, strcmp(c5_ex, c5));

    EXPECT_EQ(c6.year, 2013);
    EXPECT_EQ(c6.month, 7);
    EXPECT_EQ(c6.day, 8);
    EXPECT_EQ(c6.hour, 16);
    EXPECT_EQ(c6.minute, 4);
    EXPECT_EQ(c6.second, i);

    EXPECT_EQ(8, length[0]);
    EXPECT_EQ(4, length[1]);
    EXPECT_EQ(8, length[2]);
    EXPECT_EQ(1, length[3]);
    EXPECT_EQ(strlen(c5_ex), length[4]);
    EXPECT_EQ(sizeof(time), length[5]);
    ++i;
  }

  mysql_stmt_data_seek(stmt, 2);
  ret = mysql_stmt_fetch(stmt);
  EXPECT_EQ(0, ret);
  EXPECT_EQ(3, c1);

  mysql_stmt_data_seek(stmt, 0);
  mysql_stmt_row_seek(stmt, mysql_stmt_row_tell(stmt));
  ret = mysql_stmt_fetch(stmt);
  EXPECT_EQ(0, ret);
  EXPECT_EQ(1, c1);

  ret = mysql_stmt_fetch_column(stmt, bind_ressult, 2, 0);
  EXPECT_EQ(0, ret);
  EXPECT_EQ(1.2, c3);

  ret = mysql_stmt_free_result(stmt);
  EXPECT_EQ(0, ret);

  printf("SQL State : %s\n", mysql_stmt_sqlstate(stmt));

  mysql_stmt_close(stmt);
  mysql_close(&my_);
  mysql_library_end();
}

TEST_F(ObWithoutInit, 02_mysql_query_ps_without_store)
{
  int ret;
  my_bool bret;
  my_ulonglong lret;
  unsigned int uret;

  MYSQL_FIELD * field;
  MYSQL_STMT *stmt;
  MYSQL_BIND bind_param[6];
  MYSQL_BIND bind_ressult[6];
  MYSQL_TIME time;
  MYSQL_RES  *res;

  unsigned long length[6];
  my_bool  is_null[6];

  long c1;
  float c2;
  double c3;
  bool c4;
  char c5[STR_LEN];
  MYSQL_TIME c6;
  unsigned long  c5_len;

  memset(&c6, 0, sizeof(c6));

  int i;

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

  stmt = mysql_stmt_init(&my_);
  EXPECT_TRUE( NULL != stmt );

  ret = mysql_stmt_prepare(stmt,INSERT_PRE, strlen(INSERT_PRE));
  EXPECT_EQ(0, ret);

  ret = mysql_stmt_param_count(stmt);
  EXPECT_EQ(6, ret);

  memset(bind_param, 0, sizeof(bind_param));

  bind_param[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_param[0].buffer = (char *) &c1;
  bind_param[0].is_null = 0;
  bind_param[0].length = 0;

  bind_param[1].buffer_type = MYSQL_TYPE_FLOAT;
  bind_param[1].buffer = (char *) &c2;
  bind_param[1].is_null = 0;
  bind_param[1].length = 0;

  bind_param[2].buffer_type = MYSQL_TYPE_DOUBLE;
  bind_param[2].buffer = (char *) &c3;
  bind_param[2].is_null = 0;
  bind_param[2].length = 0;

  bind_param[3].buffer_type = MYSQL_TYPE_TINY;
  bind_param[3].buffer = (char *) &c4;
  bind_param[3].is_null = 0;
  bind_param[3].length = 0;

  bind_param[4].buffer_type = MYSQL_TYPE_VAR_STRING;
  bind_param[4].buffer = (char *) &c5;
  bind_param[4].is_null = 0;
  bind_param[4].buffer_length = STR_LEN;
  bind_param[4].length = &c5_len;

  bind_param[5].buffer_type = MYSQL_TYPE_DATETIME;
  bind_param[5].buffer = (char *) &c6;
  bind_param[5].is_null = 0;
  bind_param[5].length = 0;

  bret = mysql_stmt_bind_param(stmt, bind_param);
  EXPECT_TRUE( bret == 0 );

  printf("insert data start! \n");

  for(i=1; i<=5; i++)
  {
    c1 = i;
    c2 = i + 0.1;
    c3 = i + 0.2;

    if( i%2 == 0 )
      c4 = false;
    else
      c4 = true;

    memset(c5, '\0', STR_LEN);
    sprintf(c5, "c5-%d", i);
    c5_len = strlen(c5);

    c6.year = 2013;
    c6.month = 7;
    c6.day = 8;
    c6.hour = 16;
    c6.minute = 4;
    c6.second = i;
    ret = mysql_stmt_execute(stmt);
    EXPECT_EQ(0, ret);
    lret = mysql_stmt_affected_rows(stmt);
    EXPECT_EQ(1, lret);
  }

  printf("insert data end! \n");

  /*check data*/

  printf("select data and check it: \n");

  ret = mysql_stmt_prepare(stmt, SELECT_SQL, strlen(SELECT_SQL));
  EXPECT_EQ(0, ret);

  ret = mysql_stmt_param_count(stmt);
  EXPECT_EQ(0, ret);

  res = mysql_stmt_result_metadata(stmt);
  EXPECT_TRUE( NULL != res );

  ret = mysql_num_fields(res);
  EXPECT_EQ(6, ret);


  field = mysql_fetch_field(res);
  i = 1;
  while( field != NULL)
  {
    printf("FIELD %d: %s\n", i, field->name);
    ++i;
    field = mysql_fetch_field(res);
  }

  mysql_free_result(res);

  ret = mysql_stmt_execute(stmt);
  EXPECT_EQ(0, ret);

  memset(bind_ressult, 0, sizeof(bind_ressult));

  bind_ressult[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bind_ressult[0].buffer = (char *) &c1;
  bind_ressult[0].is_null = &is_null[0];
  bind_ressult[0].length = &length[0];

  bind_ressult[1].buffer_type = MYSQL_TYPE_FLOAT;
  bind_ressult[1].buffer = (char *) &c2;
  bind_ressult[1].is_null = &is_null[1];
  bind_ressult[1].length = &length[1];;

  bind_ressult[2].buffer_type = MYSQL_TYPE_DOUBLE;
  bind_ressult[2].buffer = (char *) &c3;
  bind_ressult[2].is_null = &is_null[2];
  bind_ressult[2].length = &length[2];;

  bind_ressult[3].buffer_type = MYSQL_TYPE_TINY;
  bind_ressult[3].buffer = (char *) &c4;
  bind_ressult[3].is_null = &is_null[3];
  bind_ressult[3].length = &length[3];;

  bind_ressult[4].buffer_type = MYSQL_TYPE_VAR_STRING;
  bind_ressult[4].buffer = (char *) &c5;
  bind_ressult[4].is_null = &is_null[4];
  bind_ressult[4].buffer_length = STR_LEN;
  bind_ressult[4].length = &length[4];;

  bind_ressult[5].buffer_type = MYSQL_TYPE_DATETIME;
  bind_ressult[5].buffer = (char *) &c6;
  bind_ressult[5].is_null = &is_null[5];
  bind_ressult[5].length = &length[5];;

  bret = mysql_stmt_bind_result(stmt, bind_ressult);
  EXPECT_EQ(0, bret);

  uret = mysql_stmt_field_count(stmt);
  EXPECT_EQ(6, uret);

  char c5_ex [STR_LEN];
  i = 1;
  while (!mysql_stmt_fetch(stmt))
  {
    printf("ROW : %d:  %ld %f %f %d %s %04d-%02d-%02d %02d:%02d:%02d\n", i, c1, c2, c3, c4, c5, c6.year, c6.month, c6.day, c6.hour, c6.minute, c6.second);

    EXPECT_EQ(i, c1);
    ASSERT_FLOAT_EQ(i+0.1, c2);
    EXPECT_EQ(i+0.2, c3);

    if( 0 == i%2 )
      EXPECT_EQ(0, c4);
    else
      EXPECT_EQ(1, c4);

    memset(c5_ex, '\0', STR_LEN);
    sprintf(c5_ex, "c5-%d", i);
    EXPECT_EQ(0, strcmp(c5_ex, c5));

    EXPECT_EQ(c6.year, 2013);
    EXPECT_EQ(c6.month, 7);
    EXPECT_EQ(c6.day, 8);
    EXPECT_EQ(c6.hour, 16);
    EXPECT_EQ(c6.minute, 4);
    EXPECT_EQ(c6.second, i);

    EXPECT_EQ(8, length[0]);
    EXPECT_EQ(4, length[1]);
    EXPECT_EQ(8, length[2]);
    EXPECT_EQ(1, length[3]);
    EXPECT_EQ(strlen(c5_ex), length[4]);
    EXPECT_EQ(sizeof(time), length[5]);

    if( i == 4)
    {
      ret = mysql_stmt_fetch_column(stmt, bind_ressult, 2, 0);
      printf("return code is %s, %d\n", mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
      EXPECT_EQ(0, ret);
      ASSERT_DOUBLE_EQ(4.2, c3);
    }

    ++i;
  }


  printf("SQL State : %s\n", mysql_stmt_sqlstate(stmt));
  mysql_stmt_close(stmt);
  mysql_close(&my_);
  mysql_library_end();
}


int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
