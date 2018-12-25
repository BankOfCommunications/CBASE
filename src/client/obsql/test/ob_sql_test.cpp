/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_test.c is for what ...
 *
 * Version: ***: ob_sql_test.c  Fri Nov 23 11:24:24 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <mysql/mysql.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include "tblog.h"
#include "libobsql.h"
//#define SELECT_SAMPLE "select * from test where c1=?"
#define SELECT_SAMPLE "select c1 from test where c2=?"
//#define SELECT_SAMPLE "select count(*) from test; select count(*) from test_table"
int main(void)
{
  MYSQL *mysql;
  MYSQL_RES *results;
  MYSQL_ROW record;
  MYSQL_STMT    *stmt;
  MYSQL_BIND    bind[3];
  MYSQL_RES     *prepare_meta_result;
  unsigned long length[4] = {8,8,8,8};
  long unsigned int param_count;
  int           column_count, row_count;
  
  //short         small_data;
  long long int int_data[3];
  my_bool       is_null[4]={0,0,0,0};
  //ob_sql_init();
  //int num_elements = sizeof(server_options)/ sizeof(char *);
  //mysql_thread_init();
  mysql = mysql_init(NULL);
  if (NULL == mysql)
  {
    TBSYS_LOG(ERROR, "mysql init failed");
    exit(0);
  }
  //if (!mysql_real_connect(mysql, "10.232.36.29", "fangji", "test", "test", 3306, NULL, 0))
  if (!mysql_real_connect(mysql, "10.232.36.197", "admin", "admin", "test", 3142, NULL, 0))
  {
    TBSYS_LOG(INFO, "connect to mysql error %s\n", mysql_error(mysql));
  }
  else
  {
    int ret = 10;
    ret = mysql_ping(mysql);
    TBSYS_LOG(INFO, "ping ret is %d\n", ret);
    mysql_close(mysql);
    mysql = mysql_init(NULL);
    //mysql_real_connect(mysql, "10.232.36.197", "admin", "admin", "test", 3142, NULL, 0);
    if (mysql_query(mysql, "drop table if exists test"))
    {
      TBSYS_LOG(INFO, "DROP TABLE failed\n");
      TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
      exit(0);
    }
    
    if (mysql_query(mysql, "create table test(c1 int primary key, c2 int, c3 int)"))
    {
      TBSYS_LOG(INFO, "CREATE TABLE failed\n");
      TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
      exit(0);
    }
    mysql_close(mysql);
    mysql = mysql_init(NULL);
    //mysql_real_connect(mysql, "10.232.36.197", "admin", "admin", "test", 3142, NULL, 0);
    if (mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)"))
    {
      TBSYS_LOG(INFO, "INSERT TABLE failed\n");
      TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
      exit(0);
    }

    mysql_query(mysql, "select c1 from test");

    results = mysql_store_result(mysql);
    if (NULL != results)
    {
      while((record = mysql_fetch_row(results)))
      {
        //printf("%s--%s\n", record[0], record[1]);
        TBSYS_LOG(INFO, "%s\n", record[0]);
      }
    }
  }
  mysql_close(mysql);
  mysql = mysql_init(NULL);
  //mysql_real_connect(mysql, "10.232.36.197", "admin", "admin", "test", 3142, NULL, 0);
  if (mysql_query(mysql, "drop table if exists test"))
  {
    TBSYS_LOG(INFO, "DROP TABLE failed\n");
    TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
    exit(0);
  }

  if (mysql_query(mysql, "create table test(c1 int primary key, c2 int, c3 int)"))
  {
    TBSYS_LOG(INFO, "CREATE TABLE failed\n");
    TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
    exit(0);
  }
  sleep(3);
  if (mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)"))
  {
    TBSYS_LOG(INFO, "INSERT TABLE failed\n");
    TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
    exit(0);
  }
  mysql_close(mysql);
  mysql = mysql_init(NULL);
  //mysql_real_connect(mysql, "10.232.36.197", "admin", "admin", "test", 3142, NULL, 0);
  stmt = mysql_stmt_init(mysql);
  if (!stmt)
  {
    TBSYS_LOG(INFO, " mysql_stmt_init(), out of memory\n");
    exit(0);
  }

  if (mysql_stmt_prepare(stmt, SELECT_SAMPLE, (unsigned long)strlen(SELECT_SAMPLE)))
  {
    TBSYS_LOG(INFO, " mysql_stmt_prepare(), SELECT failed\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  fprintf(stdout, " prepare, SELECT successful\n");


  param_count= mysql_stmt_param_count(stmt);
  fprintf(stdout, " total parameters in SELECT: %lu\n", param_count);

  if (param_count != 1) 
  {
    TBSYS_LOG(INFO, " invalid parameter count returned by MySQL\n");
    exit(0);
  }


  prepare_meta_result = mysql_stmt_result_metadata(stmt);
  if (!prepare_meta_result)
  {
    TBSYS_LOG(INFO,
            " mysql_stmt_result_metadata(), returned no meta information\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }


  column_count= mysql_num_fields(prepare_meta_result);
  fprintf(stdout, " total columns in SELECT statement: %d\n", column_count);

  if (column_count != 1) 
  {
    TBSYS_LOG(INFO, " invalid column count returned by MySQL\n");
    exit(0);
  }
  MYSQL_BIND bindp[6];
  //MYSQL_TIME tval;
  //tval.year = 2012;
  //tval.month = 2;
  //tval.day = 20;
  //tval.hour = 12;
  //tval.minute = 0;
  //tval.second = 1;
  unsigned long plong = 1;
  int i = 0;
  for (; i < 1; ++i)
  {
    bindp[i].buffer_type= MYSQL_TYPE_LONGLONG;
    bindp[i].buffer= &plong;
    bindp[i].is_null= 0;
  }


  if (mysql_stmt_bind_param(stmt, bindp))
  {
    TBSYS_LOG(INFO, " mysql_stmt_bind_param() failed\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  sleep(1);

  if (mysql_stmt_execute(stmt))
  {
    TBSYS_LOG(INFO, " mysql_stmt_execute(), failed\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

//
  memset(bind, 0, sizeof(bind));


  bind[0].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[0].buffer= (char *)&int_data[0];
  bind[0].buffer_length = 8;
  bind[0].is_null= &is_null[0];
  //bind[0].length= &length[0];
  bind[0].length= 0;


  bind[1].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[1].buffer= (char *)&int_data[1];
  bind[1].buffer_length = 8;
  bind[1].is_null= &is_null[1];
  //bind[1].length= &length[1];
  bind[1].length= 0;

  bind[2].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[2].buffer= (char *)&int_data[2];
  bind[0].buffer_length = 8;
  bind[2].is_null= &is_null[2];
  bind[2].length= 0;
//
//  bind[3].buffer_type= MYSQL_TYPE_TIMESTAMP;
//  bind[3].buffer= (char *)&ts;
//  bind[3].is_null= &is_null[3];
//  bind[3].length= &length[3];
//
  if (mysql_stmt_bind_result(stmt, bind))
  {
    TBSYS_LOG(INFO, " mysql_stmt_bind_result() failed\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  if (mysql_stmt_store_result(stmt))
  {
    TBSYS_LOG(INFO, " mysql_stmt_store_result() failed\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  row_count= 0;
  fprintf(stdout, "Fetching results ...\n");
  while (!mysql_stmt_fetch(stmt))
  {
    row_count++;
    fprintf(stdout, "  row %d\n", row_count);

    fprintf(stdout, "   column1 (integer)  : ");
    if (is_null[0])
      fprintf(stdout, " NULL\n");
    else
      fprintf(stdout, " %ld(%ld)\n", (long int)int_data[0], length[0]);

    fprintf(stdout, "   column2 (integer)   : ");
    if (is_null[1])
      fprintf(stdout, " NULL\n");
    else
      fprintf(stdout, " %ld(%ld)\n", (long int)int_data[1], length[1]);

    fprintf(stdout, "   column3 (integer) : ");
    if (is_null[2])
      fprintf(stdout, " NULL\n");
    else
    fprintf(stdout, " %ld(%ld)\n", (long int)int_data[2], length[2]);
    fprintf(stdout, "\n");
  }

  fprintf(stdout, " total rows fetched: %d\n", row_count);
  if (row_count != 1)
  {
    TBSYS_LOG(INFO, " MySQL failed to return all rows\n");
    exit(0);
  }

  mysql_free_result(prepare_meta_result);


  if (mysql_stmt_close(stmt))
  {
    TBSYS_LOG(INFO, " failed while closing the statement\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  mysql_close(mysql);
  mysql = NULL;
  mysql = mysql_init(NULL);
  //mysql_real_connect(mysql, "10.232.36.197", "admin", "admin", "test", 3142, NULL, 0);
  int index = 0;
  while(index < 100)
  {
    //mysql_real_connect(mysql, "10.235.152.9", "admin", "admin", "test", 3142, NULL, 0);
    //mysql_real_connect(mysql, "10.253.2.72", "admin", "admin", "test", 26149, NULL, 0);
    index++;
    mysql = mysql_init(NULL);
    mysql_query(mysql, "select c1 from test");
    results = mysql_store_result(mysql);
    if (NULL != results)
    {
      while((record = mysql_fetch_row(results)))
      {
        //printf("%s--%s\n", record[0], record[1]);
        TBSYS_LOG(INFO, "%s\n", record[0]);
      }
    }
    mysql_free_result(results);
    mysql_close(mysql);
    mysql = NULL;
    }
  return 0;
}
