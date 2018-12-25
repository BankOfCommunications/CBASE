/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_test_multh.c is for what ...
 *
 * Version: ***: ob_sql_test_multh.c  Fri Jan 11 09:39:59 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <mysql/mysql.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include "tblog.h"

#define SELECT_SAMPLE "select * from test where c1=?"

int prepare_env()
{
  MYSQL *mysql;
  mysql = mysql_init(NULL);

  if (NULL == mysql)
  {
    TBSYS_LOG(ERROR, "mysql init failed");
    exit(0);
  }
  if (!mysql_real_connect(mysql, "10.235.152.9", "admin", "admin", "test", 3142, NULL, 0))
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
    sleep(30);
    mysql = mysql_init(NULL);
    if (mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)"))
    {
      TBSYS_LOG(INFO, "INSERT TABLE failed\n");
      TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
      exit(0);
    }
    mysql_close(mysql);
  }
  return 0;
}

void test_query()
{
  
}

void run_select()
{
  
  /*MYSQL *mysql = NULL;
  mysql = mysql_init();
  if (NULL == mysql)
  {
    TBSYS_LOG(ERROR, "");
    exit(-1);
  }
  else
  {
    //
  }*/
}

void run_insert()
{
  
}

void run_update()
{
  
}

void run_replace()
{
  
}

static void * task(void *arg)
{
  (void)(arg);
  MYSQL *mysql;
  MYSQL_RES *results;
  MYSQL_ROW record;
  MYSQL_RES     *prepare_meta_result;
  long unsigned int param_count;
  int           column_count, row_count;
  long long int int_data[3];
  my_bool       is_null[4]={0,0,0,0};
  unsigned long length[4] = {8,8,8,8};
  char sql[1024];
  memset(sql, 0, 1024);
  int64_t idx = 0;
  mysql = mysql_init(NULL);
  //if (!mysql_real_connect(mysql, "10.232.36.29", "fangji", "test", "test", 3306, NULL, 0))
  if (!mysql_real_connect(mysql, "10.235.152.9", "admin", "admin", "test", 3142, NULL, 0))
  {
    TBSYS_LOG(INFO, "connect to mysql error %s\n", mysql_error(mysql));
  }
  mysql_close(mysql);
  mysql = NULL;
  //Test for simple query
  while(idx < 10000)
  {
    idx++;
    mysql = mysql_init(mysql);
    snprintf(sql, 1024, "select c1 from test where c1=%ld", idx);
    if (0 != mysql_query(mysql, sql))
    {
      mysql_close(mysql);
      mysql = NULL;
      TBSYS_LOG(ERROR, "query failed");
      exit(-1);
    }

    results = mysql_store_result(mysql);
    if (NULL != results)
    {
      //assert(1 == mysql_num_rows(results));
      while((record = mysql_fetch_row(results)))
      {
        //printf("%s--%s\n", record[0], record[1]);
        TBSYS_LOG(INFO, "%s\n", record[0]);
        // assert((idx%3 + 1) == atoi(record[0]));
      }
    }
    mysql_close(mysql);
    mysql = NULL;
  }

  //Test for ps protocol
  mysql = mysql_init(NULL);
  MYSQL_STMT *stmt = mysql_stmt_init(mysql);
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
  TBSYS_LOG(INFO, "prepare statement ok");
  param_count= mysql_stmt_param_count(stmt);
  TBSYS_LOG(INFO, " total parameters in SELECT: %lu", param_count);

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
  TBSYS_LOG(INFO, " total columns in SELECT statement: %d", column_count);

  if (column_count != 3) 
  {
    TBSYS_LOG(INFO, " invalid column count returned by MySQL\n");
    exit(0);
  }
  int64_t int_datas[6]={2,3,3,4,5,6};
  MYSQL_BIND bindp[6];
  int i = 0;
  for (; i < 1; ++i)
  {
    bindp[i].buffer_type= MYSQL_TYPE_LONGLONG;
    bindp[i].buffer= (char *)&int_datas[i];
    bindp[i].is_null= 0;
  }

  if (mysql_stmt_bind_param(stmt, bindp))
  {
    TBSYS_LOG(INFO, " mysql_stmt_bind_param() failed\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

  idx = 0;
  MYSQL_BIND    bind[3];
  bind[0].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[0].buffer= (char *)&int_data[0];
  bind[0].buffer_length = 8;
  bind[0].is_null= &is_null[0];
  //bind[0].length= &length[0];
  bind[0].length= 0;


  bind[1].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[1].buffer= (char *)&int_data[1];
  bind[0].buffer_length = 8;
  bind[1].is_null= &is_null[1];
  //bind[1].length= &length[1];
  bind[1].length= 0;

  bind[2].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[2].buffer= (char *)&int_data[2];
  bind[0].buffer_length = 8;
  bind[2].is_null= &is_null[2];
  bind[2].length= 0;

  while(idx < 10000)
  {
    idx ++;
    int_datas[0] = idx%3 +1;
    if (mysql_stmt_execute(stmt))
    {
      TBSYS_LOG(INFO, " mysql_stmt_execute(), failed\n");
      TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
      exit(0);
    }

    

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
    //assert(1==mysql_stmt_num_rows(stmt));
    while (!mysql_stmt_fetch(stmt))
    {
      row_count++;
      TBSYS_LOG(INFO, "  row %d\n", row_count);

      TBSYS_LOG(INFO, "   column1 (integer)  : ");
      if (is_null[0])
        TBSYS_LOG(INFO, " NULL\n");
      else
        TBSYS_LOG(INFO, " %ld(%ld)\n", (long int)int_data[0], length[0]);

      TBSYS_LOG(INFO, "   column2 (integer)   : ");
      if (is_null[1])
        TBSYS_LOG(INFO, " NULL\n");
      else
        TBSYS_LOG(INFO, " %ld(%ld)\n", (long int)int_data[1], length[1]);

      TBSYS_LOG(INFO, "   column3 (integer) : ");
      if (is_null[2])
        TBSYS_LOG(INFO, " NULL\n");
      else
        TBSYS_LOG(INFO, " %ld(%ld)\n", (long int)int_data[2], length[2]);
      TBSYS_LOG(INFO, "\n");
    }

    TBSYS_LOG(INFO, " total rows fetched: %d\n", row_count);
    if (row_count != 1)
    {
      TBSYS_LOG(INFO, " MySQL failed to return all rows\n");
      exit(0);
    }
    mysql_stmt_free_result(stmt);
  }

  if (mysql_stmt_close(stmt))
  {
    TBSYS_LOG(INFO, " failed while closing the statement\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  mysql_close(mysql);
  mysql = NULL;
  return 0;
}

int main(int argc, char* argv[])
{
  (void)(argc);
  (void)(argv);
  int idx = 0;
  prepare_env();
  TBSYS_LOG(INFO, "table test created, 3 records was inserted");
  pthread_t pids[20];
  for(; idx < 20; ++idx)
  {
    pthread_create(pids+idx, NULL, task, NULL);
    TBSYS_LOG(DEBUG, "start %d thread", idx);
  }
  idx = 0;
  for(; idx < 20; ++idx)
  {
    pthread_join(pids[idx], NULL);
  }
  TBSYS_LOG(INFO, "test exit");
  return 0;
}
