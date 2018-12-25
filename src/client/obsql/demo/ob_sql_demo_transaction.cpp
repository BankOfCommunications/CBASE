/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ob_sql_demo_transaction.cpp is for what ...
 *
 * Version: ***: ob_sql_demo_transaction.cpp  Tue Jan 15 16:53:38 2013 fangji.hcm Exp $
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

static void* query_run(void *arg)
{
  sleep(3);
  (void)(arg);
  MYSQL *mysql;
  MYSQL_RES *results;
  mysql = mysql_init(NULL);
  if (NULL == mysql)
  {
    //TBSYS_LOG(ERROR, "mysql init failed");
    exit(0);
  }
  mysql_query(mysql, "select c1 from test_tran");
  results = mysql_store_result(mysql);
  fprintf(stderr, "store result\n");
  assert(3 == mysql_num_rows(results));
  mysql_free_result(results);
  mysql_close(mysql);
  return 0;
}

static void* transaction_run(void *arg)
{
  (void)(arg);
  MYSQL *mysql;
  MYSQL_RES *results;
  MYSQL_ROW record;
  mysql = mysql_init(NULL);
  if (NULL == mysql)
  {
    exit(0);
  }
  
  if (mysql_query(mysql, "start transaction"))
  {
    exit(0);
  }
  
  if (mysql_query(mysql, "insert into test_tran values(4,2,3), (5,3,4), (6,4,5)"))
  {
    exit(0);
  }

  if (mysql_query(mysql, "insert into test_tran values(7,2,3), (8,3,4), (9,4,5)"))
  {
    exit(0);
  }

  mysql_query(mysql, "select c1 from test_tran");
  results = mysql_store_result(mysql);
  if (NULL != results)
  {
    while((record = mysql_fetch_row(results)))
    {
      fprintf(stderr, "%s\n", record[0]);
    }
  }
  else
  {
    fprintf(stderr, "result is null\n");
  }
  assert(3 == mysql_num_rows(results));
  mysql_free_result(results);
  fprintf(stderr, "sleep 10s \n");
  sleep(10);
  if (mysql_query(mysql, "commit"))
  {
    //TBSYS_LOG(INFO, "commit failed\n");
    //TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
    exit(0);
  }
  mysql_query(mysql, "select c1 from test_tran");
  results = mysql_store_result(mysql);
  assert(9 == mysql_num_rows(results));
  mysql_close(mysql);
  return 0;
}

void prepare()
{
  MYSQL *mysql;
  mysql = mysql_init(NULL);
  if (NULL == mysql)
  {
    //TBSYS_LOG(ERROR, "mysql init failed");
    exit(0);
  }
  if (!mysql_real_connect(mysql, "10.235.152.9", "admin", "admin", "test", 3142, NULL, 0))
  {
    //TBSYS_LOG(INFO, "connect to mysql error %s\n", mysql_error(mysql));
  }
  else
  {
    int ret = 10;
    ret = mysql_ping(mysql);
    //TBSYS_LOG(INFO, "ping ret is %d\n", ret);
    if (mysql_query(mysql, "drop table if exists test_tran"))
    {
      //TBSYS_LOG(INFO, "DROP TABLE failed\n");
      //TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
      exit(0);
    }

    if (mysql_query(mysql, "create table test_tran(c1 int primary key, c2 int, c3 int)"))
    {
      //TBSYS_LOG(INFO, "CREATE TABLE failed\n");
      //TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
      exit(0);
    }

    if (mysql_query(mysql, "insert into test_tran values(1,2,3), (2,3,4), (3,4,5)"))
    {
      //TBSYS_LOG(INFO, "INSERT TABLE failed\n");
      //TBSYS_LOG(INFO, "%s\n", mysql_error(mysql));
      exit(0);
    }
  }
}


int main(int argc, char*argv[])
{
  (void)(argc);
  (void)(argv);
  prepare();
  pthread_t p1,p2;
  pthread_create(&p1, NULL, query_run, NULL);
  pthread_create(&p2, NULL, transaction_run, NULL);
  pthread_join(p1, NULL);
  pthread_join(p2, NULL);
}
