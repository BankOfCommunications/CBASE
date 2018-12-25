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
#define SELECT_SAMPLE "select * from test where c1=?"

char * get_query_sql(int num)
{
  static int index = 0;
  static char sql[1048576];
  int offset = 0;
  memset(sql, 0, 1048576);
  offset += snprintf(sql + offset, 1048576, "insert into test values");
  int i = 0;
  for (; i<num-1; ++i)
  {
    offset += snprintf(sql + offset, 1048576-offset, "(%d, %d, %d), ", index+1, index+2, index+3);
    index += 3;
  }
   offset += snprintf(sql + offset, 1048576-offset, "(%d, %d, %d)", index+1, index+2, index+3);
   index += 3;
   return sql;
}

int main(void)
{
  MYSQL *mysql;
  //MYSQL_RES *results;
  //MYSQL_ROW record;
  int row = 0;
  char query_sql[64];
  memset(query_sql, 0, 64);
  mysql = mysql_init(NULL);
  mysql_real_connect(mysql, "10.235.152.9", "admin", "admin", "test", 3142, NULL, 0);
  if (0 != mysql_query(mysql, "create table test(c1 int primary key, c2 int, c3 int)"))
  {
    fprintf(stderr, "create table failed %d", mysql_errno(mysql));
  }
  else
  {
    while(row<10000)
    {
      if (0 != mysql_query(mysql, get_query_sql(100)))
      {
        fprintf(stderr, "create table failed %d", mysql_errno(mysql));
        exit(-1);
      }
      row++;
    }
  }
  mysql_close(mysql);
  return 0;
}
