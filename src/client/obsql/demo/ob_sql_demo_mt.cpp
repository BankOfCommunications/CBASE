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
static void * task(void *arg)
{
  (void)(arg);
  MYSQL *mysql;
  MYSQL_RES *results;
  MYSQL_ROW record;
  char sql[1024];
  memset(sql, 0, 1024);
  int64_t idx = 0;
  mysql = mysql_init(NULL);
  if (!mysql_real_connect(mysql, "10.235.152.9", "admin", "admin", "test", 3142, NULL, 0))
  {
    TBSYS_LOG(INFO, "connect to mysql error %s\n", mysql_error(mysql));
  }
  mysql_close(mysql);
  mysql = NULL;
  while(1)
  {
    idx++;
    mysql = mysql_init(mysql);
    snprintf(sql, 1024, "select c1 from test where c1=%ld", idx);
    mysql_query(mysql, sql);

    results = mysql_store_result(mysql);
    if (NULL != results)
    {
      while((record = mysql_fetch_row(results)))
      {
        //printf("%s--%s\n", record[0], record[1]);
        TBSYS_LOG(INFO, "%s\n", record[0]);
      }
    }
    mysql_close(mysql);
    mysql = NULL;
  }
  return 0;
}

int main(int argc, char* argv[])
{
  (void)(argc);
  (void)(argv);
  int idx = 0;
  pthread_t pids[20];
  for(; idx < 20; ++idx)
  {
    pthread_create(pids+idx, NULL, task, NULL);
  }
  idx = 0;
  for(; idx < 20; ++idx)
  {
    pthread_join(pids[idx], NULL);
  }
  return 0;
}
