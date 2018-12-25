#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <mysql/mysql.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include "tblog.h"
#define SELECT_SAMPLE "select * from test where c1=?"


void useage()
{
  
}

int do_create_table(MYSQL *mysql)
{
  int ret = 0;
  if (NULL == mysql)
  {
    fprintf(stderr, "invalid argument mysql is %p\n", mysql);
    ret = 1;
  }
  else
  {
    ret = mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)");
    if (0 == ret)
    {
      fprintf(stderr, "insert 3 records success\n");
    }
  }
  return ret;
}

int do_insert(MYSQL *mysql)
{
  int ret = 0;
  if (NULL == mysql)
  {
    fprintf(stderr, "invalid argument mysql is %p\n", mysql);
    ret = 1;
  }
  else
  {
    ret = mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)");
    if (0 == ret)
    {
      fprintf(stderr, "insert 3 records success\n");
    }
  }
  return ret;
}

int do_replace(MYSQL *mysql)
{
  int ret = 0;
  if (NULL == mysql)
  {
    fprintf(stderr, "invalid argument mysql is %p\n", mysql);
    ret = 1;
  }
  else
  {
    ret = mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)");
    if (0 == ret)
    {
      fprintf(stderr, "insert 3 records success\n");
    }
  }
  return ret;
}

int do_delete(MYSQL *mysql)
{
  int ret = 0;
  if (NULL == mysql)
  {
    fprintf(stderr, "invalid argument mysql is %p\n", mysql);
    ret = 1;
  }
  else
  {
    ret = mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)");
    if (0 == ret)
    {
      fprintf(stderr, "insert 3 records success\n");
    }
  }
  return ret;
}

int do_update(MYSQL *mysql)
{
  int ret = 0;
  if (NULL == mysql)
  {
    fprintf(stderr, "invalid argument mysql is %p\n", mysql);
    ret = 1;
  }
  else
  {
    ret = mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)");
    if (0 == ret)
    {
      fprintf(stderr, "insert 3 records success\n");
    }
  }
  return ret;
}

int do_drop_table(MYSQL *mysql)
{
  int ret = 0;
  if (NULL == mysql)
  {
    fprintf(stderr, "invalid argument mysql is %p\n", mysql);
    ret = 1;
  }
  else
  {
    ret = mysql_query(mysql, "drop table test if exists");
    if (0 == ret)
    {
      fprintf(stderr, "drop table test success\n");
    }
  }
  return ret;
}

int main()
{

  /*MYSQL *mysql = NULL;
  MYSQL_RES *results = NULL;
  MYSQL_ROW record = NULL;
  char *host = "10.235.152.9";
  char *user = "admin";
  char *passwd = "admin";
  char *db = "test";
  int port = 3306;
  
  const char* opt_string = "h:P:u:p:d:";
  struct option longopts[] =
    {
      {"host", 1, NULL, 'h'},
      {"port", 1, NULL, 'P'},
      {"user", 1, NULL, 'u'},
      {"pass", 1, NULL, 'p'},
      {"db", 1, NULL, 'd'},
      {0, 0, 0, 0}
    };

  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) 
  {
    switch (opt) 
    {
    case 'h':
      host = optarg;
      break;
    case 'P':
      port = atoi(optarg);
      break;
    case 'u':
      user = optarg;
      break;
    case 'p':
      passwd = optarg;
      break;
    case 'p':
      db = optarg;
      break;
    default:
      break;
    }
  }
  
  mysql = mysql_init(NULL);
  if (NULL == mysql)
  {
    fprintf(stderr, "mysql init failed\n");
    exit(0);
  }
  fprintf(stderr, "mysql handler init success.\n");
  if (!mysql_real_connect(mysql, host, user, passwd, db, port, NULL, 0))
  {
    fprintf(stderr, "connect to mysql(%s:%d) error %s\n", host, port, mysql_error(mysql));
  }
  else
  {
    fprintf(stderr, "connect to %s:%d success.\n", host, port);
    fprintf(stderr, "clear data base drop table if exists\n");
    ret = do_drop_table(mysql);
    if (0 != ret)
    {
      fprintf(stderr, "drop table failed\n");
      exit(0);
    }

    fprintf(stderr, "create demo table\n");
    ret = do_create_table(mysql);
    if (0 != ret)
    {
      fprintf(stderr, "creat demo table failed\n");
      exit(0);
    }

    fprintf(stderr, "insert demo\n");
    ret = do_insert(mysql);
    if (0 != ret)
    {
      fprintf(stderr, "insert demo failed\n");
      exit(0);
    }

    fprintf(stderr, "update demo\n");
    ret = do_drop_table(mysql);
    if (0 != ret)
    {
      fprintf(stderr, "update demo failed\n");
      exit(0);
    }

    fprintf(stderr, "\nreplace demo\n");
    ret = do_drop_table(mysql);
    if (0 != ret)
    {
      fprintf(stderr, "replace demo failed\n");
      exit(0);
    }

    fprintf(stderr, "\ndelete demo\n");
    ret = do_drop_table(mysql);
    if (0 != ret)
    {
      fprintf(stderr, "update demo failed\n");
      exit(0);
    }

    fprintf(stderr, "\ndrop table demo\n");
    ret = do_drop_table(mysql);
    if (0 != ret)
    {
      fprintf(stderr, "drop table demo failed\n");
      exit(0);
    }
    
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
        TBSYS_LOG(INFO, "%s\n", record[0]);
      }
    }
  }

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
  fprintf(stdout, " total parameters in SELECT: %d\n", param_count);

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
  bind[0].buffer_length = 8;
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
//
  mysql_free_result(prepare_meta_result);


  if (mysql_stmt_close(stmt))
  {
    TBSYS_LOG(INFO, " failed while closing the statement\n");
    TBSYS_LOG(INFO, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  mysql_close(mysql);*/
  return 0;
}
