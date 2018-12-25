#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <mysql/mysql.h>
#include <string.h>
#include <getopt.h>

#define STRING_SIZE 50

void printusage()
{
  fprintf(stderr, "Useage:\n");
  fprintf(stderr, "\t-h host ip\n");
  fprintf(stderr, "\t-P host port\n");
  fprintf(stderr, "\t-u user name\n");
  fprintf(stderr, "\t-p password\n");
}
#define SELECT_SAMPLE "select * from test where c1=?"
int main(int argc, char* argv[])
{
  MYSQL_STMT    *stmt;
  MYSQL         *mysql;
  MYSQL_BIND    bind[3];
  MYSQL_RES     *prepare_meta_result;
  unsigned long length[4] = {8,8,8,8};
  int           param_count, column_count, row_count;
  long int int_data[3];
  my_bool       is_null[4]={0,0,0,0};
  static const char *server_options[] = { "mysql_test", "--defaults-file=my.cnf" };
  int num_elements = sizeof(server_options)/ sizeof(char *);
  const char *host = "10.232.36.29";
  const char *user = "admin";
  const char *passwd = "admin";
  int port = 3306;
  int opt = 0;
  static const char *server_groups[] = { "libmysqld_server", "libmysqld_client" };

  mysql_server_init(num_elements, const_cast<char**>(server_options), const_cast<char**>(server_groups));
  mysql = mysql_init(NULL);
  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "libmysqld_client");
  mysql_options(mysql, MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);

  const char* opt_string = "h:P:u:p:";
  struct option longopts[] =
    {
      {"host", 1, NULL, 'h'},
      {"port", 1, NULL, 'P'},
      {"user", 1, NULL, 'u'},
      {"pass", 1, NULL, 'p'},
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
    default:
      break;
    }
  }

  fprintf(stderr, "host is %s, user is %s, passwd is %s, port is %d\n", host, user, passwd, port);

  if (NULL == mysql_real_connect(mysql, host, user, passwd, "test", port, NULL, 0))
  {
    fprintf(stderr,"can not connect to server!\n");
    printusage();
  }

//  if (mysql_query(mysql, "drop table if exists test"))
//  {
//    fprintf(stderr, " DROP TABLE failed\n");
//    fprintf(stderr, " %s\n", mysql_error(mysql));
//    exit(0);
//  }
//
//  if (mysql_query(mysql, "create table test(c1 int primary key, c2 int, c3 int)"))
//  {
//    fprintf(stderr, " CREATE TABLE failed\n");
//    fprintf(stderr, " %s\n", mysql_error(mysql));
//    exit(0);
//  }
//
//  if (mysql_query(mysql, "insert into test values(1,2,3), (2,3,4), (3,4,5)"))
//  {
//    fprintf(stderr, " INSERT TABLE failed\n");
//    fprintf(stderr, " %s\n", mysql_error(mysql));
//    exit(0);
//  }

/* Prepare a SELECT query to fetch data from test_table */
  stmt = mysql_stmt_init(mysql);
  if (!stmt)
  {
    fprintf(stderr, " mysql_stmt_init(), out of memory\n");
    exit(0);
  }

  if (mysql_stmt_prepare(stmt, SELECT_SAMPLE, strlen(SELECT_SAMPLE)))
  {
    fprintf(stderr, " mysql_stmt_prepare(), SELECT failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
  fprintf(stdout, " prepare, SELECT successful\n");

/* Get the parameter count from the statement */
  param_count= (int)mysql_stmt_param_count(stmt);
fprintf(stdout, " total parameters in SELECT: %d\n", param_count);

if (param_count != 1) /* validate parameter count */
{
  fprintf(stderr, " invalid parameter count returned by MySQL\n");
  exit(0);
}

/* Fetch result set meta information */
  prepare_meta_result = mysql_stmt_result_metadata(stmt);
  if (!prepare_meta_result)
  {
    fprintf(stderr,
            " mysql_stmt_result_metadata(), returned no meta information\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

/* Get total columns in the query */
  column_count= mysql_num_fields(prepare_meta_result);
  fprintf(stdout, " total columns in SELECT statement: %d\n", column_count);

  if (column_count != 3) /* validate column count */
  {
    fprintf(stderr, " invalid column count returned by MySQL\n");
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
      fprintf(stderr, " mysql_stmt_bind_param() failed\n");
      fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
      exit(0);
   }

///* Execute the SELECT query */
  if (mysql_stmt_execute(stmt))
  {
    fprintf(stderr, " mysql_stmt_execute(), failed\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }

///* Bind the result buffers for all 4 columns before fetching them */
//
  memset(bind, 0, sizeof(bind));

/* INTEGER COLUMN */
  bind[0].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[0].buffer= (char *)&int_data[0];
  bind[0].buffer_length = 8;
  bind[0].is_null= &is_null[0];
  //bind[0].length= &length[0];
  bind[0].length= 0;

/* INT COLUMN */
  bind[1].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[1].buffer= (char *)&int_data[1];
  bind[0].buffer_length = 8;
  bind[1].is_null= &is_null[1];
  //bind[1].length= &length[1];
  bind[1].length= 0;

/* INT COLUMN */
  bind[2].buffer_type= MYSQL_TYPE_LONGLONG;
  bind[2].buffer= (char *)&int_data[2];
  bind[0].buffer_length = 8;
  bind[2].is_null= &is_null[2];
  bind[2].length= 0;
//
///* TIMESTAMP COLUMN */
//  bind[3].buffer_type= MYSQL_TYPE_TIMESTAMP;
//  bind[3].buffer= (char *)&ts;
//  bind[3].is_null= &is_null[3];
//  bind[3].length= &length[3];
//
/* Bind the result buffers */
 if (mysql_stmt_bind_result(stmt, bind))
 {
   fprintf(stderr, " mysql_stmt_bind_result() failed\n");
   fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
   exit(0);
 }

/* Now buffer all results to client */
if (mysql_stmt_store_result(stmt))
{
  fprintf(stderr, " mysql_stmt_store_result() failed\n");
  fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
  exit(0);
}

/* Fetch all rows */
 row_count= 0;
 fprintf(stdout, "Fetching results ...\n");
 while (!mysql_stmt_fetch(stmt))
 {
   row_count++;
   fprintf(stdout, "  row %d\n", row_count);

   /* column 1 */
   fprintf(stdout, "   column1 (integer)  : ");
   if (is_null[0])
     fprintf(stdout, " NULL\n");
   else
     fprintf(stdout, " %ld(%ld)\n", int_data[0], length[0]);

   /* column 2 */
   fprintf(stdout, "   column2 (integer)   : ");
   if (is_null[1])
     fprintf(stdout, " NULL\n");
   else
     fprintf(stdout, " %ld(%ld)\n", int_data[1], length[1]);

   /* column 3 */
   fprintf(stdout, "   column3 (integer) : ");
   if (is_null[2])
     fprintf(stdout, " NULL\n");
   else
     fprintf(stdout, " %ld(%ld)\n", int_data[2], length[2]);
   fprintf(stdout, "\n");
 }

/* Validate rows fetched */
 fprintf(stdout, " total rows fetched: %d\n", row_count);
 if (row_count != 1)
 {
   fprintf(stderr, " MySQL failed to return all rows\n");
   exit(0);
 }
//
/* Free the prepared result metadata */
  mysql_free_result(prepare_meta_result);


/* Close the statement */
  if (mysql_stmt_close(stmt))
  {
    fprintf(stderr, " failed while closing the statement\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    exit(0);
  }
}
