/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_query.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <mysql/mysql.h>
#include <cstdio>
#include <cstdlib>

int main(int argc, char *argv[])
{
  if (argc != 4)
  {
    printf("Usage: sql_query <host> <port> <query_statement>\n");
    return 0;
  }
  char *host = argv[1];
  int port = atoi(argv[2]);
  char *query = argv[3];
  MYSQL my_;
  int ret = 0;
  MYSQL_RES *res = NULL;
  MYSQL_FIELD *fields = NULL;
  if (0 != mysql_library_init(0, NULL, NULL))
  {
    fprintf(stderr, "could not init mysql library\n");
  }
  else if (NULL == mysql_init(&my_))
  {
    fprintf(stderr, "failed to init libmysqlclient\n");
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  else if(NULL == mysql_real_connect(&my_, host, "admin", "admin", "test", port, NULL, 0))
  {
    fprintf(stderr, "failed to connect mysql server\n");
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  else if (0 != (ret = mysql_query(&my_, query)))
  {
    fprintf(stderr, "failed to query\n");
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  else if (NULL == (res = mysql_use_result(&my_)))
  {
    if(mysql_field_count(&my_) == 0)
    {
      // query does not return data
      // (it was not a SELECT)
      unsigned long long num_rows = mysql_affected_rows(&my_);
      printf("affected row count: %llu\n", num_rows);
    }
    else
    {
      fprintf(stderr, "failed to use result\n");
      fprintf(stderr, "%s\n", mysql_error(&my_));
    }
  }
  else if (NULL == (fields = mysql_fetch_fields(res)))
  {
    fprintf(stderr, "failed to fetch fields\n");
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  else
  {
    unsigned int num_fields = mysql_num_fields(res);
    // print detailed field info
    for (unsigned int i = 0; i < num_fields; ++i)
    {
      printf("field=%u name=%s org_name=%s table=%s org_table=%s db=%s catalog=%s length=%ld max_length=%ld type=%d\n",
             i, fields[i].name, fields[i].org_name,
             fields[i].table, fields[i].org_table,
             fields[i].db, fields[i].catalog,
             fields[i].length, fields[i].max_length,
             fields[i].type);
    }
    printf("----------------------------------------------------------------\n");
    // print headers
    for (unsigned int i = 0; i < num_fields; ++i)
    {
      if (i != 0)
      {
        printf(", ");
      }
      printf("%s", fields[i].name);
    }
    printf("\n");
    // print rows
    MYSQL_ROW row;
    unsigned long *lengths = NULL;
    int64_t row_count = 0;
    while(NULL != (row = mysql_fetch_row(res)))
    {
      row_count++;
      if (NULL == (lengths = mysql_fetch_lengths(res)))
      {
        fprintf(stderr, "failed to fetch lengths\n");
        break;
      }
      for (unsigned int j = 0; j < num_fields; ++j)
      {
        if (j != 0)
        {
          printf(", ");
        }
        printf("%.*s", row[j] ? static_cast<int>(lengths[j]) : static_cast<int>(sizeof("NULL")-1), row[j] ? row[j] : "NULL");
      } // end for
      printf("\n");
    }   // end while
    printf("----------------------------------------------------------------\n");
    printf("total row count: %ld\n", row_count);
  }
  if (NULL != res)
  {
    mysql_free_result(res);
  }
  mysql_library_end();
  return 0;
}
