/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <mysql/mysql.h>

#define ASSERT_TRUE(x) do {if (!(x)) {fprintf(stderr, "assert `%s' fail at %s:%d\n", #x, __FILE__, __LINE__); exit(1);}} while (0)
#define ASSERT_EQ(x, y) do {if ((x) != (y)) {fprintf(stderr, "assert `%s=%s' fail at %s:%d\n", #x, #y, __FILE__, __LINE__); exit(1);}} while (0)

int main(int argc, char *argv[])
{
  (void)argc;
  (void)argv;
  const char* HOST = "127.0.0.1";
  int PORT = 3306;
  HOST = "10.232.36.171";
  PORT = 4797;
  // 1. 初始化libmysqlclient
  if (0 != mysql_library_init(0, NULL, NULL))
  {
    fprintf(stderr, "could not init mysql library\n");
    exit(1);
  }

  MYSQL my_;
  ASSERT_TRUE(NULL != mysql_init(&my_)); // 此处用ASSERT省略错误检查
  fprintf(stderr, "Connecting server %s:%d...\n", HOST, PORT);
  // 2. 连接OceanBase SQL服务
  ASSERT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));
  // 3. 建表
  int ret = 0;
  ret = mysql_query(&my_, "drop table if exists ob_new_sql_test");
  if (0 != ret)
  {
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  ASSERT_EQ(0, ret);
  ret = mysql_query(&my_, "create table if not exists ob_new_sql_test "
                    "(c0 varchar(64), c1 int primary key, c2 int, "
                    "c3 int, c4 int, c5 int, c6 int, "
                    "c7 int, c8 int, c9 int, c10 int, "
                    "c11 int, c12 int, c13 int, c14 int,"
                    "c15 int)");
  if (0 != ret)
  {
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  ASSERT_EQ(0, ret);
  // 4. 插入数据
  ret = mysql_query(&my_, "insert into ob_new_sql_test (c0, c1, c2)values ('hello', 1, 2)");
  if (0 != ret)
  {
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  ASSERT_EQ(0, ret);
  // 5. 查询数据
  ASSERT_EQ(0, mysql_query(&my_, "select * from ob_new_sql_test"));
  MYSQL_RES *res = mysql_use_result(&my_); // 取结果集
  ASSERT_TRUE(NULL != res);
  unsigned int num_fields = mysql_num_fields(res); // 结果列数
  ASSERT_EQ(16U, num_fields);

  unsigned long *lengths = NULL;
  MYSQL_ROW row;
  row = mysql_fetch_row(res);
  ASSERT_TRUE(NULL != row);
  lengths = mysql_fetch_lengths(res); // row中每个值的长度，0表示NULL
  ASSERT_TRUE(NULL != lengths);
  for (unsigned int j = 0; j < num_fields; ++j)
  {
    if (0 != j)
    {
      fprintf(stdout, ",");
    }
    fprintf(stdout, "%.*s", row[j] ? static_cast<int>(lengths[j])
            : static_cast<int>(sizeof("NULL")-1), row[j] ? row[j] : "NULL");
  }
  fprintf(stdout, "\n");
  row = mysql_fetch_row(res);
  ASSERT_TRUE(NULL == row);     // no more rows
  mysql_free_result(res);
  // 6. 删表
  ret = mysql_query(&my_, "drop table if exists ob_new_sql_test");
  if (0 != ret)
  {
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  // 7. destroy
  mysql_library_end();
  return 0;
}
