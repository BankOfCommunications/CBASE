/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_new_sql_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <mysql/mysql.h>
#include <gtest/gtest.h>
#include <sys/time.h>
const char* HOST = "127.0.0.1";
int PORT = 3306;

class ObNewSqlTest: public ::testing::Test
{
  public:
    ObNewSqlTest();
    virtual ~ObNewSqlTest();
    virtual void SetUp();
    virtual void TearDown();
  protected:
    void create_table();
    void generate_data(int row_count);
    void delete_table();
    void cons_varchar_cell(char *varchar);
    void get_row(int i, char *row, int length);
    // disallow copy
    ObNewSqlTest(const ObNewSqlTest &other);
    ObNewSqlTest& operator=(const ObNewSqlTest &other);
  protected:
    // data members
    static const int VARCHAR_CELL_BUFF_SIZE = 64;
    MYSQL my_;
};

ObNewSqlTest::ObNewSqlTest()
{
}

ObNewSqlTest::~ObNewSqlTest()
{
}

void ObNewSqlTest::SetUp()
{
  ASSERT_TRUE(NULL != mysql_init(&my_));
  fprintf(stderr, "Connecting server %s:%d...\n", HOST, PORT);
  ASSERT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test", PORT, NULL, 0));
  delete_table();
  create_table();
}

void ObNewSqlTest::create_table()
{
  // see tests/sql/ob_fake_table.h
  int ret = mysql_query(&my_, "create table ob_new_sql_test "
                        "(c0 varchar(64), c1 int, c2 int, "
                        "c3 int, c4 int, c5 int, c6 int, "
                        "c7 int, c8 int, c9 int, c10 int, "
                        "c11 int, c12 int, c13 int, c14 int,"
                        "c15 int)");
  if (0 != ret)
  {
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  ASSERT_EQ(0, ret);
}

static inline int rand_int(int max)
{
  double fmax = max;
  int j = (int) (fmax * (rand() / (RAND_MAX + 1.0)));
  return j;
}

void ObNewSqlTest::cons_varchar_cell(char *varchar)
{
  int charnum = rand_int(VARCHAR_CELL_BUFF_SIZE);
  for (int i = 0; i < charnum; ++i)
  {
    varchar[i] = static_cast<char>('A'+rand_int(26));
  }
  varchar[charnum] = '\0';
}

void ObNewSqlTest::get_row(int i, char *row, int length)
{
/***************************************************************************************************
  c0       | c1      | c2        | c3        | c4        | c5         | c6    | c7   |
-----------------------------------------------------------------------------------------------------
  rand str | row_idx | row_idx%2 | row_idx%3 | row_idx/2 | row_idx/3  | c1+c2 |c3+c4 |
***************************************************************************************************/
  char varchar[VARCHAR_CELL_BUFF_SIZE];
  cons_varchar_cell(varchar);
  if (i % 2 == 0)
  {
    snprintf(row, length,
             "'%s', %d, %d, %d, %d, %d, %d, %d, "
             "NULL, %d, %d, %d, %d, %d, %d, %d",
             varchar, i, i%2, i%3, i/2, i/3, i+i%2, i%3+i/2,
             rand(), rand(), rand(), rand(), rand(), rand(), rand()
      );
  }
  else
  {
    snprintf(row, length,
             "'%s', %d, %d, %d, %d, %d, %d, %d, "
             "%d, %d, %d, %d, %d, %d, %d, %d",
             varchar, i, i%2, i%3, i/2, i/3, i+i%2, i%3+i/2,
             i, rand(), rand(), rand(), rand(), rand(), rand(), rand()
      );
  }
}

void ObNewSqlTest::generate_data(int row_count)
{
  // see tests/sql/ob_fake_table.h
  char stmt[1024];
  char row[1024];
  srand(0);
  struct timeval tv_start;
  struct timeval tv_end;
  gettimeofday(&tv_start, NULL);
  for (int i = 0; i < row_count; ++i)
  {
    get_row(i, row, 1024);
    snprintf(stmt, 1024, "insert into ob_new_sql_test values (%s)", row);
    //printf("row_i=%s\n", stmt);
    int ret = mysql_query(&my_, stmt);
    if (0 != ret)
    {
      fprintf(stderr, "%s\n", mysql_error(&my_));
    }
    if (0 != i && i % 200 == 0)
    {
      fprintf(stderr, "insert %d rows\n", i);
    }
  }
  gettimeofday(&tv_end, NULL);
  int64_t start_us = static_cast<int64_t>(tv_start.tv_sec)*1000*1000 + tv_start.tv_usec;
  int64_t end_us = static_cast<int64_t>(tv_end.tv_sec)*1000*1000 + tv_end.tv_usec;
  fprintf(stderr, "performance of insert rows: %ld (usec/row)\n", (end_us-start_us)/row_count);
}

void ObNewSqlTest::delete_table()
{
  int ret = mysql_query(&my_, "drop table if exists ob_new_sql_test");
  if (0 != ret)
  {
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
}

void ObNewSqlTest::TearDown()
{
  //delete_table();
  mysql_close(&my_);
}

static int ROW_COUNT = 4800;

TEST_F(ObNewSqlTest, select_all)
{
  fprintf(stderr, "row_count=%d\n", ROW_COUNT);
  generate_data(ROW_COUNT);

  struct timeval tv_start;
  struct timeval tv_end;
  gettimeofday(&tv_start, NULL);
  ASSERT_EQ(0, mysql_query(&my_, "select * from ob_new_sql_test"));
  MYSQL_RES *res = mysql_use_result(&my_);
  ASSERT_TRUE(NULL != res);
  unsigned int num_fields = mysql_num_fields(res);
  ASSERT_EQ(16U, num_fields);

  unsigned long *lengths = NULL;
  MYSQL_ROW row;
  char text_row[1024];
  int pos = 0;
  char expected_row[1024];
  srand(0);
  for (int i = 0; i < ROW_COUNT; ++i)
  {
    row = mysql_fetch_row(res);
    ASSERT_TRUE(NULL != row);
    lengths = mysql_fetch_lengths(res);
    ASSERT_TRUE(NULL != lengths);
    get_row(i, expected_row, 1024);
    pos = 0;
    for (unsigned int j = 0; j < num_fields; ++j)
    {
      if (j == 0)
      {
        pos += snprintf(text_row+pos, 1024-pos, "'");
      }
      pos += snprintf(text_row+pos, 1024-pos, "%.*s", row[j] ? static_cast<int>(lengths[j]) : static_cast<int>(sizeof("NULL")-1), row[j] ? row[j] : "NULL");
      if (j == 0)
      {
        pos += snprintf(text_row+pos, 1024-pos, "'");
      }
      if (j != num_fields - 1)
      {
        pos += snprintf(text_row+pos, 1024-pos,", ");
      }
    }
    ASSERT_STREQ(expected_row, text_row);
    if (0 != i && i % 200 == 0)
    {
      fprintf(stderr, "fetch %d rows\n", i);
    }
  }
  row = mysql_fetch_row(res);
  ASSERT_TRUE(NULL == row);
  mysql_free_result(res);
  gettimeofday(&tv_end, NULL);
  int64_t start_us = static_cast<int64_t>(tv_start.tv_sec)*1000*1000 + tv_start.tv_usec;
  int64_t end_us = static_cast<int64_t>(tv_end.tv_sec)*1000*1000 + tv_end.tv_usec;
  fprintf(stderr, "performance of table scan: %ld (usec/row)\n", (end_us-start_us)/ROW_COUNT);
}

TEST_F(ObNewSqlTest, comprehensive)
{
  int ROW_COUNT2 = ROW_COUNT;
  ASSERT_EQ(0, ROW_COUNT2 % 12);
  fprintf(stderr, "row_count=%d\n", ROW_COUNT2);
  generate_data(ROW_COUNT2);

  int ret = mysql_query(&my_,
                        "SELECT DISTINCT c1, max(c5), count(c2), sum(c2+c3) "
                        "FROM ob_new_sql_test "
                        "WHERE c5 % 2 = 0 "
                        "GROUP BY c5 "
                        "HAVING sum(c4) % 2 = 0 "
                        "ORDER BY c1 DESC "
                        "LIMIT 5, 20 ");
  if (0 != ret)
  {
    fprintf(stderr, "%s\n", mysql_error(&my_));
  }
  ASSERT_EQ(0, ret);
  MYSQL_RES *res = mysql_use_result(&my_);
  ASSERT_TRUE(NULL != res);
  unsigned int num_fields = mysql_num_fields(res);
  ASSERT_EQ(4U, num_fields);
  // verify field name
  MYSQL_FIELD *fields = mysql_fetch_fields(res);
  ASSERT_TRUE(NULL != fields);
  for (unsigned int i = 0; i < num_fields; ++i)
  {
    printf("field=%u name=%s org_name=%s table=%s org_table=%s db=%s catalog=%s length=%ld max_length=%ld type=%d\n",
           i, fields[i].name, fields[i].org_name,
           fields[i].table, fields[i].org_table,
           fields[i].db, fields[i].catalog,
           fields[i].length, fields[i].max_length,
           fields[i].type);
  }
  unsigned long *lengths = NULL;
  MYSQL_ROW row;
  char text_row[1024];
  int pos = 0;
  int row_count = 0;
  int i = 5;
  // expected row count before limit is INPUT_ROW_COUNT / 12
  for (; row_count < 20; row_count++, i++)
  {
    row = mysql_fetch_row(res);
    ASSERT_TRUE(NULL != row);
    lengths = mysql_fetch_lengths(res);
    ASSERT_TRUE(NULL != lengths);
    // c1
    int n = ROW_COUNT2 / 12;
    int j = n - 1 - i;
    int expect = 6 + j * 12;
    int val = atoi(row[0]);
    ASSERT_TRUE(expect == val || expect + 1 == val || expect + 2 == val);  // because the sort operator for merge_groupby is not stable
    // max(c5)
    expect = 2+j*4;
    val = atoi(row[1]);
    ASSERT_EQ(expect, val);
    // count(c2)
    val = atoi(row[2]);
    ASSERT_EQ(3, val);
    // sum(c2+c3)
    val = atoi(row[3]);
    ASSERT_EQ(4, val);
    pos = 0;
    for (unsigned int j = 0; j < num_fields; ++j)
    {
      pos += snprintf(text_row+pos, 1024-pos, "%.*s", row[j] ? static_cast<int>(lengths[j]) : static_cast<int>(sizeof("NULL")-1), row[j] ? row[j] : "NULL");
      if (j != num_fields - 1)
      {
        pos += snprintf(text_row+pos, 1024-pos,", ");
      }
    }
    //printf("%s\n", text_row);
  }
  row = mysql_fetch_row(res);
  ASSERT_TRUE(NULL == row);
  mysql_free_result(res);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  if (0 != mysql_library_init(0, NULL, NULL))
  {
    fprintf(stderr, "could not init mysql library\n");
    exit(1);
  }
  int ch = 0;
  while (-1 != (ch = getopt(argc, argv, "H:P:N:")))
  {
    switch(ch)
    {
      case 'H':
        HOST = optarg;
        break;
      case 'P':
        PORT = atoi(optarg);
        break;
      case 'N':
        ROW_COUNT = atoi(optarg);
        ROW_COUNT = ROW_COUNT / 12 * 12;
        if (ROW_COUNT <= 0)
        {
          ROW_COUNT = 1200;
        }
        break;
      case '?':
        fprintf(stderr, "option `%c' missing argument\n", optopt);
        exit(0);
      default:
        break;
    }
  }
  int ret = RUN_ALL_TESTS();
  mysql_library_end();
  return ret;
}
