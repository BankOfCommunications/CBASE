/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_session_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <mysql/mysql.h>
#include <gtest/gtest.h>
#include <sys/time.h>

static const char* HOST = "10.232.36.42";
static int PORT = 45447;
static int SESSION_COUNT = 10000;
static const char* USER = "admin";
static const char* PASSWD = "admin";
int64_t get_time()
{
  int64_t ret = 0;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  ret = tv.tv_sec * 1000000LL + tv.tv_usec;
  return ret;
}
class ObSessionTest: public ::testing::Test
{
  public:
    ObSessionTest();
    virtual ~ObSessionTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSessionTest(const ObSessionTest &other);
    ObSessionTest& operator=(const ObSessionTest &other);
  protected:
    void prepare_select(int s);
    void prepare_replace(int s);
    void prepare_update(int s);
  protected:
    // data members
    MYSQL *my_;
};

ObSessionTest::ObSessionTest()
  :my_(NULL)
{
}

ObSessionTest::~ObSessionTest()
{
}

void ObSessionTest::SetUp()
{
  MYSQL my;
  ASSERT_TRUE(NULL != mysql_init(&my));
  fprintf(stderr, "Connecting server %s:%d...\n", HOST, PORT);
  ASSERT_TRUE(NULL != mysql_real_connect(&my, HOST, USER, PASSWD, "test", PORT, NULL, 0));
  int ret = mysql_query(&my, "create table if not exists session_test (c1 int primary key, c2 int, c3 int, c4 int, c5 int, c6 int ,c7 int, c8 int, c9 int, c10 int)");
  ASSERT_EQ(0, ret);
  mysql_close(&my);
}

void ObSessionTest::TearDown()
{
}

#define ASSERT_QUERY_RET(ret)\
  do                         \
  {                          \
  if (0 != ret)              \
  {                                             \
    fprintf(stderr, "%s\n", mysql_stmt_error(stmt)); \
  }                                             \
  ASSERT_EQ(0, ret);                            \
  }while (0)

void ObSessionTest::prepare_select(int s)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_[s]);
  ASSERT_TRUE(stmt);
  static const char* SELECT_QUERY = "select * from session_test where c1 = ?";
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, SELECT_QUERY, strlen(SELECT_QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(1, param_count);
  MYSQL_RES *prepare_meta = mysql_stmt_result_metadata(stmt);
  ASSERT_TRUE(prepare_meta);
  int num_fields = mysql_num_fields(prepare_meta);
  ASSERT_EQ(10, num_fields);

  // 2. bind params
  MYSQL_BIND bind[1];
  memset(bind, 0, sizeof(bind));
/* INTEGER PARAM */
/* This is a number type, so there is no need
   to specify buffer_length */
  int int_data = 0;
  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= (char *)&int_data;
  bind[0].is_null= 0;
  bind[0].length= 0;
  ret = mysql_stmt_bind_param(stmt, bind);
  ASSERT_QUERY_RET(ret);
  // 3. execute
  int_data = 2;
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  // 4. bind results
  MYSQL_BIND row[10];
  my_bool is_null[10];
  my_bool error[10];
  unsigned long length[10];
  memset(row, 0, sizeof(row));
  // int column
  int row_data[10];
  for (int i = 0; i < 10; ++i)
  {
    row[i].buffer_type = MYSQL_TYPE_LONG;
    row[i].buffer = (char*)&row_data[i];
    row[i].is_null = &is_null[i];
    row[i].length = &length[i];
    row[i].error = &error[i];
  }
  ret = mysql_stmt_bind_result(stmt, row);
  ASSERT_QUERY_RET(ret);

  // 5. fetch rows
  fprintf(stdout, "Fetching results...\n");
  int row_count = 0;
  while (!mysql_stmt_fetch(stmt))
  {
    ++row_count;
    fprintf(stdout, "row=%d ", row_count);
  } // end while
  ASSERT_EQ(0, row_count);
  mysql_free_result(prepare_meta);
  // 6. do not close_stmt
}

void ObSessionTest::prepare_replace(int s)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_[s]);
  ASSERT_TRUE(stmt);
  static const char* SELECT_QUERY = "replace into session_test (c1) values(?)";
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, SELECT_QUERY, strlen(SELECT_QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(1, param_count);
  // 2. bind params
  MYSQL_BIND bind[1];
  memset(bind, 0, sizeof(bind));
/* INTEGER PARAM */
/* This is a number type, so there is no need
   to specify buffer_length */
  int int_data = 0;
  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= (char *)&int_data;
  bind[0].is_null= 0;
  bind[0].length= 0;
  ret = mysql_stmt_bind_param(stmt, bind);
  ASSERT_QUERY_RET(ret);
  // 3. execute
  int_data = 0;
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  // 4. results
  my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
  ASSERT_EQ(1U, affected_rows);
}

void ObSessionTest::prepare_update(int s)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_[s]);
  ASSERT_TRUE(stmt);
  const char* QUERY = "update session_test set c3 = c1 where c1 = ?";
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, QUERY, strlen(QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(1, param_count);
  // 2. bind params
  MYSQL_BIND bind[1];
  memset(bind, 0, sizeof(bind));
  int int_data = 0;
  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= (char *)&int_data;
  bind[0].is_null= 0;
  bind[0].length= 0;
  ret = mysql_stmt_bind_param(stmt, bind);
  ASSERT_QUERY_RET(ret);
  // 3. execute
  int_data = 0;
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  // 4. results
  my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
  ASSERT_EQ(1U, affected_rows);
}

TEST_F(ObSessionTest, basic_test)
{
  int64_t total_connect_timeu = 0;
  my_ = new(std::nothrow) MYSQL[SESSION_COUNT];
  ASSERT_TRUE(my_);
  for (int s = 0; s < SESSION_COUNT; ++s)
  {
    ASSERT_TRUE(NULL != mysql_init(&my_[s]));
    fprintf(stderr, "[%d] Connecting server %s:%d...\n", s, HOST, PORT);
    int64_t start_us = get_time();
    if (!mysql_real_connect(&my_[s], HOST, USER, PASSWD, "test", PORT, NULL, 0))
    {
      fprintf(stderr, "Failed to connect to database: Error: %s\n",
              mysql_error(&my_[s]));
      ASSERT_TRUE(0);
    }
    total_connect_timeu += get_time() - start_us;

    for (int j = 0; j < 5; ++j)
    {
      prepare_select(s);
    }
    for (int j = 0; j < 4; ++j)
    {
      prepare_replace(s);
    }
    /*
    for (int j = 0; j < 2; ++j)
    {
      prepare_update(s);
    }
    */
    if (s == SESSION_COUNT/2)
    {
      for (int i = 100; i >= 0; --i)
      {
        printf("\rsleep(%d)...", i);
        fflush(stdout);
        sleep(1);
      }
    }
  }
  printf("average connect rt=%ld\n", total_connect_timeu/SESSION_COUNT);
  for (int i = 100; i >= 0; --i)
  {
    printf("\rsleep(%d)...", i);
    fflush(stdout);
    sleep(1);
  }
  printf("\n");
  for (int s = 0; s < SESSION_COUNT; ++s)
  {
    mysql_close(&my_[s]);
  }
  delete [] my_;
  my_ = NULL;
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
  while (-1 != (ch = getopt(argc, argv, "H:P:N:u:p:")))
  {
    switch(ch)
    {
      case 'H':
        HOST = optarg;
        break;
      case 'P':
        PORT = atoi(optarg);
        break;
      case 'u':
        USER = optarg;
        break;
      case 'p':
        PASSWD = optarg;
        break;
      case 'N':
        SESSION_COUNT = atoi(optarg);
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
