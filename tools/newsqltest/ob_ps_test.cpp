/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ps_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <mysql/mysql.h>
#include <gtest/gtest.h>
#include <sys/time.h>
static const char* HOST = "127.0.0.1";
static int PORT = 3306;
static int ROW_COUNT = 4800;
static const char* USER = "";
static const char* PASSWD = "";

class ObPSTest: public ::testing::Test
{
  public:
    ObPSTest();
    virtual ~ObPSTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    void create_table();
    void delete_table();
    void generate_data();
    // disallow copy
    ObPSTest(const ObPSTest &other);
    ObPSTest& operator=(const ObPSTest &other);
  protected:
    void select_test(int exp_row_count);
    void replace_test(bool is_replace);
    void delete_test();
    void update_test();
    void no_param_test();
    // data members
    MYSQL my_;
};

ObPSTest::ObPSTest()
{
}

ObPSTest::~ObPSTest()
{
}

void ObPSTest::SetUp()
{
  ASSERT_TRUE(NULL != mysql_init(&my_));
  fprintf(stderr, "Connecting server %s:%d...\n", HOST, PORT);
  ASSERT_TRUE(NULL != mysql_real_connect(&my_, HOST, USER, PASSWD, "test", PORT, NULL, 0));
  delete_table();
  create_table();
  generate_data();
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

void ObPSTest::create_table()
{
  int ret = mysql_query(&my_, "create table ob_ps_test (c1 int primary key, c2 int, c3 varchar(256))");
  ASSERT_EQ(0, ret);
}

void ObPSTest::delete_table()
{
  int ret = mysql_query(&my_, "drop table if exists ob_ps_test");
  ASSERT_EQ(0, ret);
}

void ObPSTest::generate_data()
{
  int ret = mysql_query(&my_, "insert into ob_ps_test values (1, NULL, 'aa'), (2, NULL, 'bb'), (3, NULL, 'cc')");
  ASSERT_EQ(0, ret);
}

void ObPSTest::TearDown()
{
  mysql_close(&my_);
}

void ObPSTest::select_test(int exp_row_count)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_);
  ASSERT_TRUE(stmt);
  static const char* SELECT_QUERY = "select * from ob_ps_test where c1 >= ?";
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, SELECT_QUERY, strlen(SELECT_QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(1, param_count);
  MYSQL_RES *prepare_meta = mysql_stmt_result_metadata(stmt);
  ASSERT_TRUE(prepare_meta);
  int num_fields = mysql_num_fields(prepare_meta);
  ASSERT_EQ(3, num_fields);
  MYSQL_FIELD *fields = mysql_fetch_fields(prepare_meta);
  ASSERT_TRUE(fields);
  // print detailed field info
  for (int i = 0; i < num_fields; ++i)
  {
    fprintf(stdout, "field=%u name=%s org_name=%s table=%s org_table=%s db=%s catalog=%s length=%ld max_length=%ld type=%d\n",
            i, fields[i].name, fields[i].org_name,
            fields[i].table, fields[i].org_table,
            fields[i].db, fields[i].catalog,
            fields[i].length, fields[i].max_length,
            fields[i].type);
  }
  fprintf(stdout, "----------------------------------------------------------------\n");

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
  MYSQL_BIND row[3];
  my_bool is_null[3];
  my_bool error[3];
  unsigned long length[3];
  memset(row, 0, sizeof(row));
  // int column
  int c1_data;
  row[0].buffer_type = MYSQL_TYPE_LONG;
  row[0].buffer = (char*)&c1_data;
  row[0].is_null = &is_null[0];
  row[0].length = &length[0];
  row[0].error = &error[0];
  int c2_data;
  row[1].buffer_type = MYSQL_TYPE_LONG;
  row[1].buffer = (char*)&c2_data;
  row[1].is_null = &is_null[1];
  row[1].length = &length[1];
  row[1].error = &error[1];
  // string column
  static const int STRING_SIZE = 64;
  char c3_data[STRING_SIZE];
  row[2].buffer_type = MYSQL_TYPE_VAR_STRING;
  row[2].buffer = c3_data;
  row[2].buffer_length = STRING_SIZE;
  row[2].is_null = &is_null[2];
  row[2].length = &length[2];
  row[2].error = &error[2];
  ret = mysql_stmt_bind_result(stmt, row);
  ASSERT_QUERY_RET(ret);

  // 5. fetch rows
  fprintf(stdout, "Fetching results...\n");
  int row_count = 0;
  while (!mysql_stmt_fetch(stmt))
  {
    ++row_count;
    fprintf(stdout, "row=%d ", row_count);
    if (is_null[0])
    {
      fprintf(stdout, "c1=NULL ");
    }
    else
    {
      fprintf(stdout, "c1=%d(%ld) ", c1_data, length[0]);
      ASSERT_EQ(row_count+1, c1_data);
    }
    if (is_null[1])
    {
      fprintf(stdout, "c2=NULL ");
    }
    else
    {
      fprintf(stdout, "c2=%d(%ld) ", c2_data, length[1]);
    }
    if (is_null[2])
    {
      fprintf(stdout, "c3=NULL\n");
    }
    else
    {
      fprintf(stdout, "c3=%s(%ld)\n", c3_data, length[2]);
    }
  } // end while
  ASSERT_EQ(exp_row_count, row_count);
  mysql_free_result(prepare_meta);
  ret = mysql_stmt_close(stmt);
  ASSERT_QUERY_RET(ret);
}

TEST_F(ObPSTest, select)
{
  select_test(2);
}

#define OB

void ObPSTest::replace_test(bool is_replace)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_);
  ASSERT_TRUE(stmt);
  const char* QUERY = is_replace ? "replace into ob_ps_test values (?, ?, ?)": "insert into ob_ps_test values (?, ?, ?)";
  fprintf(stdout, "query=%s is_replace=%d\n", QUERY, is_replace);
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, QUERY, strlen(QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(3, param_count);
  // 2. bind params
  MYSQL_BIND bind[3];
  memset(bind, 0, sizeof(bind));
/* INTEGER PARAM */
/* This is a number type, so there is no need
   to specify buffer_length */
  int int_data = 0;
  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= (char *)&int_data;
  bind[0].is_null= 0;
  bind[0].length= 0;
  my_bool is_null = 0;
  bind[1].buffer_type= MYSQL_TYPE_LONG;
  bind[1].buffer= 0;
  bind[1].is_null= &is_null;
  bind[1].length= 0;
  static const int STRING_SIZE = 64;
  char str_data[STRING_SIZE];
  unsigned long str_length;
  bind[2].buffer_type= MYSQL_TYPE_STRING;
  bind[2].buffer= str_data;
  bind[2].is_null= 0;
  bind[2].length= &str_length;
  ret = mysql_stmt_bind_param(stmt, bind);
  ASSERT_QUERY_RET(ret);
  // 3. execute
  int_data = 4;
  is_null = 1;
  strncpy(str_data, "ps1", STRING_SIZE);
  str_length = strlen(str_data);
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  // 4. results
  my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
#ifdef OB
  ASSERT_EQ(is_replace ? 0U : 1U, affected_rows);
#else
  ASSERT_EQ(1U, affected_rows);
#endif
  int_data = 5;
  is_null = 1;
  strncpy(str_data, "ps2", STRING_SIZE);
  str_length = strlen(str_data);
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  affected_rows = mysql_stmt_affected_rows(stmt);
#ifdef OB
  ASSERT_EQ(is_replace ? 0U : 1U, affected_rows);
#else
  ASSERT_EQ(1U, affected_rows);
#endif
  int_data = 5;
  is_null = 1;
  strncpy(str_data, "ps3", STRING_SIZE);
  str_length = strlen(str_data);
  ret = mysql_stmt_execute(stmt);
  if (is_replace)
  {
    ASSERT_QUERY_RET(ret);
    affected_rows = mysql_stmt_affected_rows(stmt);
#ifdef OB
    ASSERT_EQ(0U, affected_rows);
#else
    ASSERT_EQ(2U, affected_rows);
#endif
  }
  else
  {
    ASSERT_EQ(1, ret);
  }
  // 5. clean up
  ret = mysql_stmt_close(stmt);
  ASSERT_QUERY_RET(ret);
}

TEST_F(ObPSTest, replace)
{
  replace_test(true);
  // 6. verify
  select_test(4);
}

TEST_F(ObPSTest, insert)
{
  replace_test(false);
  // 6. verify
  select_test(4);
}

void ObPSTest::delete_test()
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_);
  ASSERT_TRUE(stmt);
  const char* QUERY = "delete from ob_ps_test where c1 = ?";
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
  int_data = 3;
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  // 4. results
  my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
  ASSERT_EQ(1U, affected_rows);

  // 5. clean up
  ret = mysql_stmt_close(stmt);
  ASSERT_QUERY_RET(ret);
}

TEST_F(ObPSTest, delete)
{
  delete_test();
  // 6. verify
  select_test(1);
}

void ObPSTest::update_test()
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_);
  ASSERT_TRUE(stmt);
  const char* QUERY = "update ob_ps_test set c3 = c1 where c1 = ?";
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
  int_data = 3;
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  // 4. results
  my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
  ASSERT_EQ(1U, affected_rows);

  // 5. clean up
  ret = mysql_stmt_close(stmt);
  ASSERT_QUERY_RET(ret);
}

TEST_F(ObPSTest, update)
{
  update_test();
  // 6. verify
  select_test(2);
  // @todo strict verify
}

void ObPSTest::no_param_test()
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_);
  ASSERT_TRUE(stmt);
  const char* QUERY = "delete from ob_ps_test where c1 = 3";
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, QUERY, strlen(QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(0, param_count);
  // 3. execute
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  // 4. results
  my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
  ASSERT_EQ(1U, affected_rows);
  // execute again
  ret = mysql_stmt_execute(stmt);
  ASSERT_QUERY_RET(ret);
  affected_rows = mysql_stmt_affected_rows(stmt);
  ASSERT_EQ(0U, affected_rows);

  // 5. clean up
  ret = mysql_stmt_close(stmt);
  ASSERT_QUERY_RET(ret);
}

TEST_F(ObPSTest, no_param)
{
  no_param_test();
  // 6. verify
  select_test(1);
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
