/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include <gtest/gtest.h>
#include <mysql/mysql.h>
#include <gtest/gtest.h>
#include <sys/time.h>

const char* HOST = "127.0.0.1";
int PORT = 3306;
int ROW_COUNT = 0;
bool GEN_DATA = false;

#define ERR_OUT(fmt, arg...) fprintf(stderr, "%s:%d " fmt, __FILE__, __LINE__, ##arg)

int mysql_query_with_err(MYSQL *mysql, const char *stmt)
{
  int ret = 0;
  ret = mysql_query(mysql, stmt);
  if(0 == ret)
  {
    const char *sqlstate = mysql_sqlstate(mysql);
    if(0 != strcmp("00000", sqlstate))
    {
      ret = 1;
      ERR_OUT("server error sqlstate[%s]\n", sqlstate);
    }
  }
  else
  {
    ERR_OUT("mysql query fail:ret[%d]\n", ret);
  }
  return ret;
}

class ObTabletJoinTest: public ::testing::Test
{
  public:
    ObTabletJoinTest();
    virtual ~ObTabletJoinTest();
    virtual void SetUp();
    virtual void TearDown();

    const static int MY_INT_MAX = 10000;

    int rand_int()
    {
      return rand_int(MY_INT_MAX);
    }

    int rand_int(int max)
    {
      return abs(rand()) % max;
    }

    char *rand_string(char *buf, int size)
    {
      snprintf(buf, size, "%06d", rand_int(1000000));
      return buf;
    }

    const char *rand_string()
    {
      static char buf[1024];
      return rand_string(buf, 1024);
    }

    int replace_into(const char *stmt)
    {
      int ret = 0;
      ret = mysql_query_with_err(&my_, stmt);
      if(0 != ret)
      {
        ERR_OUT("mysql replace fail:[%d]\n", ret);
        ERR_OUT("%s\n", stmt);
      }
      return ret;
    }

    int gen_no_join_data(int start, int end, FILE *right_table)
    {
      int ret = 0;
      if (NULL == right_table)
      {
        ERR_OUT("right table is null");
        ret = -1;
      }
      if (0 == ret)
      {
        for(int i=start;0 == ret && i<=end;i++)
        {
          fprintf(right_table, "\'key2_%05d\', \'%s\', %d, \'%s\'\n", i, rand_string(), rand_int(), rand_string()); 
        }
      }
      return ret;
    }

    int write_to_ob(const char *table_name, const char *table_desc, const char *data_file)
    {
      int ret = 0;
      FILE * fp = fopen(data_file, "r");
      if(NULL == fp)
      {
        ERR_OUT("data_file[%s] does not exist", data_file);
        ret = 1;
      }

      const int STMT_SIZE = 1024 * 100; //100K
      char *stmt = new(std::nothrow) char[STMT_SIZE];
      stmt[0] = '\0';

      char row[1024];
      char tmp_row[1024];
      bool row_valid = false;
      int count = 0;
      if(0 == ret)
      {
        while(0 == ret)
        {
          if(!row_valid)
          {
            if(NULL == fgets(row, 1024, fp))
            {
              ret = replace_into(stmt);
              printf("replace to %s: %d \n", table_name, count);
              break;
            }
            else
            {
              if(strlen(row) > 0)
              {
                row[strlen(row) - 1] = '\0';
              }
              row_valid = true;
            }
          }

          sprintf(tmp_row, ",(%s)", row);
          if( strlen(tmp_row) + strlen(stmt) <= STMT_SIZE - 1)
          {
            if(strlen(stmt) == 0)
            {
              sprintf(tmp_row, "replace into %s(%s) values(%s)", table_name, table_desc, row);
            }
            strcat(stmt, tmp_row);
            count ++;
            row_valid = false;
          }
          else
          {
            ret = replace_into(stmt);
            if (0 != ret)
            {
              break;
            }
            printf("replace to %s: %d \n", table_name, count);
            stmt[0] = '\0';
          }
        }
      }

      fclose(fp);
      delete stmt;
      stmt = NULL;
      return ret;
    }

    int gen_data()
    {
      int ret = 0;
      int row_count = ROW_COUNT;
      int no_join_row_count = ROW_COUNT / 2;
      int max_left_row_count = 5;
      char key2[1024];

      FILE *left_table = fopen("/tmp/left_table", "w");
      if (NULL == left_table)
      {
        ERR_OUT("fail to open /tmp/left_table, please check");
        return 1;
      }
      FILE *right_table = fopen("/tmp/right_table", "w");
      if (NULL == right_table)
      {
        ERR_OUT("fail to open /tmp/right_table, please check");
        return 1;
      }
      FILE *result_table1 = fopen("/tmp/result_table1", "w");
      if (NULL == result_table1)
      {
        ERR_OUT("fail to open /tmp/result_table1, please check");
        return 1;
      }
      FILE *result_table2 = fopen("/tmp/result_table2", "w");
      if (NULL == result_table2)
      {
        ERR_OUT("fail to open /tmp/result_table2, please check");
        return 1;
      }

      for(int i=0;0 == ret && i<row_count;i++)
      {
        snprintf(key2, 1024, "key2_%05d", i);
        if(0 != (ret = gen_part_data(key2, rand_int(max_left_row_count), (rand_int() % 2) == 0, left_table, right_table, result_table1, result_table2)))
        {
          ERR_OUT( "gen part data fail:ret[%d]", ret);
        }
      }
      
      if(0 == ret)
      {
        if(0 != (ret = gen_no_join_data(row_count, row_count + no_join_row_count - 1, right_table)))
        {
          ERR_OUT( "gen no join data fail:ret[%d]", ret);
        }
      }

      fclose(left_table);
      fclose(right_table);
      fclose(result_table1);
      fclose(result_table2);

      if( 0 != write_to_ob("left_table", "k1, k2, v1", "/tmp/left_table") )
      {
        return 1;
      }
      if (0 != write_to_ob("right_table", "k2, v2, v3, v4", "/tmp/right_table") )
      {
        return 1;
      }
      if ( 0 != write_to_ob("result_table", "k1, k2, v1, v2, v3", "/tmp/result_table1") )
      {
        return 1;
      }
      if ( 0 != write_to_ob("result_table", "k1, k2, v1", "/tmp/result_table2") )
      {
        return 1;
      }

      return ret;
    }

    int gen_part_data(const char* key2, int64_t left_row_count, bool has_join, FILE *left_table, FILE *right_table, FILE *result_table1, FILE *result_table2)
    {
      int ret = 0;

      char v2[1024];
      rand_string(v2, 1024);
      int v3 = rand_int();

      int left_table_v1 = 0;

      if (NULL == left_table || NULL == right_table || NULL == result_table1 || NULL == result_table2)
      {
        ERR_OUT("left_table[%p], right_table[%p], result_table1[%p], result_table2[%p]", left_table, right_table, result_table1, result_table2);
        return 1;
      }

      if(has_join)
      {
        fprintf(right_table, "\'%s\', \'%s\', %d, \'%s\'\n", key2, v2, v3, rand_string()); 
      }

      if(0 == ret)
      {
        for(int i=0;0 == ret && i<left_row_count;i++)
        {
          left_table_v1 = rand_int();

          fprintf(left_table, "\'key1_%05d\', \'%s\', %d\n", i, key2, left_table_v1);

          if(0 == ret)
          {
            //生成result_table row
            if(has_join)
            {
              fprintf(result_table1, "\'key1_%05d\', \'%s\', %d, \'%s\', %d\n", i, key2, left_table_v1, v2, v3);
            }
            else
            {
              fprintf(result_table2, "\'key1_%05d\', \'%s\', %d\n", i, key2, left_table_v1);
            }
          }
        }
      }

      return ret;
    }

  private:
    // disallow copy
    ObTabletJoinTest(const ObTabletJoinTest &other);
    ObTabletJoinTest& operator=(const ObTabletJoinTest &other);
  protected:
    // data members
    MYSQL my_;
};

ObTabletJoinTest::ObTabletJoinTest()
{
}

ObTabletJoinTest::~ObTabletJoinTest()
{
}

void ObTabletJoinTest::SetUp()
{
  ASSERT_TRUE(NULL != mysql_init(&my_));
  ERR_OUT( "Connecting server %s:%d...\n", HOST, PORT);
  ASSERT_TRUE(NULL != mysql_real_connect(&my_, HOST, "admin", "admin", "test_table_join", PORT, NULL, 0));
}

void ObTabletJoinTest::TearDown()
{
  mysql_close(&my_);
}

TEST_F(ObTabletJoinTest, basic_test)
{
  srand((unsigned int)time(NULL));
  if(GEN_DATA)
  {
    printf("gen test data\n");
    ASSERT_EQ(0, gen_data());
  }

  ASSERT_EQ(0, mysql_query_with_err(&my_, "select * from left_table"));
  MYSQL_RES *res1 = mysql_store_result(&my_);
  ASSERT_TRUE(NULL != res1);

  unsigned int num_fields = mysql_num_fields(res1);
  unsigned long num_rows = mysql_num_rows(res1);
  printf("num_fields [%u]\n", num_fields);
  printf("num_rows [%lu]\n", num_rows);

  ASSERT_EQ(0, mysql_query_with_err(&my_, "select * from result_table"));
  MYSQL_RES *res2 = mysql_store_result(&my_);
  ASSERT_TRUE(NULL != res2);

  unsigned int num_fields2 = mysql_num_fields(res2);
  unsigned long num_rows2 = mysql_num_rows(res2);
  printf("num_fields2 [%u]\n", num_fields2);
  printf("num_rows2 [%lu]\n", num_rows2);

  ASSERT_EQ(num_fields, num_fields2);
  ASSERT_EQ(num_rows, num_rows2);
  
  MYSQL_ROW left_table_row;
  MYSQL_ROW result_table_row;

  printf("check data...\n");
  for(int i=0;i<(int)num_rows;i++)
  {
    left_table_row = mysql_fetch_row(res1);
    result_table_row = mysql_fetch_row(res2);
    ASSERT_TRUE(left_table_row != NULL) << "row num " << 1 << std::endl;
    ASSERT_TRUE(result_table_row != NULL) << "row num " << 1 << std::endl;

    if(i % 1000 == 0)
    {
      printf("%d rows has finshed\n", i);
    }

    //rowkey can not be null
    ASSERT_TRUE(NULL != left_table_row[0]);
    ASSERT_TRUE(NULL != result_table_row[0]);
    ASSERT_TRUE(NULL != left_table_row[1]);
    ASSERT_TRUE(NULL != result_table_row[1]);

    for(int j=0;j<(int)num_fields;j++)
    {
      char buf[1024];
      buf[0] = '\0';
      strcat(buf, "left_table_row ");
      for(int k=0;k<(int)num_fields;k++)
      {
        strcat(buf, left_table_row[k] == NULL ? "NULL": left_table_row[k]);
        strcat(buf, " ");
      }
      strcat(buf, "\nresult_table_row ");
      for(int k=0;k<(int)num_fields;k++)
      {
        strcat(buf, result_table_row[k] == NULL ? "NULL": result_table_row[k]);
        strcat(buf, " ");
      }
      ASSERT_STREQ(left_table_row[j], result_table_row[j]) << buf << std::endl;
    }
  }
  mysql_free_result(res1);
  mysql_free_result(res2);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);

  if (0 != mysql_library_init(0, NULL, NULL))
  {
    ERR_OUT( "could not init mysql library\n");
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
        GEN_DATA = true;
        ROW_COUNT = atoi(optarg);
        if (ROW_COUNT <= 0)
        {
          printf("row count should be positive: set to 100\n");
          ROW_COUNT = 100;
        }
        break;
      case '?':
        ERR_OUT( "option `%c' missing argument\n", optopt);
        exit(0);
      default:
        break;
    }
  }
  int ret = RUN_ALL_TESTS();
  mysql_library_end();
  return ret;
}



