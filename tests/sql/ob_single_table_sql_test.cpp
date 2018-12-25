/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_single_table_sql_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_sql.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObSingleTableSQLTest: public ::testing::Test
{
  public:
    ObSingleTableSQLTest();
    virtual ~ObSingleTableSQLTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSingleTableSQLTest(const ObSingleTableSQLTest &other);
    ObSingleTableSQLTest& operator=(const ObSingleTableSQLTest &other);
  private:
    // data members
};

ObSingleTableSQLTest::ObSingleTableSQLTest()
{
}

ObSingleTableSQLTest::~ObSingleTableSQLTest()
{
}

void ObSingleTableSQLTest::SetUp()
{
}

void ObSingleTableSQLTest::TearDown()
{
}

TEST_F(ObSingleTableSQLTest, basic_test)
{
  tbsys::CConfig::getCConfig().load("ob_single_table_sql_test_schema.ini");
  ObResultSet result_set;
  ObSqlContext context;
  const char* const stmt = "SELECT DISTINCT c1, max(c5), count(c2), sum(c2+c3) FROM fake_table "
    "WHERE c5 % 2 = 0 GROUP BY c5 HAVING sum(c4) % 2 = 0 ORDER BY c1 DESC LIMIT 20, 5;";
  ObString strstmt;
  strstmt.assign(const_cast<char*>(stmt), static_cast<int64_t>(strlen(stmt)));
  //ASSERT_EQ(OB_NOT_INIT, ObSql::direct_execute(strstmt, result_set, context));
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
