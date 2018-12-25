/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_expression_test.cpp,  06/04/2012 05:47:34 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   Test ObSqlExpression class
 *
 */
#include "common/ob_malloc.h"
#include "common/utility.h"
#include <gtest/gtest.h>
#include "common/ob_string_buf.h"
#include "sql/ob_sql_expression.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;


class ObSqlExpressionTest: public ::testing::Test
{
  public:
    ObSqlExpressionTest();
    virtual ~ObSqlExpressionTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSqlExpressionTest(const ObSqlExpressionTest &other);
    ObSqlExpressionTest& operator=(const ObSqlExpressionTest &other);
  private:
    // data members
};

ObSqlExpressionTest::ObSqlExpressionTest()
{
}

ObSqlExpressionTest::~ObSqlExpressionTest()
{
}

void ObSqlExpressionTest::SetUp()
{
}

void ObSqlExpressionTest::TearDown()
{
}


TEST_F(ObSqlExpressionTest, single_element_test)
{
  ObSqlExpression p;
  ExprItem item_a;
  item_a.type_ = T_INT;
  item_a.value_.int_ = 21;

  p.add_expr_item(item_a);

  ObRow row;
  const ObObj *result = NULL;
  p.add_expr_item_end();
  p.calc(row, result);
  printf("%s\n", to_cstring(p));
  int64_t re = 0;
  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
  ASSERT_EQ(re, 21);
}

TEST_F(ObSqlExpressionTest, copy_operator_test)
{
  ObString stdstring;
  stdstring.assign((char*)"hello world", 11);

  ObSqlExpression p;
  ExprItem item_b;
  item_b.type_ = T_STRING;
  item_b.string_.assign((char*)"hello world", 11);
  p.add_expr_item(item_b);

  ObRow row;
  const ObObj *result = NULL;
  p.add_expr_item_end();
  p.calc(row, result);

  //int64_t re;
  ObString mystring;
  EXPECT_EQ(OB_SUCCESS, result->get_varchar(mystring));
  EXPECT_EQ(mystring, stdstring);

  // test deep copy
  ObSqlExpression q;
  q = p;
  p.~ObSqlExpression();
  memset(&p, 0, sizeof(p));
  EXPECT_EQ(OB_SUCCESS, q.calc(row, result));
  EXPECT_EQ(OB_SUCCESS, result->get_varchar(mystring));
  EXPECT_EQ(mystring, stdstring);
  TBSYS_LOG(INFO, "%.*s", mystring.length(), mystring.ptr());
  //ASSERT_EQ();
}

TEST_F(ObSqlExpressionTest, array_of_expr)
{
  ObArray<ObSqlExpression> exprs;
  ObSqlExpression expr;
  exprs.push_back(expr);
  exprs.push_back(expr);
  exprs.clear();
  exprs.reserve(10);
  exprs.push_back(expr);
}



int main(int argc, char **argv)
{
    ob_init_memory_pool();
      ::testing::InitGoogleTest(&argc,argv);
        return RUN_ALL_TESTS();
}
