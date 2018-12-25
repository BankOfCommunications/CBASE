/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_postfix_expression_test.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */

/*
 * del all test case
 * dragon [repair_test] 2016-12-30
 * 说明：cb更改了后缀表达式的cacl函数，导致此文件编译时会提示未定义引用
 * 如有需要,请自行更改测试案例
 */

#include "sql/ob_postfix_expression.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include <gtest/gtest.h>
#include "sql/ob_postfix_expression.h"
#include "common/ob_string_buf.h"
#include "common/ob_schema.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObPostfixExpressionTest: public ::testing::Test
{
  public:
    ObPostfixExpressionTest();
    virtual ~ObPostfixExpressionTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObPostfixExpressionTest(const ObPostfixExpressionTest &other);
    ObPostfixExpressionTest& operator=(const ObPostfixExpressionTest &other);
  private:
    // data members
};

ObPostfixExpressionTest::ObPostfixExpressionTest()
{
}

ObPostfixExpressionTest::~ObPostfixExpressionTest()
{
}

void ObPostfixExpressionTest::SetUp()
{
}

void ObPostfixExpressionTest::TearDown()
{
}

// testcase:
// 19 + 2
//TEST_F(ObPostfixExpressionTest, basic_test)
//{
//  ASSERT_EQ(0, 0);
//  sql::ObPostfixExpression p;
//  printf("sizeof(ObPostfixExpression)=%lu\n", sizeof(p));
//  ExprItem item_a, item_b, item_add;
//  item_a.type_ = T_INT;
//  item_b.type_ = T_INT;
//  item_add.type_ = T_OP_ADD;
//  item_a.value_.int_ = 19;
//  item_b.value_.int_ = 2;
//  item_add.value_.int_ = 2;

//  p.add_expr_item(item_a);
//  p.add_expr_item(item_b);
//  p.add_expr_item(item_add);
//  p.add_expr_item_end();

//  printf("%s\n", to_cstring(p));

//  ObRow row;
//  const ObObj *result = NULL;
//  p.calc(row, result);

//  int64_t re;
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 21);
//}
//// testcase:like function
//TEST_F(ObPostfixExpressionTest, like_func)
//{
//  ExprItem item_left, item_right, item_like;
//  char target[20];
//  memset(target, 0, sizeof(target));
//  char pattern[20];
//  memset(pattern, 0, sizeof(pattern));
//  char buff[1024];
//  ObRow row;

//  sql::ObPostfixExpression p;
//  item_left.type_ = T_STRING;

//  target[0] = 'a';
//  target[1] = 'a';
//  target[2] = 'a';
//  item_left.string_.assign_ptr(target, 3);

//  pattern[0] = '%';
//  pattern[1] = '_';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 2);

//  item_like.type_ = T_OP_LIKE;
//  item_like.value_.int_ = 2;

//  p.add_expr_item(item_left);
//  p.add_expr_item(item_right);
//  p.add_expr_item(item_like);
//  p.add_expr_item_end();
//  p.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result = NULL;
//  bool b = false;
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(b));
//  ASSERT_EQ(true, b);
////======================================================
//  sql::ObPostfixExpression p2;
//  memset(target, 0, sizeof(target));
//  target[0] = 'a';
//  target[1] = 'a';
//  target[2] = 'a';
//  item_left.string_.assign_ptr(target, 3);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 1);

//  p2.add_expr_item(item_left);
//  p2.add_expr_item(item_right);
//  p2.add_expr_item(item_like);
//  p2.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p2.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result2 = NULL;
//  bool b2 = false;
//  ASSERT_EQ(OB_SUCCESS, p2.calc(row, result2));
//  ASSERT_EQ(OB_SUCCESS, result2->get_bool(b2));
//  ASSERT_EQ(true, b2);
////======================================================
//  sql::ObPostfixExpression p3;
//  memset(target, 0, sizeof(target));
//  target[0] = '\\';
//  item_left.string_.assign_ptr(target, 1);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '\\';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 1);

//  p3.add_expr_item(item_left);
//  p3.add_expr_item(item_right);
//  p3.add_expr_item(item_like);
//  p3.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p3.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result3 = NULL;
//  bool b3 = false;
//  ASSERT_EQ(OB_SUCCESS, p3.calc(row, result3));
//  ASSERT_EQ(OB_SUCCESS, result3->get_bool(b3));
//  ASSERT_EQ(true, b3);

////===================================================
//  sql::ObPostfixExpression p4;
//  memset(target, 0, sizeof(target));
//  target[0] = '\\';
//  target[1] = '%';
//  item_left.string_.assign_ptr(target, 2);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '\\';
//  pattern[1] = '\\';
//  pattern[2] = '\\';
//  pattern[3] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 4);

//  p4.add_expr_item(item_left);
//  p4.add_expr_item(item_right);
//  p4.add_expr_item(item_like);
//  p4.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p4.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result4 = NULL;
//  bool b4 = false;
//  ASSERT_EQ(OB_SUCCESS, p4.calc(row, result4));
//  ASSERT_EQ(OB_SUCCESS, result4->get_bool(b4));
//  ASSERT_EQ(true, b4);
//  //============================
//  sql::ObPostfixExpression p5;
//  memset(target, 0, sizeof(target));
//  target[0] = '\\';
//  target[1] = '%';
//  item_left.string_.assign_ptr(target, 2);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = 'b';
//  pattern[1] = 'b';
//  pattern[2] = 'c';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 3);

//  p5.add_expr_item(item_left);
//  p5.add_expr_item(item_right);
//  p5.add_expr_item(item_like);
//  p5.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p5.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result5 = NULL;
//  bool b5 = false;
//  ASSERT_EQ(OB_SUCCESS, p5.calc(row, result5));
//  ASSERT_EQ(OB_SUCCESS, result5->get_bool(b5));
//  ASSERT_FALSE(b5);
//  //==================================================
//  sql::ObPostfixExpression p6;
//  memset(target, 0, sizeof(target));
//  target[0] = 'X';
//  target[1] = 'a';
//  target[2] = 'b';
//  target[3] = 'c';
//  target[4] = 'Y';
//  target[5] = 'a';
//  target[6] = 'b';
//  target[7] = 'c';
//  target[8] = 'Z';
//  target[9] = 'a';
//  target[10] = 'b';
//  target[11] = 'c';
//  item_left.string_.assign_ptr(target, 12);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  pattern[1] = 'a';
//  pattern[2] = 'b';
//  pattern[3] = 'c';
//  pattern[4] = '%';
//  pattern[5] = 'a';
//  pattern[6] = 'b';
//  pattern[7] = 'c';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 8);

//  p6.add_expr_item(item_left);
//  p6.add_expr_item(item_right);
//  p6.add_expr_item(item_like);
//  p6.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p6.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result6 = NULL;
//  bool b6 = false;
//  ASSERT_EQ(OB_SUCCESS, p6.calc(row, result6));
//  ASSERT_EQ(OB_SUCCESS, result6->get_bool(b6));
//  ASSERT_EQ(true, b6);
//  //========================================================================
//  sql::ObPostfixExpression p7;
//  memset(target, 0, sizeof(target));
//  target[0] = 'a';
//  target[1] = 'b';
//  target[2] = 'c';
//  item_left.string_.assign_ptr(target, 3);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = 'a';
//  pattern[1] = '%';
//  pattern[2] = '%';
//  pattern[3] = '%';
//  pattern[4] = 'c';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 5);

//  p7.add_expr_item(item_left);
//  p7.add_expr_item(item_right);
//  p7.add_expr_item(item_like);
//  p7.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p6.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result7 = NULL;
//  bool b7 = false;
//  ASSERT_EQ(OB_SUCCESS, p7.calc(row, result7));
//  ASSERT_EQ(OB_SUCCESS, result7->get_bool(b7));
//  ASSERT_EQ(true, b7);
//  //============================================================
//  sql::ObPostfixExpression p8;
//  memset(target, 0, sizeof(target));
//  target[0] = '_';
//  item_left.string_.assign_ptr(target, 1);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '\\';
//  pattern[1] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 2);

//  p8.add_expr_item(item_left);
//  p8.add_expr_item(item_right);
//  p8.add_expr_item(item_like);
//  p8.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p6.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result8 = NULL;
//  bool b8 = false;
//  ASSERT_EQ(OB_SUCCESS, p8.calc(row, result8));
//  ASSERT_EQ(OB_SUCCESS, result8->get_bool(b8));
//  ASSERT_FALSE(b8);
//  //============================================================
//  sql::ObPostfixExpression p9;
//  memset(target, 0, sizeof(target));
//  target[0] = '\\';
//  target[1] = '%';
//  item_left.string_.assign_ptr(target, 2);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 1);

//  p9.add_expr_item(item_left);
//  p9.add_expr_item(item_right);
//  p9.add_expr_item(item_like);
//  p9.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p9.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result9 = NULL;
//  bool b9 = false;
//  ASSERT_EQ(OB_SUCCESS, p9.calc(row, result9));
//  ASSERT_EQ(OB_SUCCESS, result9->get_bool(b9));
//  ASSERT_EQ(true, b9);
//  //============================================================
//  sql::ObPostfixExpression p10;
//  memset(target, 0, sizeof(target));
//  target[0] = '\\';
//  item_left.string_.assign_ptr(target, 1);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '\\';
//  pattern[1] = '\\';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 2);

//  p10.add_expr_item(item_left);
//  p10.add_expr_item(item_right);
//  p10.add_expr_item(item_like);
//  p10.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p10.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result10 = NULL;
//  bool b10 = false;
//  ASSERT_EQ(OB_SUCCESS, p10.calc(row, result10));
//  ASSERT_EQ(OB_SUCCESS, result10->get_bool(b10));
//  ASSERT_EQ(true, b10);
//  //============================================================
//  sql::ObPostfixExpression p11;
//  memset(target, 0, sizeof(target));
//  item_left.string_.assign_ptr(target, 0);
//  memset(pattern, 0, sizeof(pattern));
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 0);

//  p11.add_expr_item(item_left);
//  p11.add_expr_item(item_right);
//  p11.add_expr_item(item_like);
//  p11.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p11.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result11 = NULL;
//  bool b11 = false;
//  ASSERT_EQ(OB_SUCCESS, p11.calc(row, result11));
//  ASSERT_EQ(OB_SUCCESS, result11->get_bool(b11));
//  ASSERT_EQ(true, b11);
//  //============================================================
//  sql::ObPostfixExpression p12;
//  memset(target, 0, sizeof(target));
//  item_left.string_.assign_ptr(target, 0);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 1);

//  p12.add_expr_item(item_left);
//  p12.add_expr_item(item_right);
//  p12.add_expr_item(item_like);
//  p12.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p12.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result12 = NULL;
//  bool b12 = false;
//  ASSERT_EQ(OB_SUCCESS, p12.calc(row, result12));
//  ASSERT_EQ(OB_SUCCESS, result12->get_bool(b12));
//  ASSERT_EQ(true, b12);
//  //============================================================
//  sql::ObPostfixExpression p13;
//  memset(target, 0, sizeof(target));
//  item_left.string_.assign_ptr(target, 0);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  pattern[1] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 2);

//  p13.add_expr_item(item_left);
//  p13.add_expr_item(item_right);
//  p13.add_expr_item(item_like);
//  p13.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p13.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result13 = NULL;
//  bool b13 = false;
//  ASSERT_EQ(OB_SUCCESS, p13.calc(row, result13));
//  ASSERT_EQ(OB_SUCCESS, result13->get_bool(b13));
//  ASSERT_EQ(true, b13);
//  //============================================================
//  sql::ObPostfixExpression p14;
//  memset(target, 0, sizeof(target));
//  item_left.string_.assign_ptr(target, 0);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '\\';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 1);

//  p14.add_expr_item(item_left);
//  p14.add_expr_item(item_right);
//  p14.add_expr_item(item_like);
//  p14.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p14.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result14 = NULL;
//  bool b14 = false;
//  ASSERT_EQ(OB_SUCCESS, p14.calc(row, result14));
//  ASSERT_EQ(OB_SUCCESS, result14->get_bool(b14));
//  ASSERT_FALSE(b14);
//  //============================
//  sql::ObPostfixExpression p15;
//  memset(target, 0, sizeof(target));
//  target[0] = 'a';
//  target[1] = '%';
//  item_left.string_.assign_ptr(target, 2);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  pattern[1] = '\\';
//  pattern[2] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 3);

//  p15.add_expr_item(item_left);
//  p15.add_expr_item(item_right);
//  p15.add_expr_item(item_like);
//  p15.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p15.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result15 = NULL;
//  bool b15 = false;
//  ASSERT_EQ(OB_SUCCESS, p15.calc(row, result15));
//  ASSERT_EQ(OB_SUCCESS, result15->get_bool(b15));
//  ASSERT_EQ(true, b15);
//  //============================
//  sql::ObPostfixExpression p16;
//  memset(target, 0, sizeof(target));
//  target[0] = '\\';
//  target[1] = 'a';
//  target[2] = 'b';
//  target[3] = 'c';
//  item_left.string_.assign_ptr(target, 4);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  pattern[1] = '\\';
//  pattern[2] = '%';
//  pattern[3] = '%';
//  pattern[4] = 'a';
//  pattern[5] = 'b';
//  pattern[6] = 'c';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 7);

//  p16.add_expr_item(item_left);
//  p16.add_expr_item(item_right);
//  p16.add_expr_item(item_like);
//  p16.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p16.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result16 = NULL;
//  bool b16 = false;
//  ASSERT_EQ(OB_SUCCESS, p16.calc(row, result16));
//  ASSERT_EQ(OB_SUCCESS, result16->get_bool(b16));
//  ASSERT_FALSE(b16);
//  //============================
//  sql::ObPostfixExpression p17;
//  memset(target, 0, sizeof(target));
//  target[0] = 'a';
//  target[1] = '%';
//  target[2] = 'b';
//  item_left.string_.assign_ptr(target, 3);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  pattern[1] = '%';
//  pattern[2] = 'a';
//  pattern[3] = '\\';
//  pattern[4] = '%';
//  pattern[5] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 6);

//  p17.add_expr_item(item_left);
//  p17.add_expr_item(item_right);
//  p17.add_expr_item(item_like);
//  p17.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p17.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result17 = NULL;
//  bool b17 = false;
//  ASSERT_EQ(OB_SUCCESS, p17.calc(row, result17));
//  ASSERT_EQ(OB_SUCCESS, result17->get_bool(b17));
//  ASSERT_EQ(true, b17);
//	//==================================================
//  sql::ObPostfixExpression p18;
//  memset(target, 0, sizeof(target));
//  target[0] = 'a';
//  target[1] = 'b';
//  target[2] = 'c';
//	target[3] = 'd';
//  item_left.string_.assign_ptr(target, 4);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = 'a';
//  pattern[1] = 'b';
//  pattern[2] = 'c';
//  pattern[3] = 'd';
//  pattern[4] = '%';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 5);

//  p18.add_expr_item(item_left);
//  p18.add_expr_item(item_right);
//  p18.add_expr_item(item_like);
//  p18.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p18.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result18 = NULL;
//  bool b18 = false;
//  ASSERT_EQ(OB_SUCCESS, p18.calc(row, result18));
//  ASSERT_EQ(OB_SUCCESS, result18->get_bool(b18));
//  ASSERT_EQ(true, b18);
//	//==================================================
//  sql::ObPostfixExpression p19;
//  memset(target, 0, sizeof(target));
//  target[0] = 'a';
//  target[1] = 'b';
//  target[2] = 'c';
//	target[3] = 'x';
//  item_left.string_.assign_ptr(target, 4);
//  memset(pattern, 0, sizeof(pattern));
//  pattern[0] = '%';
//  pattern[1] = '%';
//  pattern[2] = 'x';
//  item_right.type_ = T_STRING;
//  item_right.string_.assign_ptr(pattern, 3);

//  p19.add_expr_item(item_left);
//  p19.add_expr_item(item_right);
//  p19.add_expr_item(item_like);
//  p19.add_expr_item_end();
//  memset(buff, 0, sizeof(buff));
//  p19.to_string(buff, 1024);
//  printf("buff=%s\n", buff);

//  const ObObj *result19 = NULL;
//  bool b19 = false;
//  ASSERT_EQ(OB_SUCCESS, p19.calc(row, result19));
//  ASSERT_EQ(OB_SUCCESS, result19->get_bool(b19));
//  ASSERT_EQ(true, b19);


//}

//// testcase:
//// c1 + c2
//TEST_F(ObPostfixExpressionTest, row_add_test)
//{
//  ASSERT_EQ(0, 0);
//  sql::ObPostfixExpression p;
//  ExprItem item_a, item_b, item_add;
//  ObObj obj_a, obj_b;

//  item_a.type_ = T_REF_COLUMN;
//  item_b.type_ = T_REF_COLUMN;
//  item_add.type_ = T_OP_ADD;
//  item_a.value_.cell_.tid = 1001;
//  item_a.value_.cell_.cid = 16;
//  item_b.value_.cell_.tid = 1001;
//  item_b.value_.cell_.cid = 17;
//  item_add.value_.int_ = 2;


//  p.add_expr_item(item_a);
//  p.add_expr_item(item_b);
//  p.add_expr_item(item_add);

//  printf("%s\n", to_cstring(p));

//  ObRow row;
//  ObRowDesc row_desc;

//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  row.set_row_desc(row_desc);
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

//  const ObObj *result = NULL;
//  p.add_expr_item_end();
//  p.calc(row, result);

//  int64_t re;
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 21);
//}

//// testcase:
//// c1 + 2
//TEST_F(ObPostfixExpressionTest, row_int_add_test)
//{
//  ASSERT_EQ(0, 0);
//  sql::ObPostfixExpression p;
//  ExprItem item_a, item_b, item_add;
//  ObObj obj_a, obj_b;

//  item_a.type_ = T_REF_COLUMN;
//  item_b.type_ = T_INT;
//  item_add.type_ = T_OP_ADD;
//  item_a.value_.cell_.tid = 1001;
//  item_a.value_.cell_.cid = 16;
//  item_b.value_.int_ = 2;
//  item_add.value_.int_ = 2;


//  p.add_expr_item(item_a);
//  p.add_expr_item(item_b);
//  p.add_expr_item(item_add);
//  p.add_expr_item_end();

//  printf("%s\n", to_cstring(p));

//  ObRow row;
//  ObRowDesc row_desc;

//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  row.set_row_desc(row_desc);
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

//  const ObObj *result = NULL;
//  p.calc(row, result);

//  int64_t re;
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 21);
//}


//// testcase:
//TEST_F(ObPostfixExpressionTest, single_element_test)
//{
//  ASSERT_EQ(0, 0);
//  sql::ObPostfixExpression p;
//  ExprItem item_a;
//  item_a.type_ = T_INT;
//  item_a.value_.int_ = 21;

//  p.add_expr_item(item_a);

//  ObRow row;
//  const ObObj *result = NULL;
//  p.add_expr_item_end();
//  p.calc(row, result);

//  int64_t re;
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 21);
//}

//// length(c1)
//TEST_F(ObPostfixExpressionTest, sys_func_length_test)
//{
//  sql::ObPostfixExpression p;
//  ExprItem item_a, item_length;

//  item_a.type_ = T_REF_COLUMN;
//  item_a.value_.cell_.tid = 1001;
//  item_a.value_.cell_.cid = 16;

//  item_length.type_ = T_FUN_SYS;
//  item_length.value_.int_ = 1;       // one parameter
//  item_length.string_.assign_ptr(const_cast<char*>("LenGth"), static_cast<int32_t>(strlen("LenGth")));

//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_a));
//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_length));
//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item_end());

//  char buff[1204];
//  p.to_string(buff, 1024);
//  printf("%s\n", buff);

//  ObRow row;
//  ObRowDesc row_desc;
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  row.set_row_desc(row_desc);
//  ObString str_a;
//  str_a.assign_ptr(const_cast<char*>("hello world"), static_cast<int32_t>(strlen("hello world")));
//  ObObj obj_a;
//  obj_a.set_varchar(str_a);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));

//  const ObObj *result = NULL;
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_TRUE(result);
//  int64_t re;
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 11);
//}

//TEST_F(ObPostfixExpressionTest, sys_func_unknown_test)
//{
//  sql::ObPostfixExpression p;
//  ExprItem item_a, item_length;

//  item_a.type_ = T_REF_COLUMN;
//  item_a.value_.cell_.tid = 1001;
//  item_a.value_.cell_.cid = 16;

//  item_length.type_ = T_FUN_SYS;
//  item_length.value_.int_ = 1;       // one parameter
//  item_length.string_.assign_ptr(const_cast<char*>("kaka"), static_cast<int32_t>(strlen("kaka")));

//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_a));
//  ASSERT_EQ(OB_ERR_UNKNOWN_SYS_FUNC, p.add_expr_item(item_length));
//}

//TEST_F(ObPostfixExpressionTest, is_simple_cond_test)
//{
//  sql::ObPostfixExpression p;
//  ExprItem item_a, item_op, item_const;
//  /* column_16 <= 10 */
//  item_a.type_ = T_REF_COLUMN;
//  item_a.value_.cell_.tid = 1001;
//  item_a.value_.cell_.cid = 16;
//  item_op.type_ = T_OP_LE;
//  item_op.value_.int_ = 2;  /* 2 operands */
//  item_const.type_ = T_INT;
//  item_const.value_.int_ = 10;
//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_a));
//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_const));
//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_op));
//  ASSERT_EQ(OB_SUCCESS, p.add_expr_item_end());
//  uint64_t cid = OB_INVALID_ID;
//  int64_t op = T_INVALID;
//  ObObj val1;
//  ASSERT_EQ(true, p.is_simple_condition(false, cid, op, val1));
//  ASSERT_EQ(16U, cid);
//  ASSERT_EQ(T_OP_LE, op);
//  ASSERT_EQ(ObIntType, val1.get_type());
//  int64_t val = 0;
//  val1.get_int(val);
//  ASSERT_EQ(10, val);
//}


//// testcase:
//// c1 in (1,3,4), c1 in (null)
//TEST_F(ObPostfixExpressionTest, in_func_1_dim_test)
//{
//  ObRow row;
//  ObRowDesc row_desc;
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  row.set_row_desc(row_desc);
//  ObObj obj_a, obj_b;
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

//  const ObObj *result = NULL;
//  bool re;

//  ExprItem item_left, item_a, item_b, item_c;
//  ExprItem op_left_end, op_in, op_row;

//  item_left.type_ = T_INT;
//  item_left.value_.int_ = 2;
//  item_a.type_ = T_INT;
//  item_b.type_ = T_INT;
//  item_c.type_ = T_INT;
//  item_a.value_.int_ = 1;
//  item_b.value_.int_ = 3;
//  item_c.value_.int_ = 4;

//  op_left_end.type_ = T_OP_LEFT_PARAM_END;
//  op_left_end.value_.int_ = 1;
//  op_in.type_ = T_OP_IN;
//  op_in.value_.int_= 2;
//  op_row.type_ = T_OP_ROW;
//  op_row.value_.int_ = 3;

//  // 2 in (1,3,4)
//  sql::ObPostfixExpression p;
//  p.add_expr_item(item_left);
//  p.add_expr_item(op_left_end);
//  p.add_expr_item(item_a);
//  p.add_expr_item(item_b);
//  p.add_expr_item(item_c);
//  p.add_expr_item(op_row);
//  p.add_expr_item(op_in);
//  p.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, false);

//  // 4 in (1,3,4)
//  sql::ObPostfixExpression p2;
//  item_left.value_.int_ = 4;
//  p2.add_expr_item(item_left);
//  p2.add_expr_item(op_left_end);
//  p2.add_expr_item(item_a);
//  p2.add_expr_item(item_b);
//  p2.add_expr_item(item_c);
//  p2.add_expr_item(op_row);
//  p2.add_expr_item(op_in);
//  p2.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p2.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, true);

//  // 4 in (3, null, null)
//  sql::ObPostfixExpression p3;
//  item_a.type_ = T_INT;
//  item_a.value_.int_ = 3;
//  item_b.type_ = T_NULL;
//  item_c.type_ = T_NULL;
//  item_left.value_.int_ = 4;
//  p3.add_expr_item(item_left);
//  p3.add_expr_item(op_left_end);
//  p3.add_expr_item(item_a);
//  p3.add_expr_item(item_b);
//  p3.add_expr_item(item_c);
//  p3.add_expr_item(op_row);
//  p3.add_expr_item(op_in);
//  p3.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p3.calc(row, result));
//  ASSERT_TRUE(result->is_null());

//  // 4 in (3, null, 4)
//  sql::ObPostfixExpression p4;
//  item_a.type_ = T_INT;
//  item_a.value_.int_ = 3;
//  item_b.type_ = T_NULL;
//  item_c.type_ = T_INT;
//  item_c.value_.int_ = 4;
//  item_left.value_.int_ = 4;
//  p4.add_expr_item(item_left);
//  p4.add_expr_item(op_left_end);
//  p4.add_expr_item(item_a);
//  p4.add_expr_item(item_b);
//  p4.add_expr_item(item_c);
//  p4.add_expr_item(op_row);
//  p4.add_expr_item(op_in);
//  p4.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p4.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, true);

//  // null in (1,3,4)
//  {
//    sql::ObPostfixExpression p2;
//    item_left.type_ = T_NULL;
//    p2.add_expr_item(item_left);
//    p2.add_expr_item(op_left_end);
//    p2.add_expr_item(item_a);
//    p2.add_expr_item(item_b);
//    p2.add_expr_item(item_c);
//    p2.add_expr_item(op_row);
//    p2.add_expr_item(op_in);
//    p2.add_expr_item_end();
//    ASSERT_EQ(OB_SUCCESS, p2.calc(row, result));
//    ASSERT_FALSE(result->is_null());
//  }
//}

//// testcase:
//// (c1,c2,c3) in ((1,3,6))
//TEST_F(ObPostfixExpressionTest, in_simple_in_expression_extract_rowkeyvalue_test)
//{
//  ObRowkeyInfo rowkey_info;
//  ObRowkeyColumn columns[3];
//  static const int column_count = 3;
//  int i = 0;
//  int cid_begin = 16;
  
//  for (i = 0; i < column_count; i++)
//  {
//    // init rowkey info
//    columns[i].column_id_ = i  + cid_begin;
//    columns[i].type_ = ObIntType;
//    rowkey_info.add_column(columns[i]);
//  }

//  ObRow row;
//  ObRowDesc row_desc;
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 18));
//  row.set_row_desc(row_desc);
//  ObObj obj_a, obj_b, obj_c;
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  obj_c.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 18, obj_b));


//  sql::ObPostfixExpression p, p2;
//  ExprItem item_left_1, item_left_2, item_left_3;
//  ExprItem item_a, item_b, item_c, item_d, item_e, item_f;
//  ExprItem op_left_end, op_in;
//  ExprItem op_row_left_1, op_row_right_1, op_row_right_2, op_row_right_3;

//  item_left_1.type_ = T_REF_COLUMN;
//  item_left_1.value_.cell_.tid = 1001;
//  item_left_1.value_.cell_.cid = 16;
//  item_left_2.type_ = T_REF_COLUMN;
//  item_left_2.value_.cell_.tid = 1001;
//  item_left_2.value_.cell_.cid = 17;
//  item_left_3.type_ = T_REF_COLUMN;
//  item_left_3.value_.cell_.tid = 1001;
//  item_left_3.value_.cell_.cid = 18;

//  item_a.type_ = T_INT;
//  item_b.type_ = T_INT;
//  item_c.type_ = T_INT;
//  item_d.type_ = T_INT;
//  item_e.type_ = T_INT;
//  item_f.type_ = T_INT;

//  item_a.value_.int_ = 19;
//  item_b.value_.int_ = 2;
//  item_c.value_.int_ = 2;
//  item_d.value_.int_ = 4;
//  item_e.value_.int_ = 2;
//  item_f.value_.int_ = 4;


//  op_left_end.type_ = T_OP_LEFT_PARAM_END;
//  op_left_end.value_.int_ = 2;
//  op_in.type_ = T_OP_IN;
//  op_in.value_.int_ = 2;

//  op_row_left_1.type_ = T_OP_ROW;
//  op_row_right_1.type_ = T_OP_ROW;
//  op_row_right_3.type_ = T_OP_ROW;
//  op_row_left_1.value_.int_ = 3;
//  op_row_right_1.value_.int_ = 3;
//  op_row_right_3.value_.int_ = 1; // outter bracket

//  // (1,2,3) in ((1,3,4))
//  p.add_expr_item(item_left_1);
//  p.add_expr_item(item_left_2);
//  p.add_expr_item(item_left_3);
//  p.add_expr_item(op_row_left_1);
//  p.add_expr_item(op_left_end);
//  p.add_expr_item(item_a);
//  p.add_expr_item(item_b);
//  p.add_expr_item(item_c);
//  p.add_expr_item(op_row_right_1);
//  p.add_expr_item(op_row_right_3);
//  p.add_expr_item(op_in);
//  p.add_expr_item_end();

//  const ObObj *result = NULL;
//  bool re;
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, true);
 
//  //ObArray<ObRowkey> rowkey_array;
//  //ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
//  //TBSYS_LOG(INFO, "str=%s", to_cstring(p));
//  //ASSERT_EQ(true, p.is_simple_in_expr(rowkey_info, rowkey_array, rowkey_objs));
//  //TBSYS_LOG(INFO, "array=%s", to_cstring(rowkey_array));
//}

//// testcase:
//// (c1,c2) in ((1,3), (2, 4))
//TEST_F(ObPostfixExpressionTest, in_func_2_dim_test)
//{
//  ObRow row;
//  ObRowDesc row_desc;
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  row.set_row_desc(row_desc);
//  ObObj obj_a, obj_b;
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));


//  sql::ObPostfixExpression p, p2;
//  ExprItem item_left_1, item_left_2;
//  ExprItem item_a, item_b, item_c, item_d;
//  ExprItem op_left_end, op_in;
//  ExprItem op_row_left_1, op_row_right_1, op_row_right_2, op_row_right_3;

//  item_left_1.type_ = T_INT;
//  item_left_1.value_.int_ = 2;
//  item_left_2.type_ = T_INT;
//  item_left_2.value_.int_ = 3;

//  item_a.type_ = T_INT;
//  item_b.type_ = T_INT;
//  item_c.type_ = T_INT;
//  item_d.type_ = T_INT;
//  item_a.value_.int_ = 1;
//  item_b.value_.int_ = 3;
//  item_c.value_.int_ = 2;
//  item_d.value_.int_ = 4;

//  op_left_end.type_ = T_OP_LEFT_PARAM_END;
//  op_left_end.value_.int_ = 2;
//  op_in.type_ = T_OP_IN;

//  op_row_left_1.type_ = T_OP_ROW;
//  op_row_right_1.type_ = T_OP_ROW;
//  op_row_right_2.type_ = T_OP_ROW;
//  op_row_right_3.type_ = T_OP_ROW;
//  op_row_left_1.value_.int_ = 2;
//  op_row_right_1.value_.int_ = 2;
//  op_row_right_2.value_.int_ = 2;
//  op_row_right_3.value_.int_ = 2;

//  // (2,3) in ((1,3), (2, 4))
//  p.add_expr_item(item_left_1);
//  p.add_expr_item(item_left_2);
//  p.add_expr_item(op_row_left_1);
//  p.add_expr_item(op_left_end);
//  p.add_expr_item(item_a);
//  p.add_expr_item(item_b);
//  p.add_expr_item(op_row_right_1);
//  p.add_expr_item(item_c);
//  p.add_expr_item(item_d);
//  p.add_expr_item(op_row_right_2);
//  p.add_expr_item(op_row_right_3);
//  p.add_expr_item(op_in);
//  p.add_expr_item_end();

//  const ObObj *result = NULL;
//  bool re;
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, false);

//  // (2,4) in ((1,3), (2, 4))
//  item_left_1.value_.int_ = 2;
//  item_left_2.value_.int_ = 4;
//  p2.add_expr_item(item_left_1);
//  p2.add_expr_item(item_left_2);
//  p2.add_expr_item(op_row_left_1);
//  p2.add_expr_item(op_left_end);
//  p2.add_expr_item(item_a);
//  p2.add_expr_item(item_b);
//  p2.add_expr_item(op_row_right_1);
//  p2.add_expr_item(item_c);
//  p2.add_expr_item(item_d);
//  p2.add_expr_item(op_row_right_2);
//  p2.add_expr_item(op_row_right_3);
//  p2.add_expr_item(op_in);
//  p2.add_expr_item_end();

//  ASSERT_EQ(OB_SUCCESS, p2.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, true);
//}

//// testcase:
//// (c1,c2) in ((true, c3 in (4, 5)), (false, true))
//TEST_F(ObPostfixExpressionTest, in_func_nested_mix_dim_test)
//{
//  ObRow row;
//  ObRowDesc row_desc;
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  row.set_row_desc(row_desc);
//  ObObj obj_a, obj_b;
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

//  const ObObj *result = NULL;
//  bool re;

//  ExprItem item_left_1, item_left_2;
//  ExprItem item_1, item_2, item_3, item_4, item_5, item_6, item_7, item_8;
//  ExprItem op_left_end_1, op_left_end_2;
//  ExprItem op_in;
//  ExprItem op_row_2, op_row_3;

//  item_left_1.type_ = T_BOOL;
//  item_left_2.type_ = T_BOOL;
//  item_left_1.value_.bool_ = true;
//  item_left_2.value_.bool_ = true;
//  item_1.type_ = T_BOOL;
//  item_2.type_ = T_INT;
//  item_3.type_ = T_INT;
//  item_4.type_ = T_INT;
//  item_5.type_ = T_BOOL;
//  item_6.type_ = T_BOOL;
//  item_7.type_ = T_INT;
//  item_8.type_ = T_INT;
//  //item_7.type_ = T_BOOL;
//  //item_8.type_ = T_BOOL;
//  item_1.value_.bool_ = false;
//  item_2.value_.int_ = 4;
//  item_3.value_.int_ = 4;
//  item_4.value_.int_ = 5;
//  item_5.value_.bool_ = false;
//  item_6.value_.bool_ = true;
//  item_7.value_.int_ = 0;
//  item_8.value_.int_ = 8;
//  //item_7.value_.bool_ = false;
//  //item_8.value_.bool_ = false;

//  op_left_end_1.type_ = T_OP_LEFT_PARAM_END;
//  op_left_end_1.value_.int_ = 2;
//  op_left_end_2.type_ = T_OP_LEFT_PARAM_END;
//  op_left_end_2.value_.int_ = 1;
//  op_in.type_ = T_OP_IN;
//  op_row_2.type_ = T_OP_ROW;
//  op_row_2.value_.int_ = 2;
//  op_row_3.type_ = T_OP_ROW;
//  op_row_3.value_.int_ = 3;

//  // (true, true) in ((false, 4 in (4, 5)), (false, true), (0, 8))
//  sql::ObPostfixExpression p;
//  p.add_expr_item(item_left_1);
//  p.add_expr_item(item_left_2);
//  p.add_expr_item(op_row_2);
//  p.add_expr_item(op_left_end_1);
//  p.add_expr_item(item_1);
//  p.add_expr_item(item_2);
//  p.add_expr_item(op_left_end_2);
//  p.add_expr_item(item_3);
//  p.add_expr_item(item_4);
//  p.add_expr_item(op_row_2);
//  p.add_expr_item(op_in);
//  p.add_expr_item(op_row_2);
//  p.add_expr_item(item_5);
//  p.add_expr_item(item_6);
//  p.add_expr_item(op_row_2);
//  p.add_expr_item(item_7);
//  p.add_expr_item(item_8);
//  p.add_expr_item(op_row_2);
//  p.add_expr_item(op_row_3);
//  p.add_expr_item(op_in);
//  p.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, false);

//  // (true, true) in ((false, 4 in (4, 5)), (true, true), (0, 8))
//  sql::ObPostfixExpression p2;
//  item_5.value_.bool_ = true;
//  p2.add_expr_item(item_left_1);
//  p2.add_expr_item(item_left_2);
//  p2.add_expr_item(op_row_2);
//  p2.add_expr_item(op_left_end_1);
//  p2.add_expr_item(item_1);
//  p2.add_expr_item(item_2);
//  p2.add_expr_item(op_left_end_2);
//  p2.add_expr_item(item_3);
//  p2.add_expr_item(item_4);
//  p2.add_expr_item(op_row_2);
//  p2.add_expr_item(op_in);
//  p2.add_expr_item(op_row_2);
//  p2.add_expr_item(item_5);
//  p2.add_expr_item(item_6);
//  p2.add_expr_item(op_row_2);
//  p2.add_expr_item(item_7);
//  p2.add_expr_item(item_8);
//  p2.add_expr_item(op_row_2);
//  p2.add_expr_item(op_row_3);
//  p2.add_expr_item(op_in);
//  p2.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p2.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
//  ASSERT_EQ(re, true);


//}


//// testcase:
//// case c1 when 10 then 100 else 200
//TEST_F(ObPostfixExpressionTest, arg_case_func_test)
//{
//  ObRow row;
//  ObRowDesc row_desc;
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  row.set_row_desc(row_desc);
//  ObObj obj_a, obj_b;
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

//  const ObObj *result = NULL;
//  int64_t re;

//  sql::ObPostfixExpression p;
//  ExprItem item_case, item_when_1, item_then_1, item_else;
//  ExprItem item_when_2, item_then_2;
//  ExprItem op_arg_case;

//  item_case.type_ = T_INT;
//  item_case.value_.int_ = 10;
//  item_when_1.type_ = T_INT;
//  item_then_1.type_ = T_INT;
//  item_when_2.type_ = T_INT;
//  item_then_2.type_ = T_INT;
//  item_else.type_ = T_INT;
//  item_when_1.value_.int_ = 10;
//  item_then_1.value_.int_ = 100;
//  item_when_2.value_.int_ = 13;
//  item_then_2.value_.int_ = 250;
//  item_else.value_.int_ = 200;

//  // case 10
//  //  when 10 then 100
//  //  else 200
//  op_arg_case.type_ = T_OP_ARG_CASE;
//  op_arg_case.value_.int_ = 4;
//  p.add_expr_item(item_case);
//  p.add_expr_item(item_when_1);
//  p.add_expr_item(item_then_1);
//  p.add_expr_item(item_else);
//  p.add_expr_item(op_arg_case);
//  p.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 100);

//  // case 13
//  //  when 10 then 100
//  //  else 200
//  sql::ObPostfixExpression p2;
//  item_case.value_.int_ = 13;
//  op_arg_case.value_.int_ = 4;
//  p2.add_expr_item(item_case);
//  p2.add_expr_item(item_when_1);
//  p2.add_expr_item(item_then_1);
//  p2.add_expr_item(item_else);
//  p2.add_expr_item(op_arg_case);
//  p2.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p2.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 200);

//  // case 13
//  //  when 10 then 100
//  //  when 13 then 250
//  //  else 200
//  sql::ObPostfixExpression p3;
//  item_case.value_.int_ = 13;
//  op_arg_case.value_.int_ = 6;
//  p3.add_expr_item(item_case);
//  p3.add_expr_item(item_when_1);
//  p3.add_expr_item(item_then_1);
//  p3.add_expr_item(item_when_2);
//  p3.add_expr_item(item_then_2);
//  p3.add_expr_item(item_else);
//  p3.add_expr_item(op_arg_case);
//  p3.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p3.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 250);

//  // case 14
//  //  when 10 then 100
//  //  when 13 then 250
//  sql::ObPostfixExpression p4;
//  item_case.value_.int_ = 14000;
//  op_arg_case.value_.int_ = 5;
//  p4.add_expr_item(item_case);
//  p4.add_expr_item(item_when_1);
//  p4.add_expr_item(item_then_1);
//  p4.add_expr_item(item_when_2);
//  p4.add_expr_item(item_then_2);
//  p4.add_expr_item(op_arg_case);
//  p4.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p4.calc(row, result));
//  ASSERT_EQ(true, result->is_null());

//}
//// testcase:
//// case when true then 100 else 200
//// case when false then 100 when true then 200
//TEST_F(ObPostfixExpressionTest, case_func_test)
//{
//  ObRow row;
//  ObRowDesc row_desc;
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
//  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
//  row.set_row_desc(row_desc);
//  ObObj obj_a, obj_b;
//  obj_a.set_int(19);
//  obj_b.set_int(2);
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
//  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

//  const ObObj *result = NULL;
//  int64_t re;

//  sql::ObPostfixExpression p;
//  ExprItem item_when_1, item_then_1, item_else;
//  ExprItem item_when_2, item_then_2;
//  ExprItem op_arg_case;

//  item_when_1.type_ = T_BOOL;
//  item_then_1.type_ = T_INT;
//  item_when_2.type_ = T_BOOL;
//  item_then_2.type_ = T_INT;
//  item_else.type_ = T_INT;
//  item_when_1.value_.bool_ = false;
//  item_then_1.value_.int_ = 100;
//  item_when_2.value_.bool_ = true;
//  item_then_2.value_.int_ = 250;
//  item_else.value_.int_ = 200;

//  // case
//  //  when true then 100
//  //  else 200
//  op_arg_case.type_ = T_OP_CASE;
//  op_arg_case.value_.int_ = 3;
//  item_when_1.value_.bool_ = true;
//  p.add_expr_item(item_when_1);
//  p.add_expr_item(item_then_1);
//  p.add_expr_item(item_else);
//  p.add_expr_item(op_arg_case);
//  p.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 100);

//  // case
//  //  when false then 100
//  //  else 200
//  sql::ObPostfixExpression p2;
//  op_arg_case.value_.int_ = 3;
//  item_when_1.value_.bool_ = false;
//  p2.add_expr_item(item_when_1);
//  p2.add_expr_item(item_then_1);
//  p2.add_expr_item(item_else);
//  p2.add_expr_item(op_arg_case);
//  p2.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p2.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 200);

//  // case
//  //  when false then 100
//  //  when true then 250
//  //  else 200
//  sql::ObPostfixExpression p3;
//  op_arg_case.value_.int_ = 5;
//  item_when_1.value_.bool_ = false;
//  item_when_2.value_.bool_ = true;
//  p3.add_expr_item(item_when_1);
//  p3.add_expr_item(item_then_1);
//  p3.add_expr_item(item_when_2);
//  p3.add_expr_item(item_then_2);
//  p3.add_expr_item(item_else);
//  p3.add_expr_item(op_arg_case);
//  p3.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p3.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 250);

//  // case
//  //  when false then 100
//  //  when false then 250
//  sql::ObPostfixExpression p4;
//  op_arg_case.value_.int_ = 4;
//  item_when_1.value_.bool_ = false;
//  item_when_2.value_.bool_ = false;
//  p4.add_expr_item(item_when_1);
//  p4.add_expr_item(item_then_1);
//  p4.add_expr_item(item_when_2);
//  p4.add_expr_item(item_then_2);
//  p4.add_expr_item(op_arg_case);
//  p4.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p4.calc(row, result));
//  ASSERT_EQ(true, result->is_null());

//  // case
//  //  when false then 100
//  //  when false then 250
//  //  else 200
//  sql::ObPostfixExpression p5;
//  op_arg_case.value_.int_ = 5;
//  item_when_1.value_.bool_ = false;
//  item_when_2.value_.bool_ = false;
//  p5.add_expr_item(item_when_1);
//  p5.add_expr_item(item_then_1);
//  p5.add_expr_item(item_when_2);
//  p5.add_expr_item(item_then_2);
//  p5.add_expr_item(item_else);
//  p5.add_expr_item(op_arg_case);
//  p5.add_expr_item_end();
//  ASSERT_EQ(OB_SUCCESS, p5.calc(row, result));
//  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
//  ASSERT_EQ(re, 200);


//}


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
