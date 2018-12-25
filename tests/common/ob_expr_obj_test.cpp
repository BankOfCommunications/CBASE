/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_obj_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_expr_obj.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

#define BLOCK_FUNC()  if (true) \

enum MyBool
{
  MY_TRUE,
  MY_FALSE,
  MY_UNKNOWN
};

class ObExprObjTest: public ::testing::Test
{
  public:
    ObExprObjTest();
    virtual ~ObExprObjTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprObjTest(const ObExprObjTest &other);
    ObExprObjTest& operator=(const ObExprObjTest &other);
  protected:
    ObExprObj bool_obj(MyBool t1);
    void test_and(MyBool t1, MyBool t2, MyBool res);
    void test_or(MyBool t1, MyBool t2, MyBool res);
    void test_not(MyBool t1, MyBool res);
    void test_is(MyBool t1, MyBool t2, MyBool res);
    void test_is_not(MyBool t1, MyBool t2, MyBool res);
};

ObExprObjTest::ObExprObjTest()
{
}

ObExprObjTest::~ObExprObjTest()
{
}

void ObExprObjTest::SetUp()
{
}

void ObExprObjTest::TearDown()
{
}

#define GET_BOOL_EXPECT(type, x, expect)                 \
  do\
  { \
  ObObj obj;                                    \
  obj.set_ ## type(x);\
  ObExprObj t1;\
  t1.assign(obj);                               \
  bool bval = false;\
  ASSERT_EQ(OB_SUCCESS, t1.get_bool(bval));\
  ASSERT_ ## expect(bval);\
  }                 \
  while(0)

TEST_F(ObExprObjTest, get_bool)
{
  GET_BOOL_EXPECT(int, 10, TRUE);
  GET_BOOL_EXPECT(int, -10, TRUE);
  GET_BOOL_EXPECT(int, 0, FALSE);
  GET_BOOL_EXPECT(bool, true, TRUE);
  GET_BOOL_EXPECT(bool, false, FALSE);
  GET_BOOL_EXPECT(float, 1.0f, TRUE);
  GET_BOOL_EXPECT(float, -10.0f, TRUE);
  GET_BOOL_EXPECT(float, 0.0f, FALSE);
  GET_BOOL_EXPECT(double, 1.0, TRUE);
  GET_BOOL_EXPECT(double, -10.0, TRUE);
  GET_BOOL_EXPECT(double, 0.0, FALSE);
  GET_BOOL_EXPECT(datetime, 1234, TRUE);
  GET_BOOL_EXPECT(datetime, 0, FALSE);
  GET_BOOL_EXPECT(precise_datetime, 1234, TRUE);
  GET_BOOL_EXPECT(precise_datetime, 0, FALSE);
  GET_BOOL_EXPECT(modifytime, 1234, TRUE);
  GET_BOOL_EXPECT(modifytime, 0, FALSE);
  GET_BOOL_EXPECT(createtime, 1234, TRUE);
  GET_BOOL_EXPECT(createtime, 0, FALSE);
  // null
  ObExprObj t1;
  bool bval = false;
  t1.set_null();
  ASSERT_NE(OB_SUCCESS, t1.get_bool(bval));
}

ObExprObj ObExprObjTest::bool_obj(MyBool t1)
{
  ObExprObj ret;
  ObObj obj;
  switch(t1)
  {
    case MY_TRUE:
      obj.set_bool(true);
      break;
    case MY_FALSE:
      obj.set_bool(false);
      break;
    default:
      obj.set_null();
      break;
  }
  ret.assign(obj);
  return ret;
}

void ObExprObjTest::test_and(MyBool t1, MyBool t2, MyBool res)
{
  ObExprObj v1 = bool_obj(t1);
  ObExprObj v2 = bool_obj(t2);
  ObExprObj vres;
  ASSERT_EQ(OB_SUCCESS, v1.land(v2, vres));
  switch(res)
  {
    case MY_TRUE:
      ASSERT_TRUE(vres.is_true());
      break;
    case MY_FALSE:
      ASSERT_TRUE(vres.is_false());
      break;
    default:
      ASSERT_TRUE(vres.is_null());
      break;
  }
}

TEST_F(ObExprObjTest, logic_and)
{
  test_and(MY_TRUE, MY_TRUE, MY_TRUE);
  test_and(MY_TRUE, MY_FALSE, MY_FALSE);
  test_and(MY_TRUE, MY_UNKNOWN, MY_UNKNOWN);
  test_and(MY_FALSE, MY_TRUE, MY_FALSE);
  test_and(MY_FALSE, MY_FALSE, MY_FALSE);
  test_and(MY_FALSE, MY_UNKNOWN, MY_FALSE);
  test_and(MY_UNKNOWN, MY_TRUE, MY_UNKNOWN);
  test_and(MY_UNKNOWN, MY_FALSE, MY_FALSE);
  test_and(MY_UNKNOWN, MY_UNKNOWN, MY_UNKNOWN);
}

void ObExprObjTest::test_or(MyBool t1, MyBool t2, MyBool res)
{
  ObExprObj v1 = bool_obj(t1);
  ObExprObj v2 = bool_obj(t2);
  ObExprObj vres;
  ASSERT_EQ(OB_SUCCESS, v1.lor(v2, vres));
  switch(res)
  {
    case MY_TRUE:
      ASSERT_TRUE(vres.is_true());
      break;
    case MY_FALSE:
      ASSERT_TRUE(vres.is_false());
      break;
    default:
      ASSERT_TRUE(vres.is_null());
      break;
  }
}

TEST_F(ObExprObjTest, logic_or)
{
  test_or(MY_TRUE, MY_TRUE, MY_TRUE);
  test_or(MY_TRUE, MY_FALSE, MY_TRUE);
  test_or(MY_TRUE, MY_UNKNOWN, MY_TRUE);
  test_or(MY_FALSE, MY_TRUE, MY_TRUE);
  test_or(MY_FALSE, MY_FALSE, MY_FALSE);
  test_or(MY_FALSE, MY_UNKNOWN, MY_UNKNOWN);
  test_or(MY_UNKNOWN, MY_TRUE, MY_TRUE);
  test_or(MY_UNKNOWN, MY_FALSE, MY_UNKNOWN);
  test_or(MY_UNKNOWN, MY_UNKNOWN, MY_UNKNOWN);
}

void ObExprObjTest::test_not(MyBool t1, MyBool res)
{
  ObExprObj v1 = bool_obj(t1);
  ObExprObj vres;
  ASSERT_EQ(OB_SUCCESS, v1.lnot(vres));
  switch(res)
  {
    case MY_TRUE:
      ASSERT_TRUE(vres.is_true());
      break;
    case MY_FALSE:
      ASSERT_TRUE(vres.is_false());
      break;
    default:
      ASSERT_TRUE(vres.is_null());
      break;
  }
}

TEST_F(ObExprObjTest, logic_not)
{
  test_not(MY_TRUE, MY_FALSE);
  test_not(MY_FALSE, MY_TRUE);
  test_not(MY_UNKNOWN, MY_UNKNOWN);
}

void ObExprObjTest::test_is(MyBool t1, MyBool t2, MyBool res)
{
  ObExprObj v1 = bool_obj(t1);
  ObExprObj v2 = bool_obj(t2);
  ObExprObj vres;
  ASSERT_EQ(OB_SUCCESS, v1.is(v2, vres));
  switch(res)
  {
    case MY_TRUE:
      ASSERT_TRUE(vres.is_true());
      break;
    case MY_FALSE:
      ASSERT_TRUE(vres.is_false());
      break;
    default:
      ASSERT_TRUE(vres.is_null());
      break;
  }
}

TEST_F(ObExprObjTest, logic_is)
{
  test_is(MY_TRUE, MY_TRUE, MY_TRUE);
  test_is(MY_TRUE, MY_FALSE, MY_FALSE);
  test_is(MY_TRUE, MY_UNKNOWN, MY_FALSE);
  test_is(MY_FALSE, MY_TRUE, MY_FALSE);
  test_is(MY_FALSE, MY_FALSE, MY_TRUE);
  test_is(MY_FALSE, MY_UNKNOWN, MY_FALSE);
  test_is(MY_UNKNOWN, MY_TRUE, MY_FALSE);
  test_is(MY_UNKNOWN, MY_FALSE, MY_FALSE);
  test_is(MY_UNKNOWN, MY_UNKNOWN, MY_TRUE);
}

void ObExprObjTest::test_is_not(MyBool t1, MyBool t2, MyBool res)
{
  ObExprObj v1 = bool_obj(t1);
  ObExprObj v2 = bool_obj(t2);
  ObExprObj vres;
  ASSERT_EQ(OB_SUCCESS, v1.is_not(v2, vres));
  switch(res)
  {
    case MY_TRUE:
      ASSERT_TRUE(vres.is_true());
      break;
    case MY_FALSE:
      ASSERT_TRUE(vres.is_false());
      break;
    default:
      ASSERT_TRUE(vres.is_null());
      break;
  }
}

TEST_F(ObExprObjTest, logic_is_not)
{
  test_is_not(MY_TRUE, MY_TRUE, MY_FALSE);
  test_is_not(MY_TRUE, MY_FALSE, MY_TRUE);
  test_is_not(MY_TRUE, MY_UNKNOWN, MY_TRUE);
  test_is_not(MY_FALSE, MY_TRUE, MY_TRUE);
  test_is_not(MY_FALSE, MY_FALSE, MY_FALSE);
  test_is_not(MY_FALSE, MY_UNKNOWN, MY_TRUE);
  test_is_not(MY_UNKNOWN, MY_TRUE, MY_TRUE);
  test_is_not(MY_UNKNOWN, MY_FALSE, MY_TRUE);
  test_is_not(MY_UNKNOWN, MY_UNKNOWN, MY_FALSE);
}

TEST(ObExprObj,Math)
{
  // +=, -=, *=, /= %=
  BLOCK_FUNC()
  {
    ObExprObj t1;
    ObExprObj t2;
    int64_t i2 = 0;
    ObExprObj res;
    ObString str_temp;
    char * temp = (char*)"1234";
    str_temp.assign(temp, static_cast<int32_t>(strlen(temp)));
    t1.set_varchar(str_temp);
    t2.set_datetime(1234);
    EXPECT_TRUE(OB_SUCCESS == t1.add(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(2468, i2);

    t1.set_int(1);
    t2.set_datetime(1234);
    EXPECT_TRUE(OB_SUCCESS == t1.add(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(1235, i2);

    t1.set_int(1);
    t2.set_varchar(str_temp);
    EXPECT_TRUE(OB_SUCCESS == t1.add(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(1235, i2);

    t1.set_varchar(str_temp);
    t2.set_datetime(1234);
    EXPECT_TRUE(OB_SUCCESS == t2.add(t1, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(2468, i2);

    t1.set_int(1);
    t2.set_datetime(1234);
    EXPECT_TRUE(OB_SUCCESS == t2.add(t1, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(1235, i2);

    t1.set_int(1);
    t2.set_varchar(str_temp);
    EXPECT_TRUE(OB_SUCCESS == t2.add(t1, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(1235, i2);
  }
  BLOCK_FUNC()
  {
    ObExprObj t1;
    ObExprObj t2;
    ObExprObj res;
    int64_t i1 = 0;
    int64_t i2 = 0;
    const int64_t buf_len = ObNumber::MAX_PRINTABLE_SIZE;
    char res_buf[buf_len];
    int err = OB_SUCCESS;

    // int add int
    t1.set_int(1);
    t2.set_int(2);
    EXPECT_EQ(OB_SUCCESS, t1.add(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(3, i2);
    // int add dec
    t1.set_int(1);
    t2.set_decimal("2.2");
    EXPECT_EQ(OB_SUCCESS, t1.add(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("3.2", res_buf);
    // dec add dec
    t1.set_decimal("10");
    t2.set_decimal("2.2");
    EXPECT_EQ(OB_SUCCESS, t1.add(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("12.2", res_buf);

    // int sub int
    t1.set_int(4);
    t2.set_int(2);
    EXPECT_EQ(OB_SUCCESS, t1.sub(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(2, i2);
    // int sub dec
    t1.set_int(4);
    t2.set_decimal("3.0");
    EXPECT_EQ(OB_SUCCESS, t1.sub(t2, res));
    EXPECT_EQ(OB_SUCCESS, t1.sub(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("1.0", res_buf);
    // dec sub dec
    t1.set_decimal("4");
    t2.set_decimal("2.0");
    EXPECT_EQ(OB_SUCCESS, t1.sub(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("2.0", res_buf);

    // int mul int
    t1.set_int(4);
    t2.set_int(2);
    EXPECT_EQ(OB_SUCCESS, t1.mul(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_int(i2));
    ASSERT_EQ(8, i2);
    // int mul dec
    t1.set_int(4);
    t2.set_decimal("2.0");
    EXPECT_EQ(OB_SUCCESS, t1.mul(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("8.0", res_buf);
    // dec mul dec
    t1.set_decimal("4");
    t2.set_decimal("2.0");
    EXPECT_EQ(OB_SUCCESS, t1.mul(t2, res));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("8.0", res_buf);

    // int div int
    t1.set_int(6);
    t2.set_int(2);
    double d = 0.0;
    EXPECT_EQ(OB_SUCCESS, t1.div(t2, res, false));
    ASSERT_EQ(OB_SUCCESS, res.get_double(d));
    ASSERT_EQ(3.0, d);
    EXPECT_EQ(OB_SUCCESS, t1.div(t2, res, true));
    ASSERT_EQ(OB_SUCCESS, res.get_double(d));
    ASSERT_EQ(3.0, d);
    // int div dec
    t1.set_int(6);
    t2.set_decimal("2.0");
    EXPECT_EQ(OB_SUCCESS, t1.div(t2, res, false));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("3.00000000000000000000000000000000000000", res_buf);
    // dec div dec
    t1.set_decimal("6");
    t2.set_decimal("2.0");
    EXPECT_EQ(OB_SUCCESS, t1.div(t2, res, false));
    ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
    ASSERT_STREQ("3.00000000000000000000000000000000000000", res_buf);

    t1.set_int(5);
    t2.set_int(0);
    EXPECT_TRUE(OB_SUCCESS != t1.div(t2, res, false));
    EXPECT_TRUE(true == res.is_null());

    t1.set_decimal("5.0");
    t2.set_int(0);
    EXPECT_TRUE(OB_SUCCESS != t1.div(t2, res, false));
    EXPECT_EQ(true, res.is_null());

    t1.set_int(5);
    t2.set_int(0);
    EXPECT_TRUE(OB_SUCCESS != t1.mod(t2, res));
    EXPECT_EQ(true, res.is_null());

    t1.set_int(5);
    t2.set_int(3);
    EXPECT_EQ(OB_SUCCESS, t1.mod(t2, res));
    err = res.get_int(i1);
    EXPECT_TRUE(OB_SUCCESS == err);
    EXPECT_TRUE(i1 == 2);

    t1.set_int(5);
    t2.set_int(12);
    EXPECT_EQ(OB_SUCCESS, t1.mod(t2, res));
    err = res.get_int(i1);
    EXPECT_TRUE(OB_SUCCESS == err);
    EXPECT_TRUE(i1 == 5);
  }
}

#define COMPARE_EXPECT(cmp_op, type1, v1, type2, v2, res)\
{\
  ObExprObj t1;\
  ObExprObj t2;\
  ObExprObj vres;\
  t1.set_##type1(v1);\
  t2.set_##type2(v2);\
  t1.cmp_op(t2, vres);\
  switch(res)\
  {\
    case MY_TRUE:\
      ASSERT_TRUE(vres.is_true());\
      break;\
    case MY_FALSE:\
      ASSERT_TRUE(vres.is_false());\
      break;\
    default:\
      ASSERT_TRUE(vres.is_null());\
      break;\
  }\
} while(0)

#define COMPARE_BTW_EXPECT(cmp_op, type1, v1, type2, v2, type3, v3, res) \
{\
  ObExprObj t1;\
  ObExprObj t2;\
  ObExprObj t3;\
  ObExprObj vres;\
  t1.set_##type1(v1);\
  t2.set_##type2(v2);\
  t3.set_##type3(v3);\
  t1.cmp_op(t2, t3, vres);                     \
  switch(res)\
  {\
    case MY_TRUE:\
      ASSERT_TRUE(vres.is_true());\
      break;\
    case MY_FALSE:\
      ASSERT_TRUE(vres.is_false());\
      break;\
    default:\
      ASSERT_TRUE(vres.is_null());\
      break;\
  }\
} while(0)

TEST_F(ObExprObjTest, compare)
{
  // int vs decimal
  COMPARE_EXPECT(lt, int, 1234, decimal, "1234.0", MY_FALSE);
  COMPARE_EXPECT(gt, int, 1234, decimal, "1234.0", MY_FALSE);
  COMPARE_EXPECT(eq, int, 1234, decimal, "1234.0", MY_TRUE);
  COMPARE_EXPECT(le, int, 1234, decimal, "1234.0", MY_TRUE);
  COMPARE_EXPECT(ge, int, 1234, decimal, "1234.0", MY_TRUE);
  COMPARE_EXPECT(ne, int, 1234, decimal, "1234.0", MY_FALSE);

  COMPARE_EXPECT(lt, int, 1233, decimal, "1234.0", MY_TRUE);
  COMPARE_EXPECT(gt, int, 1233, decimal, "1234.0", MY_FALSE);
  COMPARE_EXPECT(eq, int, 1233, decimal, "1234.0", MY_FALSE);
  COMPARE_EXPECT(le, int, 1233, decimal, "1234.0", MY_TRUE);
  COMPARE_EXPECT(ge, int, 1233, decimal, "1234.0", MY_FALSE);
  COMPARE_EXPECT(ne, int, 1233, decimal, "1234.0", MY_TRUE);

  COMPARE_EXPECT(lt, int, 1233, decimal, "1232.0", MY_FALSE);
  COMPARE_EXPECT(gt, int, 1233, decimal, "1232.0", MY_TRUE);
  COMPARE_EXPECT(eq, int, 1233, decimal, "1232.0", MY_FALSE);
  COMPARE_EXPECT(le, int, 1233, decimal, "1232.0", MY_FALSE);
  COMPARE_EXPECT(ge, int, 1233, decimal, "1232.0", MY_TRUE);
  COMPARE_EXPECT(ne, int, 1233, decimal, "1232.0", MY_TRUE);

  // int vs null
  COMPARE_EXPECT(lt, null, 0, int, 1233, MY_UNKNOWN);
  COMPARE_EXPECT(gt, null, 0, int, 1233, MY_UNKNOWN);
  COMPARE_EXPECT(eq, null, 0, int, 1233, MY_UNKNOWN);
  COMPARE_EXPECT(le, null, 0, int, 1233, MY_UNKNOWN);
  COMPARE_EXPECT(ge, null, 0, int, 1233, MY_UNKNOWN);
  COMPARE_EXPECT(ne, null, 0, int, 1233, MY_UNKNOWN);

  COMPARE_EXPECT(lt, int, 1233, null, 0, MY_UNKNOWN);
  COMPARE_EXPECT(gt, int, 1233, null, 0, MY_UNKNOWN);
  COMPARE_EXPECT(eq, int, 1233, null, 0, MY_UNKNOWN);
  COMPARE_EXPECT(le, int, 1233, null, 0, MY_UNKNOWN);
  COMPARE_EXPECT(ge, int, 1233, null, 0, MY_UNKNOWN);
  COMPARE_EXPECT(ne, int, 1233, null, 0, MY_UNKNOWN);

  // int vs datetime
  COMPARE_EXPECT(lt, int, 1233, precise_datetime, 0,  MY_FALSE);
  COMPARE_EXPECT(gt, int, 1233, precise_datetime, 0,  MY_TRUE);
  COMPARE_EXPECT(eq, int, 1233, precise_datetime, 0,  MY_FALSE);
  COMPARE_EXPECT(le, int, 1233, precise_datetime, 0,  MY_FALSE);
  COMPARE_EXPECT(ge, int, 1233, precise_datetime, 0,  MY_TRUE);
  COMPARE_EXPECT(ne, int, 1233, precise_datetime, 0,  MY_TRUE);
  COMPARE_EXPECT(eq, int, 1233, precise_datetime, 1233,  MY_TRUE);

  // datetime
  COMPARE_EXPECT(lt, ctime, 1233, precise_datetime, 0,  MY_FALSE);
  COMPARE_EXPECT(gt, ctime, 1233, precise_datetime, 0,  MY_TRUE);
  COMPARE_EXPECT(eq, ctime, 1233, precise_datetime, 0,  MY_FALSE);
  COMPARE_EXPECT(le, ctime, 1233, precise_datetime, 0,  MY_FALSE);
  COMPARE_EXPECT(ge, ctime, 1233, precise_datetime, 0,  MY_TRUE);
  COMPARE_EXPECT(ne, ctime, 1233, precise_datetime, 0,  MY_TRUE);

  // string
  COMPARE_EXPECT(lt, varchar, "abd", varchar, "abce",  MY_FALSE);
  COMPARE_EXPECT(gt, varchar, "abd", varchar, "abce",  MY_TRUE);
  COMPARE_EXPECT(eq, varchar, "abd", varchar, "abce",  MY_FALSE);
  COMPARE_EXPECT(le, varchar, "abd", varchar, "abce",  MY_FALSE);
  COMPARE_EXPECT(ge, varchar, "abd", varchar, "abce",  MY_TRUE);
  COMPARE_EXPECT(ne, varchar, "abd", varchar, "abce",  MY_TRUE);
  COMPARE_EXPECT(ne, varchar, "abd", null, 0,  MY_UNKNOWN);
  COMPARE_EXPECT(ne, varchar, "1234", int, 1234,  MY_FALSE);

  // int vs bool
  COMPARE_EXPECT(eq, int, 1233, bool, false,  MY_FALSE);
  COMPARE_EXPECT(eq, int, 1233, bool, true,  MY_TRUE);
  COMPARE_EXPECT(eq, int, 0, bool, false,  MY_TRUE);
  COMPARE_EXPECT(eq, int, 0, bool, true,  MY_FALSE);

  // int between decimal and float
  COMPARE_BTW_EXPECT(btw, int, -1235, decimal, "-1234", float, 1234, MY_FALSE);
  COMPARE_BTW_EXPECT(btw, int, -1234, decimal, "-1234", float, 1234, MY_TRUE);
  COMPARE_BTW_EXPECT(btw, int, 0, decimal, "-1234", float, 1234, MY_TRUE);
  COMPARE_BTW_EXPECT(btw, int, 1234, decimal, "-1234", float, 1234, MY_TRUE);
  COMPARE_BTW_EXPECT(btw, int, 1235, decimal, "-1234", float, 1234, MY_FALSE);

  COMPARE_BTW_EXPECT(btw, null, 0, decimal, "-1234", float, 1234, MY_UNKNOWN);
  COMPARE_BTW_EXPECT(btw, int, -1234, null, 0, float, 1234, MY_UNKNOWN);
  COMPARE_BTW_EXPECT(btw, int, 0, decimal, "-1234", null, 0, MY_UNKNOWN);

  // int not between decimal and float
  COMPARE_BTW_EXPECT(not_btw, int, -1235, decimal, "-1234", float, 1234, MY_TRUE);
  COMPARE_BTW_EXPECT(not_btw, int, -1234, decimal, "-1234", float, 1234, MY_FALSE);
  COMPARE_BTW_EXPECT(not_btw, int, 0, decimal, "-1234", float, 1234, MY_FALSE);
  COMPARE_BTW_EXPECT(not_btw, int, 1234, decimal, "-1234", float, 1234, MY_FALSE);
  COMPARE_BTW_EXPECT(not_btw, int, 1235, decimal, "-1234", float, 1234, MY_TRUE);

  COMPARE_BTW_EXPECT(not_btw, null, 0, decimal, "-1234", float, 1234, MY_UNKNOWN);
  COMPARE_BTW_EXPECT(not_btw, int, -1234, null, 0, float, 1234, MY_UNKNOWN);
  COMPARE_BTW_EXPECT(not_btw, int, 0, decimal, "-1234", null, 0, MY_UNKNOWN);
}

TEST_F(ObExprObjTest, like)
{
  // @see tests/sql/ob_postfix_expression_test.cpp for more test
  // like
  COMPARE_EXPECT(like, varchar, "abcd", varchar, "abc%",  MY_TRUE);
  COMPARE_EXPECT(like, varchar, "abcd", varchar, "abd%",  MY_FALSE);
  COMPARE_EXPECT(like, varchar, "abcd", varchar, "",  MY_FALSE);
  COMPARE_EXPECT(like, varchar, "", varchar, "",  MY_TRUE);
  COMPARE_EXPECT(like, varchar, "abcd", varchar, "abc_",  MY_TRUE);
  COMPARE_EXPECT(like, varchar, "abcd", null, 0,  MY_UNKNOWN);
  COMPARE_EXPECT(like, varchar, "abcd", decimal, "1234",  MY_UNKNOWN);
  // not like
  COMPARE_EXPECT(not_like, varchar, "abcd", varchar, "abc%",  MY_FALSE);
  COMPARE_EXPECT(not_like, varchar, "abcd", varchar, "abd%",  MY_TRUE);
  COMPARE_EXPECT(not_like, varchar, "abcd", varchar, "",  MY_TRUE);
  COMPARE_EXPECT(not_like, varchar, "", varchar, "",  MY_FALSE);
  COMPARE_EXPECT(not_like, varchar, "abcd", varchar, "abd_",  MY_TRUE);
  COMPARE_EXPECT(not_like, varchar, "abcd", null, 0,  MY_UNKNOWN);
  COMPARE_EXPECT(not_like, varchar, "abcd", decimal, "1234",  MY_UNKNOWN);
  // old like
  COMPARE_EXPECT(old_like, varchar, "abcd", varchar, "",  MY_TRUE);
}

TEST_F(ObExprObjTest, others)
{
  // varchar_length
  ObExprObj t1;
  ObExprObj res;
  int64_t res_val = 0;
  t1.set_ctime(1234000001L);
  ASSERT_NE(OB_SUCCESS, t1.varchar_length(res));
  ASSERT_TRUE(res.is_null());

  t1.set_null();
  ASSERT_NE(OB_SUCCESS, t1.varchar_length(res));
  ASSERT_TRUE(res.is_null());

  t1.set_varchar("");
  ASSERT_EQ(OB_SUCCESS, t1.varchar_length(res));
  ASSERT_EQ(OB_SUCCESS, res.get_int(res_val));
  ASSERT_EQ(0, res_val);

  t1.set_varchar("abcd");
  ASSERT_EQ(OB_SUCCESS, t1.varchar_length(res));
  ASSERT_EQ(OB_SUCCESS, res.get_int(res_val));
  ASSERT_EQ(4, res_val);
}

TEST(ObExprObj, negate_test)
{
  ObExprObj t1;
  ObExprObj res;
  t1.set_ctime(1234000001L);
  ASSERT_NE(OB_SUCCESS, t1.negate(res));
  ASSERT_TRUE(res.is_null());
  t1.set_null();
  ASSERT_NE(OB_SUCCESS, t1.negate(res));
  ASSERT_TRUE(res.is_null());

  t1.set_int(1234);
  ASSERT_EQ(OB_SUCCESS, t1.negate(res));
  int64_t i;
  ASSERT_EQ(OB_SUCCESS, res.get_int(i));
  ASSERT_EQ(-1234, i);

  t1.set_decimal("-1234.567");
  ASSERT_EQ(OB_SUCCESS, t1.negate(res));
  const int64_t buf_len = ObNumber::MAX_PRINTABLE_SIZE;
  char res_buf[buf_len];
  ASSERT_EQ(OB_SUCCESS, res.get_decimal(res_buf, buf_len));
  ASSERT_STREQ("1234.567", res_buf);

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
