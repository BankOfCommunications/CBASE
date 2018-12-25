/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_number_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_number.h"
#include "common/ob_define.h"
#include <gtest/gtest.h>
#include <sys/time.h>
using namespace oceanbase::common;

class ObNumberTest: public ::testing::Test
{
  public:
    ObNumberTest();
    virtual ~ObNumberTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObNumberTest(const ObNumberTest &other);
    ObNumberTest& operator=(const ObNumberTest &other);
  protected:
    void test_add(const char* n1, const char* n2, const char* res);
    void test_sub(const char* n1, const char* n2, const char* res);
    void test_mul(const char* n1, const char* n2, const char* res);
    void test_div(const char* n1, const char* n2, const char* res);
    void test_negate(const char* str_n1, const char* str_res);
    void test_cast_to_int64(const char* str_n1, int64_t expected_i64);
};

ObNumberTest::ObNumberTest()
{
}

ObNumberTest::~ObNumberTest()
{
}

void ObNumberTest::SetUp()
{
}

void ObNumberTest::TearDown()
{
}

TEST_F(ObNumberTest, test_converters)
{
  char buff[ObNumber::MAX_PRINTABLE_SIZE];
  ObNumber n1;

  n1.from("123.00");
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n1=%s\n", buff);

  n1.from("12345678901234567890.123456789012345678");
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n1=%s\n", buff);

  n1.from("-123.456789012345678901");
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n1=%s\n", buff);

  n1.from("0");
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n1=%s\n", buff);

  n1.from("-4294967295");
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n1=%s\n", buff);

  n1.from(static_cast<int64_t>(0));
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n1=%s\n", buff);

  int64_t base = static_cast<int64_t>(UINT32_MAX)+1;
  n1.from(base);
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("base=%s\n", buff);

  n1.from(-base);
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("-base=%s\n", buff);

  ASSERT_EQ(OB_VALUE_OUT_OF_RANGE, n1.from("0.999999999999999999999999999999999999999"));
}

void ObNumberTest::test_add(const char* str_n1, const char* str_n2, const char* str_res)
{
  ObNumber n1, n2, res;
  n1.from(str_n1);
  n2.from(str_n2);
  ASSERT_EQ(OB_SUCCESS, n1.add(n2, res));
  char buff[ObNumber::MAX_PRINTABLE_SIZE];
  res.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  ASSERT_STREQ(str_res, buff);
  printf("(%s)+(%s)=%s\n", str_n1, str_n2, buff);
}

TEST_F(ObNumberTest, add_test)
{
  test_add("-123.45", "23.45", "-100.00");
  test_add("123.45", "83.4566", "206.9066");
  test_add("-123.45", "-23.45", "-146.90");
  test_add("123.45", "-23.4570", "99.9930");
  test_add("0.01", "-23.4570", "-23.4470");
  test_add("0", "-23.4570", "-23.4570");
  test_add("0", "0", "0");
  test_add("-99999999999999999999999999999999999999", "-1", "-100000000000000000000000000000000000000");
  test_add("0", "1", "1");
  test_add("1234.0", "-1234", "0");
  test_add("4294967296", "-2147483648", "2147483648");
}

void ObNumberTest::test_sub(const char* str_n1, const char* str_n2, const char* str_res)
{
  ObNumber n1, n2, res;
  n1.from(str_n1);
  n2.from(str_n2);
  ASSERT_EQ(OB_SUCCESS, n1.sub(n2, res));
  char buff[ObNumber::MAX_PRINTABLE_SIZE];
  res.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  ASSERT_STREQ(str_res, buff);
  printf("(%s)-(%s)=%s\n", str_n1, str_n2, buff);
}

TEST_F(ObNumberTest, sub_test)
{
  test_sub("-123.45", "23.45", "-146.90");
  test_sub("123.45", "-23.45", "146.90");
  test_sub("123.45", "23.45", "100.00");
  test_sub("-123.45", "-23.45", "-100.00");
  test_sub("13165465465497979795.545", "94645648971231.4564664", "13165370819849008564.0885336");
  test_sub("0.01", "-23.4570", "23.4670");
  test_sub("0", "-23.4570", "23.4570");
  test_sub("-23.4570", "0", "-23.4570");
  test_sub("0", "0", "0");
  test_sub("0", "1", "-1");
  test_sub("1234.0", "1234", "0");
  test_sub("4294967297", "4294967296", "1");
  test_sub("4294967296", "-2147483648", "6442450944");
}

void ObNumberTest::test_mul(const char* str_n1, const char* str_n2, const char* str_res)
{
  ObNumber n1, n2, res;
  n1.from(str_n1);
  n2.from(str_n2);
  ASSERT_EQ(OB_SUCCESS, n1.mul(n2, res));
  char buff[ObNumber::MAX_PRINTABLE_SIZE];
  res.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  ASSERT_STREQ(str_res, buff);
  printf("(%s)*(%s)=%s\n", str_n1, str_n2, buff);
}

TEST_F(ObNumberTest, mul_test)
{
  test_mul("1000.01", "99.99", "99990.9999");
  test_mul("1000.01", "-99.99", "-99990.9999");
  test_mul("-1000.01", "99.99", "-99990.9999");
  test_mul("-1000.01", "-99.99", "99990.9999");
  test_mul("100010001000.01", "99.99", "9999999999990.9999");
  test_mul("100010001000.01", "-99.99", "-9999999999990.9999");
  test_mul("-100010001000.01", "99.99", "-9999999999990.9999");
  test_mul("-100010001000.01", "-99.99", "9999999999990.9999");
  test_mul("100010001000.01", "100010001000.01", "10002000300022001200020.0001");
  test_mul("100010001000.01", "-100010001000.01", "-10002000300022001200020.0001");
  test_mul("-100010001000.01", "100010001000.01", "-10002000300022001200020.0001");
  test_mul("-100010001000.01", "-100010001000.01", "10002000300022001200020.0001");
  test_mul("0.01", "-99.99", "-0.9999");
  test_mul("0", "-99.99", "0");
  test_mul("-99.99", "0", "0");
  test_mul("0", "0", "0");
  test_mul("1", "0", "0");
  test_mul("99999999999999999999999999999999999999", "99999999999999999999999999999999999999", "9999999999999999999999999999999999999800000000000000000000000000000000000001");
  test_mul("-99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", "9999999999999999999999999999999999999800000000000000000000000000000000000001");
  test_mul("4294967296", "-2147483648", "-9223372036854775808");
}

void ObNumberTest::test_div(const char* str_n1, const char* str_n2, const char* str_res)
{
  ObNumber n1, n2, res;
  char buff[ObNumber::MAX_PRINTABLE_SIZE];
  n1.from(str_n1);
  n1.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n1=%s\n", buff);
  n2.from(str_n2);
  n2.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("n2=%s\n", buff);
  if (n2.is_zero())
  {
    ASSERT_EQ(OB_DIVISION_BY_ZERO, n1.div(n2, res));
  }
  else
  {
    ASSERT_EQ(OB_SUCCESS, n1.div(n2, res));
    res.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
    printf("(%s)/(%s)=%s\n", str_n1, str_n2, buff);
    ASSERT_STREQ(str_res, buff);
  }
}

TEST_F(ObNumberTest, divide_test)
{
  test_div("99999999999999999999.9999", "999999999999.99", "100000000.00000099999999990000999999999900009999"); // 4words / 2-words
  test_div("-99999999999999999999.9999", "999999999999.99", "-100000000.00000099999999990000999999999900009999"); // 4words / 2-words
  test_div("99999999999999999999.9999", "-999999999999.99", "-100000000.00000099999999990000999999999900009999"); // 4words / 2-words
  test_div("-99999999999999999999.9999", "-999999999999.99", "100000000.00000099999999990000999999999900009999"); // 4words / 2-words
  test_div("9999999999990.9999", "99.99", "100010001000.01000000000000000000000000000000000000"); // X-words/1-word
  test_div("0", "99.99", "0");
  test_div("99.99", "0", "NaN");
  test_div("0", "0", "NaN");
  test_div("0", "1", "0");
  test_div("999999999999.99", "-999999999999.99", "-1.00000000000000000000000000000000000000");
  test_div("1999999999999.99", "999999999999.99", "2.00000000000001000000000000010000000000");
  test_div("999999999999.990001", "999999999999.99", "1.00000000000000000100000000000001000000");
  test_div("1000", "10000", "0.10000000000000000000000000000000000000");
  test_div("1000", "0.1", "10000.00000000000000000000000000000000000000");
  test_div("0.000000000000000000000000001", "10000000.00", "0.00000000000000000000000000000000010000");
  test_div("1", "1", "1.00000000000000000000000000000000000000");
  test_div("11", "1", "11.00000000000000000000000000000000000000");
  test_div("0.1", "0.3", "0.33333333333333333333333333333333333333");
  test_div("0.1", "0.30000000", "0.33333333333333333333333333333333333333");
  test_div("0.1", "0.99999999999999999999999999999999999999", "0.10000000000000000000000000000000000000");
  test_div("10065037930", "70930", "141901.00000000000000000000000000000000000000");
  test_div("214748365", "2147483650", "0.10000000000000000000000000000000000000");
  test_div("-2147483648", "2147483648", "-1.00000000000000000000000000000000000000");
  test_div("10065037930", "2", "5032518965.00000000000000000000000000000000000000");
  test_div("10065037930", "1", "10065037930.00000000000000000000000000000000000000");

  ObNumber n1, n2, res;
  n1.from("99999999999999999999999999999999999999");
  n2.from("99999999999999999999999999999999999999");
  ASSERT_EQ(OB_SUCCESS, n1.mul(n2, res));
  ASSERT_EQ(OB_VALUE_OUT_OF_RANGE, res.div(n2, n1));
  n1.from("0.99999999999999999999999999999999999999");
  n2.from("0.99999999999999999999999999999999999999");
  ASSERT_EQ(OB_SUCCESS, n1.mul(n2, res));
  ASSERT_EQ(OB_SUCCESS, res.div(n2, n1));
  ASSERT_EQ(n1, n2);
}

void ObNumberTest::test_negate(const char* str_n1, const char* str_res)
{
  ObNumber n1, res;
  ASSERT_EQ(OB_SUCCESS, n1.from(str_n1));
  ASSERT_EQ(OB_SUCCESS, n1.negate(res));
  char buff[ObNumber::MAX_PRINTABLE_SIZE];
  res.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  printf("-(%s)=%s\n", str_n1, buff);
  ASSERT_STREQ(str_res, buff);
}

TEST_F(ObNumberTest, negate_test)
{
  test_negate("0", "0");
  test_negate("1", "-1");
  test_negate("-1", "1");
  test_negate("-1.00", "1.00");
  test_negate("0.99", "-0.99");
  test_negate("4294967296", "-4294967296");
  test_negate("-4294967296", "4294967296");
  test_negate("0.99999999999999999999999999999999999999", "-0.99999999999999999999999999999999999999");
  test_negate("1234567890123456789012345678901234567890123456789012345678901234567890123456",
              "-1234567890123456789012345678901234567890123456789012345678901234567890123456"); // 76 dec
  test_negate("12345678901234567890123456789012345678.90123456789012345678901234567890123456",
              "-12345678901234567890123456789012345678.90123456789012345678901234567890123456"); // 76 dec
}

int double_div(int64_t f1, int64_t f2, double &res) __attribute__((noinline));
int double_div(int64_t f1, int64_t f2, double &res)
{
  res = static_cast<double>(f1) / static_cast<double>(f2);
  return 0;
}

int sim_div(int64_t f1, int64_t f2, double &res) __attribute__((noinline));
int sim_div(int64_t f1, int64_t f2, double &res)
{
  f1 *= 1000000LL;
  int64_t ires = f1 / f2;
  //res = ires / 1000000.0f;
  res = static_cast<double>(ires);
  return 0;
}

TEST_F(ObNumberTest, div_perf_test)
{
  for (int round = 0; round < 3; round++)
  {
    ObNumber n1, n2, res;
    char buff[ObNumber::MAX_PRINTABLE_SIZE];
    n1.from("10003");
    n2.from("997");
    res.QUOTIENT_SCALE = 6;
    struct timeval tv_begin;
    struct timeval tv_finish;
    int count = 0;
    gettimeofday(&tv_begin, NULL);
    for (int64_t i = 0; i < 1000000; ++i)
    {
      n1.div(n2, res);
      count=count+1;
    }
    gettimeofday(&tv_finish, NULL);
    int64_t begin_us = tv_begin.tv_sec * 1000000LL + tv_begin.tv_usec;
    int64_t finish_us = tv_finish.tv_sec * 1000000LL + tv_finish.tv_usec;
    res.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
    printf("decimal_res=%s elapsed=%ld count=%d\n", buff, finish_us - begin_us, count);
  }
  for (int round = 0; round < 3; round++)
  {
    int64_t f1 = 10003;
    int64_t f2 = 997;
    double res = 0.0;
    struct timeval tv_begin;
    struct timeval tv_finish;
    int count = 0;
    gettimeofday(&tv_begin, NULL);
    for (int64_t i = 0; i < 1000000; ++i)
    {
      double_div(f1, f2, res);
      count=count+1;
    }
    gettimeofday(&tv_finish, NULL);
    int64_t begin_us = tv_begin.tv_sec * 1000000LL + tv_begin.tv_usec;
    int64_t finish_us = tv_finish.tv_sec * 1000000LL + tv_finish.tv_usec;
    printf("double_res=%f elapsed=%ld count=%d\n", res, finish_us - begin_us, count);
  }
  for (int round = 0; round < 3; round++)
  {
    int64_t f1 = 10003;
    int64_t f2 = 997;
    double res = 0.0;
    struct timeval tv_begin;
    struct timeval tv_finish;
    int count = 0;
    gettimeofday(&tv_begin, NULL);
    for (int64_t i = 0; i < 1000000; ++i)
    {
      sim_div(f1, f2, res);
      count=count+1;
    }
    gettimeofday(&tv_finish, NULL);
    int64_t begin_us = tv_begin.tv_sec * 1000000LL + tv_begin.tv_usec;
    int64_t finish_us = tv_finish.tv_sec * 1000000LL + tv_finish.tv_usec;
    printf("sim_res=%f elapsed=%ld count=%d\n", res, finish_us - begin_us, count);
  }
}

void ObNumberTest::test_cast_to_int64(const char* str_n1, int64_t expected_i64)
{
  ObNumber n1;
  int64_t i64 = 0;
  ASSERT_EQ(OB_SUCCESS, n1.from(str_n1));
  ASSERT_EQ(OB_SUCCESS, n1.cast_to_int64(i64));
  printf("int(%s)=%ld\n", str_n1, i64);
  ASSERT_EQ(expected_i64, i64);
}

TEST_F(ObNumberTest, cast_to_int64)
{
  test_cast_to_int64("0", 0);
  test_cast_to_int64("1230", 1230);
  test_cast_to_int64("1230.0", 1230);
  test_cast_to_int64("1230.456", 1230);
  test_cast_to_int64("-1230", -1230);
  test_cast_to_int64("-1230.0", -1230);
  test_cast_to_int64("-1230.456", -1230);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
