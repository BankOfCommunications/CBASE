/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_obj_cast_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_obj_cast.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class ObObjCastTest: public ::testing::Test
{
  public:
    ObObjCastTest();
    virtual ~ObObjCastTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObObjCastTest(const ObObjCastTest &other);
    ObObjCastTest& operator=(const ObObjCastTest &other);
  private:
    // data members
};

ObObjCastTest::ObObjCastTest()
{
}

ObObjCastTest::~ObObjCastTest()
{
}

void ObObjCastTest::SetUp()
{
}

void ObObjCastTest::TearDown()
{
}

#define MY_EXPECT(in_type, in, out_type, expected, expected2) \
do {\
  int err = OB_SUCCESS;\
  ObExprObj out;\
  ObObjCastParams params;\
  char varchar_buf[128];\
  ObString varchar_out(128, 128, varchar_buf);  \
  out.set_varchar(varchar_out);\
  err = OB_OBJ_CAST[in_type][out_type](params, in, out);\
  switch(out_type)\
  {\
    case ObNullType:\
      ASSERT_EQ(OB_NOT_SUPPORTED, err);\
      break;\
    case ObIntType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<int64_t>(expected), out.get_int());\
      break;\
    case ObFloatType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<float>(expected), out.get_float());\
      break;\
    case ObDoubleType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<double>(expected), out.get_double());\
      break;\
    case ObDateTimeType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<ObDateTime>(expected), out.get_datetime());\
      break;\
    case ObPreciseDateTimeType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<ObPreciseDateTime>(expected), out.get_precise_datetime());\
      break;\
    case ObVarcharType:\
    {\
      ASSERT_EQ(OB_SUCCESS, err);\
      const ObString &varchar = out.get_varchar();\
      if (static_cast<int64_t>(strlen(expected2)) !=  varchar.length()  \
          || 0 != memcmp(expected2, varchar.ptr(), varchar.length()))    \
      {                                                                 \
        TBSYS_LOG(WARN, "expect fail:(%s) != (%.*s)", expected2, varchar.length(), varchar.ptr()); \
      }                                                                 \
      ASSERT_EQ(static_cast<int64_t>(strlen(expected2)), varchar.length());\
      ASSERT_TRUE(0 == memcmp(expected2, varchar.ptr(), varchar.length()));\
      break;\
    }\
    case ObSeqType:\
      ASSERT_EQ(OB_NOT_SUPPORTED, err);\
      break;\
    case ObCreateTimeType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<ObCreateTime>(expected), out.get_ctime());\
      break;\
    case ObModifyTimeType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<ObModifyTime>(expected), out.get_mtime());\
      break;\
    case ObExtendType:\
      ASSERT_EQ(OB_NOT_SUPPORTED, err);\
      break;\
    case ObBoolType:\
      ASSERT_EQ(OB_SUCCESS, err);\
      ASSERT_EQ(static_cast<bool>(expected), out.get_bool());\
      break;\
    case ObDecimalType:\
    {\
      ASSERT_EQ(OB_SUCCESS, err);\
      char dec_buf[ObNumber::MAX_PRINTABLE_SIZE];\
      out.get_decimal().to_string(dec_buf, ObNumber::MAX_PRINTABLE_SIZE);\
      ASSERT_STREQ(expected2, dec_buf);\
      break;\
    }\
    default:\
      break;\
  }\
} while(0)

TEST_F(ObObjCastTest, null_to_xxx)
{
  ObExprObj in;
  in.set_null();
  ObExprObj out;
  ObObjCastParams params;
  for (ObObjType t2 = static_cast<ObObjType>(ObMinType+1);
       t2 < ObMaxType;
       t2 = static_cast<ObObjType>(t2 + 1))
  {
    ASSERT_EQ(OB_SUCCESS, OB_OBJ_CAST[ObNullType][t2](params, in, out));
    ASSERT_TRUE(out.is_null());
  }
}

TEST_F(ObObjCastTest, int_to_xxx)
{
  ObExprObj in;
  char buf[128];
  for (int64_t i = -1; i < 2; ++i)
  {
    in.set_int(i);
    snprintf(buf, 128, "%ld", i);
    MY_EXPECT(ObIntType, in, ObNullType, i, buf);
    MY_EXPECT(ObIntType, in, ObIntType, i, buf);
    MY_EXPECT(ObIntType, in, ObFloatType, i, buf);
    MY_EXPECT(ObIntType, in, ObDoubleType, i, buf);
    MY_EXPECT(ObIntType, in, ObDateTimeType, i, buf);
    MY_EXPECT(ObIntType, in, ObPreciseDateTimeType, i, buf);
    MY_EXPECT(ObIntType, in, ObVarcharType, i, buf);
    MY_EXPECT(ObIntType, in, ObSeqType, i, buf);
    MY_EXPECT(ObIntType, in, ObCreateTimeType, i, buf);
    MY_EXPECT(ObIntType, in, ObModifyTimeType, i, buf);
    MY_EXPECT(ObIntType, in, ObExtendType, i, buf);
    MY_EXPECT(ObIntType, in, ObBoolType, i, buf);
    MY_EXPECT(ObIntType, in, ObDecimalType, i, buf);
  }
}

TEST_F(ObObjCastTest, float_to_xxx)
{
  ObExprObj in;
  char buf[128];
  float cases[] = {-1.1f, 0.0f, 1.1f};
  const char *dec_expected[] = {"-1.100000", "0", "1.100000"};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    float v = cases[i];
    in.set_float(v);
    snprintf(buf, 128, "%f", v);
    MY_EXPECT(ObFloatType, in, ObNullType, v, buf);
    MY_EXPECT(ObFloatType, in, ObIntType, v, buf);
    MY_EXPECT(ObFloatType, in, ObFloatType, v, buf);
    MY_EXPECT(ObFloatType, in, ObDoubleType, v, buf);
    MY_EXPECT(ObFloatType, in, ObDateTimeType, v, buf);
    MY_EXPECT(ObFloatType, in, ObPreciseDateTimeType, v, buf);
    MY_EXPECT(ObFloatType, in, ObVarcharType, v, buf);
    MY_EXPECT(ObFloatType, in, ObSeqType, v, buf);
    MY_EXPECT(ObFloatType, in, ObCreateTimeType, v, buf);
    MY_EXPECT(ObFloatType, in, ObModifyTimeType, v, buf);
    MY_EXPECT(ObFloatType, in, ObExtendType, v, buf);
    MY_EXPECT(ObFloatType, in, ObBoolType, v, buf);
    MY_EXPECT(ObFloatType, in, ObDecimalType, v, dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, double_to_xxx)
{
  ObExprObj in;
  char buf[128];
  double cases[] = {-1.1, 0.0, 1.1};
  const char *dec_expected[] = {"-1.100000", "0", "1.100000"};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    double v = cases[i];
    in.set_double(v);
    snprintf(buf, 128, "%lf", v);

    MY_EXPECT(ObDoubleType, in, ObNullType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObIntType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObFloatType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObDoubleType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObDateTimeType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObPreciseDateTimeType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObVarcharType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObSeqType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObCreateTimeType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObModifyTimeType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObExtendType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObBoolType, v, buf);
    MY_EXPECT(ObDoubleType, in, ObDecimalType, v, dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, datetime_to_xxx)
{
  ObExprObj in;
  //tzset();
  //printf("timezone=%s %s\n", tzname[0], tzname[1]);
  const char* cases_str[] = {"1970-1-1 8:0:0", "1970-1-1 0:0:0", "2012-9-24 16:59:60"};
  time_t cases[ARRAYSIZEOF(cases_str)];
  const char *varchar_expected[] = {"1970-01-01 08:00:00", "1970-01-01 00:00:00", "2012-09-24 17:00:00"};
  const char *dec_expected[] = {"0", "-28800", "1348477200"};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    strptime(cases_str[i], "%Y-%m-%d %H:%M:%S", &tm);
    cases[i] = mktime(&tm);
    time_t v = cases[i];
    //printf("v=%s", ctime(&v));
    int64_t pv = v * 1000000L;
    in.set_datetime(v);

    MY_EXPECT(ObDateTimeType, in, ObNullType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObIntType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObFloatType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObDoubleType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObDateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObPreciseDateTimeType, pv, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObVarcharType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObSeqType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObCreateTimeType, pv, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObModifyTimeType, pv, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObExtendType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObBoolType, v, varchar_expected[i]);
    MY_EXPECT(ObDateTimeType, in, ObDecimalType, v, dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, precise_datetime_to_xxx)
{
  ObExprObj in;
  const char* cases_str[] = {"1970-1-1 8:0:0", "1970-1-1 0:0:0", "2012-9-24 16:59:60"};
  int64_t cases[ARRAYSIZEOF(cases_str)];
  const char *varchar_expected[] = {"1970-01-01 08:00:00.000000", "1970-01-01 00:00:00.000000", "2012-09-24 17:00:00.000000"};
  const char *dec_expected[] = {"0", "-28800000000", "1348477200000000"};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    strptime(cases_str[i], "%Y-%m-%d %H:%M:%S", &tm);
    cases[i] = mktime(&tm) * 1000000L;
    int64_t v = cases[i];
    int64_t pv = v / 1000000L;
    in.set_precise_datetime(v);

    MY_EXPECT(ObPreciseDateTimeType, in, ObNullType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObIntType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObFloatType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObDoubleType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObDateTimeType, pv, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObPreciseDateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObVarcharType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObSeqType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObCreateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObModifyTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObExtendType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObBoolType, v, varchar_expected[i]);
    MY_EXPECT(ObPreciseDateTimeType, in, ObDecimalType, v, dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, varchar_to_xxx)
{
  ObExprObj in;
  const char* cases[] = {"1970-1-1 8:0:0", "2012-9-24 16:59:60.000000", "-123456", "-123.456", "true", "false"};
  int64_t int_expected[ARRAYSIZEOF(cases)] = {1970, 2012, -123456, -123, 0, 0};
  float float_expected[ARRAYSIZEOF(cases)] = {1970.0f, 2012.0f, -123456.0f, -123.456f, 0.0f, 0.0f};
  double double_expected[ARRAYSIZEOF(cases)] = {1970.0, 2012.0, -123456.0, -123.456, 0.0, 0.0};
  int64_t pdt_expected[ARRAYSIZEOF(cases)] = {0, 1348477200000000, 0, 0, 0, 0};
  bool bool_expected[ARRAYSIZEOF(cases)] = {false, false, false, false, true, false};
  const char *dec_expected[ARRAYSIZEOF(cases)] = {"0", "0", "-123456", "-123.456", "0", "0"};

  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    ObString v;
    v.assign_ptr(const_cast<char*>(cases[i]), static_cast<int32_t>(strlen(cases[i])));
    in.set_varchar(v);
    //printf("case %ld: %s\n", i, cases[i]);
    MY_EXPECT(ObVarcharType, in, ObNullType, int_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObIntType, int_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObFloatType, float_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObDoubleType, double_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObDateTimeType, (pdt_expected[i]/1000000L), cases[i]);
    MY_EXPECT(ObVarcharType, in, ObPreciseDateTimeType, pdt_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObVarcharType, int_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObSeqType, int_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObCreateTimeType, pdt_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObModifyTimeType, pdt_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObExtendType, int_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObBoolType, bool_expected[i], cases[i]);
    MY_EXPECT(ObVarcharType, in, ObDecimalType, int_expected[i], dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, ctime_to_xxx)
{
  ObExprObj in;
  const char* cases_str[] = {"1970-1-1 8:0:0", "1970-1-1 0:0:0", "2012-9-24 16:59:60"};
  int64_t cases[ARRAYSIZEOF(cases_str)];
  const char *varchar_expected[] = {"1970-01-01 08:00:00.000000", "1970-01-01 00:00:00.000000", "2012-09-24 17:00:00.000000"};
  const char *dec_expected[] = {"0", "-28800000000", "1348477200000000"};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    strptime(cases_str[i], "%Y-%m-%d %H:%M:%S", &tm);
    cases[i] = mktime(&tm) * 1000000L;
    int64_t v = cases[i];
    int64_t pv = v / 1000000L;
    in.set_ctime(v);

    MY_EXPECT(ObCreateTimeType, in, ObNullType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObIntType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObFloatType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObDoubleType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObDateTimeType, pv, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObPreciseDateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObVarcharType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObSeqType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObCreateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObModifyTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObExtendType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObBoolType, v, varchar_expected[i]);
    MY_EXPECT(ObCreateTimeType, in, ObDecimalType, v, dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, mtime_to_xxx)
{
  ObExprObj in;
  const char* cases_str[] = {"1970-1-1 8:0:0", "1970-1-1 0:0:0", "2012-9-24 16:59:60"};
  int64_t cases[ARRAYSIZEOF(cases_str)];
  const char *varchar_expected[] = {"1970-01-01 08:00:00.000000", "1970-01-01 00:00:00.000000", "2012-09-24 17:00:00.000000"};
  const char *dec_expected[] = {"0", "-28800000000", "1348477200000000"};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    strptime(cases_str[i], "%Y-%m-%d %H:%M:%S", &tm);
    cases[i] = mktime(&tm) * 1000000L;
    int64_t v = cases[i];
    int64_t pv = v / 1000000L;
    in.set_mtime(v);

    MY_EXPECT(ObModifyTimeType, in, ObNullType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObIntType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObFloatType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObDoubleType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObDateTimeType, pv, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObPreciseDateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObVarcharType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObSeqType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObCreateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObModifyTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObExtendType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObBoolType, v, varchar_expected[i]);
    MY_EXPECT(ObModifyTimeType, in, ObDecimalType, v, dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, bool_to_xxx)
{
  ObExprObj in;
  bool cases[] = {true, false};
  const char *varchar_expected[] = {"true", "false"};
  const char *dec_expected[] = {"1", "0"};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    bool v = cases[i];
    in.set_bool(v);

    MY_EXPECT(ObBoolType, in, ObNullType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObIntType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObFloatType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObDoubleType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObDateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObPreciseDateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObVarcharType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObSeqType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObCreateTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObModifyTimeType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObExtendType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObBoolType, v, varchar_expected[i]);
    MY_EXPECT(ObBoolType, in, ObDecimalType, v, dec_expected[i]);
  }
}

TEST_F(ObObjCastTest, decimal_to_xxx)
{
  ObExprObj in;
  const char * cases_str[] = {"0", "123.456", "-789.012"};
  ObNumber cases[ARRAYSIZEOF(cases_str)];
  int64_t int_expected[ARRAYSIZEOF(cases)] = {0, 123, -789};
  float float_expected[ARRAYSIZEOF(cases)] = {0.0f, 123.456f, -789.012f};
  double double_expected[ARRAYSIZEOF(cases)] = {0.0, 123.456, -789.012};
  for (int64_t i = 0; i < static_cast<int64_t>(ARRAYSIZEOF(cases)); ++i)
  {
    //printf("case %ld: %s\n", i, cases_str[i]);
    cases[i].from(cases_str[i]);
    in.set_decimal(cases[i]);
    int64_t v = int_expected[i];

    MY_EXPECT(ObDecimalType, in, ObNullType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObIntType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObFloatType, float_expected[i], cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObDoubleType, double_expected[i], cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObDateTimeType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObPreciseDateTimeType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObVarcharType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObSeqType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObCreateTimeType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObModifyTimeType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObExtendType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObBoolType, v, cases_str[i]);
    MY_EXPECT(ObDecimalType, in, ObDecimalType, v, cases_str[i]);
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
