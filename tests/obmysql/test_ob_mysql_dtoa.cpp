/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $id
 *
 * Authors:
 *   fufeng <fufeng.syd@taobao.com>
 *     - some work details if you want
 */

#include <float.h>
#include <math.h>
#include <gtest/gtest.h>
#include "obmysql/ob_mysql_dtoa.h"
#include "obmysql/ob_mysql_global.h"
#include "common/ob_define.h"

using namespace oceanbase;
using namespace oceanbase::obmysql;

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestObMySQLDtoa
  : public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestObMySQLDtoa, float)
{
  int buflen = FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE;
  char buf[buflen];
  size_t len;

  len = my_gcvt(10, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("10", buf);
  len = my_gcvt(10.0, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("10", buf);
  len = my_gcvt(0.1, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("0.1", buf);

  len = my_gcvt(6e-16, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("6e-16", buf);
  len = my_gcvt(-6e-16, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("-6e-16", buf);
  len = my_gcvt(-(-6e-16), MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("6e-16", buf);
  len = my_gcvt(-6e-16+1.000000, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("1", buf);

  len = my_gcvt(1e-15, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("0.000000000000001", buf);
  len = my_gcvt(1e-16, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("1e-16", buf);

  len = my_gcvt(FLT_MIN, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("1.17549e-38", buf);
  len = my_gcvt(FLT_MAX, MY_GCVT_ARG_FLOAT, buflen - 1, buf, NULL);
  ASSERT_STREQ("3.40282e38", buf);

  bool err;
  len = my_gcvt(INFINITY, MY_GCVT_ARG_FLOAT, buflen - 1, buf, &err);
  ASSERT_TRUE(err);
  ASSERT_STREQ("0", buf);
  len = my_gcvt(NAN, MY_GCVT_ARG_FLOAT, buflen - 1, buf, &err);
  ASSERT_TRUE(err);
  ASSERT_STREQ("0", buf);

  UNUSED(len);
}

TEST_F(TestObMySQLDtoa, double)
{
  int buflen = DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE;
  char buf[buflen];
  size_t len;

  len = my_gcvt(10, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("10", buf);
  len = my_gcvt(10.0, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("10", buf);
  len = my_gcvt(0.1, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("0.1", buf);

  len = my_gcvt(6e-16, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("6e-16", buf);
  len = my_gcvt(-6e-16, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("-6e-16", buf);
  len = my_gcvt(-(-6e-16), MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("6e-16", buf);
  len = my_gcvt(-6e-16+1.000000, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("0.9999999999999994", buf);

  len = my_gcvt(1e-15, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("0.000000000000001", buf);
  len = my_gcvt(1e-16, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("1e-16", buf);

  len = my_gcvt(DBL_MIN, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("2.2250738585072014e-308", buf);
  len = my_gcvt(DBL_MAX, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, NULL);
  ASSERT_STREQ("1.7976931348623157e308", buf);

  bool err;
  len = my_gcvt(INFINITY, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, &err);
  ASSERT_TRUE(err);
  ASSERT_STREQ("0", buf);
  len = my_gcvt(NAN, MY_GCVT_ARG_DOUBLE, buflen - 1, buf, &err);
  ASSERT_TRUE(err);
  ASSERT_STREQ("0", buf);

  UNUSED(len);
}
