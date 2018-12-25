/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_helper.h,v 0.1 2010/08/23 19:28:39 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_UPDATESERVER_TEST_HELPER_H__
#define __OCEANBASE_UPDATESERVER_TEST_HELPER_H__

#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_iterator.h"
#include "common/ob_string_buf.h"
#include <gtest/gtest.h>

using namespace std;
using namespace oceanbase::common;


void check_string(const ObString& expected, const ObString& real)
{
  EXPECT_EQ(expected.length(), real.length());
  if (NULL != expected.ptr() && NULL != real.ptr())
  {
    EXPECT_EQ(0, memcmp(expected.ptr(), real.ptr(), expected.length()));
  }
  else
  {
    EXPECT_EQ((const char*) NULL, expected.ptr());
    EXPECT_EQ((const char*) NULL, real.ptr());
  }
  //printf("expected_row_key=%.*s, real_row_key=%.*s\n",
  //    expected.length(), expected.ptr(), real.length(), real.ptr());
}

void check_obj(const ObObj& expected, const ObObj& real)
{
  UNUSED(expected);
  UNUSED(real);
  // TODO
}

void check_cell(const ObCellInfo& expected, const ObCellInfo& real)
{
  EXPECT_EQ(expected.column_id_, real.column_id_);
  EXPECT_EQ(expected.table_id_, real.table_id_);
  EXPECT_TRUE(expected.row_key_ ==  real.row_key_);
}

void check_cell_with_name(const ObCellInfo& expected, const ObCellInfo& real)
{
  check_string(expected.table_name_, real.table_name_);
  EXPECT_TRUE(expected.row_key_ ==  real.row_key_);
  check_string(expected.column_name_, real.column_name_);
}

#endif //__TEST_HELPER_H__

