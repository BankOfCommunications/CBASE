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
#include <gtest/gtest.h>
#include <common/ob_define.h>
#include "obmysql/ob_mysql_state.h"

using namespace oceanbase;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestObMySQLState
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

TEST_F(TestObMySQLState, main)
{
  const ObMySQLState &state = ObMySQLState::get_instance();

  /* OB_SUCCESS, never use */
  ASSERT_STREQ("00000", state.get_odbc_state(OB_SUCCESS));
  ASSERT_STREQ("00000", state.get_jdbc_state(OB_SUCCESS));

  /* gt 0, OB_ERROR */
  ASSERT_STREQ("S1000", state.get_odbc_state(OB_ERROR));
  ASSERT_STREQ("HY000", state.get_jdbc_state(OB_ERROR));

  /* first and last */
  ASSERT_STREQ("S1000", state.get_odbc_state(OB_OBJ_TYPE_ERROR));
  ASSERT_STREQ("S1000", state.get_odbc_state(OB_ERR_ILLEGAL_INDEX));
  ASSERT_STREQ("HY004", state.get_jdbc_state(OB_OBJ_TYPE_ERROR));
  ASSERT_STREQ("42S12", state.get_jdbc_state(OB_ERR_ILLEGAL_INDEX));

  /* out range */
  ASSERT_STREQ("S1000", state.get_odbc_state(8));
  ASSERT_STREQ("HY000", state.get_jdbc_state(8));
  ASSERT_STREQ("S1000", state.get_odbc_state(OB_ERR_ILLEGAL_INDEX - 1));
  ASSERT_STREQ("HY000", state.get_jdbc_state(OB_ERR_ILLEGAL_INDEX - 1));

  /* in middle */
  ASSERT_STREQ("S1000", state.get_odbc_state(OB_ERR_PARSER_INIT));
  ASSERT_STREQ("0B000", state.get_jdbc_state(OB_ERR_PARSER_INIT));
  ASSERT_STREQ("42S22", state.get_jdbc_state(OB_ERR_COLUMN_UNKNOWN));
  ASSERT_STREQ("S1004", state.get_jdbc_state(OB_ERR_ILLEGAL_TYPE));
}
