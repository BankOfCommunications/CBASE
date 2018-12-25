/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_ob_mysql_command_packet.cpp is for what ...
 *
 * Version: ***: test_ob_mysql_command_packet.cpp  Mon Sep 17 14:10:37 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */

#include "tblog.h"
#include "easy_io_struct.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "obmysql/packet/ob_mysql_command_packet.h"
#include "gtest/gtest.h"
using namespace oceanbase::obmysql;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    class TestObMySQLCommandPacket: public ::testing::Test
    {

    };

    TEST_F(TestObMySQLCommandPacket, basic_test)
    {
      ObMySQLCommandPacket packet;
      EXPECT_EQ(0, packet.get_type());
      EXPECT_TRUE(NULL == packet.get_request());

      //test for set/get request
      easy_request_t *req = NULL;
      EXPECT_EQ(OB_INVALID_ARGUMENT, packet.set_request(req));

      req = reinterpret_cast<easy_request_t*>(ob_malloc(sizeof(easy_request_t), ObModIds::TEST));
      EXPECT_EQ(OB_SUCCESS, packet.set_request(req));

      easy_request_t *greq = packet.get_request();
      EXPECT_EQ(req, greq);

      //test set/get command
      const char* cstr = "select * from sqltest_table;";
      char* str = const_cast<char*>(cstr);
      ObString obstr(static_cast<int32_t>(strlen(str)),
                     static_cast<int32_t>(strlen(str)),
                     str);

      packet.set_command(str, static_cast<int32_t>(strlen(str)));
      EXPECT_EQ(obstr, packet.get_command());
      EXPECT_EQ(static_cast<int32_t>(strlen(str)), packet.get_command_length());
    }
  }
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
