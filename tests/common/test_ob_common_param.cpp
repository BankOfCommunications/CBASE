/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  test_ob_common_param.cpp,  10/08/2012 04:52:35 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   ob_common_param.cpp test
 *  misc things here
 */


#include <gtest/gtest.h>
#include "common/ob_common_param.h"

using namespace oceanbase;
using namespace common;

/*
TEST(TestErrMsg, err_msg)
{
  int max_len = 256;
  char buf[1024];
 
  // overflow test
  memset(buf, 'A', 1024);
  ob_set_err_msg(buf);
  ObString msg = ob_get_err_msg();
  TBSYS_LOG(INFO, "%.*s", msg.length(), msg.ptr());
  EXPECT_EQ(max_len, msg.length());

  ob_set_err_msg("err code is %d", 10);
  msg = ob_get_err_msg();
  TBSYS_LOG(INFO, "%.*s", msg.length(), msg.ptr());
  EXPECT_EQ(0, strncmp("err code is 10", msg.ptr(), msg.length()));
}
*/

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

