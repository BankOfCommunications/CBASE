/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include <gtest/gtest.h>
#include "common/ob_obi_config.h"
using namespace oceanbase::common;

TEST(ObiConfigTest, test_serialization)
{
  ObiConfig conf;
  ASSERT_EQ(0, conf.get_read_percentage());
  conf.set_read_percentage(70);
  ASSERT_EQ(70, conf.get_read_percentage());
  const int buff_size = 128;
  char buff[buff_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, conf.serialize(buff, buff_size, pos));
  int64_t len = conf.get_serialize_size();
  
  ObiConfig conf2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, conf2.deserialize(buff, len, pos));
  ASSERT_EQ(conf.get_read_percentage(), conf2.get_read_percentage());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
