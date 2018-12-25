/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * unit test for ObiRole
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include <gtest/gtest.h>
#include "common/ob_obi_role.h"
using namespace oceanbase::common;

TEST(ObObiRoleTest, test_serialization)
{
  ObiRole obi_role;
  ASSERT_TRUE(ObiRole::INIT == obi_role.get_role());
  obi_role.set_role(ObiRole::MASTER);
  char buff[64];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, obi_role.serialize(buff, 64, pos));
  ASSERT_EQ(1, pos);
  ASSERT_EQ(OB_SUCCESS, obi_role.serialize(buff, 64, pos));
  ASSERT_EQ(2, pos);
  int64_t len = obi_role.get_serialize_size();
  ObiRole role2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, role2.deserialize(buff, len, pos));
  ASSERT_TRUE(ObiRole::MASTER == role2.get_role());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
