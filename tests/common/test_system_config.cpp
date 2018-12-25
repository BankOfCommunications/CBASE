/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-02-05 15:04:40 fufeng.syd>
 * Version: $Id$
 * Filename: test_system_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include <gtest/gtest.h>
#include "common/ob_system_config.h"
#include "common/ob_system_config_key.h"
#include "common/ob_malloc.h"

using namespace oceanbase;

int main(int argc, char *argv[])
{
  ob_init_memory_pool();

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestSystemConfig
  : public ::testing::Test
{
  public:
    virtual void SetUp()
    {
      sc.init();
    }

    virtual void TearDown()
    {
    }

    void insert(const ObSystemConfigKey &key,
                const ObSystemConfigValue &value)
    {
      sc.map_.set(key, value);
    }

  protected:
    common::ObSystemConfig sc;
};

TEST_F(TestSystemConfig, HitQueryScope)
{
  ObSystemConfigKey key;
  char szValue[256] = {0};
  key.set_name("test_name");
  key.set_varchar(ObString::make_string("svr_type"), "mergeserver");
  key.set_varchar(ObString::make_string("svr_ip"), "127.0.0.1");
  key.set_int(ObString::make_string("svr_port"), 123);
  key.set_int(ObString::make_string("cluster_id"), 1);

  ObSystemConfigKey okey;
  ObSystemConfigValue value;
  okey.set_name("test_name");
  value.set_value("default_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("default_value", szValue);

  okey.set_int(ObString::make_string("cluster_id"), 1);
  value.set_value("cluster_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("cluster_value", szValue);

  okey.set_varchar(ObString::make_string("svr_type"), "mergeserver");
  value.set_value("type_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("type_value", szValue);

  okey.set_varchar(ObString::make_string("svr_ip"), "127.0.0.1");
  okey.set_int(ObString::make_string("svr_port"), 123);
  value.set_value("ip_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("ip_value", szValue);

  puts(to_cstring(sc));
}

TEST_F(TestSystemConfig, NotHitQueryScope)
{
  ObSystemConfigKey key;
  char szValue[256] = {0};
  key.set_name("test_name");
  key.set_varchar(ObString::make_string("svr_type"), "mergeserver");
  key.set_varchar(ObString::make_string("svr_ip"), "127.0.0.1");
  key.set_int(ObString::make_string("svr_port"), 123);
  key.set_int(ObString::make_string("cluster_id"), 1);

  ObSystemConfigKey okey;
  ObSystemConfigValue value;
  okey.set_name("test_name");
  value.set_value("default_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("default_value", szValue);

  okey.set_int(ObString::make_string("cluster_id"), 1);
  value.set_value("cluster_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("cluster_value", szValue);

  okey.set_varchar(ObString::make_string("svr_type"), "mergeserver");
  value.set_value("type_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("type_value", szValue);

  okey.set_varchar(ObString::make_string("svr_ip"), "127.0.0.1");
  okey.set_int(ObString::make_string("svr_port"), 123);
  value.set_value("ip_value");
  insert(okey, value);
  sc.read_str(key, szValue, 256, "default");
  EXPECT_STREQ("ip_value", szValue);

  puts(to_cstring(sc));
}
