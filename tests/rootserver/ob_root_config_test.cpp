/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_config_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include "rootserver/ob_root_config.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

TEST(ObRootConfigTest, basic_test)
{
  ObRootConfig rconf;
  ASSERT_EQ(OB_NOT_INIT, rconf.reload());
  ASSERT_TRUE(OB_SUCCESS != rconf.load("filenotexist"));
  ASSERT_EQ(OB_SUCCESS, rconf.load("ob_root_config_test.ini"));
  ASSERT_EQ(OB_SUCCESS, rconf.reload());
  ASSERT_EQ(0, strcmp(rconf.flag_my_vip_.get(), "127.0.0.127"));
  ASSERT_EQ(23, rconf.flag_thread_count_.get());
  ASSERT_EQ(501, rconf.flag_read_task_queue_size_.get());
  ASSERT_EQ(51, rconf.flag_write_task_queue_size_.get());
  ASSERT_EQ(151, rconf.flag_log_task_queue_size_.get());
  ASSERT_EQ(1000001LL, rconf.flag_network_timeout_us_.get());
  ASSERT_EQ(91, rconf.flag_migrate_wait_seconds_.get());
  ASSERT_EQ(5001LL, rconf.flag_log_replay_wait_time_us_.get());
  ASSERT_EQ(51201, rconf.flag_log_sync_limit_kb_.get());
  ASSERT_EQ(2000001LL, rconf.flag_slave_register_timeout_us_.get());
  ASSERT_EQ(123, rconf.flag_lease_on_.get());
  ASSERT_EQ(15000001LL, rconf.flag_lease_interval_us_.get());
  ASSERT_EQ(10000001LL, rconf.flag_lease_reserved_time_us_.get());
  ASSERT_EQ(33, rconf.flag_tablet_replicas_num_.get());
  ASSERT_EQ(0, strcmp(rconf.flag_schema_filename_.get(), "./schemafile.ini"));
  ASSERT_EQ(10000001LL, rconf.flag_cs_lease_duration_us_.get());
  ASSERT_EQ(28801, rconf.flag_safe_lost_one_duration_seconds_.get());
  ASSERT_EQ(61, rconf.flag_safe_wait_init_duration_seconds_.get());
  rconf.print();
}

TEST(ObRootConfigTest, least_config)
{
  // @todo
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
