/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_manager_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "rootserver/ob_ups_manager.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_server_config.h"
#include "common/ob_config_manager.h"
#include "mock_root_rpc_stub.h"
#include "root_server_tester.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase;
using ::testing::_;
using ::testing::Eq;
using ::testing::Ne;
using ::testing::AtLeast;
using ::testing::Args;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgReferee;

common::ObMsgUpsHeartbeat msg;
struct Argument
{
  ObServer addr;
  ObUpsStatus stat;
  ObiRole obi_role;
  ObUpsManager um;
};
TEST(ObUpsManagerTest, test_basic)
{
  int64_t timeout_us = 500*1000; // 500ms
  int64_t lease_duration = 5000*1000; // 3s
  int64_t lease_reserved_us = 1000*1000;     // 1s
  int64_t waiting_register_time = 5000*1000;   // 5s
  int64_t config_version = 2;

  oceanbase::rootserver::testing::MockObRootRpcStub rpc_stub;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);
  volatile int64_t schema_version = 0;
  ObiRole rs_obi_role;
  ObUpsManager um(rpc_stub, &root_worker, timeout_us, lease_duration, lease_reserved_us, waiting_register_time, rs_obi_role, schema_version, config_version);
  int32_t port_base = 1234;
  int64_t last_time = tbsys::CTimeUtil::getTime();
  int64_t start_time = last_time;
  // register 3 ups
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    int64_t log_seq_num = 567890 + i;
    ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
  }
  int64_t length = 1028;
  int64_t pos = 0;
  char buf[length];
  um.print(buf, length, pos);
  TBSYS_LOG(INFO, "um data =%s", buf);
  // send heartbeat without lease
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    TBSYS_LOG(INFO, "server = %s", to_cstring(addr));
    EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr,_))
      .Times(1);
  }
  int64_t interval = lease_duration-lease_reserved_us;
  int64_t now = tbsys::CTimeUtil::getTime();
  int64_t sleep_us = (interval + last_time - now);
  if (sleep_us > 0)
  {
    usleep(static_cast<useconds_t>(sleep_us));
  }
  last_time = tbsys::CTimeUtil::getTime();
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());
  ASSERT_TRUE(!um.has_master());
  ::testing::Mock::VerifyAndClearExpectations(&rpc_stub);

  // receive heartbeat
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  // select master
  um.check_ups_master_exist();
  ASSERT_TRUE(!um.has_master());
  now = tbsys::CTimeUtil::getTime();
  usleep(static_cast<useconds_t>(waiting_register_time - (now - start_time)));
  um.check_ups_master_exist();
  ASSERT_TRUE(um.has_master());
  ASSERT_EQ(2, um.ups_master_idx_);
  // send heartbeat with lease
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
      .Times(1);
  }
  now = tbsys::CTimeUtil::getTime();
  sleep_us = (interval + last_time - now);
  if (sleep_us > 0)
  {
    usleep(static_cast<useconds_t>(sleep_us));
  }
  last_time = tbsys::CTimeUtil::getTime();
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());
  ::testing::Mock::VerifyAndClearExpectations(&rpc_stub);

  // change master
  ObServer addr;
  addr.set_ipv4_addr("127.0.0.1", 4321);
  ASSERT_EQ(OB_NOT_REGISTERED, um.set_ups_master(addr, false)); // not exist
  addr.set_ipv4_addr("127.0.0.1", port_base+2);
  ASSERT_EQ(OB_INVALID_ARGUMENT, um.set_ups_master(addr, false)); // already master
  addr.set_ipv4_addr("127.0.0.1", port_base+1);
  ObiRole obi_role;
  ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, UPS_STAT_NOTSYNC, obi_role));
  ASSERT_EQ(OB_INVALID_ARGUMENT, um.set_ups_master(addr, false)); // not sync
  ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, UPS_STAT_SYNC, obi_role));
  ASSERT_EQ(OB_SUCCESS, um.set_ups_master(addr, false)); // ok
  ASSERT_EQ(1, um.ups_master_idx_);
  addr.set_ipv4_addr("127.0.0.1", port_base+0);
  ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, UPS_STAT_NOTSYNC, obi_role));
  ASSERT_EQ(OB_INVALID_ARGUMENT, um.set_ups_master(addr, false)); // not sync
  ASSERT_EQ(OB_SUCCESS, um.set_ups_master(addr, true)); // not sync but force to set
  ASSERT_EQ(0, um.ups_master_idx_);
  last_time = tbsys::CTimeUtil::getTime();
  // send heartbeat with lease
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
      .Times(1);
  }
  now = tbsys::CTimeUtil::getTime();
  sleep_us = (interval + last_time - now);
  if (sleep_us > 0)
  {
    usleep(static_cast<useconds_t>(sleep_us));
  }
  last_time = tbsys::CTimeUtil::getTime();
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());
  ::testing::Mock::VerifyAndClearExpectations(&rpc_stub);

  // revoke lease before select new one
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    if (0 == i)
    {
      // the current master
      EXPECT_CALL(rpc_stub, revoke_ups_lease(addr, _, _, _))
        .Times(1);
    }
    else
    {
      EXPECT_CALL(rpc_stub, revoke_ups_lease(addr, _, _, _))
        .Times(0);
    }
  }
  addr.set_ipv4_addr("127.0.0.1", port_base+2);
  ASSERT_EQ(OB_SUCCESS, um.set_ups_master(addr, true));
}

TEST(ObUpsManagerTest, test_register_lease)
{
  int64_t timeout_us = 500*1000; // 500ms
  int64_t lease_duration = 3000*1000; // 3s
  int64_t lease_reserved_us = 1000*1000;     // 1s
  int64_t waiting_register_time = 5000*1000;   // 5s

  oceanbase::rootserver::testing::MockObRootRpcStub rpc_stub;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);
  int64_t config_version = 2;
  ObiRole rs_obi_role;
  volatile int64_t schema_version = 0;
  ObUpsManager um(rpc_stub, &root_worker, timeout_us, lease_duration, lease_reserved_us, waiting_register_time, rs_obi_role, schema_version, config_version);
  int32_t port_base = 1234;
  // register 3 ups
  ObServer addr;
  addr.set_ipv4_addr("127.0.0.1", port_base+0);
  int64_t log_seq_num = 567890;
  ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
  ASSERT_TRUE(!um.has_master());
  ASSERT_EQ(OB_ALREADY_REGISTERED, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));

  addr.set_ipv4_addr("127.0.0.1", port_base+1);
  ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 1234, "0.4.1.2"));
  ASSERT_TRUE(!um.has_master());

  // send heartbeat without lease
  addr.set_ipv4_addr("127.0.0.1", port_base+0);
  EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
    .Times(1);
  addr.set_ipv4_addr("127.0.0.1", port_base+1);
  EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
    .Times(1);
  usleep(static_cast<useconds_t>(lease_duration-lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());

  addr.set_ipv4_addr("127.0.0.1", port_base+2);
  int64_t now = tbsys::CTimeUtil::getTime();
  int64_t my_lease = now + 50*1000;
  ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, my_lease, "0.4.1.2"));
  ASSERT_TRUE(um.has_master());
  ASSERT_EQ(2, um.ups_master_idx_);
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }

  // send heartbeat with lease
  for (int i = 0; i < 3; ++i)
  {
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    if (2 == i)
    {
       EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
        .Times(1);

     // EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
     //   .With(Args<0,1>(Eq()))
     //   .Times(1);
    }
    else
    {
      EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
        .Times(1);
    }
  }
  usleep(static_cast<useconds_t>(lease_duration-lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());
}

TEST(ObUpsManagerTest, test_register_lease2)
{
  int64_t timeout_us = 500*1000; // 500ms
  int64_t lease_duration = 3000*1000; // 3s
  int64_t lease_reserved_us = 1000*1000;     // 1s
  int64_t waiting_register_time = 5000*1000;   // 5s

  //ObRootConfig config;
  //config.flag_network_timeout_us_.set(timeout_us);
  //config.flag_ups_lease_us_.set(lease_duration);
  //config.flag_ups_lease_reserved_us_.set(lease_reserved_us);
  //config.flag_ups_waiting_register_duration_us_.set(waiting_register_time);

  oceanbase::rootserver::testing::MockObRootRpcStub rpc_stub;
  ObiRole rs_obi_role;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);
  int64_t config_version = 2;
  volatile int64_t schema_version = 0;
  ObUpsManager um(rpc_stub, &root_worker, timeout_us, lease_duration, lease_reserved_us, waiting_register_time, rs_obi_role, schema_version, config_version);
  int32_t port_base = 1234;
  int64_t log_seq_num = 567890;
  // register 3 ups
  ObServer addr;
  addr.set_ipv4_addr("127.0.0.1", port_base+0);
  ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
  ASSERT_TRUE(!um.has_master());

  int64_t now = tbsys::CTimeUtil::getTime();
  addr.set_ipv4_addr("127.0.0.1", port_base+1);
  ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, now+50*1000, "0.4.1.2"));
  ASSERT_TRUE(um.has_master());

  addr.set_ipv4_addr("127.0.0.1", port_base+2);
  ASSERT_EQ(OB_CONFLICT_VALUE, um.register_ups(addr, 5678, log_seq_num, now+50*1000, "0.4.1.2"));
  ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
  ASSERT_EQ(1, um.ups_master_idx_);
}
void renew_lease(void *argument)
{
  Argument *arg = reinterpret_cast<Argument*>(argument);
  for (int i = 0; i < 5; i++)
  {
    arg->um.renew_lease(arg->addr, arg->stat, arg->obi_role);
  }
}
TEST(ObUpsManagerTest, test_read_percent2)
{
  int64_t timeout_us = 500*1000; // 500ms
  int64_t lease_duration = 3000*1000; // 3s
  int64_t lease_reserved_us = 1000*1000;     // 1s
  int64_t waiting_register_time = 5000*1000;   // 5s
  int32_t ups_read_percent = 20;
  int32_t master_read_percent = 30;

  int32_t port_base = 1234;
  //ObRootConfig config;
  int32_t ups_count = 3;
  //config.flag_read_master_master_ups_percent_.set(-1);
  //config.flag_read_slave_master_ups_percent_.set(-1);
  //config.flag_network_timeout_us_.set(timeout_us);
  //config.flag_ups_lease_us_.set(lease_duration);
  //config.flag_ups_lease_reserved_us_.set(lease_reserved_us);
  //config.flag_ups_waiting_register_duration_us_.set(waiting_register_time);

  oceanbase::rootserver::testing::MockObRootRpcStub rpc_stub;
  ObiRole rs_obi_role;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);
  int64_t config_version = 2;

  volatile int64_t schema_version = 0;
  ObUpsManager um(rpc_stub, &root_worker, timeout_us, lease_duration, lease_reserved_us, waiting_register_time, rs_obi_role, schema_version, config_version);
  // register 3 ups
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    int64_t log_seq_num = 567890 + i;
    if (0 == i)
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, now+50*1000, "0.4.1.2"));
    }
    else
    {
      ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
    }
  }
  ASSERT_EQ(ups_count, um.get_ups_count());
  ASSERT_TRUE(um.has_master());
  ASSERT_EQ(0, um.ups_master_idx_);
  um.reset_ups_read_percent();
  for (int i = 0; i < 3; i++) //nosync ups have zero flow
  {
    if (i == 0)
    {
      ASSERT_EQ(100, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(100, um.ups_array_[i].cs_read_percentage_);
    }
  }
  for (int i = 0; i < 3; i++)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  um.reset_ups_read_percent();
  for (int i = 0; i < 3; i++) //with no config, all ups pingjun
  {
    ASSERT_EQ(33, um.ups_array_[i].ms_read_percentage_);
    ASSERT_EQ(33, um.ups_array_[i].cs_read_percentage_);
  }

  //ups2 notsync
  for (int i = 0; i < 3; i++)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    if (i != 2)
    {
    }
    else
    {
      stat = UPS_STAT_NOTSYNC;
    }
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  um.reset_ups_read_percent();
  for (int i =0; i < 3; i++)
  {
    if (i == 2)
    {
    }
    else
    {
      ASSERT_EQ(50, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(50, um.ups_array_[i].cs_read_percentage_);
    }
  }
  ObUpsList ups_list;
  ASSERT_EQ(OB_SUCCESS, um.get_ups_list(ups_list));
  ASSERT_EQ(2, ups_list.ups_count_);
  um.set_ups_config(master_read_percent, ups_read_percent); //with config
  for (int i = 0; i < 3; i++)
  {
    if (i == 0)
    {
      ASSERT_EQ(ups_read_percent, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(ups_read_percent, um.ups_array_[i].cs_read_percentage_);
    }
    else if (i == 1)
    {
      ASSERT_EQ((100 - ups_read_percent), um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ((100 - ups_read_percent), um.ups_array_[i].cs_read_percentage_);
    }
  }
  //ups2 sync
  for (int i = 0; i < 3; i++)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  um.reset_ups_read_percent();
  for (int i = 0; i < 3; i++)
  {
    if (i == 0)
    {
      ASSERT_EQ(ups_read_percent, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(ups_read_percent, um.ups_array_[i].cs_read_percentage_);
    }
    else
    {
      ASSERT_EQ((100 - ups_read_percent)/2, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ((100 - ups_read_percent)/2, um.ups_array_[i].cs_read_percentage_);
    }
  }
  //set ups config
  int32_t ups_config = 32;
  for (int i = 0; i < 3; i++)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ASSERT_EQ(OB_SUCCESS, um.set_ups_config(addr, ups_config + i, ups_config + i));
  }
  for (int i = 0; i < 3; i++)
  {
    ASSERT_EQ(ups_config + i, um.ups_array_[i].ms_read_percentage_);
    ASSERT_EQ(ups_config + i, um.ups_array_[i].cs_read_percentage_);
  }
  //set ups2 notsync
  for (int i = 0; i < 3; i++)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    if (i != 2)
    {
    }
    else
    {
      stat = UPS_STAT_NOTSYNC;
    }
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  ObUpsList list;
  ASSERT_EQ(OB_SUCCESS, um.get_ups_list(list));
  ASSERT_EQ(3, list.ups_count_);
  //ups2 sync
  for (int i = 0; i < 3; i++)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  //UPS0 offline
  usleep(static_cast<useconds_t>(lease_duration - lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());

  // receive heartbeat
  for (int i = 0; i < 3; ++i)
  {
    if (0 != i)
    {
      ObServer addr;
      addr.set_ipv4_addr("127.0.0.1", port_base+i);
      ObUpsStatus stat = UPS_STAT_SYNC;
      ObiRole obi_role;
      ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
    }
  }

  // check heartbeat
  usleep(static_cast<useconds_t>(lease_duration - lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());
  usleep(static_cast<useconds_t>(um.MAX_CLOCK_SKEW_US+lease_reserved_us));
  um.check_lease();
  // master ups offline
  ASSERT_EQ(2, um.get_ups_count());
  ASSERT_TRUE(um.has_master());
  for (int i = 1; i < 3; i++)
  {
    if (i == um.ups_master_idx_)
    {
      ASSERT_EQ(ups_read_percent, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(ups_read_percent, um.ups_array_[i].cs_read_percentage_);
    }
    else
    {
      ASSERT_EQ(100 - ups_read_percent, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(100 - ups_read_percent, um.ups_array_[i].cs_read_percentage_);
    }
  }
}
TEST(ObUpsManagerTest, test_read_percent)
{
  int64_t timeout_us = 500*1000; // 500ms
  int64_t lease_duration = 3000*1000; // 3s
  int64_t lease_reserved_us = 1000*1000;     // 1s
  int64_t waiting_register_time = 5000*1000;   // 5s
  int32_t ups_read_percent = 20;
  int32_t master_read_percent = 30;

  //ObRootConfig config;
  //config.flag_read_master_master_ups_percent_.set(master_read_percent);
  //config.flag_read_slave_master_ups_percent_.set(ups_read_percent);
  //config.flag_network_timeout_us_.set(timeout_us);
  //config.flag_ups_lease_us_.set(lease_duration);
  //config.flag_ups_lease_reserved_us_.set(lease_reserved_us);
  //config.flag_ups_waiting_register_duration_us_.set(waiting_register_time);

  oceanbase::rootserver::testing::MockObRootRpcStub rpc_stub;
  ObiRole rs_obi_role;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);
  int64_t config_version = 2;

  volatile int64_t schema_version = 0;
  ObUpsManager um(rpc_stub, &root_worker, timeout_us, lease_duration, lease_reserved_us, waiting_register_time, rs_obi_role, schema_version, config_version);
  int32_t port_base = 1234;
  // register 3 ups
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    int64_t log_seq_num = 567890 + i;
    if (0 == i)
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, now+50*1000, "0.4.1.2"));
    }
    else
    {
      ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
    }
  }
  um.set_ups_config(master_read_percent, ups_read_percent);
  ASSERT_EQ(3, um.get_ups_count());
  ASSERT_TRUE(um.has_master());
  ASSERT_EQ(0, um.ups_master_idx_);
  for (int i = 0; i < 3; ++i)
  {
    if (0 != i)
    {
      ObServer addr;
      addr.set_ipv4_addr("127.0.0.1", port_base+i);
      ObUpsStatus stat = UPS_STAT_SYNC;
      ObiRole obi_role;
      ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
    }
  }
  um.reset_ups_read_percent();
  for (int i = 0; i < 3; i++)
  {
    if (0 == i)
    {
      ASSERT_EQ(20, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(20, um.ups_array_[i].cs_read_percentage_);
    }
    else
    {
      ASSERT_EQ(40, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(40, um.ups_array_[i].cs_read_percentage_);
    }
  }

  //set obi role = master
  rs_obi_role.set_role(ObiRole::MASTER);
  um.reset_ups_read_percent();
  for (int i = 0; i < 3; i++)
  {
    if (0 == i)
    {
      ASSERT_EQ(30, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(30, um.ups_array_[i].cs_read_percentage_);
    }
    else
    {
      ASSERT_EQ(35, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(35, um.ups_array_[i].cs_read_percentage_);
    }
  }

  // send heartbeat
  usleep(static_cast<useconds_t>(lease_duration - lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());

  // receive heartbeat
  for (int i = 0; i < 3; ++i)
  {
    if (0 != i)
    {
      ObServer addr;
      addr.set_ipv4_addr("127.0.0.1", port_base+i);
      ObUpsStatus stat = UPS_STAT_SYNC;
      ObiRole obi_role;
      ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
    }
  }

  // check heartbeat
  usleep(static_cast<useconds_t>(lease_duration - lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());
  usleep(static_cast<useconds_t>(um.MAX_CLOCK_SKEW_US+lease_reserved_us));
  um.check_lease();
  // master ups offline
  ASSERT_EQ(2, um.get_ups_count());
  ASSERT_TRUE(um.has_master());
  for (int i = 1; i < 3; i++)
  {
    if (i == um.ups_master_idx_)
    {
      TBSYS_LOG(INFO, "master index=%d", i);
      ASSERT_EQ(30, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(30, um.ups_array_[i].cs_read_percentage_);
    }
    else
    {
      ASSERT_EQ(70, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(70, um.ups_array_[i].cs_read_percentage_);
    }
  }
  //ups reregister
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base + 0);
    int64_t log_seq_num = 567890;
    ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
    ObUpsStatus stat = UPS_STAT_SYNC;
    ObiRole obi_role;
    ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
  }
  for (int i = 1; i < 3; i++)
  {
    if (i == um.ups_master_idx_)
    {
      ASSERT_EQ(30, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(30, um.ups_array_[i].cs_read_percentage_);
    }
    else
    {
      ASSERT_EQ(35, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(35, um.ups_array_[i].cs_read_percentage_);
    }
  }
  // master_ups report slave down
  ASSERT_EQ(OB_SUCCESS, um.slave_failure(um.ups_array_[um.ups_master_idx_].addr_, um.ups_array_[0].addr_));
  ASSERT_EQ(2, um.get_ups_count());
  ASSERT_TRUE(um.has_master());
  for (int i = 1; i < 3; i++)
  {
    if (i == um.ups_master_idx_)
    {
      ASSERT_EQ(30, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(30, um.ups_array_[i].cs_read_percentage_);
    }
    else if (0 != i)
    {
      ASSERT_EQ(70, um.ups_array_[i].ms_read_percentage_);
      ASSERT_EQ(70, um.ups_array_[i].cs_read_percentage_);
    }
  }
}
TEST(ObUpsManagerTest, test_offline)
{
  int64_t timeout_us = 500*1000; // 500ms
  int64_t lease_duration = 3000*1000; // 3s
  int64_t lease_reserved_us = 1000*1000;     // 1s
  int64_t waiting_register_time = 5000*1000;   // 5s

  //ObRootConfig config;
  //config.flag_network_timeout_us_.set(timeout_us);
  //config.flag_ups_lease_us_.set(lease_duration);
  //config.flag_ups_lease_reserved_us_.set(lease_reserved_us);
  //config.flag_ups_waiting_register_duration_us_.set(waiting_register_time);

  oceanbase::rootserver::testing::MockObRootRpcStub rpc_stub;
  ObiRole rs_obi_role;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);
  int64_t config_version = 2;
  volatile int64_t schema_version = 0;
  ObUpsManager um(rpc_stub, &root_worker, timeout_us, lease_duration, lease_reserved_us, waiting_register_time, rs_obi_role, schema_version, config_version);
  int32_t port_base = 1234;
  // register 3 ups
  for (int i = 0; i < 3; ++i)
  {
    ObServer addr;
    addr.set_ipv4_addr("127.0.0.1", port_base+i);
    int64_t log_seq_num = 567890 + i;
    if (0 == i)
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, now+50*1000, "0.4.1.2"));
    }
    else
    {
      ASSERT_EQ(OB_SUCCESS, um.register_ups(addr, 5678, log_seq_num, 0, "0.4.1.2"));
    }
    EXPECT_CALL(rpc_stub, grant_lease_to_ups(addr, _))
      .Times(AtLeast(1));
  }
  ASSERT_EQ(3, um.get_ups_count());
  ASSERT_TRUE(um.has_master());
  ASSERT_EQ(0, um.ups_master_idx_);

  // send heartbeat
  usleep(static_cast<useconds_t>(lease_duration - lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());

  // receive heartbeat
  for (int i = 0; i < 3; ++i)
  {
    if (0 != i)
    {
      ObServer addr;
      addr.set_ipv4_addr("127.0.0.1", port_base+i);
      ObUpsStatus stat = UPS_STAT_SYNC;
      ObiRole obi_role;
      ASSERT_EQ(OB_SUCCESS, um.renew_lease(addr, stat, obi_role));
      uint64_t lsn = i + 1000000;
      EXPECT_CALL(rpc_stub, get_ups_max_log_seq(addr, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgReferee<1>(lsn),
                    Return(0)));
    }
  }

  // check heartbeat
  usleep(static_cast<useconds_t>(lease_duration - lease_reserved_us));
  ASSERT_EQ(OB_SUCCESS, um.grant_lease());
  usleep(static_cast<useconds_t>(um.MAX_CLOCK_SKEW_US+lease_reserved_us));
  um.check_lease();
  // master ups offline
  ASSERT_EQ(2, um.get_ups_count());
  ASSERT_TRUE(um.has_master());
  ASSERT_EQ(2, um.ups_master_idx_);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
