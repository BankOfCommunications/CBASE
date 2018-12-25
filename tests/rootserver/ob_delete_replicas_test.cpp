/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_delete_replicas_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "rootserver/ob_root_server2.h"
#include "common/ob_tablet_info.h"
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_root_worker.h"
#include "root_server_tester.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "mock_root_rpc_stub.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <cassert>
#include "../common/test_rowkey_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase;
using namespace oceanbase::rootserver::testing;
using ::testing::_;
using ::testing::Eq;
using ::testing::AtLeast;
static CharArena allocator_;

class TestRootWorker: public ObRootWorkerForTest
{
  public:
    TestRootWorker() :ObRootWorkerForTest(config_mgr_, rs_config_), config_mgr_(rs_config_, reload_)
  {
  }
    virtual ObRootRpcStub& get_rpc_stub() { return rpc_;}
    MOCK_METHOD1(submit_delete_tablets_task, int(const common::ObTabletReportInfoList& delete_list));
    //virtual int submit_delete_tablets_task(const common::ObTabletReportInfoList& delete_list)
    //{
    //  TBSYS_LOG(INFO, "delete list size = %ld", delete_list.get_tablet_size());
    //  return OB_SUCCESS;
    //}
    ObRootServerConfig rs_config_;
    ObReloadConfig reload_;
    common::ObConfigManager config_mgr_;
    MockObRootRpcStub rpc_;
};

class ObDeleteReplicasTest: public ::testing::Test
{
  public:
    ObDeleteReplicasTest()
    {
    }
    virtual void SetUp();
    virtual void TearDown();
    void register_cs(int32_t cs_num);
    void heartbeat_cs(int32_t cs_num);
    ObServer &get_addr(int32_t idx);
  public:
    TestRootWorker worker_;
    ObRootServer2 *server_;
  protected:
    ObServer addr_;
};

void ObDeleteReplicasTest::SetUp()
{
  server_ = worker_.get_root_server();
  common::ObSystemConfig sys_config;
  sys_config.init();
  //worker_.set_config_file_name("ob_delete_replicas_test.conf");
  worker_.rs_config_.init(sys_config);
  worker_.rs_config_.port = 1110;
  worker_.rs_config_.io_thread_count = 2;
  worker_.create_eio();
  worker_.rs_config_.read_queue_size = 2;
  worker_.rs_config_.root_server_ip.set_value("10.232.35.40");
  worker_.rs_config_.schema_filename.set_value("ob_delete_replicas_test_schema.ini");
  ASSERT_EQ(OB_SUCCESS, worker_.initialize());
  worker_.get_role_manager()->set_role(ObRoleMgr::STANDALONE); // for testing

  server_->start_threads();
  sleep(5);
}

void ObDeleteReplicasTest::TearDown()
{
  server_->stop_threads();
}

ObServer &ObDeleteReplicasTest::get_addr(int32_t idx)
{
  char buff[128];
  snprintf(buff, 128, "10.10.10.%d", idx);
  int port = 26666;
  addr_.set_ipv4_addr(buff, port);
  return addr_;
}

void ObDeleteReplicasTest::register_cs(int32_t cs_num)
{
  for (int i = 0; i < cs_num; ++i)
  {
    int32_t status = 0;
    ASSERT_EQ(OB_SUCCESS, server_->regist_chunk_server(get_addr(i), "0.4.1.2", status));
    TBSYS_LOG(INFO, "register cs, id=%d status=%d", i, status);
  }
}

void ObDeleteReplicasTest::heartbeat_cs(int32_t cs_num)
{
  for (int i = 0; i < cs_num; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, server_->receive_hb(get_addr(i), 0, false, OB_CHUNKSERVER));
  }
}

TEST_F(ObDeleteReplicasTest, delete_cs_replica)
{
  server_->get_boot()->set_boot_ok();
  register_cs(4);
  char* startkey = (char*)"A";
  char* endkey = (char*)"Z";
  ObTabletReportInfo tablet;
  tablet.tablet_info_.range_.set_whole_range();
  tablet.tablet_info_.range_.table_id_ = 10001;
  tablet.tablet_info_.range_.start_key_ = make_rowkey(startkey, &allocator_);
  tablet.tablet_info_.range_.end_key_ = make_rowkey(endkey, &allocator_);
  tablet.tablet_info_.row_count_ = 100;
  tablet.tablet_info_.occupy_size_ = 1024;
  tablet.tablet_info_.crc_sum_ = 3456;
  tablet.tablet_location_.tablet_version_ = 1;
  ObTabletReportInfoList tablet_list;
  for (int i = 0; i < 3; ++i)
  {
    tablet_list.reset();
    tablet.tablet_location_.chunkserver_ = get_addr(i);
    ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
    ASSERT_EQ(OB_SUCCESS, server_->report_tablets(get_addr(i), tablet_list, 1));
  }
  // int delete_replicas(const bool did_replay, const common::ObServer & cs, const common::ObTabletReportInfoList & replicas);
  for (int i = 0; i < 8; ++i)
  {
    heartbeat_cs(4);
    sleep(1);
  }
  // remove two replicas
  int ret = server_->delete_replicas(true, get_addr(1), tablet_list);
  EXPECT_TRUE(ret == OB_SUCCESS);
  ret = server_->delete_replicas(true, get_addr(1), tablet_list);
  EXPECT_TRUE(ret == OB_SUCCESS);
  ret = server_->delete_replicas(true, get_addr(2), tablet_list);
  EXPECT_TRUE(ret == OB_SUCCESS);
  ret = server_->delete_replicas(true, get_addr(2), tablet_list);
  EXPECT_TRUE(ret == OB_SUCCESS);
  // not safe
  ret = server_->delete_replicas(true, get_addr(0), tablet_list);
  EXPECT_TRUE(ret != OB_SUCCESS);
  ret = server_->delete_replicas(true, get_addr(0), tablet_list);
  EXPECT_TRUE(ret != OB_SUCCESS);
  exit(0);
}



TEST_F(ObDeleteReplicasTest, delete_in_init)
{
  server_->get_boot()->set_boot_ok();
  register_cs(4);
  char* startkey = (char*)"A";
  char* endkey = (char*)"Z";
  ObTabletReportInfo tablet;
  tablet.tablet_info_.range_.set_whole_range();
  tablet.tablet_info_.range_.table_id_ = 10001;
  tablet.tablet_info_.range_.start_key_ = make_rowkey(startkey, &allocator_);
  tablet.tablet_info_.range_.end_key_ = make_rowkey(endkey, &allocator_);
  tablet.tablet_info_.row_count_ = 100;
  tablet.tablet_info_.occupy_size_ = 1024;
  tablet.tablet_info_.crc_sum_ = 3456;
  tablet.tablet_location_.tablet_version_ = 1;
  ObTabletReportInfoList tablet_list;
  for (int i = 0; i < 4; ++i)
  {
    tablet_list.reset();
    tablet.tablet_location_.chunkserver_ = get_addr(i);
    ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
    ASSERT_EQ(OB_SUCCESS, server_->report_tablets(get_addr(i), tablet_list, 1));
    if (3 == i)
    {
     // EXPECT_CALL(worker_, submit_delete_tablets_task(_))
       // .Times(AtLeast(1));
    }
  }
  for (int i = 0; i < 2; ++i)
  {
    heartbeat_cs(4);
    sleep(1);
  }
}

TEST_F(ObDeleteReplicasTest, delete_when_rereplication)
{
  server_->get_boot()->set_boot_ok();
  const int CS_NUM = 3;
  register_cs(CS_NUM);
  char* startkey = (char*)"A";
  char* endkey = (char*)"Z";
  ObTabletReportInfo tablet;
  tablet.tablet_info_.range_.set_whole_range();
  tablet.tablet_info_.range_.table_id_ = 10001;
  tablet.tablet_info_.range_.start_key_ = make_rowkey(startkey, &allocator_);
  tablet.tablet_info_.range_.end_key_ = make_rowkey(endkey, &allocator_);
  tablet.tablet_info_.row_count_ = 100;
  tablet.tablet_info_.occupy_size_ = 1024;
  tablet.tablet_info_.crc_sum_ = 3456;
  tablet.tablet_location_.tablet_version_ = 1;
  ObTabletReportInfoList tablet_list;
  for (int i = 0; i < CS_NUM; ++i)
  {
    tablet_list.reset();
    tablet.tablet_location_.chunkserver_ = get_addr(i);
    ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
    ASSERT_EQ(OB_SUCCESS, server_->report_tablets(get_addr(i), tablet_list, 1));
  }
  for (int i = 0; i < 8; ++i)
  {
    heartbeat_cs(CS_NUM);
    sleep(1);
  }
  // case 2
  //server_->config_.flag_tablet_replicas_num_.set(2);

  tablet.tablet_location_.chunkserver_ = get_addr(0);
  tablet_list.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
 // EXPECT_CALL(worker_, submit_delete_tablets_task(_))
   // .Times(AtLeast(1));

  //worker_.get_mock_rpc_stub().set_delete_tablet(tablet);
  for (int i = 0; i < 10; ++i)  // wait balance worker
  {
    heartbeat_cs(CS_NUM);
    sleep(1);
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
