#include <gtest/gtest.h>

#include "root_server_tester.h"
#include "rootserver/ob_root_server2.h"
#include "common/ob_tablet_info.h"
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_root_table2.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

TEST(ObRootServer2LogTest, do_cs_regist)
{
  int32_t port = 1001;
  ObServer server(ObServer::IPV4, "10.10.10.1", port);
  int32_t status = 1;
  int64_t ts = 123456;

  ObRootServer2 master;
  ObRootWorkerForTest master_worker;
  ASSERT_TRUE(master.init("./root_server.conf", 100, &master_worker));
  ObRootServerTester master_wrapper(&master);
  master_wrapper.stop_thread();

  ObRootServer2 slave;
  ObRootWorkerForTest slave_worker;
  ASSERT_TRUE(slave.init("./root_server.conf", 100, &slave_worker));
  ObRootServerTester slave_wrapper(&slave);
  slave_wrapper.stop_thread();

  int ret = master.regist_server(server, false, status, ts);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRootLogWorker log_worker = slave_wrapper.get_log_worker();
  log_worker.do_cs_regist(server, ts);

  ObChunkServerManager& master_server_manager = master_wrapper.get_server_manager();
  ObChunkServerManager::iterator it = master_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != master_server_manager.end());

  int64_t master_hb = it->last_hb_time_;
  int32_t master_port = it->port_cs_;

  ObChunkServerManager& slave_server_manager = slave_wrapper.get_server_manager();
  it = slave_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != slave_server_manager.end());

  int64_t slave_hb = it->last_hb_time_;
  int32_t slave_port = it->port_cs_;

  ASSERT_EQ(ts, master_hb);
  ASSERT_EQ(port, master_port);
  ASSERT_EQ(master_hb, slave_hb);
  ASSERT_EQ(master_port, slave_port);
}

TEST(ObRootServer2LogTest, do_ms_regist)
{
  int32_t port = 1001;
  ObServer server(ObServer::IPV4, "10.10.10.1", port);
  int32_t status = 1;
  int64_t ts = 123456;

  ObRootServer2 master;
  ObRootWorkerForTest master_worker;
  ASSERT_TRUE(master.init("./root_server.conf", 100, &master_worker));
  ObRootServerTester master_wrapper(&master);
  master_wrapper.stop_thread();

  ObRootServer2 slave;
  ObRootWorkerForTest slave_worker;
  ASSERT_TRUE(slave.init("./root_server.conf", 100, &slave_worker));
  ObRootServerTester slave_wrapper(&slave);
  slave_wrapper.stop_thread();

  int ret = master.regist_server(server, true, status, ts);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRootLogWorker log_worker = slave_wrapper.get_log_worker();
  log_worker.do_ms_regist(server, ts);

  ObChunkServerManager& master_server_manager = master_wrapper.get_server_manager();
  ObChunkServerManager::iterator it = master_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != master_server_manager.end());

  int64_t master_hb = it->last_hb_time_;
  int32_t master_port = it->port_ms_;

  ObChunkServerManager& slave_server_manager = slave_wrapper.get_server_manager();
  it = slave_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != slave_server_manager.end());

  int64_t slave_hb = it->last_hb_time_;
  int32_t slave_port = it->port_ms_;

  ASSERT_EQ(port, master_port);
  ASSERT_EQ(master_hb, slave_hb);
  ASSERT_EQ(master_port, slave_port);
}

TEST(ObRootServer2LogTest, do_server_down)
{
  int32_t port = 1001;
  ObServer server(ObServer::IPV4, "10.10.10.1", port);
  int32_t status = 1;
  int64_t ts = 123456;

  ObRootServer2 master;
  ObRootWorkerForTest master_worker;
  ASSERT_TRUE(master.init("./root_server.conf", 100, &master_worker));

  ObRootServer2 slave;
  ObRootWorkerForTest slave_worker;
  ASSERT_TRUE(slave.init("./root_server.conf", 100, &slave_worker));

  ObRootServerTester wrapper(&slave);
  wrapper.stop_thread();
  int ret = master.regist_server(server, false, status, ts);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRootLogWorker log_worker = wrapper.get_log_worker();
  log_worker.do_cs_regist(server, ts); // up server

  ObChunkServerManager& server_manager = wrapper.get_server_manager();
  ObChunkServerManager::iterator it = server_manager.find_by_ip(server);
  ASSERT_TRUE(it != server_manager.end());
  ASSERT_NE(ObServerStatus::STATUS_DEAD, it->status_);

  log_worker.do_server_down(server, ts); // down server
  it = server_manager.find_by_ip(server);
  ASSERT_TRUE(it != server_manager.end());
  ASSERT_EQ(ObServerStatus::STATUS_DEAD, it->status_);
}

TEST(ObRootServer2LogTest, do_load_report)
{
  int32_t port = 1001;
  ObServer server(ObServer::IPV4, "10.10.10.1", port);
  int32_t status = 1;
  int64_t ts = 123456;

  int64_t cap = 12345;
  int64_t used = 1234;

  ObRootServer2 master;
  ObRootWorkerForTest master_worker;
  ASSERT_TRUE(master.init("./root_server.conf", 100, &master_worker));
  ObRootServerTester master_wrapper(&master);
  master_wrapper.stop_thread();

  ObRootServer2 slave;
  ObRootWorkerForTest slave_worker;
  ASSERT_TRUE(slave.init("./root_server.conf", 100, &slave_worker));
  ObRootServerTester slave_wrapper(&slave);
  slave_wrapper.stop_thread();

  int ret = master.regist_server(server, false, status, ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = master.update_capacity_info(server, cap, used);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRootLogWorker log_worker = slave_wrapper.get_log_worker();
  log_worker.do_cs_regist(server, ts);
  log_worker.do_cs_load_report(server, cap, used);

  ObChunkServerManager& master_server_manager = master_wrapper.get_server_manager();
  ObChunkServerManager::iterator it = master_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != master_server_manager.end());

  int64_t master_cap = it->disk_info_.get_capacity();
  int32_t master_used = it->disk_info_.get_used();

  ObChunkServerManager& slave_server_manager = slave_wrapper.get_server_manager();
  it = slave_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != slave_server_manager.end());

  int64_t slave_cap = it->disk_info_.get_capacity();
  int32_t slave_used = it->disk_info_.get_used();

  ASSERT_EQ(cap, master_cap);
  ASSERT_EQ(used, master_used);
  ASSERT_EQ(master_cap, slave_cap);
  ASSERT_EQ(master_used, slave_used);
}

TEST(ObRootServer2LogTest, start_switch)
{
  ObRootServer2 master;
  ObRootWorkerForTest master_worker;
  ASSERT_TRUE(master.init("./root_server.conf", 100, &master_worker));
  ObRootServerTester master_wrapper(&master);
  master_wrapper.stop_thread();

  ObRootServer2 slave;
  ObRootWorkerForTest slave_worker;
  ASSERT_TRUE(slave.init("./root_server.conf", 100, &slave_worker));
  ObRootServerTester slave_wrapper(&slave);
  slave_wrapper.stop_thread();

  master_wrapper.set_master(true);
  master_wrapper.stop_thread();
  slave_wrapper.stop_thread();
  slave_wrapper.set_master(false);

  TBSYS_LOG(DEBUG, "Server status: %d", master_wrapper.get_server_status());
  bool ret = master.start_switch();
  TBSYS_LOG(DEBUG, "Server status: %d", master_wrapper.get_server_status());
  ASSERT_TRUE(ret);
  TBSYS_LOG(DEBUG, "master start switch done");

  int64_t ts = master.get_time_stamp_changing();

  ObRootLogWorker log_worker = slave_wrapper.get_log_worker();
  log_worker.do_start_report(ts, true);
  log_worker.do_start_switch(ts);
  TBSYS_LOG(DEBUG, "slave start switch done");

  int64_t slave_ts = slave.get_time_stamp_changing();

  ASSERT_EQ(ts, slave_ts);
  ASSERT_EQ(master_wrapper.get_server_status(), slave_wrapper.get_server_status());
}

//TEST(ObRootServer2LogTest, start_report)
//{
//  ObRootServer2 master;
//  ObRootWorkerForTest master_worker;
//  ASSERT_TRUE(master.init("./root_server.conf", 100, &master_worker));
//  ObRootServerTester master_wrapper(&master);
//  master_wrapper.stop_thread();
//
//  ObRootServer2 slave;
//  ObRootWorkerForTest slave_worker;
//  ASSERT_TRUE(slave.init("./root_server.conf", 100, &slave_worker));
//  ObRootServerTester slave_wrapper(&slave);
//  slave_wrapper.stop_thread();
//
//  master_wrapper.set_master(true);
//  master_wrapper.stop_thread();
//  slave_wrapper.stop_thread();
//
//  TBSYS_LOG(DEBUG, "Server status: %d", master_wrapper.get_server_status());
//  bool ret = master.start_report(true);
//  TBSYS_LOG(DEBUG, "Server status: %d", master_wrapper.get_server_status());
//  ASSERT_TRUE(ret);
//  TBSYS_LOG(DEBUG, "master start switch done");
//
//  int64_t ts = master.get_time_stamp_changing();
//
//  ObRootLogWorker log_worker = slave_wrapper.get_log_worker();
//  log_worker.do_start_report(ts, true);
//  TBSYS_LOG(DEBUG, "slave start switch done");
//
//  int64_t slave_ts = slave.get_time_stamp_changing();
//
//  ASSERT_EQ(ts, slave_ts);
//  ASSERT_EQ(master_wrapper.get_server_status(), slave_wrapper.get_server_status());
//}
//
TEST(ObRootServer2LogTest, create_table_done)
{
  ObRootServer2 server;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(server.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&server);
  wrapper.stop_thread();

  ASSERT_FALSE(wrapper.get_create_table_done());

  wrapper.get_log_worker().do_create_table_done();

  ASSERT_TRUE(wrapper.get_create_table_done());
}

TEST(ObRootServer2LogTest, begin_balance)
{
  ObRootServer2 server;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(server.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&server);
  wrapper.stop_thread();

  ASSERT_NE(ObRootServer2::STATUS_BALANCING, wrapper.get_server_status());

  wrapper.get_log_worker().do_begin_balance();
  ASSERT_EQ(ObRootServer2::STATUS_BALANCING, wrapper.get_server_status());
}

TEST(ObRootServer2LogTest, balance_done)
{
  ObRootServer2 server;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(server.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&server);
  wrapper.stop_thread();

  wrapper.get_log_worker().do_begin_balance();
  ASSERT_EQ(ObRootServer2::STATUS_BALANCING, wrapper.get_server_status());

  wrapper.get_log_worker().do_balance_done();
  ASSERT_EQ(ObRootServer2::STATUS_SLEEP, wrapper.get_server_status());
}

TEST(ObRootServer2LogTest, us_schema_changing)
{
  ObRootServer2 server;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(server.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&server);
  wrapper.stop_thread();

  ObServerStatus* us = wrapper.get_update_server();
  us->status_ = ObServerStatus::STATUS_SERVING;

  wrapper.get_log_worker().do_us_mem_freezing(us->server_, 0);

  ASSERT_EQ(ObServerStatus::STATUS_REPORTING, us->status_);
}

TEST(ObRootServer2LogTest, us_schema_changed)
{
  ObRootServer2 server;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(server.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&server);
  wrapper.stop_thread();

  ObServerStatus* us = wrapper.get_update_server();
  us->status_ = ObServerStatus::STATUS_REPORTING;

  wrapper.get_log_worker().do_us_mem_frozen(us->server_, 0);
  ASSERT_EQ(ObServerStatus::STATUS_REPORTED, us->status_);
}

TEST(ObRootServer2LogTest, cs_schema_changing)
{
  int32_t port = 1001;
  ObServer server(ObServer::IPV4, "10.10.10.1", port);
  int64_t ts = 123456;

  ObRootServer2 root;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(root.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&root);
  wrapper.stop_thread();

  wrapper.get_log_worker().do_cs_regist(server, ts);
  ObChunkServerManager& slave_server_manager = wrapper.get_server_manager();
  ObChunkServerManager::iterator it = slave_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != slave_server_manager.end());

  it->status_ = ObServerStatus::STATUS_SERVING;

  wrapper.get_log_worker().do_cs_start_merging(it->server_);
  ASSERT_EQ(ObServerStatus::STATUS_REPORTING, it->status_);
}

TEST(ObRootServer2LogTest, cs_schema_changed)
{
  int32_t port = 1001;
  ObServer server(ObServer::IPV4, "10.10.10.1", port);
  int64_t ts = 12345;

  ObRootServer2 root;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(root.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&root);
  wrapper.stop_thread();

  wrapper.get_log_worker().do_cs_regist(server, ts);
  ObChunkServerManager& slave_server_manager = wrapper.get_server_manager();
  ObChunkServerManager::iterator it = slave_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != slave_server_manager.end());

  it->status_ = ObServerStatus::STATUS_REPORTING;

  wrapper.get_log_worker().do_cs_merge_over(it->server_, root.get_time_stamp_changing());
  ASSERT_EQ(ObServerStatus::STATUS_REPORTED, it->status_);

}

TEST(ObRootServer2LogTest, us_unload_done)
{
  ObRootServer2 server;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(server.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&server);
  wrapper.stop_thread();

  ObServerStatus* us = wrapper.get_update_server();
  us->status_ = ObServerStatus::STATUS_REPORTED;

  wrapper.get_log_worker().do_us_unload_done(us->server_);
  ASSERT_EQ(ObServerStatus::STATUS_SERVING, us->status_);
}

TEST(ObRootServer2LogTest, cs_unload_done)
{
  int32_t port = 1001;
  ObServer server(ObServer::IPV4, "10.10.10.1", port);
  int64_t ts = 123456;

  ObRootServer2 root;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(root.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&root);
  wrapper.stop_thread();

  wrapper.get_log_worker().do_cs_regist(server, ts);
  ObChunkServerManager& slave_server_manager = wrapper.get_server_manager();
  ObChunkServerManager::iterator it = slave_server_manager.find_by_ip(server);
  ASSERT_TRUE(it != slave_server_manager.end());

  it->status_ = ObServerStatus::STATUS_REPORTED;

  wrapper.get_log_worker().do_cs_unload_done(it->server_);
  ASSERT_EQ(ObServerStatus::STATUS_SERVING, it->status_);

}

TEST(ObRootServer2LogTest, do_cs_migrate_done)
{
  ObServer server(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);

  char buf1[10][30];
  char buf2[10][30];

  ObTabletReportInfoList report_list;
  ObTabletReportInfo report_info;
  ObTabletInfo info;

  ObTabletLocation location;
  location.tablet_version_ = 1;
  location.chunkserver_ = server;

  info.range_.table_id_ = 10001;
  info.range_.border_flag_.set_inclusive_end();
  info.range_.border_flag_.unset_inclusive_start();
  info.range_.start_key_.set_min_row();

  info.range_.start_key_.assign_buffer(buf1[0], 30);
  info.range_.end_key_.assign_buffer(buf2[0], 30);
  info.range_.start_key_.write("aa1", 3);
  info.range_.end_key_.write("ba1", 3);

  report_info.tablet_info_ = info;
  report_info.tablet_location_ = location;

  report_list.add_tablet(report_info);

  ObRootServer2ForTest root;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(root.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&root);
  wrapper.stop_thread();

  ASSERT_FALSE(root.has_been_called_);

  wrapper.get_log_worker().do_cs_migrate_done(info.range_, server, server2, true, 0);

  ASSERT_TRUE(root.has_been_called_);

  root.has_been_called_ = false;
}

TEST(ObRootServer2LogTest, do_report_tablets)
{
  ObServer server(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);
  int64_t ts = 12345;

  char buf1[10][30];
  char buf2[10][30];

  ObTabletReportInfoList report_list;
  ObTabletReportInfo report_info;
  ObTabletInfo info;

  ObTabletLocation location;
  location.tablet_version_ = 1;
  location.chunkserver_ = server;

  info.range_.table_id_ = 10001;
  info.range_.border_flag_.set_inclusive_end();
  info.range_.border_flag_.unset_inclusive_start();
  info.range_.start_key_.set_min_row();

  info.range_.start_key_.assign_buffer(buf1[0], 30);
  info.range_.end_key_.assign_buffer(buf2[0], 30);
  info.range_.start_key_.write("aa1", 3);
  info.range_.end_key_.write("ba1", 3);

  report_info.tablet_info_ = info;
  report_info.tablet_location_ = location;

  report_list.add_tablet(report_info);

  ObRootServer2ForTest root;
  ObRootWorkerForTest worker;
  ASSERT_TRUE(root.init("./root_server.conf", 100, &worker));
  ObRootServerTester wrapper(&root);
  wrapper.stop_thread();

  ASSERT_FALSE(root.has_been_called_);

  wrapper.get_log_worker().do_report_tablets(server, report_list, ts);

  ASSERT_TRUE(root.has_been_called_);

  root.has_been_called_ = false;
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
