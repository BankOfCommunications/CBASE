/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_new_balance_test.cpp
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
#include <gtest/gtest.h>
#include <cassert>
#include "../common/test_rowkey_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase;
static CharArena allocator_;

struct BalanceTestParams
{
  int32_t table_num_;
  int32_t cs_num_;
  int64_t disk_capacity_;
  int64_t sstable_size_;
  int32_t **sstables_dist;
  int32_t *sstables_per_table_; // 每个table的sstable个数
  BalanceTestParams(int32_t table_num, int32_t cs_num, int64_t disk_capacity, int64_t sstable_size);
  ~BalanceTestParams();
  static const int64_t DEFAULT_DISK_CAPACITY = 2LL*1024*1024*1024*1024; // 2TB
  static const int64_t DEFAULT_SSTABLE_SIZE = 256LL*1024*1024;          // 256MB
};

BalanceTestParams::BalanceTestParams(int32_t table_num, int32_t cs_num, int64_t disk_capacity, int64_t sstable_size)
{
  if (disk_capacity < 0)
  {
    disk_capacity = DEFAULT_DISK_CAPACITY;
  }
  if (sstable_size < 0)
  {
    sstable_size = DEFAULT_SSTABLE_SIZE;
  }
  assert(0 < table_num);
  assert(0 < cs_num);
  assert(0 < disk_capacity);
  assert(0 < sstable_size);
  table_num_ = table_num;
  sstable_size_ = sstable_size;
  cs_num_ = cs_num;
  disk_capacity_ = disk_capacity;
  sstables_per_table_ = new(std::nothrow) int32_t[table_num_];
  assert(NULL != sstables_per_table_);
  memset(sstables_per_table_, 0, sizeof(int32_t)*table_num_);
  sstables_dist = new(std::nothrow) int32_t*[table_num_];
  assert(NULL != sstables_dist);
  for (int i = 0; i < table_num_; ++i)
  {
    sstables_dist[i] = new(std::nothrow) int32_t[cs_num_];
    assert(NULL != sstables_dist[i]);
    memset(sstables_dist[i], 0, sizeof(int32_t)*cs_num_);
  }
}

BalanceTestParams::~BalanceTestParams()
{
  for (int i = 0; i < table_num_; ++i)
  {
    delete [] sstables_dist[i];
    sstables_dist[i] = NULL;
  }
  if (NULL != sstables_dist)
  {
    delete [] sstables_dist;
    sstables_dist = NULL;
  }
  if (NULL != sstables_per_table_)
  {
    delete [] sstables_per_table_;
    sstables_per_table_ = NULL;
  }
}
////////////////////////////////////////////////////////////////
struct MigrateMsg
{
  ObServer src_cs_;
  ObServer dest_cs_;
  ObNewRange range_;
  bool keep_src_;
  MigrateMsg* next_;
  MigrateMsg()
    :keep_src_(false), next_(NULL)
  {
  }
};

class BalanceTestRpc : public oceanbase::rootserver::ObRootRpcStub, public tbsys::CDefaultRunnable
{
  public:
    BalanceTestRpc():msg_head_(NULL), msg_count_(0), server_(NULL) {}
    virtual ~BalanceTestRpc() {}
    // synchronous rpc messages
    virtual int slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout)
    {
      UNUSED(master);
      UNUSED(slave_addr);
      UNUSED(fetch_param);
      UNUSED(timeout);
      return OB_SUCCESS;
    }
    virtual int set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us)
    {
      UNUSED(ups);
      UNUSED(role);
      UNUSED(timeout_us);
      return OB_SUCCESS;
    }
    virtual int switch_schema(const common::ObServer& ups, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us)
    {
      UNUSED(ups);
      UNUSED(schema_manager);
      UNUSED(timeout_us);
      return OB_SUCCESS;
    }
    virtual int migrate_tablet(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src, const int64_t timeout_us);
    virtual int create_tablet(const common::ObServer& cs, const common::ObNewRange& range, const int64_t mem_version, const int64_t timeout_us)
    {
      UNUSED(cs);
      UNUSED(range);
      UNUSED(mem_version);
      UNUSED(timeout_us);
      return OB_SUCCESS;
    }
    virtual int get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version)
    {
      UNUSED(ups);
      UNUSED(frozen_version);
      UNUSED(timeout_us);
      return OB_SUCCESS;
    }
    virtual int get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role)
    {
      UNUSED(master);
      UNUSED(obi_role);
      UNUSED(timeout_us);
      return OB_SUCCESS;
    }
    // asynchronous rpc messages
    virtual int heartbeat_to_cs(const common::ObServer& cs, const int64_t lease_time, const int64_t frozen_mem_version)
    {
      UNUSED(cs);
      UNUSED(lease_time);
      UNUSED(frozen_mem_version);
      return OB_SUCCESS;
    }
    virtual int heartbeat_to_ms(const common::ObServer& ms, const int64_t lease_time, const int64_t schema_version, const common::ObiRole &role)
    {
      UNUSED(ms);
      UNUSED(lease_time);
      UNUSED(schema_version);
      UNUSED(role);
      return OB_SUCCESS;
    }
    virtual void run(tbsys::CThread *thread, void *arg);
    void set_server(ObRootServer2 *server);
    int64_t get_migrate_msg_count() const {return msg_count_;}
    void set_blocking_cs(const ObServer &cs) { blocked_cs_ = cs;}
    void set_blocking_dest_cs(const ObServer &cs) { blocked_dest_cs_ = cs;}
  private:
    int push_migrate_msg(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src);
    int pop_migrate_msg(MigrateMsg* &msg);
    MigrateMsg* new_migrate_msg(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src);
    void delete_migrate_msg(MigrateMsg* msg);
  private:
    MigrateMsg* msg_head_;
    int64_t msg_count_;
    tbsys::CThreadMutex migrate_msg_mutex_;
    ObRootServer2 *server_;
    ObServer blocked_cs_;      // msg from this cs will be dropped
    ObServer blocked_dest_cs_; // msg migrate-to this cs will be dropped
};

void BalanceTestRpc::set_server(ObRootServer2 *server)
{
  server_ = server;
}

MigrateMsg* BalanceTestRpc::new_migrate_msg(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src)
{
  MigrateMsg *msg = new(std::nothrow) MigrateMsg;
  assert(msg);
  msg->next_ = NULL;
  msg->src_cs_ = src_cs;
  msg->dest_cs_ = dest_cs;
  msg->keep_src_ = keep_src;
  msg->range_ = range;
  deep_copy_range(allocator_, range, msg->range_);
  return msg;
}

void BalanceTestRpc::delete_migrate_msg(MigrateMsg* msg)
{
  assert(msg);
  //if (NULL != msg->range_.start_key_.ptr())
  //{
  //  delete [] msg->range_.start_key_.ptr();
  //}
  //if (NULL != msg->range_.end_key_.ptr())
  //{
  //  delete [] msg->range_.end_key_.ptr();
  //}
  delete msg;
}

int BalanceTestRpc::push_migrate_msg(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src)
{
  int ret = OB_SUCCESS;
  MigrateMsg* msg = new_migrate_msg(src_cs, dest_cs, range, keep_src);
  assert(msg);
  assert(msg->next_ == NULL);
  char addr_buf1[OB_IP_STR_BUFF];
  char addr_buf2[OB_IP_STR_BUFF];
  static char range_buf[OB_MAX_ROW_KEY_LENGTH * 2];
  src_cs.to_string(addr_buf1, OB_IP_STR_BUFF);
  dest_cs.to_string(addr_buf2, OB_IP_STR_BUFF);

  tbsys::CThreadGuard guard(&migrate_msg_mutex_);
  if (NULL == msg_head_)
  {
    msg_head_ = msg;
  }
  else
  {
    msg->next_ = msg_head_;
    msg_head_ = msg;
  }
  msg_count_++;
  msg->range_.to_string(range_buf, OB_MAX_ROW_KEY_LENGTH*2);
  TBSYS_LOG(DEBUG, "push migrate msg, src=%s dest=%s range=%s",
            addr_buf1, addr_buf2, range_buf);
  return ret;
}

int BalanceTestRpc::pop_migrate_msg(MigrateMsg *&msg)
{
  int ret = OB_ENTRY_NOT_EXIST;
  char addr_buf1[OB_IP_STR_BUFF];
  char addr_buf2[OB_IP_STR_BUFF];
  static char range_buf[OB_MAX_ROW_KEY_LENGTH * 2];

  tbsys::CThreadGuard guard(&migrate_msg_mutex_);
  if (NULL != msg_head_)
  {
    MigrateMsg* next = msg_head_->next_;
    msg = msg_head_;
    msg_head_ = next;

    msg->src_cs_.to_string(addr_buf1, OB_IP_STR_BUFF);
    msg->dest_cs_.to_string(addr_buf2, OB_IP_STR_BUFF);
    msg->range_.to_string(range_buf, OB_MAX_ROW_KEY_LENGTH*2);
    TBSYS_LOG(DEBUG, "pop migrate msg, src=%s dest=%s range=%s",
              addr_buf1, addr_buf2, range_buf);
    msg_count_--;
    ret = OB_SUCCESS;
  }
  else
  {
    TBSYS_LOG(DEBUG, "msg head is NULL, count=%ld", msg_count_);
  }
  return ret;
}

int BalanceTestRpc::migrate_tablet(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src, const int64_t timeout_us)
{
  UNUSED(timeout_us);
  return push_migrate_msg(src_cs, dest_cs, range, keep_src);
}

void BalanceTestRpc::run(tbsys::CThread *thread, void *arg)
{
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "balance test rpc thread begin");
  while(!_stop)
  {
    TBSYS_LOG(DEBUG, "balance test rpc thread running");
    MigrateMsg *msg = NULL;
    int ret = pop_migrate_msg(msg);
    if (OB_SUCCESS == ret)
    {
      assert(server_);
      if (blocked_cs_ == msg->src_cs_)
      {
        char addr_buf1[OB_IP_STR_BUFF];
        blocked_cs_.to_string(addr_buf1, OB_IP_STR_BUFF);
        TBSYS_LOG(DEBUG, "drop migrate msg, src_cs=%s", addr_buf1);
      }
      else if (blocked_dest_cs_ == msg->dest_cs_)
      {
        char addr_buf1[OB_IP_STR_BUFF];
        blocked_dest_cs_.to_string(addr_buf1, OB_IP_STR_BUFF);
        TBSYS_LOG(DEBUG, "drop migrate msg, dest_cs=%s", addr_buf1);
      }
      else
      {
        ObServer dest_server = msg->dest_cs_;
        ObDataSourceDesc desc1;
        desc1.type_ = ObDataSourceDesc::OCEANBASE_INTERNAL;
        desc1.range_ = msg->range_;
        desc1.src_server_ = msg->src_cs_;
        desc1.sstable_version_ = 2;
        desc1.tablet_version_ = 1;
        desc1.keep_source_ = true;
        const int64_t occupy_size = 1024;
        const uint64_t crc_sum = 12345678;
        const uint64_t row_checksum = 12345678;
        const int64_t row_count =23456789;
        const int32_t migrate_result = 0;

        ASSERT_EQ(0, server_->migrate_over(migrate_result, desc1, occupy_size, crc_sum, row_checksum, row_count));
      }
      delete_migrate_msg(msg);
    }
    else
    {
      usleep(500*1000);
    }
  }
  TBSYS_LOG(INFO, "balance test rpc thread finished");
}

class BalanceTestRootWorker: public ObRootWorkerForTest
{
  public:
    BalanceTestRootWorker() : ObRootWorkerForTest(config_mgr_, rs_config_), config_mgr_(rs_config_, reload_)
  {
  }
    virtual ObRootRpcStub& get_rpc_stub() { return rpc_;}
    BalanceTestRpc& get_test_stub() {return rpc_;}
  public:
    ObRootServerConfig rs_config_;
    ObReloadConfig reload_;
    common::ObConfigManager config_mgr_;
    BalanceTestRpc rpc_;
};

////////////////////////////////////////////////////////////////
class ObBalanceTest: public ::testing::Test
{
  public:
    virtual void SetUp();
    virtual void TearDown();
    void test_env(BalanceTestParams &params);
    void test_env2(BalanceTestParams &params);
    void heartbeat_cs(int32_t cs_num);
    ObServer &get_addr(int32_t idx);
    uint64_t get_table_id(int32_t idx);
  public:
    BalanceTestRootWorker worker_;
    ObRootServer2* server_;
  private:
    void register_cs(int32_t cs_num);
    void report_tablets(BalanceTestParams &params);
    void report_tablets_2replicas(BalanceTestParams &params);
    void get_report_tablet(ObTabletReportInfo &tablet, int sstables_count, int table_idx, int sstable_seq, int32_t cs_id, int64_t sstable_size);
    void reset_report_info_list(ObTabletReportInfoList &rlist);
    void conv_key(const int64_t in, int64_t &out);
  protected:
    ObServer addr_;
    ObServer balance_except_cs_;
};

void ObBalanceTest::SetUp()
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true, schema_manager->parse_from_file("ob_new_balance_test_schema.ini", c1));
  server_ = worker_.get_root_server();
  common::ObSystemConfig sys_config;
  sys_config.init();
  worker_.rs_config_.init(sys_config);
  worker_.rs_config_.port = 1110;
  worker_.rs_config_.io_thread_count = 2;
  worker_.rs_config_.read_queue_size = 2;
  worker_.create_eio();
  worker_.rs_config_.root_server_ip.set_value("10.232.35.40");
  worker_.rs_config_.schema_filename.set_value("ob_new_balance_test_schema.ini");
  ASSERT_EQ(OB_SUCCESS, worker_.initialize());
  worker_.rs_config_.balance_tolerance_count = 10;
  worker_.rs_config_.tablet_replicas_num = 2;
  worker_.rs_config_.tablet_migrate_disabling_period = 1000;

  server_->switch_schema_manager(*schema_manager);
  server_->start_threads();
  sleep(1);
  worker_.get_role_manager()->set_role(ObRoleMgr::STANDALONE); // for testing
  worker_.get_test_stub().set_server(server_);
  worker_.get_test_stub().start();
}

void ObBalanceTest::TearDown()
{
  sleep(1);
  server_->dump_root_table();
  worker_.get_test_stub().stop();
  worker_.get_test_stub().wait();
  server_->stop_threads();
  // test after balance thread stopped
  ASSERT_EQ(0, worker_.get_test_stub().get_migrate_msg_count());
  ASSERT_TRUE(server_->balancer_->nb_is_all_tables_balanced(balance_except_cs_));
}

ObServer &ObBalanceTest::get_addr(int32_t idx)
{
  char buff[128];
  snprintf(buff, 128, "10.10.10.%d", idx);
  int port = 26666;
  addr_.set_ipv4_addr(buff, port);
  return addr_;
}

void ObBalanceTest::register_cs(int32_t cs_num)
{
  for (int i = 0; i < cs_num; ++i)
  {
    int32_t status = 0;
    ASSERT_EQ(OB_SUCCESS, server_->regist_chunk_server(get_addr(i), "0.4.1.2", status));
    TBSYS_LOG(INFO, "register cs, id=%d status=%d", i, status);
  }
  TBSYS_LOG(INFO, "register %d cs end.", cs_num);
}

void ObBalanceTest::heartbeat_cs(int32_t cs_num)
{
  for (int i = 0; i < cs_num; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, server_->receive_hb(get_addr(i), get_addr(i).get_port(), false, OB_CHUNKSERVER));
  }
}

uint64_t ObBalanceTest::get_table_id(int32_t idx)
{
  return 1000+idx;
}

void ObBalanceTest::conv_key(const int64_t in, int64_t &out)
{
  char* b1 = (char*)&in;
  char* b2 = (char*)&out;
  for (int i = 0; i < 8; ++i)
  {
    b2[i] = b1[7-i];
  }
}

void ObBalanceTest::get_report_tablet(ObTabletReportInfo &tablet, int sstables_count, int table_idx, int sstable_seq, int32_t cs_id, int64_t sstable_size)
{
  tablet.tablet_info_.range_.table_id_ = get_table_id(table_idx);
  tablet.tablet_info_.range_.border_flag_.set_data(0);
  tablet.tablet_info_.range_.border_flag_.set_inclusive_end();
  if (0 == sstable_seq)
  {
    tablet.tablet_info_.range_.start_key_.set_min_row();
  }
  if (sstable_seq == sstables_count - 1)
  {
    tablet.tablet_info_.range_.end_key_.set_max_row();
  }
  int64_t *start_key = new(std::nothrow)int64_t;
  ASSERT_TRUE(NULL != start_key);
  int64_t key = sstable_seq;
  conv_key(key, *start_key);
  int64_t *end_key = new(std::nothrow)int64_t;
  ASSERT_TRUE(NULL != end_key);
  key = sstable_seq+1;
  conv_key(key, *end_key);
  tablet.tablet_info_.range_.start_key_ = make_rowkey((const char*)start_key, sizeof(int64_t), &allocator_);
  tablet.tablet_info_.range_.end_key_ = make_rowkey((const char*)end_key, sizeof(int64_t), &allocator_);
  tablet.tablet_info_.row_count_ = 1024;
  tablet.tablet_info_.occupy_size_ = sstable_size;
  tablet.tablet_info_.crc_sum_ = 1;
  tablet.tablet_location_.chunkserver_ = get_addr(cs_id);
  tablet.tablet_location_.tablet_version_ = 2;
}

void ObBalanceTest::reset_report_info_list(ObTabletReportInfoList &rlist)
{
  const ObTabletReportInfo* report_info = rlist.get_tablet();
  TBSYS_LOG(INFO, "report info addr = %p, size=%ld", report_info, rlist.get_tablet_size());
  //// free keys
  //for (int k = 0; k < rlist.get_tablet_size(); ++k)
  //{
  //  if (NULL != report_info[k].tablet_info_.range_.start_key_.ptr())
  //  {
  //    TBSYS_LOG(INFO, "report_info[%d].tablet_info_.range_.start_key_.ptr()=%p", k, report_info[k].tablet_info_.range_.start_key_.ptr());
  //    allocator_.report_info[k].tablet_info_.range_.start_key_.ptr();
  //    const_cast<ObRowkey*>(&(report_info[k].tablet_info_.range_.start_key_))->assign(NULL, 0);
  //  }
  //  if (NULL != report_info[k].tablet_info_.range_.end_key_.ptr())
  //  {
  //    TBSYS_LOG(INFO, "report_info[%d].tablet_info_.range_.start_key_.ptr()=%p", k, report_info[k].tablet_info_.range_.start_key_.ptr());
  //    allocator_.report_info[k].tablet_info_.range_.end_key_.ptr();
  //    const_cast<ObRowkey*>(&(report_info[k].tablet_info_.range_.end_key_))->assign(NULL, 0);
  //  }
  //}
  rlist.reset();
}

void ObBalanceTest::report_tablets(BalanceTestParams &params)
{
  TBSYS_LOG(INFO, "start to report tablet.");
  ObTabletReportInfoList* report_lists = new (std::nothrow) ObTabletReportInfoList[params.cs_num_];
  ASSERT_TRUE(NULL != report_lists);
  int32_t *posts = new(std::nothrow) int32_t[params.cs_num_];
  ASSERT_TRUE(NULL != posts);
  int64_t *tablets_per_cs = new(std::nothrow) int64_t[params.cs_num_];
  ASSERT_TRUE(NULL != tablets_per_cs);
  ObTabletReportInfo tablet;
  for (int i = 0; i < params.table_num_; ++i)
  {
    //int32_t tablets_count = params.sstables_per_table_[i]/3;
    int32_t sstables_count = params.sstables_per_table_[i];
    int32_t total_dist = 0;
    for (int j = 0; j < params.cs_num_; ++j)
    {
      total_dist += params.sstables_dist[i][j];
      reset_report_info_list(report_lists[j]);
      posts[j] = 0;
      tablets_per_cs[j] = 0;
    }
    ASSERT_TRUE(0 < total_dist);
    TBSYS_LOG(INFO, "table_id=%lu sstable_count=%d total_dist=%d\n", get_table_id(i), sstables_count, total_dist);
    for (int k = 0; k < params.cs_num_; ++k)
    {
      TBSYS_LOG(INFO, "sstables dist per cs, cs_index=%d, table_index=%d, sstable_count=%d",
          k, i, params.sstables_dist[i][k]);
    }

    for (int j = 0; j < sstables_count;)
    {
      for (int k = 0; k < params.cs_num_ && j < sstables_count; ++k)
      {
        posts[k] += params.sstables_dist[i][k];
        if (posts[k] >= total_dist)
        {
          get_report_tablet(tablet, sstables_count, i, j, k, params.sstable_size_);
          int ret2 = report_lists[k].add_tablet(tablet);
          if (OB_ARRAY_OUT_OF_RANGE == ret2)
          {
            // report
            int ret = server_->report_tablets(get_addr(k), report_lists[k], 1);
            ASSERT_EQ(OB_SUCCESS, ret);
            TBSYS_LOG(INFO, "report tablets, cs=%d count=%ld", k, report_lists[k].get_tablet_size());
            reset_report_info_list(report_lists[k]);
            ret2 = report_lists[k].add_tablet(tablet);
          }
          ASSERT_EQ(OB_SUCCESS, ret2);
          tablets_per_cs[k]++;
          posts[k] %= total_dist;
          j++;
        }
      }
    }
    for (int k = 0; k < params.cs_num_; ++k)
    {
      if (0 < report_lists[k].get_tablet_size())
      {
        int ret = server_->report_tablets(get_addr(k), report_lists[k], 1);
        ASSERT_EQ(OB_SUCCESS, ret);
        TBSYS_LOG(INFO, "report tablets, cs=%d count=%ld", k, report_lists[k].get_tablet_size());
      }
    }
    for (int k = 0; k < params.cs_num_; ++k)
    {
      TBSYS_LOG(INFO, "reported tablets, table=%lu cs_idx=%d tablets=%ld expected=%d",
                get_table_id(i), k, tablets_per_cs[k], sstables_count*params.sstables_dist[i][k]/total_dist);
    }
  } // end for each table

  if (NULL != tablets_per_cs)
  {
    delete [] tablets_per_cs;
    tablets_per_cs = NULL;
  }
  if (NULL != posts)
  {
    delete [] posts;
    posts = NULL;
  }
  if (NULL != report_lists)
  {
    delete [] report_lists;
  }
}

void ObBalanceTest::test_env(BalanceTestParams &params)
{
  register_cs(params.cs_num_);
  report_tablets(params);
  server_->dump_root_table();
  for (int i = 0; i < params.cs_num_; i++)
  {
    server_->waiting_job_done(get_addr(i), 2);
  }
}

// 每个tablet两个复本，每个CS上均匀分布
void ObBalanceTest::report_tablets_2replicas(BalanceTestParams &params)
{
  ObTabletReportInfoList* report_lists = new (std::nothrow) ObTabletReportInfoList[params.cs_num_];
  ASSERT_TRUE(NULL != report_lists);
  int64_t *tablets_per_cs = new(std::nothrow) int64_t[params.cs_num_];
  ASSERT_TRUE(NULL != tablets_per_cs);
  ObTabletReportInfo tablet;
  for (int i = 0; i < params.table_num_; ++i)
  {
    int32_t sstables_count = params.sstables_per_table_[i];
    TBSYS_LOG(INFO, "table_id=%lu sstables=%d\n", get_table_id(i), sstables_count);
    for (int j = 0; j < params.cs_num_; ++j)
    {
      TBSYS_LOG(INFO, "sstables dist per cs, %d", params.sstables_dist[i][j]);
      reset_report_info_list(report_lists[j]);
      tablets_per_cs[j] = 0;
    }
    int k = 0;
    for (int j = 0; j < sstables_count; ++j)
    {
      int replicas = 0;
      while (replicas < 2)
      {
        if (0 < params.sstables_dist[i][k])
        {
          get_report_tablet(tablet, sstables_count, i, j, k, params.sstable_size_);
          int ret2 = report_lists[k].add_tablet(tablet);
          if (OB_ARRAY_OUT_OF_RANGE == ret2)
          {
            // report
            int ret = server_->report_tablets(get_addr(k), report_lists[k], 1);
            ASSERT_EQ(OB_SUCCESS, ret);
            TBSYS_LOG(INFO, "report tablets, cs=%d count=%ld", k, report_lists[k].get_tablet_size());
            reset_report_info_list(report_lists[k]);
            ret2 = report_lists[k].add_tablet(tablet);
          }
          ASSERT_EQ(OB_SUCCESS, ret2);
          tablets_per_cs[k]++;
          ++replicas;
        }
        k++;
        if (k >= params.cs_num_)
        {
          k = 0;
        }
      }
    }
    for (int k = 0; k < params.cs_num_; ++k)
    {
      if (0 < report_lists[k].get_tablet_size())
      {
        int ret = server_->report_tablets(get_addr(k), report_lists[k], 1);
        ASSERT_EQ(OB_SUCCESS, ret);
        TBSYS_LOG(INFO, "report tablets, cs=%d count=%ld", k, report_lists[k].get_tablet_size());
      }
    }
    for (int k = 0; k < params.cs_num_; ++k)
    {
      TBSYS_LOG(INFO, "reported tablets, table=%lu cs_idx=%d tablets=%ld",
                get_table_id(i), k, tablets_per_cs[k]);
    }
  } // end for each table

  if (NULL != tablets_per_cs)
  {
    delete [] tablets_per_cs;
    tablets_per_cs = NULL;
  }
  if (NULL != report_lists)
  {
    delete [] report_lists;
  }
}

void ObBalanceTest::test_env2(BalanceTestParams &params)
{
  register_cs(params.cs_num_);
  report_tablets_2replicas(params);
  for (int i = 0; i < params.cs_num_; i++)
  {
    server_->waiting_job_done(get_addr(i), 2);
  }
  server_->dump_root_table();
}

TEST_F(ObBalanceTest, test_half_to_half)
{
  server_->get_boot()->set_boot_ok();
  // 2 tables, 10 cs
  BalanceTestParams params(2, 10, -1, -1);
  // distribution for table 0
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[0][i] = 10*(i+1);
  }
  // distribution for table 1
  int32_t prev = 0;
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[1][i] = prev + 20;
    prev = params.sstables_dist[1][i];
  }
  params.sstables_per_table_[0] = 200;
  params.sstables_per_table_[1] = 100;
  this->test_env(params);
  for (int i = 0; i < 60; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
}

TEST_F(ObBalanceTest, test_n_to_1)
{
  server_->get_boot()->set_boot_ok();
  // 2 tables, 11 cs
  BalanceTestParams params(2, 11, -1, -1);
  // distribution for table 0
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[0][i] = 10;
  }
  params.sstables_dist[0][10] = 0;
  // distribution for table 1
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[1][i] = 10;
  }
  params.sstables_dist[0][10] = 0;

  params.sstables_per_table_[0] = 800;
  params.sstables_per_table_[1] = 700;
  this->test_env(params);
  for (int i = 0; i < 100; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
}

TEST_F(ObBalanceTest, test_n_to_2)
{
  server_->get_boot()->set_boot_ok();
  // 2 tables, 11 cs
  BalanceTestParams params(2, 12, -1, -1);
  // distribution for table 0
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[0][i] = 10;
  }
  params.sstables_dist[0][10] = 0;
  params.sstables_dist[0][11] = 0;
  // distribution for table 1
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[1][i] = 10;
  }
  params.sstables_dist[0][10] = 0;
  params.sstables_dist[0][11] = 0;

  params.sstables_per_table_[0] = 800;
  params.sstables_per_table_[1] = 600;
  this->test_env(params);
  for (int i = 0; i < 80; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
  TearDown();
  int64_t avg_size = 0;
  int64_t avg_count = 0;
  int32_t cs_num = 0;
  int64_t out_per_cs = 0;
  int32_t shutdown_num = 0;
  for (int i = 0; i < 2; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, server_->balancer_->nb_calculate_sstable_count(get_table_id(i), avg_size, avg_count,
                                                                         cs_num, out_per_cs, shutdown_num));
    ASSERT_EQ(12, cs_num);
    ObServerStatus* cs10 = server_->server_manager_.find_by_ip(get_addr(10));
    ObServerStatus* cs11 = server_->server_manager_.find_by_ip(get_addr(11));
    ASSERT_TRUE(NULL != cs10);
    ASSERT_TRUE(NULL != cs11);
    ASSERT_LT(cs10->balance_info_.table_sstable_count_ - cs11->balance_info_.table_sstable_count_, 10);
    ASSERT_LT(cs11->balance_info_.table_sstable_count_ - cs10->balance_info_.table_sstable_count_, 10);
  }
}

TEST_F(ObBalanceTest, test_faulty_src_cs)
{
  server_->get_boot()->set_boot_ok();
  // 2 tables, 10 cs
  BalanceTestParams params(2, 10, -1, -1);
  // distribution for table 0
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[0][i] = 10*(i+1);
  }
  // distribution for table 1
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[1][i] = 10; // table 1 is balanced
  }
  params.sstables_per_table_[0] = 500;
  params.sstables_per_table_[1] = 300;
  // block cs 9
  worker_.get_test_stub().set_blocking_cs(get_addr(9));

  this->test_env(params);
  for (int i = 0; i < 500; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
  // test balaced except cs 9
  balance_except_cs_ = get_addr(9);
}

TEST_F(ObBalanceTest, test_rereplication)
{
  server_->get_boot()->set_boot_ok();
  // 2 tables, 10 cs
  BalanceTestParams params(2, 10, -1, -1);
  // distribution for table 0
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[0][i] = 10*(i+1);
  }
  // distribution for table 1
  int32_t prev = 0;
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[1][i] = prev + 20;
    prev = params.sstables_dist[1][i];
  }
  params.sstables_per_table_[0] = 400;
  params.sstables_per_table_[1] = 400;
  this->test_env(params);

  for (int i = 0; i < 10; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
  //server_->config_.flag_enable_rereplication_.set(1);
  //server_->config_.flag_tablet_replicas_num_.set(2);
  for (int i = 0; i < 160; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
  ASSERT_TRUE(server_->balancer_->nb_is_all_tablets_replicated(2));
}

#if 0
TEST_F(ObBalanceTest, test_n_to_2_with_faulty_dest_cs)
{
  // 2 tables, 5 cs
  BalanceTestParams params(2, 5, -1, -1);
  // distribution for table 0
  for (int i = 0; i < 3; ++i)
  {
    params.sstables_dist[0][i] = 10;
  }
  params.sstables_dist[0][3] = 0;
  params.sstables_dist[0][4] = 0;
  // distribution for table 1
  for (int i = 0; i < 3; ++i)
  {
    params.sstables_dist[1][i] = 10;
  }
  params.sstables_dist[0][3] = 0;
  params.sstables_dist[0][4] = 0;

  params.sstables_per_table_[0] = 900;
  params.sstables_per_table_[1] = 900;
  // block cs 4
  worker_.get_test_stub().set_blocking_dest_cs(get_addr(4));

  this->test_env2(params);
  for (int i = 0; i < 160; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
  // test balaced except cs 4
  balance_except_cs_ = get_addr(4);
}
#endif

TEST_F(ObBalanceTest, test_shutdown_servers)
{
  server_->get_boot()->set_boot_ok();
  // 2 tables, 10 cs
  BalanceTestParams params(2, 10, -1, -1);
  // distribution for table 0
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[0][i] = 10;
  }
  // distribution for table 1
  for (int i = 0; i < 10; ++i)
  {
    params.sstables_dist[1][i] = 10;
  }
  params.sstables_per_table_[0] = 200;
  params.sstables_per_table_[1] = 200;
  this->test_env(params);
  for (int i = 0; i < 10; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }

  ObArray<ObServer> shutdown_servers;
  ASSERT_EQ(OB_SUCCESS, shutdown_servers.push_back(get_addr(9)));
  ASSERT_EQ(OB_SUCCESS, shutdown_servers.push_back(get_addr(0)));
  ASSERT_EQ(OB_SUCCESS, server_->shutdown_cs(shutdown_servers, SHUTDOWN));

  for (int i = 0; i < 60; ++i)
  {
    heartbeat_cs(params.cs_num_);
    sleep(1);
  }
  ASSERT_TRUE(server_->balancer_->nb_did_cs_have_no_tablets(get_addr(0)));
  ASSERT_TRUE(server_->balancer_->nb_did_cs_have_no_tablets(get_addr(9)));
}

TEST_F(ObBalanceTest, checkpoint)
{
  server_->get_boot()->set_boot_ok();
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  //TBSYS_LOGGER.setFileName("a.log");
  //TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
