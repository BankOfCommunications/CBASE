#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_config_manager.h"
#include "rootserver/ob_root_server_config.h"
#include "root_server_tester.h"
#include "ob_chunk_server_manager.h"
#include "ob_ups_manager.h"
#include "ob_root_monitor_table.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

static const int MAX_CS_COUNT = 44;
static const int MAX_MS_COUNT = 35;
static const int MAX_UPS_COUNT = 5;

class VirtualMonitorTable:public ObRootMonitorTable
{
  public:
    VirtualMonitorTable(const ObServer & rs, const ObChunkServerManager & cs_mgr,
        const ObUpsManager & ups_mgr):ObRootMonitorTable(rs, cs_mgr, ups_mgr)
    {
    }
    void check_sort()
    {
      // check sorted
      ObRootMonitorTable::ServerVector servers;
      int64_t count = 0;
      init_all_servers(servers, count);
      EXPECT_TRUE(servers.size() == count);
      // random shuffle
      for (int64_t times = 0; times < 20; ++times)
      {
        std::random_shuffle(servers.begin(), servers.end());
        sort_all_servers(servers);
        EXPECT_TRUE(servers.size() == count);
        for (int32_t i = 1; i < count; ++i)
        {
          EXPECT_TRUE(servers[i - 1] < servers[i]);
          EXPECT_FALSE(servers[i] < servers[i]);
        }
      }
    }
    int find_first_tablet(const ObRowkey & rowkey)
    {
      ObRootMonitorTable::ServerVector servers;
      int64_t count = 0;
      init_all_servers(servers, count);
      EXPECT_TRUE(servers.size() == count);
      sort_all_servers(servers);
      return ObRootMonitorTable::find_first_tablet(rowkey, servers);
    }
    void check_result(const int32_t pos, ObScanner & result)
    {
      ObRootMonitorTable::ServerVector servers;
      int64_t count = 0;
      init_all_servers(servers, count);
      EXPECT_TRUE(servers.size() == count);
      sort_all_servers(servers);
      ObScanner::RowIterator it;
      ObCellInfo* ci = NULL;
      int64_t cell_num = 0;
      int ret = OB_SUCCESS;
      int64_t row_count = 0;
      ObClusterServer server;
      ObCellInfo* last_ci = NULL;
      for (it = result.row_begin();it != result.row_end(); ++it)
      {
        ret = it.get_row(&ci, &cell_num);
        EXPECT_TRUE(ret == OB_SUCCESS);
        TBSYS_LOG(INFO, "row:%ld, table:%.*s, rowkey:%s", row_count,
            ci[0].table_name_.length(), ci[0].table_name_.ptr(), to_cstring(ci[0].row_key_));
        ++row_count;
        if (last_ci != NULL)
        {
          // EXPECT_TRUE(last_ci[0].row_key_ < ci[0].row_key_);
        }
        if ((row_count == 1) && (pos == 0))
        {
          EXPECT_TRUE(ci[0].row_key_.is_min_row());
        }
        else if (row_count == 2)
        {
          if (ci[0].row_key_.is_max_row())
          {
            // the last server
          }
          else
          {
            ret = convert_rowkey_2_server(ci[0].row_key_, server);
            EXPECT_TRUE(ret == OB_SUCCESS);
            EXPECT_TRUE(server.role == servers[pos].role);
            if (server.role == servers[pos + 1].role)
            {
              EXPECT_TRUE(server.addr == servers[pos].addr);
            }
          }
        }
        last_ci = ci;
      }
    }
};

class TestMonitorTable: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};


static const int CS_PORT = 2600;
static const int UPS_PORT = 2700;
static const int MS_PORT = 2800;

TEST_F(TestMonitorTable, sort)
{
  int ret = OB_SUCCESS;
  char host[64] = "";
  ObServer rs;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  rs.set_ipv4_addr("127.0.0.1", 2500);
  // register cs \ ms
  ObChunkServerManager cs_mgr;
  // one cs per machine
  ObServer cs;
  for (int64_t i = 0; i < MAX_CS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    cs.set_ipv4_addr(host, CS_PORT);
    ret = cs_mgr.receive_hb(cs, timestamp, false, true);
  }
  // one ms per machine
  ObServer ms;
  for (int64_t i = 0; i < MAX_MS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    ms.set_ipv4_addr(host, MS_PORT);
    ret = cs_mgr.receive_hb(ms, timestamp, true, true);
  }
  // register ups
  ObRootRpcStub rpc;
  ObServer ups;
  ObiRole role;
  int64_t factor = 10 * 1000 * 1000;
  int64_t version = 0;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);
  ObUpsManager ups_mgr(rpc, &root_worker, factor, factor, factor, factor, role, version, version);
  for (int64_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    ups.set_ipv4_addr(host, UPS_PORT);
    ret = ups_mgr.register_ups(ups, UPS_PORT + 1, 1, 0, "0.4.1.2");
    EXPECT_TRUE(ret == OB_SUCCESS);
    ret = ups_mgr.renew_lease(ups, UPS_STAT_SYNC, role);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  VirtualMonitorTable mtable(rs, cs_mgr, ups_mgr);
  mtable.print_info();
  mtable.print_info(true);
  mtable.check_sort();
}

TEST_F(TestMonitorTable, no_server)
{
  int ret = OB_SUCCESS;
  char host[64] = "";
  ObServer rs;
  rs.set_ipv4_addr("127.0.0.1", 2500);
  ObServer cs;
  ObServer ms;
  ObChunkServerManager cs_mgr;
  ObRootRpcStub rpc;
  ObServer ups;
  ObiRole role;
  int64_t version = 0;
  int64_t factor = 10 * 1000 * 1000;
  ObRowkey rowkey;

  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);

  ObUpsManager ups_mgr(rpc, &root_worker, factor, factor, factor, factor, role, version, version);
  VirtualMonitorTable mtable(rs, cs_mgr, ups_mgr);
  mtable.print_info();
  mtable.print_info(true);
  mtable.check_sort();
  ObObj objs[4];
  rowkey.assign(objs, 4);
  objs[0].set_min_value();
  ObString addr;
  // must the first root server
  ObString server_role;
  server_role = ObString::make_string(print_role(OB_ROOTSERVER));
  objs[0].set_varchar(server_role);
  sprintf(host, "126.0.0.1");
  addr.assign(host, (int32_t)strlen(host));
  objs[1].set_varchar(addr);
  objs[2].set_min_value();
  ObScanner result;
  // one server
  // less than the server not find return the first pos
  int pos = mtable.find_first_tablet(rowkey);
  EXPECT_TRUE(0 == pos);
  ret = mtable.get(rowkey, result);
  EXPECT_TRUE(ret == OB_SUCCESS);
  mtable.check_result(pos, result);
  ret = mtable.get(rowkey, result);
  // gt than the server not find return the first pos
  server_role = ObString::make_string(print_role(OB_UPDATESERVER));
  objs[0].set_varchar(server_role);
  pos = mtable.find_first_tablet(rowkey);
  EXPECT_TRUE(1 == pos);
  ret = mtable.get(rowkey, result);
  EXPECT_TRUE(ret == OB_SUCCESS);
  mtable.check_result(pos, result);
  mtable.check_sort();
}

TEST_F(TestMonitorTable, get_server)
{
  int ret = OB_SUCCESS;
  char host[64] = "";
  ObServer rs;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  // register rs
  rs.set_ipv4_addr("127.0.0.1", 2500);
  // register cs \ ms
  ObChunkServerManager cs_mgr;
  // one cs per machine
  ObServer cs;
  for (int64_t i = 0; i < MAX_CS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    cs.set_ipv4_addr(host, CS_PORT);
    ret = cs_mgr.receive_hb(cs, timestamp, false, true);
  }
  // one ms per machine
  ObServer ms;
  for (int64_t i = 0; i < MAX_MS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    ms.set_ipv4_addr(host, MS_PORT);
    ret = cs_mgr.receive_hb(ms, timestamp, true, true);
  }
  // register ups
  ObRootRpcStub rpc;
  ObServer ups;
  ObiRole role;
  int64_t factor = 10 * 1000 * 1000;
  int64_t version = 0;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);

  ObUpsManager ups_mgr(rpc, &root_worker, factor, factor, factor, factor, role, version, version);
  for (int64_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    ups.set_ipv4_addr(host, UPS_PORT);
    ret = ups_mgr.register_ups(ups, UPS_PORT + 1, 1, 0, "0.4.1.2");
    EXPECT_TRUE(ret == OB_SUCCESS);
    ret = ups_mgr.renew_lease(ups, UPS_STAT_SYNC, role);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  VirtualMonitorTable mtable(rs, cs_mgr, ups_mgr);

  // no server find
  ObRowkey rowkey;
  ObObj objs[4];
  rowkey.assign(objs, 4);
  objs[0].set_min_value();
  ObString addr;
  int pos = mtable.find_first_tablet(rowkey);
  EXPECT_TRUE(pos == 0);
  ObString server_role;
  server_role = ObString::make_string(print_role(OB_CHUNKSERVER));
  for (int64_t i = 0; i < 100; ++i)
  {
    sprintf(host, "126.255.255.%ld", i);
    objs[0].set_varchar(server_role);
    objs[1].set_varchar(addr);
    objs[2].set_min_value();
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == 0);
  }

  // must the first chunk server
  objs[0].set_varchar(server_role);
  sprintf(host, "127.0.0.0");
  addr.assign(host, (int32_t)strlen(host));
  objs[1].set_varchar(addr);
  pos = mtable.find_first_tablet(rowkey);
  EXPECT_TRUE(pos == 0);
  // find the chunkserver
  server_role = ObString::make_string(print_role(OB_CHUNKSERVER));
  for (int64_t i = 0; i < MAX_CS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == i);
  }
  for (int64_t i = MAX_CS_COUNT; i < 2 * MAX_CS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == MAX_CS_COUNT);
  }
  // find the mergeserver
  server_role = ObString::make_string(print_role(OB_MERGESERVER));
  for (int64_t i = 0; i < MAX_MS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == MAX_CS_COUNT + i);
  }
  for (int64_t i = MAX_MS_COUNT; i < 2 * MAX_MS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == MAX_CS_COUNT + MAX_MS_COUNT);
  }
  // find the rootserver
  server_role = ObString::make_string(print_role(OB_ROOTSERVER));
  for (int64_t i = 0; i < 10; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    if (i <= 1)
    {
      EXPECT_TRUE(pos == MAX_CS_COUNT + MAX_MS_COUNT);
    }
    else
    {
      EXPECT_TRUE(pos == 1 + MAX_CS_COUNT + MAX_MS_COUNT);
    }
  }
  // find the updateserver
  server_role = ObString::make_string(print_role(OB_UPDATESERVER));
  for (int64_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == 1 + MAX_CS_COUNT + MAX_MS_COUNT + i);
  }
  // not find
  for (int64_t i = MAX_UPS_COUNT; i < 3 * MAX_UPS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == 1 + MAX_CS_COUNT + MAX_MS_COUNT + MAX_UPS_COUNT);
  }
}

TEST_F(TestMonitorTable, fill_scanner)
{
  int ret = OB_SUCCESS;
  char host[64] = "";
  ObServer rs;
  ObScanner result;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  // register rs
  rs.set_ipv4_addr("127.0.0.1", 2500);
  // register cs \ ms
  ObChunkServerManager cs_mgr;
  // one cs per machine
  ObServer cs;
  for (int64_t i = 0; i < MAX_CS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    cs.set_ipv4_addr(host, CS_PORT);
    ret = cs_mgr.receive_hb(cs, timestamp, false, true);
  }
  // one ms per machine
  ObServer ms;
  for (int64_t i = 0; i < MAX_MS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    ms.set_ipv4_addr(host, MS_PORT);
    ret = cs_mgr.receive_hb(ms, timestamp, true, true);
  }
  // register ups
  ObRootRpcStub rpc;
  ObServer ups;
  ObiRole role;
  int64_t factor = 10 * 1000 * 1000;
  int64_t version = 0;
  ObRootServerConfig rs_config;
  ObReloadConfig reload;
  ObConfigManager config_mgr(rs_config, reload);
  ObRootWorkerForTest root_worker(config_mgr, rs_config);

  ObUpsManager ups_mgr(rpc, &root_worker, factor, factor, factor, factor, role, version, version);
  for (int64_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    ups.set_ipv4_addr(host, UPS_PORT);
    ret = ups_mgr.register_ups(ups, UPS_PORT + 1, 1, 0, "0.4.1.2");
    EXPECT_TRUE(ret == OB_SUCCESS);
    ret = ups_mgr.renew_lease(ups, UPS_STAT_SYNC, role);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  VirtualMonitorTable mtable(rs, cs_mgr, ups_mgr);

  // no server find
  ObRowkey rowkey;
  ObObj objs[4];
  rowkey.assign(objs, 4);
  objs[0].set_min_value();
  ObString addr;
  int pos = mtable.find_first_tablet(rowkey);
  EXPECT_TRUE(pos == 0);
  ObString server_role;
  server_role = ObString::make_string(print_role(OB_CHUNKSERVER));
  for (int64_t i = 0; i < 100; ++i)
  {
    sprintf(host, "126.255.255.%ld", i);
    objs[0].set_varchar(server_role);
    objs[1].set_varchar(addr);
    objs[2].set_min_value();
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == 0);
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }

  // must the first chunk server
  objs[0].set_varchar(server_role);
  sprintf(host, "127.0.0.0");
  addr.assign(host, (int32_t)strlen(host));
  objs[1].set_varchar(addr);
  pos = mtable.find_first_tablet(rowkey);
  EXPECT_TRUE(pos == 0);
  // find the chunkserver
  server_role = ObString::make_string(print_role(OB_CHUNKSERVER));
  for (int64_t i = 0; i < MAX_CS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == i);
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }
  for (int64_t i = MAX_CS_COUNT; i < 2 * MAX_CS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == MAX_CS_COUNT);
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }
  // find the mergeserver
  server_role = ObString::make_string(print_role(OB_MERGESERVER));
  for (int64_t i = 0; i < MAX_MS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == MAX_CS_COUNT + i);
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }
  for (int64_t i = MAX_MS_COUNT; i < 2 * MAX_MS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == MAX_CS_COUNT + MAX_MS_COUNT);
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }
  // find the rootserver
  server_role = ObString::make_string(print_role(OB_ROOTSERVER));
  for (int64_t i = 0; i < 10; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    if (i <= 1)
    {
      EXPECT_TRUE(pos == MAX_CS_COUNT + MAX_MS_COUNT);
    }
    else
    {
      EXPECT_TRUE(pos == 1 + MAX_CS_COUNT + MAX_MS_COUNT);
    }
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }
  // find the updateserver
  server_role = ObString::make_string(print_role(OB_UPDATESERVER));
  for (int64_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    objs[0].set_varchar(server_role);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == 1 + MAX_CS_COUNT + MAX_MS_COUNT + i);
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }
  // not find
  for (int64_t i = MAX_UPS_COUNT; i < 3 * MAX_UPS_COUNT; ++i)
  {
    sprintf(host, "127.0.0.%ld", i);
    addr.assign(host, (int32_t)strlen(host));
    objs[1].set_varchar(addr);
    int pos = mtable.find_first_tablet(rowkey);
    EXPECT_TRUE(pos == 1 + MAX_CS_COUNT + MAX_MS_COUNT + MAX_UPS_COUNT);
    ret = mtable.get(rowkey, result);
    EXPECT_TRUE(ret == 0);
    mtable.check_result(pos, result);
  }
}
