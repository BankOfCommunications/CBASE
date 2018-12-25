#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include "test_mock_root_server.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/thread_buffer.h"
#include "task_info.h"
#include "task_factory.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::tools;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestTaskFactory: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};


TEST_F(TestTaskFactory, test_add_table)
{
  TaskFactory factory;
  EXPECT_TRUE(factory.add_table("table1") == OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table1") != OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table2") == OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table2") != OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table3") == OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table3") != OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table4") == OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table4") != OB_SUCCESS);
  EXPECT_TRUE(factory.add_table("table1") != OB_SUCCESS);
}


TEST_F(TestTaskFactory, test_is_max_key)
{
  const int64_t MAX_LEN = 10;
  char temp[MAX_LEN] = "";
  
  // length is max
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN - 1, temp, MAX_LEN) == false);
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN - 1, temp, MAX_LEN - 1) == false);
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN, temp, MAX_LEN) == false);
  
  for (int64_t i = 0; i < MAX_LEN; ++i)
  {
    temp[i] = 0xFF;
  }
  
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN, temp, MAX_LEN) == true);
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN - 1, temp, MAX_LEN - 1) == true);
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN - 1, temp, MAX_LEN) == false);
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN - 1, temp, MAX_LEN + 1) == false);
  EXPECT_TRUE(TaskFactory::is_max_rowkey(MAX_LEN + 1, temp, MAX_LEN) == false);
}


TEST_F(TestTaskFactory, test_get_tablets)
{
  ObClientManager client_manager;
  tbnet::Transport transport;
  tbnet::DefaultPacketStreamer streamer; 
  ObPacketFactory packet_factory;
  streamer.setPacketFactory(&packet_factory);
  transport.start();
  EXPECT_TRUE(OB_SUCCESS == client_manager.initialize(&transport, &streamer));
  ThreadSpecificBuffer buffer;
  RpcStub rpc(&client_manager, &buffer);
  ObSchemaManagerV2 schema;
  tbsys::CConfig conf;
  EXPECT_TRUE(true == schema.parse_from_file("schema.ini", conf));

  uint64_t count = 0;
  TaskFactory factory;
  EXPECT_TRUE(OB_SUCCESS != factory.get_all_tablets(count));
  ObServer root_server;
  const char * addr = "localhost";
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  TaskManager task_manager;
  EXPECT_TRUE(OB_SUCCESS == factory.init(1, 1000 * 1000, root_server,
      &schema, &rpc, &task_manager));
  
  // no table
  EXPECT_TRUE(OB_SUCCESS == factory.get_all_tablets(count));
  
  
  // not init network
  EXPECT_TRUE(OB_SUCCESS == factory.add_table("join_table"));
  EXPECT_TRUE(OB_SUCCESS != factory.get_all_tablets(count));

  MockRootServer root;
  tbsys::CThread root_server_thread;
  BaseServerRunner test_root_server(root);
  root_server_thread.start(&test_root_server, NULL);
  
  sleep(5);
  EXPECT_TRUE(OB_SUCCESS == factory.get_all_tablets(count));
  
  // table not exist
  count = 0;
  EXPECT_TRUE(OB_SUCCESS == factory.add_table("xxxx"));
  EXPECT_TRUE(OB_SUCCESS != factory.get_all_tablets(count));

  root.stop();
  transport.stop();
}



