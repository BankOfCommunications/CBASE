#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_ms_tablet_location.h"
#include "common/ob_schema_manager.h"
#include "ob_ms_rpc_stub.h"
#include "ob_ms_schema_task.h"

#include "mock_server.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "mock_chunk_server.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;

const char * addr = "localhost";

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestTimerTask: public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


TEST_F(TestTimerTask, test_fetch_schema)
{
  /*
  const char * test = "test";
  printf("ret[%d]\n", memcmp(NULL, test, 0));
  printf("ret[%d]\n", memcmp(test, NULL, 0));
  */

  ObMergerRpcStub stub;
  ThreadSpecificBuffer buffer;
  ObPacketFactory factory;
  tbnet::Transport transport;
  tbnet::DefaultPacketStreamer streamer; 
  streamer.setPacketFactory(&factory);
  transport.start();
  ObClientManager client_manager;
  EXPECT_TRUE(OB_SUCCESS == client_manager.initialize(&transport, &streamer));
  EXPECT_TRUE(OB_SUCCESS == stub.init(&buffer, &client_manager));

  int64_t timeout = 100000;
  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);

  ObServer merge_server;
  ObMergerRpcProxy proxy(1, timeout, root_server, merge_server);

  EXPECT_TRUE(OB_SUCCESS != proxy.init(NULL, NULL, NULL));
  EXPECT_TRUE(OB_SUCCESS != proxy.init(&stub, NULL, NULL));

  ObMergerSchemaManager * schema = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != schema);
  ObSchemaManagerV2 temp(200);
  EXPECT_TRUE(OB_SUCCESS == schema->init(false, temp));

  ObTabletLocationCache * location = new ObMergerTabletLocationCache;
  EXPECT_TRUE(NULL != location);
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub, schema, location));

  // not start root server
  ObMergerSchemaTask task;
  task.init(&proxy, schema);
  task.runTimerTask();
  EXPECT_TRUE(200 == schema->get_latest_version());

  // stat failed
  ObMergerSchemaTask task1;
  task1.init(NULL, NULL);
  task1.runTimerTask();
  EXPECT_TRUE(200 == schema->get_latest_version());

  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);

  task.set_version(1024, 1025);
  task.runTimerTask();
  // root server returned
  EXPECT_TRUE(1025 == schema->get_latest_version());

  transport.stop();
  server.stop();
  sleep(5);
}


TEST_F(TestTimerTask, test_timer_fetch)
{
  ObMergerRpcStub stub;
  ThreadSpecificBuffer buffer;
  ObPacketFactory factory;
  tbnet::Transport transport;
  tbnet::DefaultPacketStreamer streamer; 
  streamer.setPacketFactory(&factory);
  transport.start();
  ObClientManager client_manager;
  EXPECT_TRUE(OB_SUCCESS == client_manager.initialize(&transport, &streamer));
  EXPECT_TRUE(OB_SUCCESS == stub.init(&buffer, &client_manager));

  int64_t timeout = 1000000;
  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);

  ObServer merge_server;
  ObMergerRpcProxy proxy(1, timeout, root_server, merge_server);

  EXPECT_TRUE(OB_SUCCESS != proxy.init(NULL, NULL, NULL));
  EXPECT_TRUE(OB_SUCCESS != proxy.init(&stub, NULL, NULL));

  ObMergerSchemaManager * schema = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != schema);
  ObSchemaManagerV2 temp(200);
  EXPECT_TRUE(OB_SUCCESS == schema->init(false, temp));

  ObTabletLocationCache * location = new ObMergerTabletLocationCache;
  EXPECT_TRUE(NULL != location);
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub, schema, location));

  // not start root server
  ObMergerSchemaTask task;
  task.init(&proxy, schema);
  ObTimer timer;
  EXPECT_TRUE(OB_SUCCESS == timer.init());
  task.set_version(1024, 1025);
  EXPECT_TRUE(OB_SUCCESS == timer.schedule(task, 2000 * 1000, false));
  EXPECT_TRUE(200 == schema->get_latest_version());

  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(5);

  // wait timer task processed 
  EXPECT_TRUE(1025 == schema->get_latest_version());
  timer.destroy();

  transport.stop();
  server.stop();
  sleep(5);
}




