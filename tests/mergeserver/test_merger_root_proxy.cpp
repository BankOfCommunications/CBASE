#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include "common/ob_schema_manager.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_schema_proxy.h"
#include "ob_ms_rpc_stub.h"

#include "mock_server.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "mock_chunk_server.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;

const uint64_t timeout = 5000000;
const char * addr = "localhost";

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


ObMergerSchemaManager * manager = NULL;

class TestRootRpcProxy: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }

    static void * fetch_schema(void * argv)
    {
      ObMergerRootRpcProxy * proxy = (ObMergerRootRpcProxy *) argv;
      EXPECT_TRUE(NULL != proxy);
      const ObSchemaManagerV2 * schema = NULL;
      EXPECT_EQ(OB_SUCCESS , proxy->fetch_newest_schema(manager, &schema));
      EXPECT_TRUE(NULL != schema);
      if (NULL != schema) EXPECT_TRUE(schema->get_version() == 1025);
      return NULL;
    }
};

// multi-thread schema test
TEST_F(TestRootRpcProxy, test_multi_schema)
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

  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);

  ObServer update_server;
  update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);
  
  ObServer merge_server;
  merge_server.set_ipv4_addr(addr, 10256);
  ObMergerRootRpcProxy proxy(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub));

  manager = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != manager);
  ObSchemaManagerV2 sample(1022);
  EXPECT_TRUE(OB_SUCCESS == manager->init(false, sample));
  
  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);
  
  const int MAX_THREAD_COUNT = 15;
  pthread_t threads[MAX_THREAD_COUNT];
  for (int i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    int ret = pthread_create(&threads[i], NULL, fetch_schema, &proxy);
    if (ret != OB_SUCCESS)
    {
      break;
    }
  }

  for (int i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    pthread_join(threads[i], NULL);
  }
  
  if (NULL != manager)
  {
    delete manager;
    manager = NULL;
  }
  transport.stop();
  server.stop();
  sleep(5);
}


TEST_F(TestRootRpcProxy, test_init)
{
  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);

  ObServer update_server;
  update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);

  ObServer merge_server;
  ObMergerRootRpcProxy proxy(0, timeout, root_server);

  ObMergerRpcStub stub;
  EXPECT_TRUE(OB_SUCCESS != proxy.init(NULL));
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub));
}

TEST_F(TestRootRpcProxy, test_register)
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

  //
  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);

  ObServer merge_server;
  merge_server.set_ipv4_addr(addr, 10256);
  ObMergerRootRpcProxy proxy(0, timeout, root_server);

  // not init
  EXPECT_TRUE(OB_SUCCESS != proxy.register_merger(merge_server));
  // server not start
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub));
  EXPECT_TRUE(OB_SUCCESS != proxy.register_merger(merge_server));

  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);

  // success 
  EXPECT_TRUE(OB_SUCCESS == proxy.register_merger(merge_server));

  transport.stop();
  server.stop();
  sleep(5);
}

TEST_F(TestRootRpcProxy, test_heartbeat)
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

  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRootRpcProxy proxy(0, timeout, root_server);

  ObServer merge_server;
  merge_server.set_ipv4_addr(addr, 10256);
  
  // not init
  EXPECT_TRUE(OB_SUCCESS != proxy.async_heartbeat(merge_server));
  
  // server not start
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub));
  EXPECT_TRUE(OB_SUCCESS == proxy.async_heartbeat(merge_server));
  
  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);

  // success 
  EXPECT_TRUE(OB_SUCCESS == proxy.async_heartbeat(merge_server));
  server.stop();
  transport.stop();
  sleep(5);
}


TEST_F(TestRootRpcProxy, test_version)
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

  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRootRpcProxy proxy(0, timeout, root_server);

  // not init
  int64_t version = 0;
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_schema_version(version));
  // server not start
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub));
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_schema_version(version));

  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);

  // success 
  EXPECT_TRUE(OB_SUCCESS == proxy.fetch_schema_version(version));
  EXPECT_TRUE(version == 1025);

  transport.stop();
  server.stop();
  sleep(5);
}

TEST_F(TestRootRpcProxy, test_schema)
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

  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRootRpcProxy proxy(0, timeout, root_server);

  // not init
  const ObSchemaManagerV2 * schema = NULL;
  manager = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != manager);
  
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_newest_schema(manager, &schema));

  ObSchemaManagerV2 sample(1022);
  EXPECT_TRUE(OB_SUCCESS == manager->init(false, sample));

  // server not start
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub));
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_newest_schema(NULL, &schema));
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_newest_schema(manager, NULL));

  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);
  
  if (manager)
  {
    delete manager;
    manager = NULL;
  }

  transport.stop();
  server.stop();
  sleep(5);
}


