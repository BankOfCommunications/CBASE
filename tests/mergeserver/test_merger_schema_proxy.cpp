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

const uint64_t timeout = 500000;
const char * addr = "localhost";

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestSchemaProxy: public ::testing::Test, public ObMergerSchemaProxy
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
      ObMergerSchemaProxy * proxy = (ObMergerSchemaProxy *) argv;
      EXPECT_TRUE(NULL != proxy);
      int64_t timestamp = 1024;
      const ObSchemaManagerV2 * schema = NULL;
      EXPECT_TRUE(OB_SUCCESS == proxy->get_schema(timestamp, &schema));
      EXPECT_TRUE(NULL != schema);
      EXPECT_TRUE(schema->get_version() == 1025);
      EXPECT_TRUE(OB_SUCCESS == proxy->release_schema(schema));
      return NULL;
    }
};


TEST_F(TestSchemaProxy, test_get)
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
  ObMergerRootRpcProxy root_proxy(0, timeout, root_server);

  // not init
  const ObSchemaManagerV2 * schema = NULL;
  ObMergerSchemaManager * manager = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != manager);
  int64_t timestamp = ObMergerSchemaProxy::LOCAL_NEWEST;
  ObMergerSchemaProxy proxy(&root_proxy, manager);
  EXPECT_TRUE(OB_SUCCESS == proxy.get_schema(timestamp, &schema));
  EXPECT_TRUE(schema == NULL);
  
  timestamp = 1001;
  EXPECT_TRUE(OB_SUCCESS != proxy.get_schema(timestamp, &schema));

  timestamp = ObMergerSchemaProxy::LOCAL_NEWEST;
  EXPECT_TRUE(OB_SUCCESS == root_proxy.init(&stub));
  EXPECT_TRUE(OB_SUCCESS == proxy.get_schema(timestamp, &schema));
  EXPECT_TRUE(schema == NULL);

  // init schema manger
  ObSchemaManagerV2 sample(1022);
  EXPECT_TRUE(OB_SUCCESS == manager->init(false, sample));

  EXPECT_TRUE(OB_SUCCESS == proxy.get_schema(timestamp, &schema));
  EXPECT_TRUE(NULL != schema);
  EXPECT_TRUE(schema->get_version() == 1022);
  EXPECT_TRUE(OB_SUCCESS == proxy.release_schema(schema));
  
  // server not start
  timestamp = 1024;
  EXPECT_TRUE(OB_SUCCESS != proxy.get_schema(timestamp, &schema));
  
  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);
  
  // not exist but server exist
  timestamp = 1024;
  EXPECT_TRUE(OB_SUCCESS == proxy.get_schema(timestamp, &schema));
  EXPECT_TRUE(NULL != schema);
  EXPECT_TRUE(schema->get_version() == 1025);
  EXPECT_TRUE(OB_SUCCESS == proxy.release_schema(schema));

  // get server newest for init
  timestamp = 100;
  // add schema failed
  EXPECT_TRUE(OB_SUCCESS != proxy.get_schema(timestamp, &schema));
  
  // local version
  timestamp = ObMergerSchemaProxy::LOCAL_NEWEST;
  EXPECT_TRUE(OB_SUCCESS == proxy.get_schema(timestamp, &schema));
  EXPECT_TRUE(NULL != schema);
  EXPECT_TRUE(schema->get_version() == 1025);
  EXPECT_TRUE(OB_SUCCESS == proxy.release_schema(schema));
  
  if (manager)
  {
    delete manager;
    manager = NULL;
  }

  transport.stop();
  server.stop();
  sleep(5);
}


TEST_F(TestSchemaProxy, test_fetch)
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
  ObMergerRootRpcProxy root_proxy(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == root_proxy.init(&stub));

  // not init
  const ObSchemaManagerV2 * schema = NULL;
  ObMergerSchemaManager * manager = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != manager);
  int64_t timestamp = ObMergerSchemaProxy::LOCAL_NEWEST;
  ObMergerSchemaProxy proxy(&root_proxy, manager);
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_schema(timestamp, &schema));

  // init schema manger
  ObSchemaManagerV2 sample(1022);
  EXPECT_TRUE(OB_SUCCESS == manager->init(false, sample));
  
  timestamp = 1021;
  EXPECT_TRUE(OB_SUCCESS == proxy.fetch_schema(timestamp, &schema));
  EXPECT_TRUE(NULL != schema);
  EXPECT_TRUE(schema->get_version() == 1022);
  EXPECT_TRUE(OB_SUCCESS == proxy.release_schema(schema));
  
  // server not start
  timestamp = 1024;
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_schema(timestamp, &schema));
  
  // start root server
  MockRootServer server;
  MockServerRunner test_root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&test_root_server, NULL); 
  sleep(2);
  
  // not exist but server exist
  timestamp = 1024;
  EXPECT_TRUE(OB_SUCCESS == proxy.fetch_schema(timestamp, &schema));
  EXPECT_TRUE(NULL != schema);
  EXPECT_TRUE(schema->get_version() == 1025);
  EXPECT_TRUE(OB_SUCCESS == proxy.release_schema(schema));

  // too nearby last fetch time
  timestamp = 1026;
  EXPECT_TRUE(OB_SUCCESS != proxy.fetch_schema(timestamp, &schema));
  if (manager)
  {
    delete manager;
    manager = NULL;
  }

  transport.stop();
  server.stop();
  sleep(5);
}

// multi-thread schema test
TEST_F(TestSchemaProxy, test_multi_schema)
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
  ObMergerRootRpcProxy root_proxy(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == root_proxy.init(&stub));

  ObMergerSchemaManager * manager = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != manager);
  ObSchemaManagerV2 sample(1022);
  EXPECT_TRUE(OB_SUCCESS == manager->init(false, sample));
  
  ObMergerSchemaProxy proxy(&root_proxy, manager);
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


