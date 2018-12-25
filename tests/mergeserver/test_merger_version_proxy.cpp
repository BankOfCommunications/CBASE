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
#include "ob_ms_rpc_proxy.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_rpc_stub.h"
#include "ob_ms_version_proxy.h"

#include "mock_server.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "mock_chunk_server.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestMergerVersionProxy: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

const char * addr = "localhost";

TEST_F(TestMergerVersionProxy, test_version)
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
  ObServer update_server;
  update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);

  proxy.set_master_ups(update_server);
  EXPECT_TRUE(OB_SUCCESS != proxy.init(NULL, NULL, NULL));
  EXPECT_TRUE(OB_SUCCESS != proxy.init(&stub, NULL, NULL));

  ObMergerSchemaManager * schema = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != schema);
  ObSchemaManagerV2 temp(200);
  EXPECT_TRUE(OB_SUCCESS == schema->init(false, temp));

  ObTabletLocationCache * location = new ObMergerTabletLocationCache;
  EXPECT_TRUE(NULL != location);
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub, schema, location));

  int64_t version_timeout = 1000 * 1000 * 2L;
  ObMergerVersionProxy version_proxy(version_timeout);
  int64_t version = 0;

  // not init
  EXPECT_TRUE(OB_SUCCESS != version_proxy.get_version(version));
  EXPECT_TRUE(OB_SUCCESS != version_proxy.init(NULL));
  EXPECT_TRUE(OB_SUCCESS == version_proxy.init(&proxy));
  // not run server
  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(version));
  EXPECT_TRUE(-1 == version);

  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(version));
  EXPECT_TRUE(-1 == version);

  // start mock server
  MockUpdateServer server;
  MockServerRunner test_update_server(server);
  tbsys::CThread update_server_thread;
  update_server_thread.start(&test_update_server, NULL); 

  sleep(2);

  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(version));
  EXPECT_TRUE(-1 != version);

  int64_t new_version = 0;
  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(new_version));
  EXPECT_TRUE(new_version == version);

  usleep(static_cast<useconds_t>(version_timeout * 2));
  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(new_version));
  EXPECT_TRUE(new_version != version);

  version = new_version;
  new_version = 0;
  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(new_version));
  EXPECT_TRUE(new_version == version);

  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(new_version));
  EXPECT_TRUE(new_version == version);

  // cache timeout
  usleep(static_cast<useconds_t>(version_timeout * 2));
  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(new_version));
  EXPECT_TRUE(new_version != version);
  server.stop();
  version = new_version;
  new_version = 0;
  // server stoped
  usleep(static_cast<useconds_t>(version_timeout * 2));
  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(new_version));
  EXPECT_TRUE(new_version == version);

  EXPECT_TRUE(OB_SUCCESS == version_proxy.get_version(new_version));
  EXPECT_TRUE(new_version == version);

  transport.stop();
}




