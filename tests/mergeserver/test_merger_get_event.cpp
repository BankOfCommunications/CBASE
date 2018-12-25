#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_malloc.h"
#include "common/ob_common_param.h"

#include "ob_rpc_event.h"
#include "ob_ms_async_rpc.h"
#include "ob_ms_rpc_event.h"
#include "ob_ms_request_event.h"
#include "ob_ms_get_event.h"
#include "ob_ms_rpc_stub.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_tablet_location_proxy.h"

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

class TestGetRequestEvent: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

#define TIMEOUT false 

const static uint64_t MAX_COUNT = 10;
const static int64_t timeout = 1000000L;
const char * addr = "127.0.0.1";

TEST_F(TestGetRequestEvent, test_get)
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
  ObMergerRpcProxy proxy(3, timeout, update_server);

  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRootRpcProxy rpc(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == rpc.init(&stub));
  
  ObTabletLocationCache * location = new ObMergerTabletLocationCache;
  EXPECT_TRUE(NULL != location);
  EXPECT_TRUE(OB_SUCCESS == location->init(50000 * 5, 1000, 10000));
  
  ObMergerLocationCacheProxy cache(merge_server, &rpc, location);

  // init tablet cache 
  char temp[256] = "";
  char temp_end[256] = "";
  ObServer server;
  const uint64_t START_ROW = 100L;
  const uint64_t MAX_ROW = 300L;
  for (uint64_t i = START_ROW; i < MAX_ROW - 100; i += 100)
  {
    server.set_ipv4_addr(addr, MockChunkServer::CHUNK_SERVER_PORT);
    ObTabletLocation addr(i, server);

    ObTabletLocationList list;
    EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    EXPECT_TRUE(OB_SUCCESS == list.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 100);
    ObString start_key(100, strlen(temp), temp);
    ObString end_key(100, strlen(temp_end), temp_end);

    ObRange range;
    range.table_id_ = 234;
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    list.set_timestamp(tbsys::CTimeUtil::getTime()); 
    EXPECT_TRUE(OB_SUCCESS == location->set(range, list));
  }

  ObMergerTabletLocation succ_addr;
  ObTabletLocationList list;
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub, &cache, NULL));

  // start root server
  MockRootServer root;
  tbsys::CThread root_server_thread;
  MockServerRunner test_root_server(root);
  root_server_thread.start(&test_root_server, NULL); 

  // start chunk server
  MockChunkServer chunk;
  tbsys::CThread chunk_server_thread;
  MockServerRunner test_chunk_server(chunk);
  chunk_server_thread.start(&test_chunk_server, NULL); 
  sleep(2);

  ObMergerAsyncRpcStub async;
  ObMergerLocationCacheProxy location_proxy(root_server, &rpc, location);
  ObGetRequestEvent request(&location_proxy, &async);
  
  ObGetParam get_param;
  ObCellInfo cell;
  ObString row_key;
  snprintf(temp, 100, "row_101");
  row_key.assign(temp, strlen(temp));
  cell.table_id_ = 234;
  cell.column_id_ = 111;
  cell.row_key_ = row_key;
  // add same cells
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
  
  // not init
  EXPECT_TRUE(OB_SUCCESS != request.set_request_param(get_param, timeout));
  EXPECT_TRUE(OB_SUCCESS != request.wait(timeout));
  
  EXPECT_TRUE(OB_SUCCESS == async.init(&buffer, &client_manager));
  EXPECT_TRUE(OB_SUCCESS == request.init(100, 10));
  EXPECT_TRUE(OB_SUCCESS == request.set_request_param(get_param, timeout));
  EXPECT_TRUE(OB_SUCCESS == request.wait(timeout));
  
  uint64_t count = 0;
  ObScannerIterator iter;
  ObCellInfo * cell_info = NULL;
  int ret = OB_SUCCESS;
  while (OB_ITER_END != (ret = request.next_cell()))
  {
    EXPECT_TRUE(OB_SUCCESS == ret);
    EXPECT_TRUE(OB_SUCCESS == request.get_cell(&cell_info));
    EXPECT_TRUE(cell_info != NULL);
    printf("client:%.*s\n", cell_info->row_key_.length(), cell_info->row_key_.ptr());
    ++count;
  }
  EXPECT_TRUE(OB_SUCCESS == request.reset());

  root.stop();
  chunk.stop();
  sleep(3);
  transport.stop();

  if (location != NULL)
  {
    delete location;
    location = NULL;
  }
}


