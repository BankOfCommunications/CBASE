#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include "ob_ms_schema_manager.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_tablet_location_proxy.h"
#include "ob_ms_rpc_stub.h"
#include "ob_ms_scan_event.h"
#include "ob_ms_async_rpc.h"

#include "mock_server.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "mock_chunk_server.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;

const uint64_t timeout = 10000000;
const char * addr = "127.0.0.1";
static CharArena allocator_;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestObMergerScanEvent: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};
/*
ObMergerLocationCacheProxy * create_merger_location_cache_proxy()
{
 return cache_proxy;
}

ThreadSpecificBuffer *create_rpc_buffer()
{
 return rpc_buffer;
}

ObClientManager *create_rpc_frame()
{
 return rpc_frame;
}

ObMergerAsyncRpcStub * create_merger_async_rpc_stub()
{
 return async_rpc;
}

ObMergerScanEvent * create_merger_scan_event()
{
  return scan_event;
}
*/

ObScanParam *create_scan_param()
{
  ObScanParam *param = new ObScanParam();

  param->set_is_result_cached(true);

  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  range.start_version_ = 11;
  range.end_version_ = 2001;
  param->set_version_range(range);

  // table name
  uint64_t table_id = 123;
  ObString table_name;
  ObNewRange scan_range;

  // scan range
  scan_range.table_id_ = 23455;
  char * start_key = (char*)"start_row_key_start";
  char * end_key = (char*)"end_row_key_end";
  scan_range.end_key_.set_max_row();
  scan_range.start_key_ = make_rowkey(start_key, &allocator_);
  scan_range.end_key_ = make_rowkey(end_key, &allocator_);
  param->set(table_id, table_name, scan_range);

  // columns
  ObString column;
  char * temp_buff[32];
  for (uint64_t i = 11; i < 21; ++i)
  {
    temp_buff[i] = new char[32];
    sprintf(temp_buff[i], "%lu_scan_column_%lu", i, i);
    column.assign(temp_buff[i], static_cast<int32_t>(strlen(temp_buff[i])));
    EXPECT_TRUE(OB_SUCCESS == param->add_column(column));
  }

  return param;
}

TEST_F(TestObMergerScanEvent, test_single_scan)
{
  /// 1. setup transport
  /// 2. setup servers (root, ups, CSes)
  /// 3. setup scan request (location list, scan range)
  //

  TBSYS_LOG(INFO, "( 1 )");
  tbnet::Transport * transport = new tbnet::Transport();
  tbnet::DefaultPacketStreamer * streamer = new tbnet::DefaultPacketStreamer();
  ObPacketFactory * factory = new ObPacketFactory();
  streamer->setPacketFactory(factory);
  ObClientManager client_manager;
  client_manager.initialize(transport, streamer);
  transport->start();



  TBSYS_LOG(INFO, "( 2 )");
  ObServer update_server;
  ObServer root_server;
  ObServer merge_server;
  update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRpcProxy proxy(3, timeout, root_server, merge_server);

  ObMergerRpcStub stub;
  ThreadSpecificBuffer buffer;
  stub.init(&buffer, &client_manager);
  ObMergerRootRpcProxy rpc(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == rpc.init(&stub));

  merge_server.set_ipv4_addr(addr, 10256);
  ObTabletLocationCache * location = new ObMergerTabletLocationCache;
  location->init(50000 * 5, 1000, 100000);

  // init tablet cache
  char temp[256] = "";
  char temp_end[256] = "";
  ObServer server;
  const uint64_t START_ROW = 100L;
  const uint64_t MAX_ROW = 200L;
  ObTabletLocationList list;
  for (uint64_t i = START_ROW; i <= MAX_ROW - 100; i += 100)
  {
    server.set_ipv4_addr(addr, MockChunkServer::CHUNK_SERVER_PORT);
    ObTabletLocation addr(i, server);

    EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    //EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    ///EXPECT_TRUE(OB_SUCCESS == list.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 100);

    ObNewRange range;
    range.table_id_ = 123;
    range.start_key_ = make_rowkey(temp, &allocator_);
    range.end_key_ = make_rowkey(temp_end, &allocator_);
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    list.set_tablet_range(range);
    list.set_timestamp(tbsys::CTimeUtil::getTime());
    EXPECT_TRUE(OB_SUCCESS == location->set(range, list));
  }

  TBSYS_LOG(INFO, "( 3 )");

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


  /// ( 3 )
  ObScanParam scan_param;
  uint64_t table_id = 123;
  ObString table_name;
  ObNewRange scan_range;
  char * start_key = (char*)"row_100";
  char * end_key = (char*)"row_200";
  scan_range.table_id_ = table_id;
  //scan_range.border_flag_.set_max_value();
  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.start_key_ = make_rowkey(start_key, &allocator_);
  scan_range.end_key_ = make_rowkey(end_key, &allocator_);
  scan_param.set(table_id, table_name, scan_range);
  scan_param.add_column(101);

  ObMergerScanParam param;
  param.set_param(scan_param);

  /// (4) do it!
  ObMergerAsyncRpcStub async;
  async.init(&buffer, &client_manager);
  ObMergerLocationCacheProxy location_proxy(root_server, &rpc, location);
  ObMergerScanEvent scan_event(&location_proxy, &async);
  scan_event.init(100, 10);
  scan_event.set_request_param(param, 100000);
  scan_event.wait(timeout);

  ObInnerCellInfo * cur_cell = NULL;
  while(1)
  {
    if (OB_ITER_END == scan_event.next_cell())
    {
      break;
    }
    if(OB_ITER_END == scan_event.get_cell(&cur_cell))
    {
      break;
    }
    TBSYS_LOG(DEBUG, "[id:%lu][key:%s][obj:%s]", cur_cell->table_id_,
        to_cstring(cur_cell->row_key_), to_cstring(cur_cell->value_));
  }

  TBSYS_LOG(INFO, "( request sent )");
  transport->stop();
  test_chunk_server.~MockServerRunner();
  chunk_server_thread.join();
  test_root_server.~MockServerRunner();
  root_server_thread.join();
}

#if 0
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

  ObServer merge_server;
  merge_server.set_ipv4_addr(addr, 10256);
  ObServer update_server;
  update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);
  ObMergerRpcProxy proxy(0, timeout, update_server);

  // not init
  ObMergerTabletLocation cs_addr;
  ObGetParam get_param;
  ObString row_key;
  char * temp = "test";
  row_key.assign(temp, strlen(temp));
  ObCellInfo cell;
  cell.table_id_ = 234;
  cell.column_id_ = 111;
  cell.row_key_ = row_key;
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));

  ObScanner scanner;
  EXPECT_TRUE(OB_SUCCESS != proxy.ups_get(cs_addr, get_param, scanner));
  ObScanParam scan_param;
  EXPECT_TRUE(OB_SUCCESS != proxy.ups_scan(cs_addr, scan_param, scanner));

  // init
  ObTabletLocationCache * location = new ObMergerTabletLocationCache;
  EXPECT_TRUE(NULL != location);

  ObServer root_server;
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRootRpcProxy rpc(0, timeout, root_server);
  ObMergerLocationCacheProxy cache(merge_server, &rpc, location);

  // server not start
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub, &cache, NULL));

  EXPECT_TRUE(OB_SUCCESS != proxy.ups_get(cs_addr, get_param, scanner));
  EXPECT_TRUE(OB_SUCCESS != proxy.ups_scan(cs_addr, scan_param, scanner));

  // start root server
  MockUpdateServer server;
  MockServerRunner test_update_server(server);
  tbsys::CThread update_server_thread;
  update_server_thread.start(&test_update_server, NULL);
  sleep(2);
  //
  EXPECT_TRUE(OB_SUCCESS == proxy.ups_get(cs_addr, get_param, scanner));
  uint64_t count = 0;
  ObScannerIterator iter;
  for (iter = scanner.begin(); iter != scanner.end(); ++iter)
  {
    EXPECT_TRUE(OB_SUCCESS == iter.get_cell(cell));
    printf("client:%.*s\n", cell.row_key_.length(), cell.row_key_.ptr());
    ++count;
  }
  // return 10 cells
  EXPECT_TRUE(count == 10);
  scanner.clear();

  ObScanParam param;
  ObNewRange range;
  range.border_flag_.set_min_value();
  ObString end_row;
  end_row.assign(temp, strlen(temp));
  //range.start_key_ = end_row;
  range.end_key_ = end_row;

  ObString name;
  name.assign(temp, strlen(temp));
  scan_param.set(OB_INVALID_ID, name, range);
  EXPECT_TRUE(OB_SUCCESS == proxy.ups_scan(cs_addr, scan_param, scanner));

  count = 0;
  for (iter = scanner.begin(); iter != scanner.end(); ++iter)
  {
    EXPECT_TRUE(OB_SUCCESS == iter.get_cell(cell));
    printf("client:%.*s\n", cell.row_key_.length(), cell.row_key_.ptr());
    ++count;
  }
  // return 10 cells
  EXPECT_TRUE(count == 10);

  if (location != NULL)
  {
    delete location;
    location = NULL;
  }
  transport.stop();
  server.stop();
  sleep(5);
#endif

#if 0
TEST_F(TestRpcProxy, test_cs)
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

  // not init
  ObMergerTabletLocation cs_addr;
  ObGetParam get_param;
  ObCellInfo cell;
  ObString row_key;
  char temp[256] = "";
  snprintf(temp, 100, "row_100");
  row_key.assign(temp, strlen(temp));
  cell.table_id_ = 234;
  cell.column_id_ = 111;
  cell.row_key_ = row_key;
  EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));

  ObScanner scanner;
  EXPECT_TRUE(OB_SUCCESS != proxy.ups_get(cs_addr, get_param, scanner));
  ObScanParam scan_param;
  EXPECT_TRUE(OB_SUCCESS != proxy.ups_scan(cs_addr, scan_param, scanner));

  // init
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRootRpcProxy rpc(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == rpc.init(&stub));
  ObTabletLocationCache * location = new ObMergerTabletLocationCache;
  EXPECT_TRUE(NULL != location);
  EXPECT_TRUE(OB_SUCCESS == location->init(50000 * 5, 1000, 10000));
  ObMergerLocationCacheProxy cache(merge_server, &rpc, location);

  // init tablet cache
  char temp_end[256] = "";
  ObServer server;
  const uint64_t START_ROW = 100L;
  const uint64_t MAX_ROW = 790L;
  for (uint64_t i = START_ROW; i < MAX_ROW - 100; i += 100)
  {
    server.set_ipv4_addr(i + 256, 1024 + i);
    ObTabletLocation addr(i, server);

    ObTabletLocationList list;
    EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    EXPECT_TRUE(OB_SUCCESS == list.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 100);
    ObString start_key(100, strlen(temp), temp);
    ObString end_key(100, strlen(temp_end), temp_end);

    ObNewRange range;
    range.table_id_ = 234;
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    list.set_timestamp(tbsys::CTimeUtil::getTime());
    EXPECT_TRUE(OB_SUCCESS == location->set(range, list));
    ObString rowkey;
    snprintf(temp, 100, "row_%lu", i + 40);
    rowkey.assign(temp, strlen(temp));
    EXPECT_TRUE(OB_SUCCESS == location->get(234, rowkey, list));

    rowkey.assign(temp_end, strlen(temp_end));
    EXPECT_TRUE(OB_SUCCESS == location->get(234, rowkey, list));
  }

  // server not start
  ObIterator *it = NULL;
  ObMergerTabletLocation succ_addr;
  ObTabletLocationList list;
  EXPECT_TRUE(OB_SUCCESS == proxy.init(&stub, &cache, NULL));
  EXPECT_TRUE(OB_SUCCESS != proxy.cs_get(get_param, succ_addr, scanner, it));
  EXPECT_TRUE(OB_SUCCESS != proxy.cs_scan(scan_param, succ_addr, scanner, it));

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

  // init get param
  snprintf(temp, 100, "row_105");
  row_key.assign(temp, strlen(temp));
  cell.table_id_ = 234;
  cell.column_id_ = 111;
  cell.row_key_ = row_key;
  get_param.add_cell(cell);

  // cache server not exist so fetch from root server error
  ObTabletLocationList addr_temp;
  EXPECT_TRUE(OB_SUCCESS == location->get(234, row_key, addr_temp));

  // scan from root server
  EXPECT_TRUE(OB_SUCCESS == proxy.cs_get(get_param, succ_addr, scanner, it));

  // delete the cache item and fetch from root server
  EXPECT_TRUE(OB_SUCCESS == location->get(234, row_key, addr_temp));

  ObGetParam get_param2;
  ObTabletLocationList list2;
  // cache not exist, get from root server
  snprintf(temp, 100, "row_998");
  ObString row_key2;
  row_key2.assign(temp, strlen(temp));
  ObCellInfo cell2;
  cell2.table_id_ = 234;
  cell2.column_id_ = 111;
  cell2.row_key_ = row_key2;
  get_param2.add_cell(cell2);
  EXPECT_TRUE(OB_SUCCESS == proxy.cs_get(get_param2, succ_addr, scanner, it));

  // not find the cs
  snprintf(temp, 100, "zzz_row_999");
  EXPECT_TRUE(OB_SUCCESS != proxy.cs_get(get_param2, succ_addr, scanner, it));

  // ok
  snprintf(temp, 100, "row_%lu", MAX_ROW - 10);
  EXPECT_TRUE(OB_SUCCESS == proxy.cs_get(get_param2, succ_addr, scanner, it));

  uint64_t count = 0;
  ObScannerIterator iter;
  for (iter = scanner.begin(); iter != scanner.end(); ++iter)
  {
    EXPECT_TRUE(OB_SUCCESS == iter.get_cell(cell));
    printf("client:%.*s\n", cell.row_key_.length(), cell.row_key_.ptr());
    ++count;
  }
  scanner.clear();

  root.stop();
  chunk.stop();
  sleep(10);
  transport.stop();

  if (location != NULL)
  {
    delete location;
    location = NULL;
  }
}

#endif
