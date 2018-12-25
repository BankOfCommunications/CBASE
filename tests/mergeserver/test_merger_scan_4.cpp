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
#include "ob_location_list_cache_loader.h"
#include "ob_scan_param_loader.h"
#include "ob_scanner_loader.h"

#include "mock_server.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "mock_chunk_server.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;

#define MIN(a,b)  ((a) < (b) ? (a) : (b))

const uint64_t timeout = 10000000;
const char * addr = "127.0.0.1";

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class ObMockMergerAsyncRpcStub : public ObMergerAsyncRpcStub
{
  private:
   mutable int file_idx;
   char *filename;
   char **scanner_section_names;
  public:
    ObMockMergerAsyncRpcStub()
    {
      rpc_events_list_.init(1024, rpc_events_, 0);
      file_idx = 0;
    }

    void test_init(char*fname, char **sname)
    {
      filename = fname;
      scanner_section_names = sname;
    }

    virtual ~ObMockMergerAsyncRpcStub(){}
    virtual int scan(const int64_t timeout, const ObServer & server,
        const ObScanParam & scan_param, ObMergerRpcEvent & result) const
    {
      result.set_result_code(OB_SUCCESS);
      ObScanner &scanner = result.get_result();
      
      ObScannerLoader loader;
      loader.load(filename, scanner_section_names[file_idx]);

      file_idx = (++file_idx % 6);
      loader.get_decoded_scanner(scanner);

      TBSYS_LOG(INFO, "+==============DUMP===============+");
      scanner.dump();
      TBSYS_LOG(INFO, "+=============END DUMP============+");

      if(!rpc_events_list_.push_back(&result))
      {
        TBSYS_LOG(ERROR, "fail to save rpc result");
      }
      else
      {
        TBSYS_LOG(INFO, "currently totally %d results saved....", rpc_events_list_.get_array_index());
        TBSYS_LOG(INFO, "a RPC result save to rpc_event_list_");
      }
      return OB_SUCCESS;
    }

    mutable ObMergerRpcEvent *rpc_events_[1024];
    mutable ObArrayHelper<ObMergerRpcEvent*> rpc_events_list_;
};


TEST(ObMergerScanEvent, no_parallel_count_limit_scanner_fulfill)
{
  char * filename = "test_data/schema_scan_test_4.txt";

  // 2_a
  char * scanner_section_names[] = {
    "schema_scanner_1_2",
    "schema_scanner_3_4",
    "schema_scanner_5_8"
  };


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
  ObMergerRpcProxy proxy(3, timeout, update_server);
  ObMergerRpcStub stub;
  ThreadSpecificBuffer buffer;
  stub.init(&buffer, &client_manager);
  ObMergerRootRpcProxy rpc(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == rpc.init(&stub));

  ObLocationListCacheLoader *cache_loader = new ObLocationListCacheLoader();
  cache_loader->load(filename, "tablet_location_cache");
  ObTabletLocationCache cache;
  cache_loader->get_decoded_location_list_cache(cache);
  TBSYS_LOG(INFO, "+==============DUMP===============+");
  cache.dump();
  TBSYS_LOG(INFO, "+=============END DUMP============+");
  ObTabletLocationCache * location = &cache;  //new ObMergerTabletLocationCache;

  ObScanParam scan_param;
  ObScanParam org_param;
  ObScanParamLoader *scan_param_loader = new ObScanParamLoader();
  scan_param_loader->load(filename, "scan_param");
  scan_param_loader->get_org_scan_param(org_param);
  TBSYS_LOG(INFO, "+==============DUMP===============+");
  org_param.dump();
  TBSYS_LOG(INFO, "+=============END DUMP============+");

  scan_param_loader->decoded_scan_param(org_param, scan_param);
  ObMergerScanParam param;
  param.set_param(scan_param);


  //ObMergerAsyncRpcStub async;
  ObMockMergerAsyncRpcStub async;
  async.init(&buffer, &client_manager);  
  async.test_init(filename, scanner_section_names);
  ObMergerLocationCacheProxy location_proxy(root_server, &rpc, location);
  ObMergerScanEvent scan_event(&location_proxy, &async);
  scan_event.init(100, 10);
  scan_event.set_request_param(param, 100000, 1000);
  /// scan_event.wait(timeout);
  int i = 0;
  int ret = OB_SUCCESS;
  bool finish = false;
  int first_batch_count = async.rpc_events_list_.get_array_index();

  /* update location cache */
  if (0)
  {    
    ObLocationListCacheLoader *cache_loader2 = new ObLocationListCacheLoader();
    cache_loader2->load(filename, "tablet_location_cache_updated");
    cache_loader2->get_decoded_location_list_cache(cache);
  }

  for (i = 0; i < async.rpc_events_list_.get_array_index(); i++)
  {

    ret = scan_event.process_result(timeout, async.rpc_events_[i], finish);
    EXPECT_TRUE(OB_SUCCESS == ret);
    TBSYS_LOG(INFO, "total=%d, finished=%d", 
        scan_event.get_total_sub_request_count(),
        scan_event.get_finished_sub_request_count());

    EXPECT_TRUE(scan_event.get_total_sub_request_count() == async.rpc_events_list_.get_array_index());
    EXPECT_TRUE(scan_event.get_finished_sub_request_count() == i+1);

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "fail to process result %d!", i);
    }
    else
    {
      TBSYS_LOG(INFO, "succ to process result %d ;) finish=%s", i, finish?"true":"false");
    }
  }
  EXPECT_TRUE(scan_event.get_total_sub_request_count() == i);
  EXPECT_TRUE(scan_event.get_finished_sub_request_count() == i);


/*
  TBSYS_LOG(INFO, "first_batch_count=%ld; async.rpc_events_list_length=%ld", 
      first_batch_count, async.rpc_events_list_.get_array_index());

  for (i = first_batch_count; i < async.rpc_events_list_.get_array_index(); i++)
  {
    ret = scan_event.process_result(timeout, async.rpc_events_[i], finish);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "second round: fail to process result %d!", i);
    }
    else
    {
      TBSYS_LOG(INFO, "second round: succ to process result %d ;) finish=%s", i, finish?"true":"false");
    }    
  }
*/
  TBSYS_LOG(INFO, "totally %d results saved", async.rpc_events_list_.get_array_index());

}

