#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_malloc.h"
#include "ob_rpc_event.h"
#include "ob_ms_get_event.h"
#include "ob_ms_async_rpc.h"
#include "ob_ms_rpc_event.h"
#include "ob_ms_request_event.h"
#include "ob_ms_tablet_location_proxy.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

#define FUNC() // 

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestRpcEvent: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

// test common rpc event
TEST_F(TestRpcEvent, test_common_event)
{
  FUNC()
  {
    ObCommonRpcEvent event;
    EXPECT_TRUE(event.get_event_id() == 1);
    EXPECT_TRUE(event.get_result_code() == OB_INVALID_ERROR);
  }
  
  FUNC()
  {
    ObCommonRpcEvent event;
    EXPECT_TRUE(event.get_event_id() == 2);
    EXPECT_TRUE(event.get_result_code() == OB_INVALID_ERROR);
  }

  ObServer server;
  server.set_ipv4_addr("localhost", 1234);

  ObCommonRpcEvent event;
  event.set_server(server);
  event.set_result_code(OB_INNER_STAT_ERROR);
  // event.print_info(stdout);
  
  // test get
  EXPECT_TRUE(server == event.get_server());
  EXPECT_TRUE(OB_INNER_STAT_ERROR == event.get_result_code());
  
  int32_t code = OB_SUCCESS;
  const ObScanner & scanner = event.get_result(code);
  EXPECT_TRUE(0 == event.get_result().get_size());
  EXPECT_TRUE(0 == scanner.get_size());
  EXPECT_TRUE(OB_INNER_STAT_ERROR == code);
  
  // not implemented
  EXPECT_TRUE(OB_SUCCESS != event.handlePacket(NULL, NULL));
}

// test merger rpc event
TEST_F(TestRpcEvent, test_merger_event)
{
  ObMergerRpcEvent event;
  event.set_result_code(OB_SUCCESS);
  
  EXPECT_TRUE(event.get_client_id() == OB_INVALID_ID);
  EXPECT_TRUE(event.get_result_code() == OB_SUCCESS);
  ObMergerLocationCacheProxy proxy;
  ObMergerAsyncRpcStub rpc_stub;
  ObGetRequestEvent rpc(&proxy, &rpc_stub);
  EXPECT_TRUE(OB_SUCCESS == rpc.init(100, 1));
  event.init(1234, &rpc);
  EXPECT_TRUE(event.get_client_id() == 1234);
  event.print_info(stdout);
  event.reset();
  EXPECT_TRUE(event.get_client_id() == OB_INVALID_ID);
  EXPECT_TRUE(event.get_result_code() == OB_INVALID_ERROR);
}

