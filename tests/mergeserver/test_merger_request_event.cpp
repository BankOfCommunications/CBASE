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

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestRequestEvent: public ::testing::Test
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

// implement all virtual functions
class TestObMergerRequestEvent:public ObMergerRequestEvent
{
public:
  uint64_t process_count_;
  TestObMergerRequestEvent(ObMergerAsyncRpcStub * rpc):ObMergerRequestEvent(rpc)
  {
    process_count_ = 0;
  }

  int next_cell()
  {
    return OB_SUCCESS;
  }

  int get_cell(ObCellInfo ** cell, bool * row_change)
  {
    return OB_SUCCESS;
  }
  
  int get_cell(ObCellInfo ** cell)
  {
    return OB_SUCCESS;
  }

  int set_request_param(ObReadParam & param, const int64_t timeout)
  {
    return OB_SUCCESS;
  }

  int process_result(const uint64_t index, ObMergerRpcEvent * rpc, bool & finish)
  {
    finish = false;
    if (++process_count_ == MAX_COUNT)
    {
      finish = true;
    }
    return OB_SUCCESS;
  }
};

TEST_F(TestRequestEvent, test_request)
{
  ObMergerAsyncRpcStub stub;
  TestObMergerRequestEvent request(&stub);
  EXPECT_TRUE(OB_SUCCESS == request.init(100, 1));
  
  EXPECT_TRUE(&stub == request.get_rpc());
  EXPECT_TRUE(request.get_request_id() == 1);
  
  request.set_timeout(1234);
  EXPECT_TRUE(request.get_timeout() == 1234);

  ObReadParam param;
  EXPECT_TRUE(OB_SUCCESS == request.set_request_param(param, 1233));
  // no change not implemented
  EXPECT_TRUE(request.get_timeout() == 1234);
}

TEST_F(TestRequestEvent, test_create_destroy)
{
  ObMergerAsyncRpcStub stub;
  TestObMergerRequestEvent request(&stub);
  EXPECT_TRUE(OB_SUCCESS == request.init(100, 1));
  ObMergerRpcEvent * event = NULL;
  const static int64_t max_count = 100;
  ObMergerRpcEvent * events[max_count];
  for (int64_t i = 0; i < max_count; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == request.create(&event));
    EXPECT_TRUE(NULL != event);
    events[i] = event;
  }
  
  for (int64_t i = 0; i < max_count; ++i)
  {
    event = events[i];
    EXPECT_TRUE(OB_SUCCESS == request.destroy(event));
  }
  
  request.print_info(stdout);
  
  for (int64_t i = 0; i < max_count; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == request.create(&event));
    EXPECT_TRUE(NULL != event);
  }
  
  for (int64_t i = 0; i < max_count; ++i)
  {
    if (0 == (i % 3))
    {
      event = events[i];
      EXPECT_TRUE(OB_SUCCESS == request.destroy(event));
    }
  }

  request.print_info(stdout);
  EXPECT_TRUE(request.reset() == OB_SUCCESS);
}

void * signal(void * argv)
{
  ObMergerRpcEvent ** rpcs = reinterpret_cast<ObMergerRpcEvent**>(argv);
  EXPECT_TRUE(NULL != rpcs);
  #if TIMEOUT
  for (uint64_t i = 0; i < MAX_COUNT - 1; ++i)
  {
    rpcs[i]->get_result().set_is_req_fullfilled(true, 1);
    const_cast<ObMergerRequestEvent *>(rpcs[i]->get_client_request())->signal(*rpcs[i]);
  }
  #else 
  for (uint64_t i = 0; i < MAX_COUNT; ++i)
  {
    rpcs[i]->get_result().set_is_req_fullfilled(true, 1);
    const_cast<ObMergerRequestEvent *>(rpcs[i]->get_client_request())->signal(*rpcs[i]);
  }
  #endif
  return NULL;
}

/// multi-thread test case
TEST_F(TestRequestEvent, test_wait_signal)
{
  ObMergerAsyncRpcStub stub;
  TestObMergerRequestEvent request(&stub);
  EXPECT_TRUE(OB_SUCCESS == request.init(100, 1));
  
  ObMergerRpcEvent * rpcs[MAX_COUNT];
  for (uint64_t i = 0; i < MAX_COUNT; ++i)
  {
    EXPECT_TRUE(request.create(&(rpcs[i])) == OB_SUCCESS);
  }
  
  // signal thread
  pthread_t thread;
  EXPECT_TRUE(OB_SUCCESS == pthread_create(&thread, NULL, signal, rpcs));
  
  // wait signal
  int ret = request.wait(timeout);
  #if TIMEOUT 
  EXPECT_TRUE(ret != OB_SUCCESS); 
  #else
  EXPECT_TRUE(ret == OB_SUCCESS); 
  #endif
  request.print_info(stdout);

  EXPECT_TRUE(OB_SUCCESS == pthread_join(thread, NULL));
}


