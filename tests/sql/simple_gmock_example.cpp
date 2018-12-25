#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <tbsys.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include  "mergeserver/ob_ms_schema_manager.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "mergeserver/ob_ms_rpc_stub.h"
#include "mergeserver/ob_ms_scan_event.h"
#include "mergeserver/ob_ms_async_rpc.h"


#include "ob_fake_table.h"

#include "sql/ob_rpc_scan.h"
#include "sql/ob_item_type.h"
#include "common/ob_row.h"
using namespace std;
using namespace oceanbase::common;
using namespace tbsys;
using namespace oceanbase::mergeserver;
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace testing;

class TargetClass
{
  public:
  TargetClass(){
    TBSYS_LOG(INFO, "construct targetClass");
  }

  ~TargetClass(){
    TBSYS_LOG(INFO, "destruct targetClass");
  }

  virtual int sendPacket(const char *data, char *result_buf, int result_buf_len)
  {
    // very complex implementation here
    // for example:
    // 1. connect to network
    // 2. send request to mock server
    // 3. wait for response
    // 4. return value
    TBSYS_LOG(WARN, "input data=[%s]", data);
    memset(result_buf, 0, result_buf_len);
    return OB_SUCCESS;
  }
};



class MockTargetClass: public TargetClass 
{
  public:
    MOCK_METHOD3(sendPacket,
             int(const char *data, char *result_buf, int result_buf_len));
};


int callback_func(Unused, char *result_buf, int result_buf_len)
{
  TBSYS_LOG(WARN, "callback invoiked~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  strcpy(result_buf, (char*)"biubiu");
  return OB_SUCCESS;
}


TEST(ABC, abc)
{
  TBSYS_LOG(WARN, "RUN ABC");
  MockTargetClass mock;
  char buf[10];

  EXPECT_CALL(mock, sendPacket(_,_,_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke(callback_func));

  //mock.sendPacket((const char*)"hello", buf, 10);
  TBSYS_LOG(INFO, "return buf=[%s]", buf);
}


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

