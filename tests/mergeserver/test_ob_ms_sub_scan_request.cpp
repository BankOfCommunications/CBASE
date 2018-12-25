
#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_sub_scan_request.h"
#include "ob_chunk_server.h"
#include "ob_ms_async_rpc.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

const static int64_t timeout = 1000 * 1000 * 1000L;

TEST(TestSubScanRequest, test_init)
{
  int ret = OB_SUCCESS;
  ObMergerSubScanRequest  request;
  
  ObScanParam scan_param;
  ObRange query_range;
  bool scan_full_tablet = false;
  ObChunkServer cs_replicas[10];
  int replica_count = 0;
  ObStringBuf string_buffer;

  replica_count = ObTabletLocationList::MAX_REPLICA_COUNT - 1;
  
  ret = request.init(&scan_param, 
      query_range, 
      reinterpret_cast<ObChunkServer*>(cs_replicas), 
      replica_count,
      scan_full_tablet, 
      &string_buffer);

  EXPECT_TRUE(ret == OB_SUCCESS);

  request.reset();

  replica_count = ObTabletLocationList::MAX_REPLICA_COUNT + 1;  
  ret = request.init(&scan_param, 
      query_range, 
      reinterpret_cast<ObChunkServer*>(cs_replicas), 
      replica_count,
      scan_full_tablet,
      &string_buffer); 
  EXPECT_TRUE(ret == OB_INVALID_ARGUMENT);
}


int create(ObMergerRpcEvent **rpc_event)
{
  uint64_t client_id = 2;
  //common::ThreadSpecificBuffer * rpc_buffer;
  //ObMergerAsyncRpcStub * async_rpc;
  //common::ObClientManager * rpc_frame;
  //common::ThreadSpecificBuffer thread_buffer(1024*1024*10);
  //common::ObClientManager cm;


  //ObMergerRequestEvent *request = new ObMergerRequestEvent(async_rpc);
  *rpc_event = new ObMergerRpcEvent();
  return OB_SUCCESS;
}



TEST(TestSubScanRequest, test_add_event)
{
   int ret = OB_SUCCESS;
  ObMergerSubScanRequest  request;
  
  ObScanParam scan_param;
  ObRange query_range;
  bool scan_full_tablet = false;
  ObChunkServer cs_replicas[10];
  int replica_count = 0;
  ObStringBuf string_buffer;

  replica_count = ObTabletLocationList::MAX_REPLICA_COUNT - 1;
  
  ret = request.init(&scan_param, 
      query_range, 
      reinterpret_cast<ObChunkServer*>(cs_replicas), 
      replica_count,
      scan_full_tablet, 
      &string_buffer);

  EXPECT_TRUE(ret == OB_SUCCESS);

 

  ObMergerRpcEvent *agent_event = NULL;
  EXPECT_TRUE(OB_SUCCESS == create(&agent_event));

  //add_event(agent_event, );

  bool belong_to_this = false;
  ret = request.agent_event_finish(agent_event, belong_to_this);
  EXPECT_TRUE(ret != OB_SUCCESS);
  EXPECT_TRUE(belong_to_this == false);   /// rpc_event2已经收到过，重复包无效
}


#if 0



TEST(TestSubScanRequest, test_create_rpc_event_duplicated_case)
{
  int ret = OB_SUCCESS;
  ObMergerSubScanRequest  scan_request;

  ObRange query_range;
  bool scan_full_tablet = false;
  ObServer cs_replicas[10];
  int replica_count = 0;

  ObMergerRpcEvent *agent_event = NULL; 
  bool belong_to_this = false;

  replica_count = ObTabletLocationList::MAX_REPLICA_COUNT - 1;
  ret = scan_request.init(query_range, scan_full_tablet, cs_replicas, replica_count);
  EXPECT_TRUE(ret == OB_SUCCESS);
  
  /* request event的作用在于提供一个id，用于请求消息过滤.测试中默认为0 */
  ObMergerRequestEvent request_event;
  /* dispatcher用于做cs选择，测试中默认选择0号cs */
  ObChunkServerTaskDispatcher dispatcher;
  
  ObServer selected_server;
  ObMergerRpcEvent * rpc_event1 = NULL;
  ObMergerRpcEvent * rpc_event2 = NULL;

  rpc_event1 = scan_request.create_rpc_event(&request_event, dispatcher, selected_server);
  EXPECT_TRUE(rpc_event1 != NULL);
  rpc_event2 = scan_request.create_rpc_event(&request_event, dispatcher, selected_server);
  EXPECT_TRUE(rpc_event2 != NULL);

  /* 对selected_server发请求，请求内容在外部，返回的结果放入到rpc_event1中 */
  agent_event = rpc_event1;
  /* 检查结果 */
  ret = scan_request.agent_event_finish(agent_event, belong_to_this);
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(agent_event == rpc_event1 && belong_to_this == true);

  /* 模拟收到已完成的sub task备份包 */
  agent_event = rpc_event2;
  /* 检查结果 */
  ret = scan_request.agent_event_finish(agent_event, belong_to_this);
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(agent_event == NULL && belong_to_this == true);


  /* 模拟收到已完成的sub task备份包 */
  agent_event = rpc_event2;
  /* 检查结果 */
  ret = scan_request.agent_event_finish(agent_event, belong_to_this);
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(belong_to_this == false);   /// rpc_event2已经收到过，重复包无效
} 




#if 0
  ObTabletLocationCache cache;
  EXPECT_TRUE(cache.size() == 0);
  int ret = cache.clear();
  EXPECT_TRUE(ret != OB_SUCCESS);

  ret = cache.init(1000, 100, timeout);
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(cache.size() == 0);
  ret = cache.clear();
  EXPECT_TRUE(ret == OB_SUCCESS);
  
  char temp[100];
  char temp_end[100];
  uint64_t count = 0;
  // not more than 10 bit rows because of the string key
  const uint64_t START_ROW = 10L;
  const uint64_t MAX_ROW = 79L;
  // set
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(i + 256, 1024 + i);
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, strlen(temp), temp);
    ObString end_key(100, strlen(temp_end), temp_end);

    ObRange range;
    range.table_id_ = 1;
    range.start_key_ = start_key;
    range.end_key_ = end_key;

    ret = cache.set(range, location);
    EXPECT_TRUE(OB_SUCCESS == ret);
    EXPECT_TRUE(cache.size() == ++count);
  }
  ret = cache.clear();
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(cache.size() == 0);
  
  // not find items
  for (uint64_t i = START_ROW; i < MAX_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    ObTabletLocationList location;
    ret = cache.get(1, start_key, location);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }
  
  // set again
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(i + 256, 1024 + i);
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, strlen(temp), temp);
    ObString end_key(100, strlen(temp_end), temp_end);

    ObRange range;
    range.table_id_ = 1;
    range.start_key_ = start_key;
    range.end_key_ = end_key;

    ret = cache.set(range, location);
    EXPECT_TRUE(OB_SUCCESS == ret);
  }
  
  // find in cache
  for (uint64_t i = START_ROW; i < MAX_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    ObTabletLocationList location;
    ret = cache.get(1, start_key, location);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  
  EXPECT_TRUE(cache.size() == count);
  ret = cache.clear();
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(cache.size() == 0);
  // not find
  for (uint64_t i = START_ROW; i < MAX_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    ObTabletLocationList location;
    ret = cache.get(1, start_key, location);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }
}


TEST_F(TestTabletLocation, test_set)
{
  ObTabletLocationCache cache;
  int ret = cache.init(50000 * 5, 1000, 10000);
  EXPECT_TRUE(ret == OB_SUCCESS);

  char temp[100];
  char temp_end[100];
  uint64_t count = 0;
  ObTabletLocationList temp_location;
  // warning
  // not more than 10 bit rows because of the string key
  const uint64_t START_ROW = 10L;
  const uint64_t MAX_ROW = 79L;
  // set
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(i + 256, 1024 + i);
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, strlen(temp), temp);
    ObString end_key(100, strlen(temp_end), temp_end);

    ObRange range;
    range.table_id_ = 1;
    range.start_key_ = start_key;
    range.end_key_ = end_key;

    ret = cache.set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);

    // get location 
    ret = cache.get(1, end_key, temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    EXPECT_TRUE(temp_location.get_timestamp() < tbsys::CTimeUtil::getTime());
    //printf("tablet_id[%lu]\n", temp_location.begin()->tablet_id_);
    
    // small data
    //EXPECT_TRUE(temp_location.begin()->tablet_id_ == i);
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_port() == (int32_t)(i + 1024));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_ipv4() == (int32_t)(i + 256));
    printf("row_key[%s], host[%u]\n", temp, temp_location[0].server_.chunkserver_.get_ipv4());
    
    // overwrite
    ret = cache.set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);

    // cache item count
    ++count;
    EXPECT_TRUE(cache.size() == count);
  }
  //
  EXPECT_TRUE(cache.size() == count);
  
  // get 
  for (uint64_t i = START_ROW; i < MAX_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    
    ret = cache.get(1, start_key, temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    EXPECT_TRUE(temp_location.get_timestamp() < tbsys::CTimeUtil::getTime());
    //EXPECT_TRUE(temp_location.begin()->tablet_id_ == (i/10 * 10));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_port() == (int32_t)(i/10 * 10 + 1024));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_ipv4() == (int32_t)(i/10 * 10 + 256));
    printf("row_key[%s], host[%u]\n", temp, temp_location[0].server_.chunkserver_.get_ipv4());
  }
  
  // update
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(i + 255, 1023 + i);
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    location.set_timestamp(i);
    ret = cache.update(1, start_key, location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    
    // update not exist
    snprintf(temp, 100, "wor_%ld", i + 10);
    start_key.assign(temp, strlen(temp));

    ret = cache.update(1, start_key, location);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }


  for (uint64_t i = START_ROW; i < MAX_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    ret = cache.get(1, start_key, temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    //EXPECT_TRUE(temp_location.begin()->tablet_id_ == (i/10 * 10));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_port() == (int32_t)(i/10 * 10 + 1023));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_ipv4() == (int32_t)(i/10 * 10 + 255));
    //printf("timestamp[%ld], row_key[%s], host[%u]\n", temp_location.get_timestamp(), 
    //    temp, temp_location[0].server_.chunkserver_.get_ipv4());
  }

  // del
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    
    printf("del rowkey[%s]\n", temp);
    ret = cache.del(1, start_key);
    EXPECT_TRUE(ret == OB_SUCCESS);
    
    --count;
    EXPECT_TRUE(cache.size() == count);
    
    ret = cache.del(2, start_key);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }
  
  // after delete
  for (uint64_t i = START_ROW; i < MAX_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    //printf("get rowkey[%s]\n", temp);
    ret = cache.get(1, start_key, temp_location);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }
}

TEST_F(TestTabletLocation, test_pressure)
{
  ObTabletLocationCache cache;
  int ret = cache.init(50000 * 5, 1000, 10000);
  EXPECT_TRUE(ret == OB_SUCCESS);

  char temp[100];
  char tempkey[100];
  char temp_end[100];
  snprintf(tempkey, 100, "rowkey_rowkey_12_rowkey_rowkey");
  ObString search_key(100, strlen(tempkey), tempkey);
  //ObString search_key(100, strlen(temp), temp);

  snprintf(temp, 100, "rowkey_rowkey_11_rowkey_rowkey");
  snprintf(temp_end, 100, "rowkey_rowkey_15_rowkey_rowkey");
  ObString start_key(100, strlen(temp), temp);
  ObString end_key(100, strlen(temp_end), temp_end);

  ObRange range;
  range.table_id_ = 1;
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  ObTabletLocationList location;

  // for debug cache memory leak
  int64_t count = 0;//1000000;
  while (count < 100)
  {
    ret = cache.set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    ret = cache.del(1, search_key);
    EXPECT_TRUE(ret == OB_SUCCESS);
    ++count;
  }
}

static const uint64_t START = 10000L;
static const uint64_t MAX = 90000L;

void * del_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObMergerTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);

  char temp[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t MAX_ROW = MAX;
  ObTabletLocationList temp_location;
  // get 
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    cache->del(1, start_key);
  }
  return NULL;
}

void * get_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObMergerTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);

  char temp[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t MAX_ROW = MAX;
  ObTabletLocationList temp_location;
  // get 
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    cache->get(1, start_key, temp_location);
  }
  return NULL;
}


void * set_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObMergerTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);

  char temp[100];
  char temp_end[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t MAX_ROW = MAX;
  // set
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(i + 256, 1024 + i);
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, strlen(temp), temp);
    ObString end_key(100, strlen(temp_end), temp_end);

    ObRange range;
    range.table_id_ = 1;
    range.start_key_ = start_key;
    range.end_key_ = end_key;

    int ret = cache->set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  return NULL;
}

void * update_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObMergerTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);
  char temp[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t MAX_ROW = MAX;
  // update
  for (uint64_t i = START_ROW; i < MAX_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(i + 255, 1023 + i);
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, strlen(temp), temp);
    location.set_timestamp(i);

    cache->update(1, start_key, location);
  }
  return NULL;
}

#if true 
TEST_F(TestTabletLocation, test_thread)
{
  ObTabletLocationCache cache;
  int ret = cache.init(50000 * 5, 1000, 10000);
  EXPECT_TRUE(ret == OB_SUCCESS);
  
  static const int THREAD_COUNT = 30;
  pthread_t threads[THREAD_COUNT][4];
  
  for (int i = 0; i < THREAD_COUNT; ++i)
  {
    ret = pthread_create(&threads[i][0], NULL, set_routine, &cache);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }

  for (int i = 0; i < THREAD_COUNT; ++i)
  {
    ret = pthread_create(&threads[i][1], NULL, get_routine, &cache);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }

  for (int i = 0; i < THREAD_COUNT; ++i)
  {
    ret = pthread_create(&threads[i][2], NULL, update_routine, &cache);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  
  for (int i = 0; i < THREAD_COUNT; ++i)
  {
    ret = pthread_create(&threads[i][3], NULL, del_routine, &cache);
    EXPECT_TRUE(ret == OB_SUCCESS);
  } 

  for (int i = 0; i < 4; ++i)
  {
    for (int j = 0; j < THREAD_COUNT; ++j)
    {
      ret = pthread_join(threads[j][i], NULL);
    }
  }
}

#endif
#endif
#endif  /* end of xiaochu */
