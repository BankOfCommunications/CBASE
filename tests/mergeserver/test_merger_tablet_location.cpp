
#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/location/ob_tablet_location_cache.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
static CharArena allocator_;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestTabletLocation: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

const static int64_t timeout = 1000 * 1000 * 1000L;

TEST_F(TestTabletLocation, test_init)
{
  ObTabletLocationCache cache;
  int ret = cache.init(1000, 100, timeout);
  EXPECT_TRUE(ret == OB_SUCCESS);
}


TEST_F(TestTabletLocation, test_size)
{
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
  const uint64_t END_ROW = 79L;
  // set
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 256), static_cast<int32_t>(1024 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);

    ObNewRange range;
    range.table_id_ = 1;
    range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
    range.end_key_ = TestRowkeyHelper(end_key, &allocator_);

    ret = cache.set(range, location);
    EXPECT_TRUE(OB_SUCCESS == ret);
    EXPECT_TRUE(cache.size() == ++count);
  }
  ret = cache.clear();
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(cache.size() == 0);

  // not find items
  for (uint64_t i = START_ROW; i < END_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey key = TestRowkeyHelper(start_key, &allocator_);
    ObTabletLocationList location;
    ret = cache.get(1, key, location);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }

  // set again
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 256), static_cast<int32_t>(1024 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);

    ObNewRange range;
    range.table_id_ = 1;
    range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
    range.end_key_ = TestRowkeyHelper(end_key, &allocator_);

    ret = cache.set(range, location);
    EXPECT_TRUE(OB_SUCCESS == ret);
  }

  // find in cache
  for (uint64_t i = START_ROW; i < END_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(key_str, &allocator_);
    ObTabletLocationList location;
    ret = cache.get(1, start_key, location);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }

  EXPECT_TRUE(cache.size() == count);
  ret = cache.clear();
  EXPECT_TRUE(ret == OB_SUCCESS);
  EXPECT_TRUE(cache.size() == 0);
  // not find
  for (uint64_t i = START_ROW; i < END_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(key_str, &allocator_);
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
  const uint64_t END_ROW = 79L;
  // set
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 256), static_cast<int32_t>(1024 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);

    ObNewRange range;
    range.table_id_ = 1;
    range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
    range.end_key_ = TestRowkeyHelper(end_key, &allocator_);

    ret = cache.set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);

    // get location
    ret = cache.get(1, range.end_key_, temp_location);
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
  for (uint64_t i = START_ROW; i < END_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);

    ret = cache.get(1, start_key, temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    EXPECT_TRUE(temp_location.get_timestamp() < tbsys::CTimeUtil::getTime());
    //EXPECT_TRUE(temp_location.begin()->tablet_id_ == (i/10 * 10));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_port() == (int32_t)(i/10 * 10 + 1024));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_ipv4() == (int32_t)(i/10 * 10 + 256));
    printf("row_key[%s], host[%u]\n", temp, temp_location[0].server_.chunkserver_.get_ipv4());
  }

  // update
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 255), static_cast<int32_t>(1023 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);
    location.set_timestamp(i);
    ret = cache.update(1, start_key, location);
    EXPECT_TRUE(ret == OB_SUCCESS);

    // update not exist
    snprintf(temp, 100, "wor_%ld", i + 10);
    start_key_str.assign(temp, static_cast<int32_t>(strlen(temp)));
    start_key = TestRowkeyHelper(start_key_str, &allocator_);

    ret = cache.update(1, start_key, location);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }


  for (uint64_t i = START_ROW; i < END_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);
    ret = cache.get(1, start_key, temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    //EXPECT_TRUE(temp_location.begin()->tablet_id_ == (i/10 * 10));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_port() == (int32_t)(i/10 * 10 + 1023));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_ipv4() == (int32_t)(i/10 * 10 + 255));
  }

  // del
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);

    printf("del rowkey[%s]\n", temp);
    ret = cache.del(1, start_key);
    EXPECT_TRUE(ret == OB_SUCCESS);

    --count;
    EXPECT_TRUE(cache.size() == count);

    ret = cache.del(2, start_key);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }

  // after delete
  for (uint64_t i = START_ROW; i < END_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);
    //printf("get rowkey[%s]\n", temp);
    ret = cache.get(1, start_key, temp_location);
    EXPECT_TRUE(ret != OB_SUCCESS);
  }
}


TEST_F(TestTabletLocation, test_split)
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
  const uint64_t END_ROW = 85L;
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 256), static_cast<int32_t>(1024 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);

    ObNewRange range;
    range.table_id_ = 1;
    range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
    range.end_key_ = TestRowkeyHelper(end_key, &allocator_);

    ret = cache.set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);

    // get location
    ret = cache.get(1, range.end_key_, temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    EXPECT_TRUE(temp_location.get_timestamp() < tbsys::CTimeUtil::getTime());

    // small data
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_port() == (int32_t)(i + 1024));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_ipv4() == (int32_t)(i + 256));

    // overwrite
    ret = cache.set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);

    // cache item count
    ++count;
    EXPECT_TRUE(cache.size() == count);
  }
  //
  EXPECT_TRUE(cache.size() == count);

  // split
  count = 0;
  for (uint64_t i = START_ROW; i < END_ROW; i += 5)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 255), static_cast<int32_t>(1023 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 5);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);
    ObNewRange range;
    range.table_id_ = 1;
    range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
    range.end_key_ = TestRowkeyHelper(end_key, &allocator_);
    location.set_timestamp(i);
    ret = cache.set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    snprintf(temp, 100, "row_%ld", i+1);
    ret = cache.get(1, make_rowkey(temp, &allocator_), temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    ++count;
  }

  // just overwrite all
  for (uint64_t i = START_ROW; i < END_ROW; ++i)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);
    ret = cache.get(1, start_key, temp_location);
    EXPECT_TRUE(ret == OB_SUCCESS);
    printf("expect[%ld:%ld], real[%d:%d]\n", (i/5 * 5 + 255), (i/5 * 5 + 1023),
        temp_location[0].server_.chunkserver_.get_ipv4(), temp_location[0].server_.chunkserver_.get_port());
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_port() == (int32_t)(i/5 * 5 + 1023));
    EXPECT_TRUE(temp_location[0].server_.chunkserver_.get_ipv4() == (int32_t)(i/5 * 5 + 255));
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
  ObString search_key_str(100, static_cast<int32_t>(strlen(tempkey)), tempkey);
  ObRowkey search_key = TestRowkeyHelper(search_key_str, &allocator_);
  //ObString search_key(100, strlen(temp), temp);

  snprintf(temp, 100, "rowkey_rowkey_11_rowkey_rowkey");
  snprintf(temp_end, 100, "rowkey_rowkey_15_rowkey_rowkey");
  ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
  ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);

  ObNewRange range;
  range.table_id_ = 1;
  range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
  range.end_key_ = TestRowkeyHelper(end_key, &allocator_);
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
static const uint64_t END = 90000L;

void * del_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);

  char temp[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t END_ROW = END;
  ObTabletLocationList temp_location;
  // get
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);
    cache->del(1, start_key);
  }
  return NULL;
}

void * get_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);

  char temp[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t END_ROW = END;
  ObTabletLocationList temp_location;
  // get
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);
    cache->get(1, start_key, temp_location);
  }
  return NULL;
}

void * set_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);

  char temp[100];
  char temp_end[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t END_ROW = END;
  // set
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 256), static_cast<int32_t>(1024 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 10);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);

    ObNewRange range;
    range.table_id_ = 1;
    range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
    range.end_key_ = TestRowkeyHelper(end_key, &allocator_);

    int ret = cache->set(range, location);
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  return NULL;
}

void * update_routine(void * argv)
{
  ObTabletLocationCache * cache = (ObTabletLocationCache *) argv;
  EXPECT_TRUE(cache != NULL);
  char temp[100];
  const uint64_t START_ROW = START + (int64_t(pthread_self())%10 * 100);
  const uint64_t END_ROW = END;
  // update
  for (uint64_t i = START_ROW; i < END_ROW; i += 10)
  {
    ObServer server;
    server.set_ipv4_addr(static_cast<int32_t>(i + 255), static_cast<int32_t>(1023 + i));
    ObTabletLocation addr(i, server);
    ObTabletLocationList location;
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));
    EXPECT_TRUE(OB_SUCCESS == location.add(addr));

    snprintf(temp, 100, "row_%ld", i+1);
    ObString start_key_str(100, static_cast<int32_t>(strlen(temp)), temp);
    ObRowkey start_key = TestRowkeyHelper(start_key_str, &allocator_);
    location.set_timestamp(i);

    cache->update(1, start_key, location);
  }
  return NULL;
}

#if false
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

