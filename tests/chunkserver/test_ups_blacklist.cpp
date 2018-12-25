#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_ups_info.h"
#include "common/ob_malloc.h"
#include "ob_ups_blacklist.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestBlackList: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

static const int MAX_COUNT = 5;

TEST_F(TestBlackList, init)
{
  ObUpsBlackList list;
  ObUpsList ups_list;
  EXPECT_TRUE(OB_SUCCESS != list.init(10, 10000, MERGE_SERVER, ups_list));
  ObUpsInfo info;
  ObServer server;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    info.addr_ = server;
    info.ms_read_percentage_ = 10;
    ups_list.ups_count_ = i + 1;
    ups_list.ups_array_[i] = info;
  }
  EXPECT_TRUE(OB_SUCCESS == list.init(10, 10000, MERGE_SERVER, ups_list));
}

TEST_F(TestBlackList, fail_check)
{
  ObUpsBlackList list;
  ObUpsList ups_list;
  EXPECT_TRUE(OB_SUCCESS != list.init(10, 10000, MERGE_SERVER, ups_list));

  ObServer server;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    EXPECT_TRUE(true == list.check(i));
    list.fail(i, server);
  }
  
  ObUpsInfo info;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    info.addr_ = server;
    info.ms_read_percentage_ = 10;
    ups_list.ups_count_ = i + 1;
    ups_list.ups_array_[i] = info;
  }
  EXPECT_TRUE(OB_SUCCESS == list.init(10, 10000, MERGE_SERVER, ups_list));
  
  for (int i = MAX_COUNT; i < MAX_COUNT + 10; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    EXPECT_TRUE(true == list.check(i));
    for (int j = 0; j < 10; ++j)
    {
      list.fail(i - MAX_COUNT, server);
    }
    EXPECT_TRUE(list.get_valid_count() ==  MAX_COUNT);
  }
  
  EXPECT_TRUE(list.get_valid_count() ==  MAX_COUNT);

  for (int i = 0; i < MAX_COUNT; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    EXPECT_TRUE(true == list.check(i));
    for (int j = 0; j < 10; ++j)
    {
      EXPECT_TRUE(true == list.check(i));
      EXPECT_TRUE(list.get_valid_count() == (MAX_COUNT - i));
      list.fail(i, server);
    }
    list.fail(i, server);
    EXPECT_TRUE(list.get_valid_count() == (MAX_COUNT - i - 1));
    EXPECT_TRUE(false == list.check(i));
  }
  EXPECT_TRUE(list.get_valid_count() == 0);
  list.reset();
  EXPECT_TRUE(list.get_valid_count() == MAX_COUNT);
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    EXPECT_TRUE(true == list.check(i));
  }

  for (int i = 0; i < MAX_COUNT; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    EXPECT_TRUE(true == list.check(i));
    for (int j = 0; j < 10; ++j)
    {
      EXPECT_TRUE(true == list.check(i));
      EXPECT_TRUE(list.get_valid_count() == (MAX_COUNT - i));
      list.fail(i, server);
    }
    list.fail(i, server);
    EXPECT_TRUE(list.get_valid_count() == (MAX_COUNT - i - 1));
    EXPECT_TRUE(false == list.check(i));
  }

  // timeout to alive again
  usleep(10000);
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    EXPECT_TRUE(true == list.check(i));
    EXPECT_TRUE(list.get_valid_count() == MAX_COUNT);
  }

  usleep(10000);
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    server.set_ipv4_addr(i+1024, i+2048);
    EXPECT_TRUE(true == list.check(i));
    EXPECT_TRUE(list.get_valid_count() == MAX_COUNT);
  }
}



