#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "tablet_location.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::tools;

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

TEST_F(TestTabletLocation, test_add)
{
  ObTabletLocation server;
  TabletLocation list;
  EXPECT_TRUE(0 == list.size());
  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    server.tablet_version_ = i;
    EXPECT_TRUE(OB_SUCCESS == list.add(server));
    EXPECT_TRUE(i + 1 == list.size());
  }
  EXPECT_TRUE(OB_SUCCESS != list.add(server));
  EXPECT_TRUE(OB_SUCCESS != list.add(server));
  EXPECT_TRUE(OB_SUCCESS != list.add(server));
  EXPECT_TRUE(TabletLocation::MAX_REPLICA_COUNT == list.size());
}


TEST_F(TestTabletLocation, test_del)
{
  ObTabletLocation server;
  TabletLocation list;
  EXPECT_TRUE(OB_SUCCESS != list.del(0, server));
  EXPECT_TRUE(OB_SUCCESS != list.del(1, server));
  EXPECT_TRUE(OB_SUCCESS != list.del(2, server));
  EXPECT_TRUE(OB_SUCCESS != list.del(3, server));
  ObTabletLocation temp_server;
  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    temp_server.tablet_version_ = i;
    EXPECT_TRUE(OB_SUCCESS == list.add(temp_server));
    EXPECT_TRUE(i + 1 == list.size());
  }

  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == list.del(0, server));
    EXPECT_TRUE(i == server.tablet_version_);
    EXPECT_TRUE(TabletLocation::MAX_REPLICA_COUNT - i - 1 == list.size());
  }
  
  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    temp_server.tablet_version_ = i;
    EXPECT_TRUE(OB_SUCCESS == list.add(temp_server));
    EXPECT_TRUE(i + 1 == list.size());
  }

  for (int64_t i = TabletLocation::MAX_REPLICA_COUNT - 1; i >= 0; --i)
  {
    EXPECT_TRUE(OB_SUCCESS == list.del(0, server));
    EXPECT_TRUE(TabletLocation::MAX_REPLICA_COUNT - i - 1 == server.tablet_version_);
    EXPECT_TRUE(i == list.size());
  }

  EXPECT_TRUE(0 == list.size());
  
  for (int64_t i = TabletLocation::MAX_REPLICA_COUNT - 1; i >= 0; --i)
  {
    EXPECT_TRUE(OB_SUCCESS != list.del(0, server));
  }

  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    temp_server.tablet_version_ = i;
    EXPECT_TRUE(OB_SUCCESS == list.add(temp_server));
    EXPECT_TRUE(i + 1 == list.size());
  }
  
  EXPECT_TRUE(OB_SUCCESS == list.del(1, server));
  EXPECT_TRUE(1 == server.tablet_version_);
  EXPECT_TRUE(TabletLocation::MAX_REPLICA_COUNT - 1 == list.size());
}

TEST_F(TestTabletLocation, test_serialize)
{
  ObTabletLocation server;
  TabletLocation list;
  ObTabletLocation temp_server;
  ObServer chunkserver;
  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    chunkserver.set_ipv4_addr(i + 300000, i + 1024);
    temp_server.tablet_version_ = i;
    temp_server.chunkserver_ = chunkserver;
    EXPECT_TRUE(OB_SUCCESS == list.add(temp_server));
    EXPECT_TRUE(i + 1 == list.size());
  }
  
  list.print_info();
  int64_t size = list.get_serialize_size();
  EXPECT_TRUE(size != 0);
  
  char * temp = new char[size];
  EXPECT_TRUE(NULL != temp);
  int64_t pos = 0;
  EXPECT_TRUE(OB_SUCCESS != list.serialize(temp, size - 1 , pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == list.serialize(temp, size, pos));
  EXPECT_TRUE(pos == size);
  
  TabletLocation list2;
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == list2.deserialize(temp, size, pos));
  list2.print_info();
  list.print_info();
}

TEST_F(TestTabletLocation, test_sort)
{
  TabletLocation list;
  ObServer root_server;
  const char * addr = "localhost";
  root_server.set_ipv4_addr(addr, 8888);
  int32_t server_ip = root_server.get_ipv4();
  
  // one server
  ObTabletLocation location;
  srand(100);
  ObServer temp;
  temp.set_ipv4_addr(rand() % 10245, 1023);
  //location.tablet_id_ = 1;
  location.chunkserver_ = temp;
  list.add(location);
  EXPECT_TRUE(OB_SUCCESS == list.sort(root_server));
  
  int64_t count = list.size();
  for (int64_t i = 0; i < count - 1; ++i)
  {
    EXPECT_TRUE(abs(list[i].chunkserver_.get_ipv4() - server_ip) 
        <= abs(list[i+1].chunkserver_.get_ipv4() - server_ip));
  } 

  // two server
  temp.set_ipv4_addr(rand() % 10245, 1024);
  location.chunkserver_ = temp;
  list.add(location);
  EXPECT_TRUE(OB_SUCCESS == list.sort(root_server));
  
  count = list.size();
  for (int64_t i = 0; i < count - 1; ++i)
  {
    EXPECT_TRUE(abs(list[i].chunkserver_.get_ipv4() - server_ip) 
        <= abs(list[i+1].chunkserver_.get_ipv4() - server_ip));
  } 
  
  // three server
  location.chunkserver_ = root_server;
  list.add(location);
  EXPECT_TRUE(OB_SUCCESS == list.sort(root_server));
  
  count = list.size();
  for (int64_t i = 0; i < count - 1; ++i)
  {
    EXPECT_TRUE(abs(list[i].chunkserver_.get_ipv4() - server_ip) 
        <= abs(list[i+1].chunkserver_.get_ipv4() - server_ip));
  } 
}



