#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "task_info.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestTaskInfo: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestTaskInfo, test_param)
{
  TaskInfo task;
  task.set_token(1234);
  task.set_timestamp(2345);
  task.set_limit(123);
  task.set_id(1);
  task.set_index(3);
  char row[] = "FFFFFFFF";
  ObString row_key;
  row_key.assign(row, strlen(row));

  ObScanParam param;
  ObString table_name;
  char name[] = "test";
  table_name.assign(name, strlen(name));
  
  ObRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.unset_max_value();
  range.end_key_ = row_key;
  param.set(OB_INVALID_ID, table_name, range);
  EXPECT_TRUE(task.set_param(param) == OB_SUCCESS);
  
  TabletLocation list;
  ObServer chunkserver;
  ObTabletLocation temp_server;
  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    chunkserver.set_ipv4_addr(i + 300000, i + 1024);
    temp_server.tablet_version_ = i;
    temp_server.chunkserver_ = chunkserver;
    EXPECT_TRUE(OB_SUCCESS == list.add(temp_server));
    EXPECT_TRUE(i + 1 == list.size());
  }
  task.set_location(list);
  EXPECT_TRUE(task.set_param(param) == OB_SUCCESS);
  
  char temp_row[] = "XXXXXXXFFFFFFFF";
  row_key.assign(temp_row, strlen(temp_row));
  range.border_flag_.unset_min_value();
  range.border_flag_.unset_max_value();
  range.start_key_ = row_key;
  range.end_key_ = row_key;
  param.set(OB_INVALID_ID, table_name, range);
  EXPECT_TRUE(task.set_param(param) == OB_SUCCESS);

  TaskInfo task_temp = task;
  TaskInfo task_test = task_temp;
}

TEST_F(TestTaskInfo, test_get_set)
{
  TaskInfo task;
  int64_t token = 1023;
  task.set_token(token);
  EXPECT_TRUE(task.get_token() == token);

  uint64_t id = 1024;
  task.set_id(id);
  EXPECT_TRUE(task.get_id() == id);

  int64_t timestamp = 100;
  task.set_timestamp(timestamp);
  EXPECT_TRUE(task.get_timestamp() == timestamp);

  uint64_t limit = 203;
  task.set_limit(limit);
  EXPECT_TRUE(task.get_limit() == limit);
  
  int64_t index = 3;
  task.set_index(index);
  EXPECT_TRUE(task.get_index() == index);
  
  ObScanParam param;
  uint64_t table_id = 10;
  ObString table_name;
  char name[] = "test";
  table_name.assign(name, strlen(name));
  ObRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();

  param.set(table_id, table_name, range);
  EXPECT_TRUE(task.set_param(param) == OB_SUCCESS);
  
  TabletLocation list;
  ObServer chunkserver;
  ObTabletLocation server;
  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    chunkserver.set_ipv4_addr(i + 300000, i + 1024);
    server.tablet_version_ = i;
    server.chunkserver_ = chunkserver;
    EXPECT_TRUE(OB_SUCCESS == list.add(server));
    EXPECT_TRUE(i + 1 == list.size());
  }
  task.set_location(list);
  //EXPECT_TRUE(task.get_location() == list);
}

TEST_F(TestTaskInfo, test_serailize)
{
  TaskInfo task;
  task.set_token(1234);
  task.set_timestamp(2345);
  task.set_limit(123);
  task.set_id(1);
  task.set_index(2);
  char row[] = "FFFFFFFF";
  ObString row_key;
  row_key.assign(row, strlen(row));

  ObScanParam param;
  ObString table_name;
  char name[] = "test";
  table_name.assign(name, strlen(name));
  
  ObRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.unset_max_value();
  range.end_key_ = row_key;
  param.set(OB_INVALID_ID, table_name, range);
  EXPECT_TRUE(task.set_param(param) == OB_SUCCESS);
  
  TabletLocation list;
  ObServer chunkserver;
  ObTabletLocation temp_server;
  for (int64_t i = 0; i < TabletLocation::MAX_REPLICA_COUNT; ++i)
  {
    chunkserver.set_ipv4_addr(i + 300000, i + 1024);
    temp_server.tablet_version_ = i;
    temp_server.chunkserver_ = chunkserver;
    EXPECT_TRUE(OB_SUCCESS == list.add(temp_server));
    EXPECT_TRUE(i + 1 == list.size());
  }
  
  task.set_location(list);
  EXPECT_TRUE(task.get_location().size() == TabletLocation::MAX_REPLICA_COUNT);
  int64_t size = task.get_serialize_size();
  char * buffer = new char[size];
  EXPECT_TRUE(buffer != NULL);
  int64_t pos = 0;
  EXPECT_TRUE(task.serialize(buffer, size - 1, pos) != OB_SUCCESS);
  pos = 0;
  EXPECT_TRUE(task.serialize(buffer, size, pos) == OB_SUCCESS);
  EXPECT_TRUE(pos == size);
  
  TaskInfo task_new;
  pos = 0;
  EXPECT_TRUE(task_new.deserialize(buffer, size, pos) == OB_SUCCESS);
  EXPECT_TRUE(task_new == task);
  // scan param not overload ==
  EXPECT_TRUE(task_new.get_param().get_table_name() == table_name);
  EXPECT_TRUE(task_new.get_param().get_range()->end_key_ == range.end_key_);
  if (buffer)
  {
    delete []buffer;
    buffer = NULL;
  }
}


