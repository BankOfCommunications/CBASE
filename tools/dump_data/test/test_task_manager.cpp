#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include "task_info.h"
#include "task_manager.h"

#define MULTI_THREAD_TEST true 

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::tools;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestTaskManager: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestTaskManager, test_check_validate)
{
  int64_t task_count = 30;
  TaskManager manager;
  for (int64_t i = 0; i < task_count; ++i)
  {
    TaskInfo task;
    uint64_t limit = 203;
    task.set_limit(limit);
    ObScanParam param;
    ObString table_name;
    char name[] = "test";
    table_name.assign(name, strlen(name));
    char row[] = "FFFFFFFF";
    ObString row_key;
    row_key.assign(row, strlen(row));
    
    ObRange range;
    range.border_flag_.set_min_value();
    range.border_flag_.unset_max_value();
    range.end_key_ = row_key;
    
    param.set(OB_INVALID_ID, table_name, range);
    task.set_param(param);

    TabletLocation list;
    ObServer chunkserver;
    ObTabletLocation server;
    for (int64_t j = 0; j < TabletLocation::MAX_REPLICA_COUNT; ++j)
    {
      chunkserver.set_ipv4_addr(j + 300000 + i%10, j + 1024);
      server.tablet_version_ = j;
      server.chunkserver_ = chunkserver;
      EXPECT_TRUE(OB_SUCCESS == list.add(server));
      EXPECT_TRUE(j + 1 == list.size());
    }
    
    //task.set_location(list);
    EXPECT_TRUE(manager.insert_task(list, task) == OB_SUCCESS);
    EXPECT_TRUE(task.get_limit() == limit);
    static const int64_t token = task.get_token();
    EXPECT_TRUE(task.get_token() == token);
    EXPECT_TRUE(task.get_id() == (uint64_t)(i + 1));
    //EXPECT_TRUE(task.get_location() == list);
  }
  // rowkey error
  EXPECT_TRUE(manager.check_tasks(task_count) != true);
}

TEST_F(TestTaskManager, test_insert_task)
{
  int64_t task_count = 5000;
  TaskManager manager;
  for (int64_t i = 0; i < task_count; ++i)
  {
    TaskInfo task;
    uint64_t limit = 203;
    task.set_limit(limit);
    ObScanParam param;
    ObString table_name;
    char name[] = "test";
    table_name.assign(name, strlen(name));
    char row[] = "FFFFFFFF";
    ObString row_key;
    row_key.assign(row, strlen(row));
    
    ObRange range;
    range.border_flag_.set_min_value();
    range.border_flag_.unset_max_value();
    range.end_key_ = row_key;
    
    param.set(OB_INVALID_ID, table_name, range);
    task.set_param(param);

    TabletLocation list;
    ObServer chunkserver;
    ObTabletLocation server;
    for (int64_t j = 0; j < TabletLocation::MAX_REPLICA_COUNT; ++j)
    {
      chunkserver.set_ipv4_addr(j + 300000 + i%10, j + 1024);
      server.tablet_version_ = j;
      server.chunkserver_ = chunkserver;
      EXPECT_TRUE(OB_SUCCESS == list.add(server));
      EXPECT_TRUE(j + 1 == list.size());
    }
    
    //task.set_location(list);
    EXPECT_TRUE(manager.insert_task(list, task) == OB_SUCCESS);
    EXPECT_TRUE(task.get_limit() == limit);
    static const int64_t token = task.get_token();
    EXPECT_TRUE(task.get_token() == token);
    EXPECT_TRUE(task.get_id() == (uint64_t)(i + 1));
    //EXPECT_TRUE(task.get_location() == list);
  }
  manager.print_info();
}

TEST_F(TestTaskManager, test_fetch_task)
{
  TaskManager manager;
  TaskInfo task_info;
  TaskCounter counter;
  // no task for fetch
  EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
  int64_t task_count = 100;
  for (int64_t i = 0; i < task_count; ++i)
  {
    TaskInfo task;
    uint64_t limit = 203;
    task.set_limit(limit);
    ObScanParam param;
    ObString table_name;
    char name[] = "test";
    table_name.assign(name, strlen(name));
    char row[] = "FFFFFFFF";
    ObString row_key;
    row_key.assign(row, strlen(row));
    
    ObRange range;
    range.border_flag_.set_min_value();
    range.border_flag_.unset_max_value();
    range.end_key_ = row_key;
    
    param.set(OB_INVALID_ID, table_name, range);
    task.set_param(param);

    TabletLocation list;
    ObServer chunkserver;
    ObTabletLocation server;
    for (int64_t j = 0; j < TabletLocation::MAX_REPLICA_COUNT; ++j)
    {
      chunkserver.set_ipv4_addr(j + 300000 + i, j + 1024);
      server.tablet_version_ = j;
      server.chunkserver_ = chunkserver;
      EXPECT_TRUE(OB_SUCCESS == list.add(server));
      EXPECT_TRUE(j + 1 == list.size());
    }
    //task.set_location(list);
    EXPECT_TRUE(manager.insert_task(list, task) == OB_SUCCESS);
    EXPECT_TRUE(task.get_limit() == limit);
    static const int64_t token = task.get_token();
    EXPECT_TRUE(task.get_token() == token);
    EXPECT_TRUE(task.get_id() == uint64_t(i + 1));
    //EXPECT_TRUE(task.get_location() == list);
  }
  manager.print_info();
  
  for (int64_t i = 0; i < task_count; ++i)
  {
    EXPECT_TRUE(manager.fetch_task(counter, task_info) == OB_SUCCESS);
    EXPECT_TRUE(task_info.get_id() == uint64_t(i + 1));
  }
  
  manager.print_info();

  // no more task
  EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
  EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
  EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);

  // sleep for timeout
  sleep(3 * TaskManager::MAX_WAIT_TIME/(1000 * 1000));
  manager.print_info();
  
  for (int64_t i = 0; i < task_count; ++i)
  {
    EXPECT_TRUE(manager.fetch_task(counter, task_info) == OB_SUCCESS);
    if (task_info.get_id() != uint64_t(i+1))
    {
      EXPECT_TRUE(task_info.get_id() == uint64_t(i + 1));
    }
    EXPECT_TRUE(task_info.get_id() == uint64_t(i + 1));
  }
  manager.print_info();

  // insert new task for same server aaaa
  for (int64_t i = 0; i < TaskManager::DEFAULT_COUNT + 10; ++i)
  {
    TaskInfo task;
    uint64_t limit = 203;
    task.set_limit(limit);
    ObScanParam param;
    ObString table_name;
    char name[] = "test";
    table_name.assign(name, strlen(name));
    char row[] = "FFFFFFFF";
    ObString row_key;
    row_key.assign(row, strlen(row));
    
    ObRange range;
    range.border_flag_.set_min_value();
    range.border_flag_.unset_max_value();
    range.end_key_ = row_key;
    param.set(OB_INVALID_ID, table_name, range);
    task.set_param(param);

    TabletLocation list;
    ObServer chunkserver;
    ObTabletLocation server;
    for (int64_t j = 0; j < TabletLocation::MAX_REPLICA_COUNT; ++j)
    {
      chunkserver.set_ipv4_addr(200, 1024);
      server.tablet_version_ = j;
      server.chunkserver_ = chunkserver;
      EXPECT_TRUE(OB_SUCCESS == list.add(server));
      EXPECT_TRUE(j + 1 == list.size());
    }
    EXPECT_TRUE(manager.insert_task(list, task) == OB_SUCCESS);
    EXPECT_TRUE(task.get_limit() == limit);
    static const int64_t token = task.get_token();
    EXPECT_TRUE(task.get_token() == token);
  }
  manager.print_info();
  
  for (int64_t i = 0; i < TaskManager::DEFAULT_COUNT + 10; ++i)
  {
    // fetch task for same server
    if (i < TaskManager::DEFAULT_COUNT)
    {
      EXPECT_TRUE(manager.fetch_task(counter, task_info) == OB_SUCCESS);
    }
    else
    {
      EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
    }
  }
  manager.print_info();
}

TEST_F(TestTaskManager, test_finish_task)
{
  TaskManager manager;
  TaskInfo task_info;
  task_info.set_id(1);
  TaskCounter counter;
  // no task for finish 
  EXPECT_TRUE(manager.finish_task(true, task_info) != OB_SUCCESS);
  EXPECT_TRUE(manager.finish_task(false, task_info) != OB_SUCCESS);
  
  int64_t task_count = 10;
  for (int64_t i = 0; i < task_count; ++i)
  {
    TaskInfo task;
    uint64_t limit = 203;
    task.set_limit(limit);
    ObScanParam param;
    ObString table_name;
    char name[] = "test";
    table_name.assign(name, strlen(name));
    char row[] = "FFFFFFFF";
    ObString row_key;
    row_key.assign(row, strlen(row));
    
    ObRange range;
    range.border_flag_.set_min_value();
    range.border_flag_.unset_max_value();
    range.end_key_ = row_key;
    
    param.set(OB_INVALID_ID, table_name, range);
    task.set_param(param);

    TabletLocation list;
    ObServer chunkserver;
    ObTabletLocation server;
    for (int64_t j = 0; j < TabletLocation::MAX_REPLICA_COUNT; ++j)
    {
      chunkserver.set_ipv4_addr(j + 300000 + i%30, j + 1024);
      server.tablet_version_ = j;
      server.chunkserver_ = chunkserver;
      EXPECT_TRUE(OB_SUCCESS == list.add(server));
      EXPECT_TRUE(j + 1 == list.size());
    }
    //task.set_location(list);
    EXPECT_TRUE(manager.insert_task(list, task) == OB_SUCCESS);
    EXPECT_TRUE(task.get_limit() == limit);
    static const int64_t token = task.get_token();
    EXPECT_TRUE(task.get_token() == token);
    EXPECT_TRUE(task.get_id() == uint64_t(i + 1));
    //EXPECT_TRUE(task.get_location() == list);
    
    // not doing this task
    EXPECT_TRUE(manager.finish_task(true, task) != OB_SUCCESS);
    EXPECT_TRUE(manager.finish_task(false, task) != OB_SUCCESS);
  }
  manager.print_info();
  //
  for (int64_t i = 0; i < task_count; ++i)
  {
    EXPECT_TRUE(manager.fetch_task(counter, task_info) == OB_SUCCESS);
    EXPECT_TRUE(task_info.get_id() == uint64_t(i + 1));
    if (i < task_count/2)
    {
      EXPECT_TRUE(manager.finish_task(true, task_info) == OB_SUCCESS);
    }
    else
    {
      EXPECT_TRUE(manager.finish_task(false, task_info) == OB_SUCCESS);
    }
  }
  
  // no more task for fetch task not timeout
  EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
  EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
  EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
  
  manager.print_info();

  // sleep for timeout
  sleep(TaskManager::MAX_WAIT_TIME/(1000 * 1000));
  manager.print_info();
  for (int64_t i = 0; i < task_count; ++i)
  {
    if (i < (task_count + 1)/2)
    {
      EXPECT_TRUE(manager.fetch_task(counter, task_info) == OB_SUCCESS);
      EXPECT_TRUE(manager.finish_task(true, task_info) == OB_SUCCESS);
    }
    else
    {
      // remaider already finished
      EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
    }
  }
  manager.print_info();

  // no new task for fetch and finish
  for (int64_t i = 0; i < task_count; ++i)
  {
    EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
  }
  manager.print_info();

  // insert new task for same server aaaa
  for (int64_t i = 0; i < TaskManager::DEFAULT_COUNT + 10; ++i)
  {
    TaskInfo task;
    uint64_t limit = 203;
    task.set_limit(limit);
    ObScanParam param;
    ObString table_name;
    char name[] = "test";
    table_name.assign(name, strlen(name));
    char row[] = "FFFFFFFF";
    ObString row_key;
    row_key.assign(row, strlen(row));
    
    ObRange range;
    range.border_flag_.set_min_value();
    range.border_flag_.unset_max_value();
    range.end_key_ = row_key;
    param.set(OB_INVALID_ID, table_name, range);
    task.set_param(param);

    TabletLocation list;
    ObServer chunkserver;
    ObTabletLocation server;
    for (int64_t j = 0; j < TabletLocation::MAX_REPLICA_COUNT; ++j)
    {
      chunkserver.set_ipv4_addr(200, 1024);
      server.tablet_version_ = j;
      server.chunkserver_ = chunkserver;
      EXPECT_TRUE(OB_SUCCESS == list.add(server));
      EXPECT_TRUE(j + 1 == list.size());
    }
    EXPECT_TRUE(manager.insert_task(list, task) == OB_SUCCESS);
    EXPECT_TRUE(task.get_limit() == limit);
    static const int64_t token = task.get_token();
    EXPECT_TRUE(task.get_token() == token);
  }
  manager.print_info();
  
  for (int64_t i = 0; i < TaskManager::DEFAULT_COUNT + 10; ++i)
  {
    // fetch task for same server
    if (i < TaskManager::DEFAULT_COUNT)
    {
      EXPECT_TRUE(manager.fetch_task(counter, task_info) == OB_SUCCESS);
    }
    else
    {
      EXPECT_TRUE(manager.fetch_task(counter, task_info) != OB_SUCCESS);
    }
  }
  manager.print_info();
}

// multi-thread fetch task test
void * fetch_task(void * argv)
{
  EXPECT_TRUE(argv != NULL);
  TaskManager * manager = (TaskManager*) (argv);
  EXPECT_TRUE(manager != NULL);
  TaskInfo task;
  TaskCounter counter;
  int64_t count = 0;
  int ret = OB_SUCCESS;
  while(!manager->check_finish())
  {
    ++count;
    ret = manager->fetch_task(counter, task);
    if (ret != OB_SUCCESS)
    {
      usleep(1000 * 1000);
    }
    else
    {
      TBSYS_LOG(INFO, "fetch task succ:id[%lu]", task.get_id());
      usleep(1000 * count);
      if (count % 3 == 0)
      {
        EXPECT_TRUE(OB_SUCCESS == manager->finish_task(-13, task));
      }
      else
      {
        EXPECT_TRUE(OB_SUCCESS == manager->finish_task(OB_SUCCESS, task));
      }
    }
  }
  return NULL;
}

void * dump_task(void * argv)
{
  EXPECT_TRUE(argv != NULL);
  TaskManager * manager = (TaskManager*) (argv);
  EXPECT_TRUE(manager != NULL);
  while(false == manager->check_finish())
  {
    manager->print_info();
    sleep(10);
  }
  // at last
  manager->print_info();
  return NULL;
}

#if MULTI_THREAD_TEST

TEST_F(TestTaskManager, test_multi_thread)
{
  TaskManager manager;
  int64_t task_count = 10000;
  for (int64_t i = 0; i < task_count; ++i)
  {
    TaskInfo task;
    uint64_t limit = 203;
    task.set_limit(limit);
    ObScanParam param;
    ObString table_name;
    char name[] = "test";
    table_name.assign(name, strlen(name));
    char row[] = "FFFFFFFFXXXXXXX";
    ObString row_key;
    row_key.assign(row, strlen(row));
    
    ObRange range;
    range.border_flag_.unset_min_value();
    range.border_flag_.unset_max_value();
    range.start_key_ = row_key;
    range.end_key_ = row_key;
    
    param.set(OB_INVALID_ID, table_name, range);
    task.set_param(param);

    TabletLocation list;
    ObServer chunkserver;
    ObTabletLocation server;
    for (int64_t j = 0; j < TabletLocation::MAX_REPLICA_COUNT; ++j)
    {
      chunkserver.set_ipv4_addr(i%16 + 30000 + j, j + 1024);
      server.tablet_version_ = j;
      server.chunkserver_ = chunkserver;
      EXPECT_TRUE(OB_SUCCESS == list.add(server));
      EXPECT_TRUE(j + 1 == list.size());
    }
    
    //task.set_location(list);
    EXPECT_TRUE(manager.insert_task(list, task) == OB_SUCCESS);
    EXPECT_TRUE(task.get_limit() == limit);
    static const int64_t token = task.get_token();
    EXPECT_TRUE(task.get_token() == token);
    EXPECT_TRUE(task.get_id() == (uint64_t)(i + 1));
    //EXPECT_TRUE(task.get_location() == list);
  }
  
  pthread_t thread;
  EXPECT_TRUE(pthread_create(&thread, NULL, dump_task, &manager) == OB_SUCCESS);

  int64_t thread_count = 50;
  pthread_t threads[thread_count];
  for (int64_t i = 0; i < thread_count; ++i)
  {
    EXPECT_TRUE(pthread_create(&threads[i], NULL, fetch_task, &manager) == OB_SUCCESS);
  }

  for (int64_t i = 0; i < thread_count; ++i)
  {
    EXPECT_TRUE(pthread_join(threads[i], NULL) == OB_SUCCESS);
  }

  EXPECT_TRUE(pthread_join(thread, NULL) == OB_SUCCESS);
}

#endif
