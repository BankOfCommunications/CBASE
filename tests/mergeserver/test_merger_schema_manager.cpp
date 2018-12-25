
#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_schema_manager.h"

using namespace std;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestSchemaManager : public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestSchemaManager, test_init)
{
  ObSchemaManagerV2 schema(100);
  ObMergerSchemaManager * pmanager = new ObMergerSchemaManager;
  EXPECT_TRUE(pmanager != NULL);
  ObMergerSchemaManager & manager = *pmanager;
  int ret = manager.init(false, schema);
  EXPECT_TRUE(ret == OB_SUCCESS);
  // init twice
  ret = manager.init(false, schema);
  EXPECT_TRUE(ret == OB_INIT_TWICE);
  manager.print_info();
  delete pmanager;
  pmanager = NULL;
}

static void * routine(void * argv)
{
  int ret = OB_SUCCESS;
  ObMergerSchemaManager * manager = (ObMergerSchemaManager *) argv;
  EXPECT_TRUE(NULL != manager);
  const ObSchemaManagerV2 * schema_ptr = NULL;
  for (uint64_t i = 0; i < 10; i++)
  {
    ObSchemaManagerV2 * schema = new ObSchemaManagerV2(1024 + i);
    EXPECT_TRUE(NULL != schema);
    ret = manager->add_schema(*schema, &schema_ptr);
    if (ret != 0)
    {
      printf("add schema failed:version[%lu], thread[%lu]\n", 1024 + i, pthread_self());
    }
    else
    {
      usleep(static_cast<useconds_t>(i + 50));
      EXPECT_TRUE(OB_SUCCESS == manager->release_schema(schema_ptr, schema->get_version()));
    }

    schema_ptr = manager->get_user_schema(1024 + i);
    if (schema_ptr == NULL)
    {
      printf("get schema failed:version[%lu], thread[%lu]\n", 1024 + i, pthread_self());
    }
    else
    {
      usleep(static_cast<useconds_t>(i + 50));
      EXPECT_TRUE(OB_SUCCESS == manager->release_schema(schema_ptr, schema_ptr->get_version()));
    }

    delete schema;
    schema = NULL;
  }
  return NULL;
}


TEST_F(TestSchemaManager, test_multi_thread)
{
  ObMergerSchemaManager * schema = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != schema);
  ObSchemaManagerV2 * sample = new ObSchemaManagerV2(1022);
  EXPECT_TRUE(NULL != sample);
  EXPECT_TRUE(OB_SUCCESS == schema->init(false, *sample));
  
  const uint64_t MAX_THREAD_COUNT = 15;
  pthread_t threads[MAX_THREAD_COUNT];
  for (uint64_t i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == pthread_create(&threads[i], NULL, routine, schema));
  }
  for (uint64_t i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    pthread_join(threads[i], NULL);
  }
  delete sample;
  sample = NULL;
  delete schema;
  schema = NULL;
}

TEST_F(TestSchemaManager, test_add_schema)
{
  ObSchemaManagerV2 schema(100);
  ObMergerSchemaManager * pmanager = new ObMergerSchemaManager;
  EXPECT_TRUE(pmanager != NULL);
  ObMergerSchemaManager & manager = *pmanager;

  // not init
  int ret = manager.add_schema(schema);
  EXPECT_TRUE(ret != OB_SUCCESS);
  // init
  ret = manager.init(false, schema);
  EXPECT_TRUE(ret == OB_SUCCESS);
  
  // not new version
  ret = manager.add_schema(schema);
  EXPECT_TRUE(ret != OB_SUCCESS);
  
  do 
  { 
    ObSchemaManagerV2 schema2(200);
    ret = manager.add_schema(schema2);
    EXPECT_TRUE(ret == OB_SUCCESS);
  } while(0);
  
  const ObSchemaManagerV2 * pschema = NULL; 
  // add 2 
  for (uint64_t i = 0; i < ObMergerSchemaManager::MAX_VERSION_COUNT; ++i)
  {
    ObSchemaManagerV2 schema(300 + i * 2);
    ret = manager.add_schema(schema, &pschema);
    EXPECT_TRUE(ret == OB_SUCCESS);
    EXPECT_TRUE(pschema->get_version() == int64_t(300 + i * 2));
    ret = manager.release_schema(pschema, pschema->get_version());
  }

  // full  
  ObSchemaManagerV2 schema3(300 + ObMergerSchemaManager::MAX_VERSION_COUNT * 2);
  ret = manager.add_schema(schema3);
  EXPECT_TRUE(ret == OB_SUCCESS);
  
  // full and inuse
  for (uint64_t i = 0; i <= ObMergerSchemaManager::MAX_VERSION_COUNT; ++i)
  {
    const ObSchemaManagerV2 * schema = manager.get_user_schema(300 + i * 2);
    // do nothing
    if (schema == NULL)
    {
    }
    // not release
  }
  
  // real full 
  ObSchemaManagerV2 schema4(300 + ObMergerSchemaManager::MAX_VERSION_COUNT * 2 + 1);
  ret = manager.add_schema(schema4);
  EXPECT_TRUE(ret != OB_SUCCESS);
  
  manager.print_info();

  delete pmanager;
  pmanager = NULL;
}

TEST_F(TestSchemaManager, test_get_user_schema)
{
  ObMergerSchemaManager * pmanager = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != pmanager);

  ObMergerSchemaManager & manager = *pmanager;
  // not init
  const ObSchemaManagerV2 * schema = manager.get_user_schema(100);
  EXPECT_TRUE(schema == NULL);
  
  ObSchemaManagerV2 schema1(100);
  int ret = manager.init(false, schema1);
  EXPECT_TRUE(ret == OB_SUCCESS);
  
  // add more
  for (uint64_t i = 0; i < ObMergerSchemaManager::MAX_VERSION_COUNT * 2; ++i)
  {
    ObSchemaManagerV2 schema2(200 * (i + 1)); 
    ret = manager.add_schema(schema2);
    EXPECT_TRUE(ret == OB_SUCCESS);
    schema = manager.get_user_schema(200 * (i + 1));
    EXPECT_TRUE(schema != NULL);
    ret = manager.release_schema(schema, 200 * (i + 1));
    EXPECT_TRUE(ret == OB_SUCCESS);
  }
  
  manager.print_info();

  // get schema
  for (uint64_t i = 0; i < ObMergerSchemaManager::MAX_VERSION_COUNT * 2; ++i)
  {
    schema = manager.get_user_schema(200 * (i + 1));
    if (i < ObMergerSchemaManager::MAX_VERSION_COUNT)
    {
      // not find
      EXPECT_TRUE(schema == NULL);
    }
    else
    {
      // find
      EXPECT_TRUE(schema != NULL);
    }
  }
  
  manager.print_info();
  delete pmanager;
  pmanager = NULL;
}


TEST_F(TestSchemaManager, test_version)
{
  ObMergerSchemaManager * pmanager = new ObMergerSchemaManager;
  EXPECT_TRUE(NULL != pmanager);
  ObMergerSchemaManager & manager = *pmanager;
  
  // not init
  int64_t newest = manager.get_latest_version();
  EXPECT_TRUE(-1 == newest);
  
  // timestamp
  int64_t oldest = manager.get_oldest_version();
  EXPECT_TRUE(ObMergerSchemaManager::MAX_INT64_VALUE == oldest);
  
  // init
  ObSchemaManagerV2 schema1(0);
  int ret = manager.init(false, schema1);
  EXPECT_TRUE(ret == OB_SUCCESS);
  
  // add more to full
  for (uint64_t i = 0; i < ObMergerSchemaManager::MAX_VERSION_COUNT - 1; ++i)
  {
    ObSchemaManagerV2 schema(200 * (i + 1)); 
    ret = manager.add_schema(schema);
    EXPECT_TRUE(ret == OB_SUCCESS);
    newest = manager.get_latest_version();
    EXPECT_TRUE(int64_t(200 * (i + 1)) == newest);
    
    oldest = manager.get_oldest_version();
    EXPECT_TRUE(0 == oldest);
  }
  
  manager.print_info();
  
  // add more
  for (uint64_t i = ObMergerSchemaManager::MAX_VERSION_COUNT - 1;
      i < ObMergerSchemaManager::MAX_VERSION_COUNT * 2; ++i)
  {
    ObSchemaManagerV2 schema(200 * (i + 1)); 
    ret = manager.add_schema(schema);
    EXPECT_TRUE(ret == OB_SUCCESS);
    
    newest = manager.get_latest_version();
    EXPECT_TRUE(int64_t(200 * (i + 1)) == newest);
    
    manager.print_info();
    
    oldest = manager.get_oldest_version();
    EXPECT_TRUE(int64_t(200 * (i - ObMergerSchemaManager::MAX_VERSION_COUNT + 2)) == oldest);
  }
  
  delete pmanager;
  pmanager = NULL;
}


