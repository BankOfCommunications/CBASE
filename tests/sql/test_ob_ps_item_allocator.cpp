/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * test_ob_ps_item_allocator.cpp is for what ...
 *
 * Version: ***: test_ob_ps_item_allocator.cpp  Wed Jul 31 14:32:16 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want 
 *
 */
#include "gtest/gtest.h"
#include "sql/ob_ps_store_item.h"
#include "thread.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace tbsys;
namespace oceanbase
{
  namespace tests
  {
    namespace sql
    {
      class TestObPsStoreItem: public ::testing::Test
      {
      public:
        TestObPsStoreItem(){}
        ~TestObPsStoreItem(){}
      public:
        ObPsStoreItemAllocator alloc_;
      };
      
      class AllocFreeRunnable: public tbsys::CDefaultRunnable
      {
      public:
        void run(tbsys::CThread *thread, void *arg)
        {
          TBSYS_LOG(DEBUG, "start thread %p:", thread);
          TestObPsStoreItem *titem = reinterpret_cast<TestObPsStoreItem*>(arg);
          ObPsStoreItemAllocator *alloc = &titem->alloc_;
          ObPsStoreItem *item = NULL;
          for (int i = 0; i < 10000; ++i)
          {
            item = reinterpret_cast<ObPsStoreItem*>(alloc->alloc(sizeof(ObPsStoreItem)));
            TBSYS_LOG(ERROR, "alloc item %p", item);
            EXPECT_TRUE(NULL != item);
            alloc->free(item);
            TBSYS_LOG(ERROR, "free item %p", item);
            item = reinterpret_cast<ObPsStoreItem*>(alloc->alloc(sizeof(ObPsStoreItem)));
            TBSYS_LOG(ERROR, "alloc item %p", item);
            EXPECT_TRUE(NULL != item);
            alloc->free(item);
            TBSYS_LOG(ERROR, "free item %p", item);
          }
        }
      };
      TEST_F(TestObPsStoreItem, mixallocfree)
      {
        AllocFreeRunnable runner;
        CThread threads[100];
        for (int i = 0; i<100; ++i)
        {
          threads[i].start(&runner, this);
        }
        for (int i = 0; i<100; ++i)
        {
          threads[i].join();
        }
      }
    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  int ret = OB_ERROR;
  ret = ObPsStoreItemGFactory::get_instance()->init();
  EXPECT_EQ(OB_SUCCESS, ret);
  TBSYS_LOGGER.setFileName("test.log");
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
