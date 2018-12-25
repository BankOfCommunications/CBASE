/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_stack_allocator.h"
#include "common/thread_buffer.h"
#include "common/utility.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace test
  {
#define cfg(k, v) getenv(k)?:v
#define cfgi(k, v) atoll(cfg(k ,v))
    struct Config
    {
      static int64_t MAX_N_DATA_ITEMS;
      int64_t mem_limit;
      int64_t max_num_items;
      int64_t block_size;
      int64_t alloc_total_limit_mb;
      int64_t max_alloc_size;
      int64_t rollback_percent;
      int64_t alloc_big_percent;
      int64_t reserve_percent;
      int64_t reserve_big_percent;
      int64_t n_thread;
      Config()
      {
        mem_limit = (cfgi("alloc_total_limit_mb", "1024")+1)<<20;
        max_num_items = MAX_N_DATA_ITEMS;
        block_size = 1<<21;
        alloc_total_limit_mb = cfgi("alloc_total_limit_mb", "1024");
        max_alloc_size = 1<<7;
        rollback_percent = 5;
        alloc_big_percent = 5;
        reserve_percent = 5;
        reserve_big_percent = 5;
        n_thread = cfgi("n_thread", "2");
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<18;

    class ObStackAllocatorTest: public ::testing::Test, public tbsys::CDefaultRunnable, public Config {
      public:
        ObStackAllocatorTest() {}
        ~ObStackAllocatorTest(){}
      protected:
        int64_t n_err;        
        DefaultBlockAllocator block_allocator;
        TSIStackAllocator allocator;
      protected:
        virtual void SetUp(){
          srandom(static_cast<int32_t>(time(NULL)));
          ASSERT_EQ(OB_SUCCESS, block_allocator.set_limit(mem_limit));
          ASSERT_EQ(OB_SUCCESS, allocator.init(&block_allocator, block_size));
          ASSERT_EQ(0, block_allocator.get_allocated());
          n_err = 0;
        }
        virtual void TearDown(){
          inspect(true);
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObStackAllocator{max_num_items=%ld}\n", max_num_items);
          if(verbose)
          {
          }
        }

        int alloc(const int64_t size) {
          int err = OB_SUCCESS;
          char* p = NULL;
          if (NULL == (p = (char*)allocator.alloc(size)))
          {}
          else
          {
            memset(p, 0, size);
          }
          return err;
        }
        int alloc_once(int64_t idx) {
          int err = OB_SUCCESS;
          bool start_batch = true;
          int64_t size = random() % max_alloc_size + 1;
          bool rollback = (random()%100) < rollback_percent;
          bool alloc_big = (random()%100) < alloc_big_percent;
          bool need_reserve = (random()%100) < reserve_percent;
          bool need_reserve_big = (random()%100) < reserve_big_percent;
          UNUSED(idx);
          if (OB_SUCCESS != (err = allocator.start_batch_alloc()))
          {
            start_batch = false;
            TBSYS_LOG(ERROR, "allocator.start_batch_alloc()=>%d", err);
          }
          else if (need_reserve && OB_SUCCESS != (err = allocator.reserve(size)))
          {
            TBSYS_LOG(ERROR, "reserver(size=%ld)=>%d", size, err);
          }
          else if (need_reserve_big && OB_SUCCESS != (err = allocator.reserve(size + block_size)))
          {
            TBSYS_LOG(ERROR, "reserver(size=%ld)=>%d", size + block_size, err);
          }
          else if (OB_SUCCESS != (err = alloc(size)))
          {
            TBSYS_LOG(ERROR, "allocator.alloc(size=%ld)=>%d", size, err);
          }
          else if (alloc_big && OB_SUCCESS != (err = alloc(size + block_size)))
          {
            TBSYS_LOG(ERROR, "allocator.alloc(size=%ld)=>%d", size + block_size, err);
          }
          if (start_batch && OB_SUCCESS != (err = allocator.end_batch_alloc(rollback)))
          {
            TBSYS_LOG(ERROR, "allocator.end_batch_alloc(rollback=%s)=>%d", STR_BOOL(rollback), err);
          }
          return err;
        }
        void run(tbsys::CThread *thread, void* arg){
          int err = OB_SUCCESS;
          int64_t idx = (long)(arg);
          UNUSED(thread);
          for(int i = 0; OB_SUCCESS == err; i++) {
            if (OB_SUCCESS != (err = alloc_once(idx)))
            {
              TBSYS_LOG(ERROR, "alloc_once(idx=%ld)=>%d", idx, err);
            }
            if (block_allocator.get_allocated() > alloc_total_limit_mb<<20)
            {
              break;
            }
          }
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "alloc_once(idx=%ld)=>%d", idx, err);
            __sync_fetch_and_add(&n_err, 1);
          }
        }
    };

    TEST_F(ObStackAllocatorTest, AllocAndClear){
      setThreadCount((int32_t)n_thread);
      start();
      wait();
      TBSYS_LOG(INFO, "total alloc: %ld", block_allocator.get_allocated());
      ASSERT_EQ(OB_SUCCESS, allocator.clear(false));
      ASSERT_EQ(0, block_allocator.get_allocated());
    }
  } // end namespace updateserver
} // end namespace oceanbase

using namespace oceanbase::test;
int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  int n_data_items_shift = 0;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  if(argc >= 2)
  {
    TBSYS_LOGGER.setLogLevel(argv[1]);
  }
  if(argc >= 3)
  {
    n_data_items_shift = atoi(argv[2]);
    if(n_data_items_shift > 4)
      Config::MAX_N_DATA_ITEMS = 1 << n_data_items_shift;
  }
  return RUN_ALL_TESTS();
}
