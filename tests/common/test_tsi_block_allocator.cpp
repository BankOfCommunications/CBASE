/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/ob_tsi_block_allocator.h"
#include "../updateserver/rwt.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace test
  {
#define _cfg(k, v) getenv(k)?:v
#define _cfgi(k, v) atoll(getenv(k)?:v)
    struct Config
    {
      int64_t duration;
      int64_t max_alloc_size;
      int64_t alloc_thread_num;
      Config()
      {
        duration = _cfgi("duration", "5000000");
        max_alloc_size = 1<<22;
        alloc_thread_num = 4;
      }
    };
    class ObBlockAllocatorTest: public ::testing::Test, public Config, public RWT {
      public:
        ObBlockAllocatorTest(): err(0), n_alloc(0), n_failed(0) {
          RWT::set(0, alloc_thread_num);
        }
        ~ObBlockAllocatorTest() {}
      protected:
        int err;
        ObMalloc block_allocator;
        ObTSIBlockAllocator allocator;
        int64_t n_alloc;
        int64_t n_failed;
      protected:
        virtual void SetUp(){
          srandom(static_cast<unsigned int>(time(NULL)));
          ASSERT_EQ(OB_SUCCESS, allocator.init(&block_allocator));
          allocator.set_normal_block_tclimit(0);
          allocator.set_big_block_tclimit(0);
        }
        virtual void TearDown(){
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObLogGeneratorTest{duration=%ld}\n", duration);
          if(verbose)
          {
          }
        }
        int report() {
          int err = OB_SUCCESS;
          TBSYS_LOG(INFO, "n_alloc=%ld, n_failed=%ld", n_alloc, n_failed);
          return err;
        }
        int read(const int64_t idx) {
          int err = OB_SUCCESS;
          UNUSED(idx);
          return err;
        }
        int write(const int64_t idx) {
          int err = OB_SUCCESS;
          int64_t size_list[] = {ObTSIBlockAllocator::NORMAL_BLOCK_SIZE, ObTSIBlockAllocator::BIG_BLOCK_SIZE, 0};
          int64_t size = 0;
          void* p = NULL;
          UNUSED(idx);
          while(!stop_ && OB_SUCCESS == err) {
            size_list[2] = random() % max_alloc_size;
            size = size_list[random() % 3];
            __sync_fetch_and_add(&n_alloc, 1);
            if (NULL == (p = allocator.alloc(size)))
            {
              __sync_fetch_and_add(&n_failed, 1);
            }
            else
            {
              allocator.free(p);
            }
          }
          return err;
        }
    };
    TEST_F(ObBlockAllocatorTest, Alloc){
      BaseWorker worker;
      ASSERT_EQ(0, PARDO(get_thread_num(), this, duration));
    }
  }
}
using namespace oceanbase::test;

int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
