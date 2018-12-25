#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "common/ob_malloc.h"
#include "common/page_arena.h"
#include "common/ob_fifo_allocator.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;

TEST(TestFIFOAllocator, init)
{
  common::FIFOAllocator allocator;
  uint64_t total_limit = 100 * 1024 * 1024;
  uint64_t hold_limit = 50 * 1024 * 1024;
  uint64_t page_size = 2 * 1024 * 1024;
  void *nil = NULL;

  int ret = allocator.init(total_limit, hold_limit, page_size);
  EXPECT_EQ(OB_SUCCESS, ret);

  void *ptr = allocator.alloc(page_size);
  EXPECT_EQ(nil, ptr);

  int64_t alloc_size = page_size - sizeof(uint64_t);
  void *ptr1 = allocator.alloc(alloc_size);
  EXPECT_NE(nil, ptr1);
  void *ptr2 = allocator.alloc(alloc_size);
  EXPECT_NE(nil, ptr2);

  int64_t allocated_size = 2 * page_size;
  int64_t hold_size = 0;
  EXPECT_EQ(allocated_size, allocator.allocated());
  EXPECT_EQ(hold_size, allocator.hold());

  allocator.free(ptr1);
  allocator.free(ptr2);

  allocator.free(ptr2);
  ptr2 = allocator.alloc(alloc_size);
  EXPECT_NE(nil, ptr2);

  hold_size = page_size;
  EXPECT_EQ(hold_size, allocator.hold());

  allocator.free(ptr2);
  allocator.destroy();
}

const int64_t ALLOC_SIZE = 1000;
const int64_t ALLOC_NUM = 10000;
const int64_t TOTAL_NUM = 100000;
const int64_t THREAD_NUM = 4;
FIFOAllocator g_allocator;
void *thread_func(void *data)
{
  UNUSED(data);
  void *pbuf[ALLOC_NUM];
  for (int64_t i = 0; i < TOTAL_NUM; i++)
  {
    for (int64_t j = 0; j < ALLOC_NUM; j++)
    {
      pbuf[j] = g_allocator.alloc(ALLOC_SIZE);
    }
    for (int64_t j = 0; j < ALLOC_NUM; j++)
    {
      g_allocator.free(pbuf[j]);
      pbuf[j] = NULL;
    }
  }
  return NULL;
}

TEST(TestFIFOAllocator, perf)
{
  uint64_t total_limit = 10L * 1024L * 1024L * 1024L;
  uint64_t hold_limit = 5L * 1024L * 1024L * 1024L;
  uint64_t page_size = 2 * 1024 * 1024;
  int ret = g_allocator.init(total_limit, hold_limit, page_size);
  EXPECT_EQ(OB_SUCCESS, ret);

  pthread_t pd[THREAD_NUM];

  int64_t timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 0; i < THREAD_NUM; i++)
  {
    pthread_create(&pd[i], NULL, thread_func, NULL);
  }
  for (int64_t i = 0; i < THREAD_NUM; i++)
  {
    pthread_join(pd[i], NULL);
  }
  timeu = tbsys::CTimeUtil::getTime() - timeu;
  fprintf(stderr, "timeu=%ld speed=%0.2f\n", timeu, 1000000.0 * ALLOC_NUM * TOTAL_NUM * THREAD_NUM / static_cast<double>(timeu));
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
