#include <limits.h>
#include <btree_default_alloc.h>
#include <gtest/gtest.h>

namespace oceanbase
{
  namespace common
  {

    TEST(BtreeAllocTest, DefaultConstructor)
    {
      BtreeDefaultAlloc allocator;
      EXPECT_EQ(sizeof(int64_t), static_cast<uint32_t>(allocator.get_alloc_size()));
    }

    TEST(BtreeAllocTest, init)
    {
      BtreeDefaultAlloc allocator;
      allocator.init(1);
      EXPECT_EQ(sizeof(int64_t), static_cast<uint32_t>(allocator.get_alloc_size()));
      allocator.init(32);
      EXPECT_EQ(32, allocator.get_alloc_size());
    }

    TEST(BtreeAllocTest, alloc)
    {
      BtreeDefaultAlloc allocator;
      char *p, *p1;
      p = allocator.alloc();
      EXPECT_EQ(1, allocator.get_use_count());
      p1 = allocator.alloc();
      EXPECT_EQ(2, allocator.get_use_count());
      allocator.release(p);
      allocator.release(NULL);
      EXPECT_EQ(1, allocator.get_free_count());
      EXPECT_EQ(1, allocator.get_use_count());
      p = allocator.alloc();
      EXPECT_EQ(2, allocator.get_use_count());

      // 可以多次初始化
      allocator.init(1024);
      EXPECT_EQ(0, allocator.get_use_count());
      EXPECT_EQ(0, allocator.get_malloc_count());
      char *list[1025];
      for(int i = 0; i < 1025; i++)
      {
        list[i] = allocator.alloc();
      }
      EXPECT_EQ(1025, allocator.get_use_count());
#ifndef OCEAN_BASE_BTREE_DEBUG
      //EXPECT_EQ(2, allocator.get_malloc_count());
#endif
      for(int i = 0; i < 1025; i++)
      {
        allocator.release(list[i]);
      }
      allocator.release(p);
      allocator.release(p1);
    }

  } // end namespace common
} // end namespace oceanbase
