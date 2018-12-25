/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         test_page_arena.cpp is for what ...
 *
 *  Version: $Id: test_page_arena.cpp 02/21/2013 03:00:38 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */




#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/page_arena.h"

using namespace oceanbase;
using namespace common;

static const int64_t SIZE_OF_PAGE = 32;

TEST(TestPageArena, test_alloc_mix)
{
  const int64_t default_page_size = CharArena::DEFAULT_PAGE_SIZE;
  const int64_t size =  default_page_size - SIZE_OF_PAGE;
  const int64_t size1 = size - SIZE_OF_PAGE * 2;
  const int64_t size2 = size - size1;

  int64_t current_used = 0;
  CharArena arena;
  char* ptr = arena.alloc(size1);
  current_used += size1;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(1 , arena.pages());

  ptr = arena.alloc(size2);
  current_used += size2;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(1 , arena.pages());

  ptr = arena.alloc(1);
  current_used += 1;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*2, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

  ptr = arena.alloc(SIZE_OF_PAGE);
  current_used += SIZE_OF_PAGE;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*2, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

  const int64_t large_size = size + 1;
  ptr = arena.alloc(large_size);
  current_used += large_size;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*2+large_size+SIZE_OF_PAGE, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(3 , arena.pages());

  const int64_t remain_of_last_page = size - 1 - SIZE_OF_PAGE;
  ptr = arena.alloc(remain_of_last_page);
  current_used += remain_of_last_page;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*2+large_size+SIZE_OF_PAGE, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(3 , arena.pages());

  arena.reuse();
  current_used = 0;
  ASSERT_EQ(default_page_size*2, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

  ptr = arena.alloc(size1);
  current_used += size1;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*2, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

  ptr = arena.alloc(size2);
  current_used += size2;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*2, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

  ptr = arena.alloc(SIZE_OF_PAGE * 10);
  current_used += SIZE_OF_PAGE * 10;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*2, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

  // normal overflow;
  const int64_t normal_overflow_size = size - SIZE_OF_PAGE * 9;
  ptr = arena.alloc(normal_overflow_size);
  current_used += normal_overflow_size;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(default_page_size*3, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(3 , arena.pages());


}

TEST(TestPageArena, test_reuse_large_pages)
{
  const int64_t default_page_size = CharArena::DEFAULT_PAGE_SIZE;
  const int64_t size =  default_page_size - SIZE_OF_PAGE;

  char* ptr = NULL;
  int64_t current_used = 0;
  int64_t current_total = 0;
  CharArena arena;

  const int64_t large_size = size + SIZE_OF_PAGE * 10;
  ptr = arena.alloc(large_size);
  current_used += large_size;
  current_total += large_size + SIZE_OF_PAGE + default_page_size;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(current_total, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

  ptr = arena.alloc(large_size);
  current_used += large_size;
  current_total += large_size + SIZE_OF_PAGE;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(current_total, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(3 , arena.pages());

  arena.reuse();
  ASSERT_EQ(default_page_size, arena.total());
  ASSERT_EQ(0, arena.used());
  ASSERT_EQ(1 , arena.pages());

  current_total = arena.total(); 
  current_used = arena.used();

  ptr = arena.alloc(SIZE_OF_PAGE * 10);
  current_used += SIZE_OF_PAGE * 10;
  current_total += 0;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(current_total, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(1 , arena.pages());

  // normal overflow;
  const int64_t normal_overflow_size = size - SIZE_OF_PAGE * 9;
  ptr = arena.alloc(normal_overflow_size);
  current_used += normal_overflow_size;
  current_total += default_page_size;
  ASSERT_TRUE(NULL != ptr);
  ASSERT_EQ(current_total, arena.total());
  ASSERT_EQ(current_used, arena.used());
  ASSERT_EQ(2 , arena.pages());

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}



