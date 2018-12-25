#include <gtest/gtest.h>
#include <stdlib.h>
#include "common/ob_define.h"
#include "common/ob_memory_pool.h"
#include "common/ob_malloc.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace testing;



TEST(ObFixedMemPoolTest, basicTest)
{
  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    char *direct_newed_ptr = new(std::nothrow)char[512];
    ASSERT_NE(direct_newed_ptr, reinterpret_cast<void*>(NULL));
    void *ptr = NULL;
    /// use when not initialized
    ObFixedMemPool mem_pool;
    mem_pool.free(direct_newed_ptr);
    ptr = mem_pool.malloc(1024);
    ASSERT_EQ(mem_pool.shrink(0),0);
    ASSERT_EQ(reinterpret_cast<void*>(0), ptr);
    ASSERT_EQ(mem_pool.get_block_num(), 0);
    ASSERT_EQ(mem_pool.get_used_block_num(), 0);
    ASSERT_EQ(mem_pool.get_memory_size_handled(), 0);
    /// initialize with invalid agument
    ASSERT_GT(0, mem_pool.init(1024,-1));
    ASSERT_GT(0, mem_pool.init(-1024,1));
    /// initialize
    ASSERT_EQ(0, mem_pool.init(1024,1));
    ASSERT_LT(mem_pool.init(1024,1),0);
    ASSERT_GT(mem_pool.get_block_size(),1024);
    ASSERT_EQ(mem_pool.malloc(-1), reinterpret_cast<void*>(0));
    /// allocate from system, handled by pool when free
    void *pool_handle_1 = mem_pool.malloc(1024);
    ASSERT_NE(pool_handle_1, reinterpret_cast<void*>(0));
    memset(pool_handle_1,0,1024);
    ASSERT_EQ(mem_pool.get_block_num(), 1);
    ASSERT_EQ(mem_pool.get_used_block_num(), 1);

    /// allocate from system, freed directly when free
    void *system_handle_1 = mem_pool.malloc(1025);
    memset(system_handle_1,0,1025);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);

    /// allocate from system, handled by pool when free
    void *pool_handle_2 = mem_pool.malloc(512);
    ASSERT_NE(pool_handle_2, reinterpret_cast<void*>(0));
    memset(pool_handle_1,0,512);
    ASSERT_EQ(mem_pool.get_block_num(), 3);
    ASSERT_EQ(mem_pool.get_used_block_num(), 3);

    /// check memory reuse
    mem_pool.free(pool_handle_2);
    ASSERT_EQ(mem_pool.get_block_num(), 3);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);
    /// allocate from free list, handled by pool when free
    void *pool_handle_3 = mem_pool.malloc(1024);
    ASSERT_NE(pool_handle_3, reinterpret_cast<void*>(0));
    memset(pool_handle_1,0,1024);
    ASSERT_EQ(mem_pool.get_block_num(), 3);
    ASSERT_EQ(mem_pool.get_used_block_num(), 3);
    ASSERT_EQ(pool_handle_3, pool_handle_2);

    /// check memory return to syste directly
    mem_pool.free(system_handle_1);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);

    /// shrink not work
    ASSERT_EQ(mem_pool.shrink(1), 0);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);

    mem_pool.free(pool_handle_1);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 1);

    mem_pool.free(pool_handle_3);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 0);

    /// shrink work
    mem_pool.shrink(1);
    ASSERT_EQ(mem_pool.get_block_num(), 0);
    ASSERT_EQ(mem_pool.get_used_block_num(), 0);
    {
      /// check with unfreed memory
      void *unfree_ptr = NULL;
      ObFixedMemPool unfree_pool;
      ASSERT_EQ(unfree_pool.init(1024,1), 0);
      unfree_ptr = unfree_pool.malloc(512);
      ASSERT_NE(unfree_ptr, reinterpret_cast<void*>(0));
    }
    {
      /// unfreed but clear
      /// check with unfreed memory
      void *unfree_ptr = NULL;
      ObFixedMemPool unfree_pool;
      ASSERT_EQ(unfree_pool.init(1024,1), 0);
      unfree_ptr = unfree_pool.malloc(512);
      ASSERT_NE(unfree_ptr, reinterpret_cast<void*>(0));
      unfree_pool.clear();
    }
  }
}

/// @fn memory overwrite
TEST(ObFixedMemPoolTest, OverWrite)
{
  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    void *ptr1 = NULL;
    ObFixedMemPool mem_pool;
    ASSERT_EQ(mem_pool.init(1024,1), 0);
    ptr1 = mem_pool.malloc(256);
    ASSERT_NE(ptr1, (void*)0);
    ASSERT_EQ(mem_pool.get_block_num(),1);
    int32_t reset_size = 32;
    memset((char*)ptr1 - reset_size ,0,reset_size);
    mem_pool.free(ptr1);
  }
}

TEST(ObFixedMemPoolTest, basicTestNuma)
{
  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    char *direct_newed_ptr = new(std::nothrow)char[512];
    ASSERT_NE(direct_newed_ptr, reinterpret_cast<void*>(NULL));
    void *ptr = NULL;
    /// use when not initialized
    ObFixedMemPool mem_pool;
    mem_pool.free(direct_newed_ptr);
    ptr = mem_pool.malloc(1024);
    ASSERT_EQ(mem_pool.shrink(0),0);
    ASSERT_EQ(reinterpret_cast<void*>(0), ptr);
    ASSERT_EQ(mem_pool.get_block_num(), 0);
    ASSERT_EQ(mem_pool.get_used_block_num(), 0);
    ASSERT_EQ(mem_pool.get_memory_size_handled(), 0);
    /// initialize with invalid agument
    ASSERT_GT(0, mem_pool.init(1024,-1,NULL));
    ASSERT_GT(0, mem_pool.init(-1024,1,NULL));
    /// initialize
    ASSERT_EQ(0, mem_pool.init(1024,1,NULL));
    ASSERT_LT(mem_pool.init(1024,1,NULL),0);
    ASSERT_GT(mem_pool.get_block_size(),1024);
    ASSERT_EQ(mem_pool.malloc(-1), reinterpret_cast<void*>(0));
    /// allocate from system, handled by pool when free
    void *pool_handle_1 = mem_pool.malloc(1024);
    ASSERT_NE(pool_handle_1, reinterpret_cast<void*>(0));
    memset(pool_handle_1,0,1024);
    ASSERT_EQ(mem_pool.get_block_num(), 1);
    ASSERT_EQ(mem_pool.get_used_block_num(), 1);

    /// allocate from system, freed directly when free
    void *system_handle_1 = mem_pool.malloc(1025);
    memset(system_handle_1,0,1025);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);

    /// allocate from system, handled by pool when free
    void *pool_handle_2 = mem_pool.malloc(512);
    ASSERT_NE(pool_handle_2, reinterpret_cast<void*>(0));
    memset(pool_handle_1,0,512);
    ASSERT_EQ(mem_pool.get_block_num(), 3);
    ASSERT_EQ(mem_pool.get_used_block_num(), 3);

    /// check memory reuse
    mem_pool.free(pool_handle_2);
    ASSERT_EQ(mem_pool.get_block_num(), 3);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);
    /// allocate from free list, handled by pool when free
    void *pool_handle_3 = mem_pool.malloc(1024);
    ASSERT_NE(pool_handle_3, reinterpret_cast<void*>(0));
    memset(pool_handle_1,0,1024);
    ASSERT_EQ(mem_pool.get_block_num(), 3);
    ASSERT_EQ(mem_pool.get_used_block_num(), 3);
    ASSERT_EQ(pool_handle_3, pool_handle_2);

    /// check memory return to syste directly
    mem_pool.free(system_handle_1);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);

    /// shrink not work
    ASSERT_EQ(mem_pool.shrink(1), 0);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 2);

    mem_pool.free(pool_handle_1);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 1);

    mem_pool.free(pool_handle_3);
    ASSERT_EQ(mem_pool.get_block_num(), 2);
    ASSERT_EQ(mem_pool.get_used_block_num(), 0);

    /// shrink work
    mem_pool.shrink(1);
    ASSERT_EQ(mem_pool.get_block_num(), 0);
    ASSERT_EQ(mem_pool.get_used_block_num(), 0);
    {
      /// check with unfreed memory
      void *unfree_ptr = NULL;
      ObFixedMemPool unfree_pool;
      ASSERT_EQ(unfree_pool.init(1024,1,NULL), 0);
      unfree_ptr = unfree_pool.malloc(512);
      ASSERT_NE(unfree_ptr, reinterpret_cast<void*>(0));
    }
    {
      /// unfreed but clear
      /// check with unfreed memory
      void *unfree_ptr = NULL;
      ObFixedMemPool unfree_pool;
      ASSERT_EQ(unfree_pool.init(1024,1,NULL), 0);
      unfree_ptr = unfree_pool.malloc(512);
      ASSERT_NE(unfree_ptr, reinterpret_cast<void*>(0));
      unfree_pool.clear();
    }
  }
}

/// @fn memory overwrite
TEST(ObFixedMemPoolTest, OverWriteNuma)
{
  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    void *ptr1 = NULL;
    ObFixedMemPool mem_pool;
    ASSERT_EQ(mem_pool.init(1024,1,NULL), 0);
    ptr1 = mem_pool.malloc(256);
    ASSERT_NE(ptr1, (void*)0);
    ASSERT_EQ(mem_pool.get_block_num(),1);
    int32_t reset_size = 32;
    memset((char*)ptr1 - reset_size ,0,reset_size);
    mem_pool.free(ptr1);
  }
}

TEST(ObFixedMemPoolTest, directly)
{
  if (getenv("__OB_MALLOC_DIRECT__") != NULL)
  {
    void *ptr1 = NULL;
    ObFixedMemPool mem_pool;
    ASSERT_EQ(mem_pool.init(1024,1), 0);
    ptr1 = mem_pool.malloc(256);
    ASSERT_NE(ptr1, (void*)0);
    memset(ptr1,0,256);
    mem_pool.free(ptr1);
    ASSERT_EQ(mem_pool.get_block_num(),0);
  }
}


TEST(ObVarMemPoolTest, basicTest)
{
  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    ObVarMemPool mem_pool(2*1024*1024);
    void *temp = NULL;
    void *ptr[1024];
    void *system_handle_ptr = NULL;
    void *second_ptr = NULL;
    int64_t nbytes[1024];
    ASSERT_GT(mem_pool.get_block_size(), 2*1024*1024);
    ASSERT_EQ(mem_pool.malloc(-1), (void*)0);
    mem_pool.free(NULL);
    fprintf(stdout, "used=%ld\n", mem_pool.get_used_block_num());
    for (size_t i = 0; i < sizeof(nbytes)/sizeof(int64_t); i++)
    {
      nbytes[i] = rand()%2048+1;
      ptr[i] = mem_pool.malloc(nbytes[i]);
      ASSERT_NE(ptr[i], reinterpret_cast<void*>(0));
      memset(ptr[i],0,nbytes[i]);
    }
    fprintf(stdout, "used=%ld\n", mem_pool.get_used_block_num());
    ASSERT_EQ(mem_pool.get_block_num(),1);
    ASSERT_EQ(mem_pool.get_used_block_num(),1);

    /// allocate memory handled by system
    system_handle_ptr = mem_pool.malloc(1024*1024*10);
    memset(system_handle_ptr,0,1024*1024*10);
    ASSERT_NE(system_handle_ptr, reinterpret_cast<void*>(0));

    /// free system handled memory
    ASSERT_EQ(mem_pool.get_block_num(),2);
    ASSERT_EQ(mem_pool.get_used_block_num(),2);
    mem_pool.free(system_handle_ptr);
    ASSERT_EQ(mem_pool.get_block_num(),1);
    ASSERT_EQ(mem_pool.get_used_block_num(),1);

    /// shrink doesn't work
    ASSERT_EQ(mem_pool.shrink(0), 0);
    ASSERT_EQ(mem_pool.get_block_num(),1);
    ASSERT_EQ(mem_pool.get_used_block_num(),1);

    /// allocate a new block
    while (mem_pool.get_block_num() == 1)
    {
      temp = mem_pool.malloc(1024);
      memset(temp, 0,1024);
      mem_pool.free(temp);
    }
    second_ptr = mem_pool.malloc(1024);
    memset(second_ptr,0,1024);
    ASSERT_EQ(mem_pool.get_block_num(),2);
    ASSERT_EQ(mem_pool.get_used_block_num(),2);
    mem_pool.free(second_ptr);
    ASSERT_EQ(mem_pool.get_block_num(),2);
    ASSERT_EQ(mem_pool.get_used_block_num(),1);

    /// recycle block
    for (size_t i = 0; i < sizeof(nbytes)/sizeof(int64_t); i++)
    {
      mem_pool.free(ptr[i]);
    }
    ASSERT_EQ(mem_pool.get_block_num(),2);
    ASSERT_EQ(mem_pool.get_used_block_num(),0);

    {
      /// check with unfreed memory
      void *unfree_ptr1 = NULL;
      void *unfree_ptr2 = NULL;
      ObVarMemPool unfree_pool(-1);
      unfree_ptr1 = unfree_pool.malloc(64*1024-512);
      ASSERT_NE(unfree_ptr1, reinterpret_cast<void*>(0));
      unfree_ptr2 = unfree_pool.malloc(64*1024-512);
      ASSERT_NE(unfree_ptr2, reinterpret_cast<void*>(0));
    }
    {
      /// unfreed but clear
      /// check with unfreed memory
      void *unfree_ptr1 = NULL;
      void *unfree_ptr2 = NULL;
      void *ptr3 = NULL;
      ObVarMemPool unfree_pool(-1);
      ptr3 = unfree_pool.malloc(64*1024-512);
      ASSERT_NE(ptr3, reinterpret_cast<void*>(0));
      unfree_ptr1 = unfree_pool.malloc(64*1024-512);
      ASSERT_NE(unfree_ptr1, reinterpret_cast<void*>(0));
      unfree_ptr2 = unfree_pool.malloc(64*1024-512);
      ASSERT_NE(unfree_ptr2, reinterpret_cast<void*>(0));
      unfree_pool.free(ptr3);
      ASSERT_GT(unfree_pool.shrink(0),0);
      unfree_pool.clear();
    }
  }
}


/// @fn memory overwrite
TEST(ObVarMemPoolTest, OverWrite)
{
  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    void *ptr1 = NULL;
    void *ptr2 = NULL;
    ObVarMemPool mem_pool(-1);
    ptr1 = mem_pool.malloc(256);
    ASSERT_NE(ptr1, (void*)0);
    ptr2 = mem_pool.malloc(256);
    ASSERT_NE(ptr2, (void*)0);
    ASSERT_EQ(mem_pool.get_block_num(),1);
    memset(ptr1,0,256*2);
    mem_pool.free(ptr1);
    mem_pool.free(ptr2);
  }
}

TEST(ObVarMemPoolTest, directly)
{
  if (getenv("__OB_MALLOC_DIRECT__") != NULL)
  {
    void *ptr1 = NULL;
    ObVarMemPool mem_pool(-1);
    ptr1 = mem_pool.malloc(256);
    ASSERT_NE(ptr1, (void*)0);
    memset(ptr1,0,256);
    mem_pool.free(ptr1);
    ASSERT_EQ(mem_pool.get_block_num(),0);
  }
}

TEST(ob_malloc, basicTest)
{
  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    ASSERT_EQ(ob_init_memory_pool(1024*1024*2), 0);
    void *ptr1 = ob_malloc(2048, ObModIds::TEST);
    void *ptr2 = NULL;
    memset(ptr1,0,2048);
    ob_free(ptr1);
    ptr2 = ob_malloc(2048, ObModIds::TEST);
    ASSERT_EQ(ptr1, ptr2);
    memset(ptr2,0,2048);
    ob_safe_free(ptr2);
    ASSERT_EQ(ptr2, reinterpret_cast<void*>(0));

    /// allocate nbyte < 0
    ASSERT_EQ(ob_malloc(-1, ObModIds::TEST), reinterpret_cast<void*>(0));

    /// ObMemBuffer default constructor
    ObMemBuffer empty_buf;
    ASSERT_EQ(empty_buf.get_buffer(), reinterpret_cast<void*>(0));
    ASSERT_EQ(empty_buf.get_buffer_size(), 0);


    ObMemBuffer buf(1024);
    void *ptr3 = buf.get_buffer();
    ///used old buffer
    void *ptr4 = buf.malloc(512);
    ASSERT_EQ(ptr4, ptr3);
    ASSERT_EQ(buf.get_buffer_size(), 1024);
    /// free and allocate new buffer
    void *ptr5 = buf.malloc(1025);
    ASSERT_EQ(ptr5, ptr3);
    ASSERT_EQ(buf.get_buffer_size(), 1025);
  }

  if (getenv("__OB_MALLOC_DIRECT__") == NULL)
  {
    int64_t size = 512;
    int64_t meta_size = 48;
    int64_t block_size = 2*1024*1024 + meta_size; // 48 is meta info
    void *cell_array_ptr = NULL;
    void *cache_ptr = NULL;
    void *ptr = NULL;
    ob_free(cell_array_ptr,ObModIds::OB_MS_CELL_ARRAY);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_CELL_ARRAY), 0);

    ob_free(cache_ptr, ObModIds::OB_MS_LOCATION_CACHE);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_LOCATION_CACHE), 0);

    cell_array_ptr = ob_malloc(size, ObModIds::OB_MS_CELL_ARRAY);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_CELL_ARRAY), block_size);

    cache_ptr = ob_malloc(size, ObModIds::OB_MS_LOCATION_CACHE);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_LOCATION_CACHE), block_size);

    ptr = ob_malloc(size, ObModIds::TEST);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::TEST), block_size);

    ob_print_mod_memory_usage(true);


    EXPECT_EQ(ob_get_memory_size_direct_allocated(), 0);

    ob_free(cell_array_ptr,ObModIds::OB_MS_CELL_ARRAY);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_CELL_ARRAY), 0);

    ob_free(cache_ptr, ObModIds::OB_MS_LOCATION_CACHE);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_LOCATION_CACHE), 0);

    ob_free(ptr);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::TEST), 0);

    cache_ptr = ob_malloc(block_size,ObModIds::OB_MS_LOCATION_CACHE);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_LOCATION_CACHE), block_size+meta_size);
    EXPECT_EQ(ob_get_memory_size_direct_allocated(), block_size + meta_size);
    ob_free(cache_ptr, ObModIds::OB_MS_LOCATION_CACHE);
    EXPECT_EQ(ob_get_mod_memory_usage(ObModIds::OB_MS_LOCATION_CACHE), 0);
    EXPECT_EQ(ob_get_memory_size_direct_allocated(), 0);
  }
}

TEST(MemPoolTest, malloc_directly)
{
  if (getenv("__OB_MALLOC_DIRECT__") != NULL)
  {
    void *ptr = NULL;
    ObVarMemPool vmem_pool(2*1024*1024);
    ObFixedMemPool mem_pool;
    ASSERT_EQ(0, mem_pool.init(1024,1));
    /// allocate from system, handled by pool when free
    ptr = mem_pool.malloc(1024);
    mem_pool.free(ptr);
    ptr = vmem_pool.malloc(1024);
    vmem_pool.free(ptr);
  }

}



int main(int argc, char **argv)
{
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
