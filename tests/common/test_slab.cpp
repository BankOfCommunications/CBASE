/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_slab.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_memory_pool.h"
#include "common/ob_slab.h"
#include "common/ob_malloc.h"
using namespace oceanbase;
using namespace oceanbase::common;

TEST(ObSlab,create)
{
  ObSlabCacheManager slab_manager;
  ObSlabCache *slab_cache = slab_manager.slab_cache_create("test",63,8);
  ASSERT_TRUE(slab_cache != NULL);
  ASSERT_EQ(slab_manager.get_slab_cache_num(),1);
  
  ObSlabCache *slab_cache_1 = slab_manager.slab_cache_create("test",63,8);
  ASSERT_TRUE(slab_cache_1 == NULL);
  ASSERT_EQ(slab_manager.get_slab_cache_num(),1);

  slab_manager.slab_cache_destroy(slab_cache);
}

TEST(ObSlab,alloc)
{
  ObSlabCacheManager slab_manager;
  ObSlabCache *slab_cache = slab_manager.slab_cache_create("test",201,8);
  ASSERT_TRUE(slab_cache != NULL);
  ASSERT_EQ(slab_manager.get_slab_cache_num(),1);

  char *obj_list[slab_cache->get_obj_per_slab()];
 
  for(int32_t i = 0; i < slab_cache->get_obj_per_slab(); ++i) 
  {
    obj_list[i] = slab_cache->slab_cache_alloc();
    ASSERT_TRUE(obj_list[i] != NULL);
    OBSLAB_CHECK_ITEM(reinterpret_cast<ObSlabItem*>(obj_list[i]-OB_SLAB_ITEM_SIZE));
  }

  ASSERT_EQ(slab_cache->get_slabs_full_num()   , 1);
  ASSERT_EQ(slab_cache->get_slabs_partial_num(), 0);
  ASSERT_EQ(slab_cache->get_slabs_free_num ()  , 0);
  ASSERT_EQ(slab_cache->get_total_inuse_objs() , slab_cache->get_obj_per_slab());
  ASSERT_EQ(slab_cache->get_alloc_slab_count() , 1);
  ASSERT_EQ(slab_cache->get_free_slab_count () , 0);
  ASSERT_EQ(slab_cache->get_alloc_item_count() , slab_cache->get_obj_per_slab());
  ASSERT_EQ(slab_cache->get_free_item_count () , 0);

  for(int32_t i = 0; i < slab_cache->get_obj_per_slab(); ++i) 
  {
    slab_cache->slab_cache_free(obj_list[i] );
  }

  ASSERT_EQ(slab_cache->get_slabs_full_num()   , 0);
  ASSERT_EQ(slab_cache->get_slabs_partial_num(), 0);
  ASSERT_EQ(slab_cache->get_slabs_free_num ()  , 0);
  ASSERT_EQ(slab_cache->get_total_inuse_objs() , 0);
  ASSERT_EQ(slab_cache->get_alloc_slab_count() , 1);
  ASSERT_EQ(slab_cache->get_free_slab_count () , 1);
  ASSERT_EQ(slab_cache->get_alloc_item_count() , slab_cache->get_obj_per_slab());
  ASSERT_EQ(slab_cache->get_free_item_count () , slab_cache->get_obj_per_slab());

  for(int32_t i = 0; i < slab_cache->get_obj_per_slab(); ++i) 
  {
    obj_list[i] = slab_cache->slab_cache_alloc();
    ASSERT_TRUE(obj_list[i] != NULL);
    OBSLAB_CHECK_ITEM(reinterpret_cast<ObSlabItem*>(obj_list[i]-OB_SLAB_ITEM_SIZE));
  }

  ASSERT_EQ(slab_cache->get_slabs_full_num()   , 1);
  ASSERT_EQ(slab_cache->get_slabs_partial_num(), 0);
  ASSERT_EQ(slab_cache->get_slabs_free_num ()  , 0);
  ASSERT_EQ(slab_cache->get_total_inuse_objs() , slab_cache->get_obj_per_slab());
  ASSERT_EQ(slab_cache->get_alloc_slab_count() , 2);
  ASSERT_EQ(slab_cache->get_free_slab_count () , 1);
  ASSERT_EQ(slab_cache->get_alloc_item_count() , 2 * slab_cache->get_obj_per_slab());
  ASSERT_EQ(slab_cache->get_free_item_count () , slab_cache->get_obj_per_slab());


  for(int32_t i = 0; i < slab_cache->get_obj_per_slab(); ++i) 
  {
    slab_cache->slab_cache_free(obj_list[i] );
  }

  ASSERT_EQ(slab_cache->get_slabs_full_num()   , 0);
  ASSERT_EQ(slab_cache->get_slabs_partial_num(), 0);
  ASSERT_EQ(slab_cache->get_slabs_free_num ()  , 0);
  ASSERT_EQ(slab_cache->get_total_inuse_objs() , 0);
  ASSERT_EQ(slab_cache->get_alloc_slab_count() , 2);
  ASSERT_EQ(slab_cache->get_free_slab_count () , 2);
  ASSERT_EQ(slab_cache->get_alloc_item_count() , 2 * slab_cache->get_obj_per_slab());
  ASSERT_EQ(slab_cache->get_free_item_count () , 2 * slab_cache->get_obj_per_slab());

  slab_manager.slab_cache_destroy(slab_cache);
}

TEST(ObSlab,alloc_more)
{
  ObSlabCacheManager slab_manager;
  ObSlabCache *slab_cache = slab_manager.slab_cache_create("test",300,8);
  ASSERT_TRUE(slab_cache != NULL);
  ASSERT_EQ(slab_manager.get_slab_cache_num(),1);
  srand(static_cast<uint32_t>(time(NULL)));

  char *obj_list[100000];
 
  for(int32_t i = 0; i < 100000; ++i) 
  {
    obj_list[i] = slab_cache->slab_cache_alloc();
    ASSERT_TRUE(obj_list[i] != NULL);
    OBSLAB_CHECK_ITEM(reinterpret_cast<ObSlabItem*>(obj_list[i]-OB_SLAB_ITEM_SIZE));
  }

  ASSERT_EQ(slab_cache->get_total_inuse_objs() , 100000);
  ASSERT_EQ(slab_cache->get_alloc_item_count() , 100000);
  ASSERT_EQ(slab_cache->get_free_item_count () , 0);

  int32_t index = 0;
  for(int32_t i = 0; i < 10000; ++i) 
  {
    index = static_cast<int32_t>(random() % 100000);
    if (obj_list[index] != NULL)
    {
      slab_cache->slab_cache_free(obj_list[index] );
      obj_list[index] = NULL;
    }
  }

  ASSERT_EQ(slab_cache->get_alloc_item_count() , 100000);

  char* obj_list_2[20000];
  for(int32_t i = 0; i < 20000; ++i)
  {
    obj_list_2[i] = slab_cache->slab_cache_alloc();
    ASSERT_TRUE(obj_list_2[i] != NULL);
    OBSLAB_CHECK_ITEM(reinterpret_cast<ObSlabItem*>(obj_list_2[i]-OB_SLAB_ITEM_SIZE));
  }
 
  for(int32_t i = 0; i < 100000; ++i) 
  {
    if (obj_list [i] != NULL)
      slab_cache->slab_cache_free(obj_list[i] );
  }
  
  for(int32_t i = 0; i < 20000; ++i) 
  {
    if (obj_list_2 [i] != NULL)
      slab_cache->slab_cache_free(obj_list_2[i] );
  }
  
  slab_manager.slab_cache_destroy(slab_cache);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
