/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_slab.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#ifndef OCEANBASE_COMMON_OB_SLAB_H_
#define OCEANBASE_COMMON_OB_SLAB_H_

#include "tbsys.h"
#include "Mutex.h"
#include "Monitor.h"
#include "ob_define.h"
#include "ob_vector.h"
#include <sys/queue.h>
#include <assert.h>

#define OB_SLAB_DEBUG 1

namespace oceanbase
{
  namespace common
  {
    class ObSlab;
    class ObSlabItem;

    const int64_t OB_MAX_SLAB_NAME = 32;
    
    class ObSlabCache
    {
      public:
        ObSlabCache(const char *name,int64_t size,int64_t align = 8);
        ~ObSlabCache();

        char *slab_cache_alloc();
        void slab_cache_free(char *buf);
        int clear();
        const char *get_slab_name() const { return slab_name_;}

      public:

        int64_t get_obj_size() const { return obj_size_; }
        int64_t get_obj_per_slab() const { return obj_per_slab_;}

        int64_t get_slabs_full_num()    const { return slabs_full_num_;  }
        int64_t get_slabs_partial_num() const { return slabs_partial_num_;}
        int64_t get_slabs_free_num ()   const { return slabs_free_num_;  }
        int64_t get_total_inuse_objs()  const { return total_inuse_objs_;}
        int64_t get_alloc_slab_count()  const { return alloc_slab_count_;}
        int64_t get_free_slab_count ()  const { return free_slab_count_; }
        int64_t get_alloc_item_count()  const { return alloc_item_count_;}
        int64_t get_free_item_count ()  const { return free_item_count_; }
        int64_t get_total_memory()      const { return total_memory_; }

        void slab_for_each_free(ObSlab *slab) const;

      private:
        const static int64_t MAX_SLAB_SIZE = 1 << 20; //1M 
        ObSlab* alloc_new_slab();
        void cache_init_objs(ObSlab *slab);
        
        tbutil::Monitor<tbutil::Mutex> lock_;
        char slab_name_[OB_MAX_SLAB_NAME];
        LIST_HEAD(list_head,ObSlab);

        list_head slabs_full_;
        list_head slabs_partial_;
        list_head slabs_free_;

        int64_t obj_size_;
        int64_t obj_per_slab_;
        int64_t align_;

        //stat
        int64_t slabs_full_num_;
        int64_t slabs_partial_num_;
        int64_t slabs_free_num_;
        int64_t total_inuse_objs_;
        int64_t alloc_slab_count_;
        int64_t free_slab_count_;
        int64_t alloc_item_count_;
        int64_t free_item_count_;
        int64_t total_memory_;
    };

    
    class ObSlabCacheManager 
    {
      public:
        ObSlabCacheManager();
        ~ObSlabCacheManager();
        ObSlabCache* slab_cache_create(const char *name,int64_t size,int64_t align = 8);
        void slab_cache_destroy(ObSlabCache *slab);
        int64_t get_slab_cache_num() const;
      private:
        tbutil::Monitor<tbutil::Mutex> lock_;
        ObVector<ObSlabCache *> cache_list_;
        ObSlabCache cache_cache_;
    };

    
    const uint64_t OB_SLAB_MAGIC = 0x6153; //Sa
    const uint64_t OB_ITEM_MAGIC = 0x6C45; //El

    struct ObSlab
    {
      LIST_ENTRY(ObSlab) entries_;
      int64_t inuse_;
      ObSlabItem *free_;
#ifdef OB_SLAB_DEBUG
      uint64_t magic_;
#endif
      char mem_[0];
    };

    struct ObSlabItem 
    {
#ifdef OB_SLAB_DEBUG
      uint64_t magic_;
#endif
      ObSlab *slab_;
      union 
      {
        ObSlabItem *next_;
        char item_[0];
      }u_;
    };
    const int64_t OB_SLAB_ITEM_SIZE=sizeof(ObSlabItem) - sizeof(ObSlabItem *);

#ifdef OB_SLAB_DEBUG
#define OBSLAB_CHECK_SLAB(slab) assert((slab)->magic_ == OB_SLAB_MAGIC)
#define OBSLAB_CHECK_ITEM(item) assert((item)->magic_ == OB_ITEM_MAGIC)
#else
#define OBSLAB_CHECK_SLAB(slab) 
#define OBSLAB_CHECK_ITEM(item)
#endif

  } /* common */
} /* oceanbase */

#endif

