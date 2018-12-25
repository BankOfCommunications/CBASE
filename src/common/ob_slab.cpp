/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_slab.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_slab.h"
#include "ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    using namespace tbsys;
    using namespace tbutil;

    /*-----------------------------------------------------------------------------
     *  ObSlabCacheManager
     *-----------------------------------------------------------------------------*/
    ObSlabCacheManager::ObSlabCacheManager():cache_cache_("cache_cache",sizeof(ObSlabCache) )
    {
    }

    ObSlabCacheManager::~ObSlabCacheManager()
    {
    }

    ObSlabCache* ObSlabCacheManager::slab_cache_create(const char *name,int64_t size,int64_t align)
    {
      int err = OB_SUCCESS;
      ObSlabCache *cache_ = NULL;
      if (NULL == name || static_cast<int64_t>(strlen(name)) >= OB_MAX_SLAB_NAME)
      {
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err)
      {
        Monitor<Mutex>::Lock guard(lock_);
        for(ObVector<ObSlabCache *>::iterator it = cache_list_.begin(); it != cache_list_.end();++it)
        {
          const char *slab_name = (*it)->get_slab_name();

          if (strlen(name) == strlen(slab_name) && 
              0 == strncmp(name,slab_name,strlen(name)))
          {
            TBSYS_LOG(ERROR,"slab %s has been created",name);
            err = OB_ERROR;
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO,"create cache %s,size:%ld,align:%ld",name,size,align);
        char *buf = cache_cache_.slab_cache_alloc();
        if (NULL == buf)
        {
          TBSYS_LOG(ERROR,"alloc ObSlabCache failed");
          err = OB_ERROR;
        }
        else
        {
          cache_ = new (buf) ObSlabCache(name,size,align);
        }
      }

      if (OB_SUCCESS == err && cache_ != NULL)
      {
        cache_list_.push_back(cache_);
      }
      return cache_;
    }

    void ObSlabCacheManager::slab_cache_destroy(ObSlabCache *slab_cache)
    {
      Monitor<Mutex>::Lock guard(lock_);
      if (NULL == slab_cache || slab_cache->clear() != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"clear slab failed,cannot destroy");
      }
      else
      {
        ObVector<ObSlabCache *>::iterator it = cache_list_.begin();
        bool found = false;
        const char *name = slab_cache->get_slab_name();
        for(; it != cache_list_.end();++it)
        {
          const char *slab_name = (*it)->get_slab_name();
          if (strlen(name) == strlen(slab_name) && 
              0 == strncmp(name,slab_name,strlen(name)))
          {
            found = true;
            break;
          }
        }
        if (found)
        {
          cache_list_.remove(it);
          cache_cache_.slab_cache_free(reinterpret_cast<char *>(slab_cache));
        }
      }
    }

    int64_t ObSlabCacheManager::get_slab_cache_num() const
    {
      return cache_list_.size();
    }


    /*-----------------------------------------------------------------------------
     *  ObSlabCache
     *-----------------------------------------------------------------------------*/

    ObSlabCache::ObSlabCache(const char *name,int64_t size,int64_t align) :obj_size_(0),
                                                                             obj_per_slab_(0),
                                                                             align_(0),
                                                                             slabs_full_num_(0),
                                                                             slabs_partial_num_(0),
                                                                             slabs_free_num_(0),
                                                                             total_inuse_objs_(0),
                                                                             alloc_slab_count_(0),
                                                                             free_slab_count_(0),
                                                                             alloc_item_count_(0),
                                                                             free_item_count_(0)
    {
      int err = OB_SUCCESS;
      if (NULL == name || static_cast<int64_t>(strlen(name)) >= OB_MAX_SLAB_NAME)
      {
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err)
      {
        snprintf(slab_name_,strlen(name)+1,"%s",name);
        obj_size_ = (size + sizeof(ObSlabItem) + align - 1) & (~(align - 1)); 
        align_ = align;
        obj_per_slab_ =  (MAX_SLAB_SIZE - sizeof(ObSlab) ) / obj_size_;
        LIST_INIT(&slabs_partial_);
        LIST_INIT(&slabs_free_);
        LIST_INIT(&slabs_full_);
        TBSYS_LOG(INFO,"create slab %s success,obj_size_:%ld,obj_per_slab:%ld",slab_name_,obj_size_,obj_per_slab_);
      }
    }

    ObSlabCache::~ObSlabCache()
    {
      clear();
    }

    char* ObSlabCache::slab_cache_alloc()
    {
      ObSlab *slab = NULL;
      ObSlabItem *item = NULL;
      Monitor<Mutex>::Lock guard(lock_);

      ++alloc_item_count_;

      if ( !LIST_EMPTY(&slabs_partial_))
      {
        slab = LIST_FIRST(&slabs_partial_);
      }

      if (NULL == slab && !LIST_EMPTY(&slabs_free_))
      {
        slab = LIST_FIRST(&slabs_free_);
      }

      if (NULL == slab)
      {
        //slow path,alloc a new slab
        slab = alloc_new_slab();
      }

      if (NULL != slab && NULL != slab->free_)
      {
        OBSLAB_CHECK_SLAB(slab);
        item = slab->free_;
        OBSLAB_CHECK_ITEM(item);
        slab->free_ = item->u_.next_;
        item->u_.next_ = NULL;
        ++slab->inuse_;

        ++total_inuse_objs_;

        if (1 == slab->inuse_)
        {
          --slabs_free_num_;
          LIST_REMOVE(slab,entries_);
          LIST_INSERT_HEAD(&slabs_partial_,slab,entries_);
        }
        else if ( obj_per_slab_ == slab->inuse_)
        {
          LIST_REMOVE(slab,entries_);
          LIST_INSERT_HEAD(&slabs_full_,slab,entries_);
          ++slabs_full_num_;
        }
      }
      return item != NULL ? item->u_.item_ : NULL;
    }

    void ObSlabCache::slab_cache_free(char *buf)
    {
      ObSlabItem *item = reinterpret_cast<ObSlabItem *>(buf - OB_SLAB_ITEM_SIZE);
      if (NULL == buf || NULL == item->slab_)
      {
        TBSYS_LOG(ERROR,"data error");
      }
      else
      {
        Monitor<Mutex>::Lock guard(lock_);
        ObSlab *slab = item->slab_;
        OBSLAB_CHECK_ITEM(item);
        OBSLAB_CHECK_SLAB(slab);

        item->u_.next_ = slab->free_;
        slab->free_ = item;
        --slab->inuse_;
        --total_inuse_objs_;
        ++free_item_count_;
        if ( 0 == slab->inuse_)
        {
          --slabs_partial_num_;
          LIST_REMOVE(slab,entries_);
          ob_free(reinterpret_cast<char *>(slab));
          ++free_slab_count_;
          total_memory_ -= (obj_per_slab_ * obj_size_ + sizeof(ObSlab));
        }
        else if (obj_per_slab_ - 1 == slab->inuse_)
        {
          --slabs_full_num_;
          ++slabs_partial_num_;

          LIST_REMOVE(slab,entries_);
          LIST_INSERT_HEAD(&slabs_partial_,slab,entries_);
        }
      }
    }

    int ObSlabCache::clear()
    {
      int ret = OB_SUCCESS;
      
      Monitor<Mutex>::Lock guard(lock_);
      if ( !LIST_EMPTY(&slabs_partial_) || !LIST_EMPTY(&slabs_full_) )
      {
        TBSYS_LOG(ERROR,"this slab is not empty,cann't clear");
        ret = OB_ERROR;
      }
      else
      {
        ObSlab *slab = NULL;
        ObSlab *slab_next = NULL;
        if (!LIST_EMPTY(&slabs_free_))
        {
          slab = LIST_FIRST(&slabs_free_);
          for(;slab != NULL;)
          {
            slab_next = LIST_NEXT(slab,entries_);
            LIST_REMOVE(slab,entries_);
            ob_free(slab);
            slab = slab_next;
            
            --slabs_free_num_;
          }
        }
      }
      return ret;
    }

    ObSlab* ObSlabCache::alloc_new_slab()
    {
      int64_t len = obj_per_slab_ * obj_size_ + sizeof(ObSlab);
      ObSlab* slab = static_cast<ObSlab *>(ob_malloc(len, ObModIds::OB_SLAB));
      if (NULL != slab)
      {
        slab->inuse_ = 0;
#ifdef OB_SLAB_DEBUG
        slab->magic_ = OB_SLAB_MAGIC;
#endif
        cache_init_objs(slab);
        LIST_INSERT_HEAD(&slabs_free_,slab,entries_);

        //stat
        ++slabs_free_num_;
        ++alloc_slab_count_;
        total_memory_ += len;
      }
      return slab;
    }

    void ObSlabCache::slab_for_each_free(ObSlab *slab) const
    {
      if (NULL != slab)
      {
        ObSlabItem *item = slab->free_;
        for(;item != NULL; item = item->u_.next_)
        {
          TBSYS_LOG(DEBUG,"slab:%p,slab->next_:%p",item,item->u_.next_);
        }
      }
    }
    
    void ObSlabCache::cache_init_objs(ObSlab *slab)
    {
      if (NULL != slab)
      {
        ObSlabItem *item = NULL;
        slab->free_ = reinterpret_cast<ObSlabItem *>(slab->mem_);
        for(int64_t i=0;i < obj_per_slab_; ++i)
        {
          item = reinterpret_cast<ObSlabItem *>(slab->mem_ + i * obj_size_);
          item->slab_ = slab;
#ifdef OB_SLAB_DEBUG
          item->magic_ = OB_ITEM_MAGIC;
          //TBSYS_LOG(INFO,"slab:%p,slab->mem_:%p,obj_size_:%ld,item:%p,item->magic_:%lu",
           //   slab,slab->mem_,obj_size_,item,item->magic_);
#endif
          item->u_.next_ = reinterpret_cast<ObSlabItem *>(slab->mem_ + (i + 1) * obj_size_);
        }
        item->u_.next_ = NULL;
      }
    }
  } /* common */
  
} /* oceanbase */
