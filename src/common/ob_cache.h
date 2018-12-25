/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_cache.h is for what ...
 *
 * Version: $id: ob_cache.h,v 0.1 8/19/2010 1:42p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_COMMON_OB_CACHE_H_ 
#define OCEANBASE_COMMON_OB_CACHE_H_
#include "ob_define.h"
#include <pthread.h>
#include "ob_link.h"
#include "ob_memory_pool.h"
#include "ob_malloc.h"
#include "ob_string.h"
#include "murmur_hash.h"
#include "Time.h"
#include "ob_atomic.h"
#include "ob_range2.h"
#include "ob_spin_lock.h"

namespace oceanbase
{
  namespace common
  {
    class ObCacheBase;
    /// @class  <key,value> cache pair handler
    /// @author wushi(wushi.ly@taobao.com)  (8/19/2010)
    class ObCachePair
    {
    public:
      /// @fn constructors
      ObCachePair();
      /// @fn destructor
      ~ObCachePair();
      /// @fn get key
      oceanbase::common::ObString& get_key();
      /// @fn get key
      /// @warning 由于ObString没有支持深拷贝，因此这里返回const引用是无效的，仅仅作为提示
      const oceanbase::common::ObString& get_key() const;
      /// @fn get value
      oceanbase::common::ObString& get_value();
      /// @fn get value
      /// @warning 由于ObString没有支持深拷贝，因此这里返回const引用是无效的，仅仅作为提示
      const oceanbase::common::ObString& get_value() const;
    public:
      /// @fn initialize properties
      void init();
      /// @fn 归还cache
      void revert();
      /// @fn reinitialize
      void init(ObCacheBase &cache, char *key, const int32_t key_size, 
                char *value, const int32_t value_size, void *cache_item_handle);
    private:
      friend class ObCacheBase;
      DISALLOW_COPY_AND_ASSIGN(ObCachePair);

    private:
      /// @property handle of cache item
      void *cache_item_handle_;
      /// @property 
      ObCacheBase *cache_;
      /// @property cache pair info
      ObString key_;
      ObString value_;
    };

    /// @class  base class of memory cache
    /// @author wushi(wushi.ly@taobao.com)  (8/19/2010)
    class ObCacheBase
    {
    public:
      /// @fn virtual destructor
      ObCacheBase(int64_t mod_id);
      /// @fn virtual destructor
      virtual ~ObCacheBase();

      /// @fn int oceanbase/common/ObCacheBase::init(int64_t cache_mem_size, int64_t max_no_active_usec)
      ///   初始化 
      /// 
      /// @param cache_mem_size         cache最多可以使用的内存byte
      /// @param max_no_active_usec 
      ///   -# >=0 按照lru淘汰
      ///   -# <0 不被淘汰，此时cache_mem_size无含义，但是可以提示内存的使用
      /// @param hash_slot_num  hash槽数量
      /// 
      /// @return int 0 on success, < 0 for failure
      virtual int init(const int64_t cache_mem_size, const int64_t max_no_active_usec,
                       const int32_t hash_slot_num) = 0;

      /// @fn 分配内存存放新的pair
      virtual int malloc(ObCachePair &cache_pair, const int32_t key_size, 
                         const int32_t value_size) = 0;

      /// @fn 在cache中增加一项，调用该函数后cache_pair中的指针为空;
      ///       ObCachePair中的内存通过malloc获取
      virtual int add(ObCachePair & cache_pair, ObCachePair &old_pair) = 0;

      /// @fn 查询cache项
      /// @warning cache_pair持有指向cache内部的指针，此处应该get到const的cache_pair，
      ///          但是，暂时没有想到好的实现方法，因此使用者不能够修改得到的cache_pair
      virtual int get(const common::ObNewRange &key, ObCachePair &cache_pair) = 0;

      /// @fn 归还cache项目；cache_pair可以是通过malloc得到，也可以是通过get得到；
      ///       前一种情况出现在获取内存后产生cache_pair的数据的过程中失败，使用者无需
      ///       显示调用，ObCachePair的析构函数会自动调用
      virtual int revert(ObCachePair & cache_pair) = 0;

      /// @fn 删除cache项
      virtual int remove(const common::ObNewRange &key) = 0;

      /// @fn clear all cache item
      virtual int clear() = 0;

      /// @fn 增加cache item的引用计数
      /// virtual void inc_ref(void * cache_item_handle) = 0;

      /// @fn get the number of cache item in the cache
      virtual int64_t get_cache_item_num() = 0;

      /// @fn get memory sized used by the cache
      virtual int64_t get_cache_mem_size() = 0;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObCacheBase);
    protected:
      int64_t cache_mod_id_;
      tbsys::CThreadMutex mutex_;
    };

    /// @modified by xielun.szd@taobao.com
    /// @struct  lru item head
    struct LRUMemBlockHead
    {
      /// @property link memory block into lru list
      oceanbase::common::ObDLink lru_list_link_;
      /// @property the number of item on this block
      volatile int32_t item_num_;
      /// @property reference number
      uint32_t ref_num_;
      /// @property block size allocated
      int64_t block_size_;
      /// @property buffer addr
      char     buf_[0];

      void inc_item_num()
      {
        atomic_inc((uint32_t*)(&item_num_));
      } 
      void dec_item_num()
      {
        atomic_dec((uint32_t*)(&item_num_));
      }
      int32_t get_item_num()
      {
        return atomic_add((uint32_t*)&item_num_,0);
      }
      void inc_ref()
      {
        atomic_inc((uint32_t*)(&ref_num_));
      }
      void dec_ref()
      {
        atomic_dec((uint32_t*)(&ref_num_));
      }
      int32_t get_ref()
      {
        return atomic_add((uint32_t*)&ref_num_,0);
      }
    };

    /// @struct  cache item head
    struct CacheItemHead
    {
      /// @property magic number for error check, 8byte align
      uint32_t magic_;
      /// @property mother block offset (in bytes) relative to self
      int32_t  mother_block_offset_;
      /// @property link item into hash table
      oceanbase::common::ObDLink  hash_list_link_; 
      /// @property size of key
      int32_t key_size_;
      /// @property size of value
      int32_t value_size_;
      /// @property address of key
      char key_[0];

      LRUMemBlockHead *get_mother_block()
      {
        return(LRUMemBlockHead*)(((char*)this) +  mother_block_offset_);
      }
    };

    /// @class  cache特用的hash表
    /// @author wushi(wushi.ly@taobao.com)  (8/19/2010)
    /// @modified by xielun.szd@taobao.com)  (9/26/2010)
    /// @warning: the class not really a template class,
    ///    only for template ob_cache class abstraction
    template <class _key, class _value>
    class ObCHashTable
    {
    public:
      /// @enum error number
      enum ObCHashTableError
      {
        ERR_SUCCESS = 0,
      };
      /// @fn constructor 
      ObCHashTable()
      {
        hash_slot_num_  = 0;
        hash_slot_array_ = NULL;
        hash_item_num_ = 0; 
      }
      /// @fn destructor
      ~ObCHashTable()
      {
        for (int32_t slot_idx = 0; slot_idx < hash_slot_num_ && hash_slot_array_ != NULL; slot_idx ++)
        {
          pthread_mutex_destroy(hash_slot_mutex_array_ + slot_idx);
        }
        clear(); 
      }

      void clear() 
      {
        hash_slot_num_ = 0; 
        hash_slot_array_ = NULL; 
        hash_item_num_ = 0; 
      } 

      /// @fn initialize, hash_slot_num hash桶的数量
      int init(int32_t hash_slot_num)
      {
        int err = OB_SUCCESS;
        if (hash_slot_num <= 0)
        {
          TBSYS_LOG(WARN, "hash slot number must be greater than 0 [hash_slot_num:%d]", 
                    hash_slot_num );
          err = oceanbase::common::OB_INVALID_ARGUMENT;
        }
        if (hash_slot_array_ != NULL)
        {
          TBSYS_LOG(WARN, "hash table has been already initialized [hash_slot_array:%p]", 
                    hash_slot_array_ );
          err = oceanbase::common::OB_INIT_TWICE;
        }
        if (OB_SUCCESS ==  err )
        {
          hash_slot_buffer_.malloc(hash_slot_num*sizeof(oceanbase::common::ObDLink));
          hash_slot_mutex_buffer_.malloc(hash_slot_num*sizeof(pthread_mutex_t));
          if (hash_slot_buffer_.get_buffer() != NULL && hash_slot_mutex_buffer_.get_buffer() != NULL)
          {
            hash_slot_array_=new(hash_slot_buffer_.get_buffer())oceanbase::common::ObDLink[hash_slot_num];
            hash_slot_mutex_array_=reinterpret_cast<pthread_mutex_t*>(hash_slot_mutex_buffer_.get_buffer());
            hash_slot_num_ = hash_slot_num;
            for (int32_t slot_idx = 0; slot_idx < hash_slot_num_; slot_idx ++)
            {
              pthread_mutex_init(hash_slot_mutex_array_ + slot_idx, NULL);
            }
          }
          else
          {
            err = oceanbase::common::OB_ALLOCATE_MEMORY_FAILED;
          }
        }
        return err;
      }

      /// @fn add an item into the hashtable, 
      ///     if key already exist, delete and return old one
      int add(CacheItemHead & item, CacheItemHead *&old_item)
      {
        int err = OB_SUCCESS;
        oceanbase::common::ObString key;
        int32_t hash_slot_index = -1;
        old_item = NULL;
        key.assign(item.key_,item.key_size_);
        if (hash_slot_array_ == NULL)
        {
          err = oceanbase::common::OB_NOT_INIT;
          TBSYS_LOG(WARN, "hashtable not initialized");
        }
        if (OB_SUCCESS == err )
        {
          hash_slot_index = get_slot_idx(item);
          pthread_mutex_lock(hash_slot_mutex_array_+hash_slot_index);
          old_item = get(key,hash_slot_index);
          if (&item == old_item)
          {
            err = oceanbase::common::OB_ENTRY_EXIST;
            TBSYS_LOG(WARN, "add an exist item [item:%p]",old_item);
            old_item = NULL;
          }
          else if (old_item != NULL)
          {
            old_item->get_mother_block()->inc_ref();
            old_item->hash_list_link_.replace(item.hash_list_link_);
          }
          else
          {
            hash_slot_array_[hash_slot_index].insert_next(item.hash_list_link_);
            hash_item_num_ ++;
          }
          pthread_mutex_unlock(hash_slot_mutex_array_+hash_slot_index);
        }
        return err;
      }

      /// @fn get an item in the hash table
      CacheItemHead *get(const oceanbase::common::ObString & key)
      {
        int err = OB_SUCCESS;
        CacheItemHead * result = NULL;
        if (hash_slot_array_ == NULL)
        {
          err = oceanbase::common::OB_NOT_INIT;
          TBSYS_LOG(WARN, "hashtable not initialized");
        }
        if (OB_SUCCESS ==  err )
        {
          int32_t hash_slot_index = get_slot_idx(key);
          pthread_mutex_lock(hash_slot_mutex_array_+hash_slot_index);
          result = get(key,hash_slot_index);
          if (NULL != result)
          {
            result->get_mother_block()->inc_ref();
          }
          pthread_mutex_unlock(hash_slot_mutex_array_+hash_slot_index);
        }
        return result;
      }

      /// @fn remove an item from hash table
      CacheItemHead * remove(const oceanbase::common::ObString & key)
      {
        int err = OB_SUCCESS;
        CacheItemHead * result = NULL;
        if (hash_slot_array_ == NULL)
        {
          err = oceanbase::common::OB_NOT_INIT;
          TBSYS_LOG(WARN, "hashtable not initialized");
        }
        if (err == OB_SUCCESS)
        {
          int32_t hash_slot_index = get_slot_idx(key);
          pthread_mutex_lock(hash_slot_mutex_array_+hash_slot_index);
          result = get(key,hash_slot_index);
          if (result != NULL)
          {
            result->hash_list_link_.remove();
            hash_item_num_ --;
            result->get_mother_block()->inc_ref();
          }
          pthread_mutex_unlock(hash_slot_mutex_array_+hash_slot_index);
        }
        return result;
      }

      /// @fn remove the given item from hash table, make sure item really exists in table
      void remove(CacheItemHead &item)
      {
        int err = OB_SUCCESS;
        if (hash_slot_array_ == NULL)
        {
          err = oceanbase::common::OB_NOT_INIT;
          TBSYS_LOG(WARN, "hashtable not initialized");
        }

        // modified by xielun.szd (9/27/2010) 
        // check the node is empty before remove
        if ((OB_SUCCESS == err) && (!item.hash_list_link_.is_empty()))
        {
          uint32_t hash_slot_index  = get_slot_idx(item);
          pthread_mutex_lock(hash_slot_mutex_array_+hash_slot_index);
          item.hash_list_link_.remove();
          hash_item_num_ --;
          pthread_mutex_unlock(hash_slot_mutex_array_+hash_slot_index);
        }
      }

      /// @fn get the number of hash items
      int64_t get_item_num()
      {
        int64_t result  = 0;
        if (hash_slot_array_ == NULL)
        {
          TBSYS_LOG(WARN, "hashtable not initialized");
        }
        else
        {
          /// @warning not protected by mutex
          result = hash_item_num_;
        }
        return result;
      }

    private:
      DISALLOW_COPY_AND_ASSIGN(ObCHashTable);

    private:
      /// @fn caculate hash slot index
      uint32_t get_slot_idx(const CacheItemHead & item)
      {
        return oceanbase::common::murmurhash2(item.key_,item.key_size_,0)%hash_slot_num_;
      }

      uint32_t get_slot_idx(const oceanbase::common::ObString & key)
      {
        return oceanbase::common::murmurhash2(key.ptr(),key.length(),0)%hash_slot_num_;
      }
      /// @fn get an from an given hash slot
      CacheItemHead *get(const oceanbase::common::ObString & key, int32_t hash_slot_idx)
      {
        oceanbase::common::ObDLink *item_it = NULL;
        CacheItemHead * cur_item = NULL;
        CacheItemHead * result_item = NULL;
        item_it = hash_slot_array_[hash_slot_idx].next();
        while (item_it != (hash_slot_array_ + hash_slot_idx))
        {
          cur_item = CONTAINING_RECORD(item_it, CacheItemHead, hash_list_link_);
          if (key.length() == cur_item->key_size_ 
              && 0 == memcmp(key.ptr(), cur_item->key_, key.length()))
          {
            result_item = cur_item;
            break;
          }
          else
          {
            item_it = item_it->next();
          }
        }
        return result_item;
      }
      /// @property memory buffer for hash slot
      oceanbase::common::ObMemBuffer  hash_slot_buffer_;
      oceanbase::common::ObMemBuffer  hash_slot_mutex_buffer_;
      /// @property hash桶的数量
      int32_t                         hash_slot_num_;
      /// @property hash桶数组
      oceanbase::common::ObDLink    *hash_slot_array_;
      /// @property slot mutex array
      pthread_mutex_t               *hash_slot_mutex_array_;
      /// @property item的数量
      int64_t                         hash_item_num_;
    };

    /// @class  变长cache
    /// @author wushi(wushi.ly@taobao.com)  (8/19/2010)
    /// @modified by xielun.szd@taobao.com (9/26/2010)
    ///     modify to template class to support search engine plugin
    template <class key_ = int, class value_ = int, 
    template <class, class> class SearchEngine = ObCHashTable>
    class ObVarCache:public ObCacheBase
    {
    public:
      /// @fn constructor
      ObVarCache(int64_t cache_mod_id = oceanbase::common::ObModIds::OB_VARCACHE):
      ObCacheBase(cache_mod_id)
      {
        allocated_memory_size_ = 0;
        available_mem_size_ = 0;
        max_no_active_usec_ = 0;
        inited_ = false;
        cur_mem_block_ = NULL;
        cur_mem_block_offset_ = 0;
        total_ref_num_ = 0;
      }
      /// @fn destructor
      ~ObVarCache()
      {
        if (total_ref_num_ != 0)
        {
          TBSYS_LOG(ERROR, "some cache pairs not reverted");
        }
        clear();
        total_ref_num_ = 0;
        available_mem_size_ = 0;
        max_no_active_usec_ = 0;
        inited_ = false;
        cur_mem_block_ = NULL;
        cur_mem_block_offset_ = 0;
      }

      inline bool is_inited() const
      {
        return inited_;
      }

      inline int set_max_mem_size(const int64_t max_mem_size)
      {
        int err = OB_SUCCESS;
        if (max_mem_size <= 0)
        {
          err = oceanbase::common::OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "check max memory size failed:size[%ld]", max_mem_size);
        }
        else
        {
          available_mem_size_ = max_mem_size;
        }
        return err;
      }

      virtual int init(const int64_t cache_mem_size, const int64_t max_no_active_usec,
                       const int32_t hash_slot_num)
      {
        int err = OB_SUCCESS;
        if ((max_no_active_usec >= 0 && cache_mem_size <= 0)
            || hash_slot_num <= 0)
        {
          err = oceanbase::common::OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "param error [cache_mem_size:%ld,max_no_active_usec:%ld,"
                    "hash_slot_num:%d]", 
                    cache_mem_size, max_no_active_usec, hash_slot_num);
        }

        tbsys::CThreadGuard guard(&mutex_);
        if (OB_SUCCESS == err && inited_)
        {
          TBSYS_LOG(WARN, "cache has already been initialized");
          err = oceanbase::common::OB_INIT_TWICE;
        }
        if (OB_SUCCESS == err)
        {
          available_mem_size_ = cache_mem_size;
          max_no_active_usec_ = max_no_active_usec;
          if (max_no_active_usec_ > 0)
          {
            /// 现在没有实现基于时间的淘汰，因此不能大于0
            max_no_active_usec_ = 0;
          }
          err = hash_table_.init(hash_slot_num);
          if (OB_SUCCESS == err )
          {
            inited_ = true;
          }
          else
          {
            cur_mem_block_ = NULL;
            cur_mem_block_offset_ = 0;
            available_mem_size_ = 0;
            max_no_active_usec_ = 0;
            inited_ = false;
          }
        }
        return err;
      }

      virtual int malloc(ObCachePair &cache_pair, const int32_t key_size, 
                         const int32_t value_size)
      {
        int err = OB_SUCCESS;
        CacheItemHead *item = NULL;
        if (key_size <= 0 || value_size <= 0)
        {
          TBSYS_LOG(WARN, "param error [key_size:%d,value_size:%d]", key_size, value_size);
          err = oceanbase::common::OB_INVALID_ARGUMENT;
        }
        /// @warning 这里必须调用revert，否则会发生死锁
        cache_pair.revert();
        tbsys::CThreadGuard guard(&mutex_);
        if (OB_SUCCESS == err && !inited_)
        {
          TBSYS_LOG(WARN, "cache not initialized yet");
          err = oceanbase::common::OB_NOT_INIT;
        }
        if (OB_SUCCESS == err)
        {
          item = allocate_cache_item(key_size,value_size);
          if (item == NULL)
          {
            err = oceanbase::common::OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            cache_pair.init(*this, item->key_,key_size, item->key_ + item->key_size_, value_size, item);
          }
        }
        return err;
      }

      virtual int add(ObCachePair &cache_pair, ObCachePair &old_pair)
      {
        int err = OB_SUCCESS;
        CacheItemHead *item = NULL;
        CacheItemHead *old_item = NULL;
        oceanbase::common::ObString &key = cache_pair.get_key();
        if (key.ptr() == NULL)
        {
          err = oceanbase::common::OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "ObCachePair is empty [key:%p]", key.ptr());
        }
        else
        {
          item = reinterpret_cast<CacheItemHead *>(key.ptr() - sizeof(CacheItemHead));
          if (item->magic_ != CACHE_ITEM_MAGIC)
          {
            TBSYS_LOG(ERROR, "cache memory overflow [item_ptr:%p]", item);
            err = oceanbase::common::OB_MEM_OVERFLOW;
          }
        }
        /// @warning 这里必须调用revert，否则会发生死锁
        old_pair.revert();
        if (OB_SUCCESS == err && !inited_)
        {
          TBSYS_LOG(WARN, "cache not initialized yet");
          err = oceanbase::common::OB_NOT_INIT;
        }
        if (OB_SUCCESS == err)
        {
          /// this block has already been recycled
          tbsys::CThreadGuard guard(&mutex_);
          if (item->get_mother_block()->get_item_num() == 0)
          {
            CacheItemHead *new_item = allocate_cache_item(item->key_size_,item->value_size_);
            if (new_item == NULL)
            {
              err = oceanbase::common::OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
              memcpy(new_item->key_,item->key_, item->key_size_ + item->value_size_);
              new_item->get_mother_block()->dec_ref();
              /// @warning 不能够修改cache_pair的内容，因为调用者可能正在使用cache_pair的指针
              /* 
              cache_pair.revert();
              cache_pair.init(*this, new_item->key_, new_item->key_size_, 
                              new_item->key_ + new_item->key_size_, 
                              new_item->value_size_, new_item);
              */
              err = hash_table_.add(*new_item,old_item);
            }
          }
          else
          {
            err = hash_table_.add(*item,old_item);
          }
        }
        if (OB_SUCCESS == err && NULL != old_item)
        {
          inc_ref();
          old_pair.init(*this,old_item->key_,old_item->key_size_, old_item->key_ + old_item->key_size_,
                        old_item->value_size_, old_item);
        }
        wash_out();
        return err;
      }

      virtual int get(const common::ObNewRange &key, ObCachePair &cache_pair)
      {
        int err = OB_SUCCESS;
        CacheItemHead   *item = NULL;
        LRUMemBlockHead *block = NULL;
        /*
        if (key.ptr() == NULL || key.length() <= 0)
        {
          TBSYS_LOG(WARN, "argument error [key.ptr:%p,key.length:%d]",key.ptr(), key.length());
          err = oceanbase::common::OB_INVALID_ARGUMENT;
        }
        */
        /// @warning 这里必须调用revert，否则会发生死锁
        cache_pair.revert();
        if (OB_SUCCESS == err && !inited_)
        {
          TBSYS_LOG(WARN, "cache not initialized yet");
          err = oceanbase::common::OB_NOT_INIT;
        }
        if (OB_SUCCESS == err)
        {
          item = hash_table_.get(key);
          if (item == NULL)
          {
            err = oceanbase::common::OB_ENTRY_NOT_EXIST;
          }
          else
          {
            /// item->get_mother_block()-> inc_ref();
            inc_ref();
            cache_pair.init(*this,item->key_, item->key_size_, 
                            item->key_ + item->key_size_,item->value_size_, item);
            block = item->get_mother_block();
            /// adjust lru
            tbsys::CThreadGuard guard(&mutex_);
            if (block != cur_mem_block_ && 0 <= max_no_active_usec_)
            {
              block->lru_list_link_.remove();
              lru_list_.insert_next(block->lru_list_link_);
            }
          }
        }
        wash_out();
        return err;
      }

      virtual int revert(ObCachePair &cache_pair)
      {
        int err = OB_SUCCESS;
        CacheItemHead *item = NULL;
        oceanbase::common::ObString &key = cache_pair.get_key();
        if (key.ptr() == NULL)
        {
          err = oceanbase::common::OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "ObCachePair is empty [key:%p]", key.ptr());
        }
        else
        {
          item = reinterpret_cast<CacheItemHead *>(key.ptr() - sizeof(CacheItemHead));
          if (item->magic_ != CACHE_ITEM_MAGIC)
          {
            TBSYS_LOG(ERROR, "cache memory overflow [item_ptr:%p]", item);
            err = oceanbase::common::OB_MEM_OVERFLOW;
          }
        }
        if (OB_SUCCESS == err && !inited_)
        {
          TBSYS_LOG(WARN, "cache not initialized yet");
          err = oceanbase::common::OB_NOT_INIT;
        }
        if (OB_SUCCESS == err)
        {
          cache_pair.init();
          item->get_mother_block()->dec_ref();
          dec_ref();
        }
        return err;
      }

      /// virtual int remove(const ObString &key)
      /// {
      ///   int err = OB_SUCCESS;
      ///   CacheItemHead   *item = NULL;
      ///   if (key.ptr() == NULL || key.length() <= 0)
      ///   {
      ///     TBSYS_LOG(WARN, "argument error [key.ptr:%p,key.length:%d]",key.ptr(), key.length());
      ///     err = oceanbase::common::OB_INVALID_ARGUMENT;
      ///   }
      ///   ObSpinLockGuard guard(lock_);
      ///   if (OB_SUCCESS == err && !inited_)
      ///   {
      ///     TBSYS_LOG(WARN, "cache not initialized yet");
      ///     err = oceanbase::common::OB_NOT_INIT;
      ///   }
      ///   if ( OB_SUCCESS == err)
      ///   {
      ///     item = hash_table_.get(key);
      ///     if (item == NULL)
      ///     {
      ///       err = oceanbase::common::OB_ENTRY_NOT_EXIST;
      ///     }
      ///     else
      ///     {
      ///       /// @warning 这个地方item的引用计数可能不为0，但是没有关系，这里并没有
      ///       ///          从内存中删除
      ///       /// item->hash_list_link_.remove();
      ///       hash_table_.remove(*item);
      ///       item->get_mother_block()->dec_ref();
      ///     }
      ///   }
      ///   return err;
      /// }

      virtual int remove(const ObNewRange &key)
      {
        int err = OB_SUCCESS;
        CacheItemHead   *item = NULL;
        /*
        if (key.ptr() == NULL || key.length() <= 0)
        {
          TBSYS_LOG(WARN, "argument error [key.ptr:%p,key.length:%d]",key.ptr(), key.length());
          err = oceanbase::common::OB_INVALID_ARGUMENT;
        }
        */
        if (OB_SUCCESS == err && !inited_)
        {
          TBSYS_LOG(WARN, "cache not initialized yet");
          err = oceanbase::common::OB_NOT_INIT;
        }
        if ( OB_SUCCESS == err)
        {
          item = hash_table_.remove(key);
          if (item == NULL)
          {
            err = oceanbase::common::OB_ENTRY_NOT_EXIST;
          }
          else
          {
            /// @warning 这个地方item的引用计数可能不为0，但是没有关系，这里并没有
            ///          从内存中删除
            item->get_mother_block()->dec_ref();
          }
        }
        return err;
      }

      virtual int clear()
      {
        int err = OB_SUCCESS;
        tbsys::CThreadGuard guard(&mutex_);
        if (OB_SUCCESS == err && !inited_)
        {
          err = oceanbase::common::OB_NOT_INIT;
        }
        if (OB_SUCCESS == err)
        {
          oceanbase::common::ObDLink *block_it = NULL;
          block_it = lru_list_.next();
          while (OB_SUCCESS == err && block_it != &lru_list_)
          {
            LRUMemBlockHead *block = CONTAINING_RECORD(block_it, LRUMemBlockHead, lru_list_link_);
            block_it = block_it->next();
            recycle_block(block);
          }
          if (NULL != cur_mem_block_)
          {
            recycle_block(cur_mem_block_);
            if (NULL == cur_mem_block_ )
            {
              cur_mem_block_offset_ = 0;
            }
          }
          if (OB_SUCCESS == err 
              && (cur_mem_block_ != NULL || !lru_list_.is_empty()))
          {
            TBSYS_LOG(WARN, "there exist item not reverted, clear fail, please revert all items first");
            err = OB_REF_NUM_NOT_ZERO;
          }
        }
        allocated_memory_size_ = 0;
        return err;
      }

      int destroy()
      {
        int ret = OB_SUCCESS;

        if (inited_)
        {
          ret = clear();
          if (OB_SUCCESS == ret)
          {
            hash_table_.clear(); 
            total_ref_num_ = 0; 
            available_mem_size_ = 0; 
            max_no_active_usec_ = 0; 
            inited_ = false; 
            cur_mem_block_ = NULL; 
            cur_mem_block_offset_ = 0; 
          }
        }

        return ret;
      }

      /// virtual void inc_ref(void * cache_item_handle);
      virtual int64_t get_cache_item_num()
      {
        int64_t result = 0;
        if (!inited_)
        {
          TBSYS_LOG(WARN, "cache not initialized yet");
        }
        else
        {
          result = hash_table_.get_item_num();
        }
        return result;
      }

      virtual int64_t get_cache_mem_size()
      {
        int64_t result = 0;
        if (inited_)
        {
          result = allocated_memory_size_;
        }
        return result;
      }

    private:
      DISALLOW_COPY_AND_ASSIGN(ObVarCache);
      /// @property 每次lru淘汰的时候最多遍历这么多数量的item，防止每次遍历整个lru链表
      static const int32_t WASH_OUT_TRAVERS_MAX_ITEM_NUM = 16382;
      /// @property magic number
      static const unsigned int CACHE_ITEM_MAGIC = 0xf9a1c380;


      /// @property initialize LRUMemBlockHead
      void init_lru_mem_block_head(LRUMemBlockHead &block_head, int64_t block_size)
      {
        memset(&block_head, 0x00, sizeof(block_head));
        oceanbase::common::ObDLink *link = NULL;
        link = new(&block_head.lru_list_link_)oceanbase::common::ObDLink;
        block_head.block_size_ = block_size;
      }

      /// @fn initialize CacheItemHead
      void init_cache_item_head(CacheItemHead &head, const int32_t key_size, 
                                const int32_t value_size, LRUMemBlockHead & mother_block)
      {
        oceanbase::common::ObDLink *link = NULL;
        link = new(&head.hash_list_link_)oceanbase::common::ObDLink;
        head.key_size_ = key_size;
        head.magic_ = CACHE_ITEM_MAGIC;
        head.mother_block_offset_ = static_cast<int32_t>(reinterpret_cast<int64_t>(&mother_block) 
                                    - reinterpret_cast<int64_t>(&head) );
        head.value_size_ = value_size;
      }

    private:
      /// @fn recycle memory used by the block, set block to null on success
      bool recycle_block(LRUMemBlockHead *&block)
      {
        bool result = false;
        char *cur_buf_ptr = block->buf_;
        CacheItemHead *cur_item = NULL;
        while (block->get_item_num() > 0)
        {
          cur_item = reinterpret_cast<CacheItemHead*>(cur_buf_ptr);
          cur_buf_ptr += cur_item->key_size_ + cur_item->value_size_ + sizeof(CacheItemHead);
          // modified by xielun.szd (26/9/2010)
          // move check item.hash_list_link_.is_empty() into hash_table implemention
          // so the hashtable can plugin
          hash_table_.remove(*cur_item);
          block->dec_item_num();
        }
        if (block->get_ref() == 0)
        {
          block->lru_list_link_.remove();
          allocated_memory_size_ -= block->block_size_;
          ob_free(block);
          result  = true;
          block = NULL;
        }
        return result;
      }

      /// @fn 按照lru策略进行淘汰
      void wash_out()
      {
        static __thread uint64_t thread_call_times = 0; 
        static const uint64_t wash_out_frequence = 4095;
        thread_call_times ++;
        if (0 == (thread_call_times & wash_out_frequence))
        {
          /// need not to do wash out
          if (max_no_active_usec_ < 0)
          {
            /// return;
          }
          /// do lru wash out
          else if (max_no_active_usec_ == 0)
          {
            tbsys::CThreadGuard guard(&mutex_);
            LRUMemBlockHead *block = NULL;
            oceanbase::common::ObDLink * block_it = NULL;
            block_it = lru_list_.prev();
            int32_t traversed_item_num = -1;
            int64_t memory_size_handled = 0;
            int64_t size_recycled = 0;
            memory_size_handled  = allocated_memory_size_;
            while ( memory_size_handled - size_recycled > available_mem_size_
                    && block_it != &lru_list_
                    && (++ traversed_item_num) < WASH_OUT_TRAVERS_MAX_ITEM_NUM)
            {
              block = CONTAINING_RECORD(block_it, LRUMemBlockHead, lru_list_link_);
              block_it = block->lru_list_link_.prev();
              if (recycle_block(block))
              {
                size_recycled += CACHE_MEM_BLOCK_SIZE;
              }
            }
          }
        }
      }

      /// @fn allocate a new cache item from memory pool
      CacheItemHead *allocate_cache_item(const int32_t key_size, const int32_t value_size)
      {
        int err = OB_SUCCESS;
        CacheItemHead *item = NULL;
        LRUMemBlockHead *block = NULL;
        int32_t item_size = static_cast<int32_t>(sizeof(CacheItemHead) + key_size + value_size);
        int64_t block_size = item_size + sizeof(LRUMemBlockHead);
        /// 进行lru淘汰
        /// block size bigger than CACHE_MEM_BLOCK_SIZE, allocate new one
        if (block_size <= CACHE_MEM_BLOCK_SIZE)
        {
          block_size = CACHE_MEM_BLOCK_SIZE;
        }
        /// allocate from cur_mem_block_
        if ( NULL != cur_mem_block_
             && (item_size + cur_mem_block_offset_ +static_cast<int64_t>(sizeof(LRUMemBlockHead))) 
             <= CACHE_MEM_BLOCK_SIZE)
        {
          block = cur_mem_block_;
          item = reinterpret_cast<CacheItemHead*>(block->buf_ + cur_mem_block_offset_);
          block->inc_item_num();
          init_cache_item_head(*item,key_size,value_size, *cur_mem_block_);
          item->get_mother_block()->inc_ref();
          inc_ref();
          cur_mem_block_offset_ += item_size;
        }
        /// cur_mem_block_ doesn't have enough space, allocate new one
        if (OB_SUCCESS == err && NULL == item)
        {
          if (cur_mem_block_ != NULL)
          {
            /// push front lru
            lru_list_.insert_next(cur_mem_block_->lru_list_link_);
            cur_mem_block_ = NULL;
          }
          cur_mem_block_offset_ = 0;
          cur_mem_block_ = reinterpret_cast<LRUMemBlockHead*>( ob_malloc(block_size, static_cast<int32_t>(cache_mod_id_)));
          if (NULL == cur_mem_block_)
          {
            err = oceanbase::common::OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            init_lru_mem_block_head(*cur_mem_block_, block_size);
            allocated_memory_size_ += block_size;
            block = cur_mem_block_;
            item = reinterpret_cast<CacheItemHead*>(block->buf_ + cur_mem_block_offset_);
            block->inc_item_num();
            init_cache_item_head(*item,key_size,value_size, *cur_mem_block_);
            item->get_mother_block()->inc_ref();
            inc_ref();
            cur_mem_block_offset_ += item_size;
          }
        }
        return item;
      }

      void inc_ref()
      {
        atomic_inc(reinterpret_cast<uint64_t*>(&total_ref_num_));
      }
      void dec_ref()
      {
        atomic_dec(reinterpret_cast<uint64_t*>(&total_ref_num_));
      }

      /// @property allocated memory each time
      const static int64_t CACHE_MEM_BLOCK_SIZE = 2*1024*1024;

      /// @property plugin search engine index
      SearchEngine<key_, value_> hash_table_;

      oceanbase::common::ObDLink lru_list_;
      int64_t     allocated_memory_size_;
      int64_t     max_no_active_usec_;
      int64_t     available_mem_size_;
      bool        inited_;
      /// @property allocate memory from current memory block 
      LRUMemBlockHead *cur_mem_block_;
      /// @property allocate memory from this offset
      int64_t          cur_mem_block_offset_; 
      /// @property reference number of the cache
      int64_t          total_ref_num_;  
    };
  }
}


#endif /* COMMON_OB_CACHE_H_ */
