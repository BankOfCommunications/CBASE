/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_blockcache.h for block cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_SSTABLE_BLOCKCACHE_H_
#define  OCEANBASE_SSTABLE_BLOCKCACHE_H_

#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_fileinfo_manager.h"
#include "ob_aio_buffer_mgr.h"

namespace oceanbase
{
  namespace sstable
  {
    struct ObDataIndexKey
    {
      uint64_t sstable_id;
      int64_t offset;
      int64_t size;
      int64_t hash() const
      {
        return common::murmurhash2(this, sizeof(ObDataIndexKey), 0);
      };
      bool operator == (const ObDataIndexKey &other) const
      {
        return (sstable_id == other.sstable_id
                && offset == other.offset
                && size == other.size);
      };
    };

    struct BlockCacheValue
    {
      int64_t nbyte;
      char *buffer;
    };
  }

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct CSBlockCacheValueDeepCopyTag {};

      template <>
      struct traits<sstable::BlockCacheValue>
      {
        typedef CSBlockCacheValueDeepCopyTag Tag;
      };

      inline sstable::BlockCacheValue *do_copy(const sstable::BlockCacheValue &other,
                                               char *buffer, CSBlockCacheValueDeepCopyTag)
      {
        sstable::BlockCacheValue *ret = (sstable::BlockCacheValue*)buffer;
        if (NULL != ret)
        {
          ret->nbyte = other.nbyte;
          ret->buffer = buffer + sizeof(sstable::BlockCacheValue);
          memcpy(ret->buffer, other.buffer, other.nbyte);
        }
        return ret;
      }

      inline int32_t do_size(const sstable::BlockCacheValue &data,
                      CSBlockCacheValueDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(sstable::BlockCacheValue) + data.nbyte);
      }

      inline void do_destroy(sstable::BlockCacheValue *data,
                      CSBlockCacheValueDeepCopyTag)
      {
        UNUSED(data);
      }
    }
  }

  namespace sstable
  {
    class ObBufferHandle;

    class ObBlockCache
    {
      friend class ObBufferHandle;
      static const int64_t KVCACHE_ITEM_SIZE = 16 * 1024;      //16K
      static const int64_t KVCACHE_BLOCK_SIZE = 2 * 1024 * 1024L;  //2M
      static const int64_t MAX_READ_AHEAD_SIZE = 1024 * 1024L; //1M

    public:
      typedef common::KeyValueCache<ObDataIndexKey, BlockCacheValue, 
        KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObBlockCache();
      explicit ObBlockCache(common::IFileInfoMgr& fileinfo_cache);
      ~ObBlockCache();

      int init(const int64_t cache_mem_size);
      int enlarg_cache_size(const int64_t cache_mem_size);
      int destroy();
      const int64_t size() const;
      int clear();
      
      /**
       * first try to get block data from cache, if fail, sync read 
       * only one block data from sstable file. the function is called 
       * by get interface to get one block. 
       * 
       * @param sstable_id sstable id of sstable file to read
       * @param offset offset in sstable file to read
       * @param nbyte how much data to read from cache or sstable file
       * @param buffer_handle store the return block data buffer, and 
       *                      it will revert buffer handle automaticly
       * @param table_id table id 
       * @param check_crc whether check the block data record 
       * 
       * @return int32_t if success, return the read block data size, 
       *         else return -1
       */
      int32_t get_block(const uint64_t sstable_id,
                        const int64_t offset,
                        const int64_t nbyte,
                        ObBufferHandle& buffer_handle,
                        const uint64_t table_id,
                        const bool check_crc = true);

      /**
       * first try to get block data at %cursor from cache,
       * if cache missed, then read continuous blocks in 
       * %block_infos as one piece to save i/o count.
       *
       * @param sstable_id sstable id of sstable file to read
       * @param table_id  for Stat.
       * @param block_infos block position info (offset,size) array.
       * @param cursor index of block_infos get from cache
       * @param is_reverse read ahead blocks direction if true means
       *                   from down to top.
       * @param buffer_handle store the return block data buffer, and 
       *                      it will revert buffer handle automaticly
       * @param check_crc whether check the block data record 
       *
       * @return int32_t if success, return the read block data size, 
       *         else return -1
       */
      int32_t get_block_readahead(
          const uint64_t sstable_id,
          const uint64_t table_id,
          const ObBlockPositionInfos &block_infos,
          const int64_t cursor, 
          const bool is_reverse, 
          ObBufferHandle &buffer_handle,
          const bool check_crc = true);

      /**
       * try to get block data from cache, if success, return the 
       * block data size, else return -1. this function is used to get 
       * block data from cache, then we can copy the block data to aio 
       * buffer.
       * 
       * @param sstable_id sstable id of sstable file to read
       * @param offset offset in sstable file to read
       * @param nbyte how much data to read from cache 
       * @param buffer_handle store the return block data buffer, and 
       *                      it will revert buffer handle automaticly
       * 
       * @return int32_t if success, return the block data size, else 
       *         return -1
       */
      int32_t get_cache_block(const uint64_t sstable_id,
                              const int64_t offset,
                              const int64_t nbyte,
                              ObBufferHandle& buffer_handle);

      /**
       * when reading multiple continious blocks by aio, first call 
       * this function to tell aio buffer manager which continuous 
       * blocks need read. it make it's easy to preread blocks for aio
       * buffer. this function must be called before get_block_aio() 
       * function, and after call this function, call get_block_aio() 
       * function to get block one by one. 
       * 
       * @param sstable_id sstable id of sstable file to read
       * @param block_infos block position infos array
       * @param table_id table id of block to read
       * @param column_group_id column group id of block to read
       * @param copy2cache whether copy the block data from sstable 
       *                   file to block cache, and whether check
       *                   block data is in block cache, if it's true,
       *                   copy to cache and check cache
       * @param reverse_scan whether reverse return block
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int advise(const uint64_t sstable_id, 
                 const ObBlockPositionInfos& block_infos, 
                 const uint64_t table_id,
                 const uint64_t column_group_id,
                 const bool copy2cache = false,
                 const bool reverse_scan = false);

      /**
       * get continuous block from aio buffer manager, the order of 
       * blocks to get must be consistent with the order of blocks 
       * which advise() function specified. and if get_block success, 
       * don't get the same block again, otherwise, get_block_aio() 
       * will return error. this function can speed up block reading 
       * because it can preread the next blocks.
       * 
       * @param sstable_id sstable id of sstable file to read
       * @param offset offset in sstable file to read
       * @param nbyte how much data to read from cache or sstable file
       * @param buffer_handle store the return block data buffer, and 
       *                      it will revert buffer handle automaticly
       * @param timeout_us timeout in us
       * @param table_id table id of block to read
       * @param column_group_id column group id of block to read 
       * @param check_crc whether check the block data record 
       * 
       * @return int32_t if success, return the block data size, else 
       *         return -1
       */
      int32_t get_block_aio(const uint64_t sstable_id,
                            const int64_t offset,
                            const int64_t nbyte,
                            ObBufferHandle &buffer_handle,
                            const int64_t timeout_us,
                            const uint64_t table_id,
                            const uint64_t column_group_id,
                            const bool check_crc = true);

      
      /**
       * read block data from disk, and donot put into cache;
       */
      int32_t get_block_sync_io(const uint64_t sstable_id,
                        const int64_t offset,
                        const int64_t nbyte,
                        ObBufferHandle& buffer_handle,
                        const uint64_t table_id,
                        const bool check_crc = true);

      /**
       * get next block in block cache, it's used to traverse the 
       * block cache. 
       * 
       * @param data_index key of block data
       * @param buffer_handle store the return block data buffer, and 
       *                      it will revert buffer handle automaticly
       * 
       * @return int if success, returns OB_SUCCESS or OB_ITER_END, 
       *         else return OB_ERROR
       */
      int get_next_block(ObDataIndexKey &data_index, 
                         ObBufferHandle &buffer_handle);

      /**
       * if using the default constructor, must call this function to 
       * set the file info cache 
       * 
       * @param fileinfo_cache finle info cache to set= 
       */
      inline void set_fileinfo_cache(common::IFileInfoMgr& fileinfo_cache)
      {
        fileinfo_cache_ = &fileinfo_cache;
      }

      inline common::IFileInfoMgr& get_fileinfo_cache()
      {
        return *fileinfo_cache_;
      }

      inline KVCache &get_kv_cache()
      {
        return kv_cache_;
      }

    private:
      int read_record(common::IFileInfoMgr& fileinfo_cache, 
                      const uint64_t sstable_id, 
                      const int64_t offset, 
                      const int64_t size, 
                      const char*& out_buffer);

      ObAIOBufferMgr* get_aio_buf_mgr(const uint64_t sstable_id, 
                                      const uint64_t table_id, 
                                      const uint64_t column_group_id,
                                      const bool free_mgr = false);
      
    private:
      bool inited_;
      common::IFileInfoMgr* fileinfo_cache_;
      KVCache kv_cache_;
    };

    class ObBufferHandle
    {
      friend class ObBlockCache;

    public:
      ObBufferHandle() : block_cache_(NULL), buffer_(NULL)
      {
      };

      explicit ObBufferHandle(const char* buffer) : block_cache_(NULL), buffer_(buffer)
      {
      };

      ~ObBufferHandle()
      {
        if (NULL != block_cache_)
        {
          block_cache_->kv_cache_.revert(handle_);
        }
        block_cache_ = NULL;
        buffer_ = NULL;
      };

      explicit ObBufferHandle(const ObBufferHandle &other)
      {
        *this = other;
      };

      inline void reset()
      {
        ObBufferHandle tmp_handle;
        *this = tmp_handle;
      }

    public:
      const char *get_buffer() const
      {
        return buffer_;
      };

      ObBufferHandle &operator = (const ObBufferHandle &other)
      {
        ObBlockCache *blockcache_tmp = block_cache_;
        ObBlockCache::Handle handle_tmp = handle_;

        if (NULL != other.block_cache_)
        {
          if (common::OB_SUCCESS == other.block_cache_->kv_cache_.dup_handle(other.handle_, handle_))
          {
            block_cache_ = other.block_cache_;
            buffer_ = other.buffer_;
          }
          else
          {
            TBSYS_LOG(ERROR, "copy handle fail this_block_cache=%p this_buffer=%p "
                             "other_block_cache=%p other_buffer=%p",
                      block_cache_, buffer_, other.block_cache_, other.buffer_);
            block_cache_ = NULL;
            buffer_ = NULL;
          }
        }
        else
        {
          block_cache_ = other.block_cache_;
          handle_ = other.handle_;
          buffer_ = other.buffer_;
        }

        if (NULL != blockcache_tmp)
        {
          blockcache_tmp->kv_cache_.revert(handle_tmp);
        }
        
        return *this;
      };

    private:
      ObBlockCache *block_cache_;
      ObBlockCache::Handle handle_;
      const char *buffer_;
    };
  }
}

#endif //OCEANBASE_SSTABLE_BLOCKCACHE_H_
