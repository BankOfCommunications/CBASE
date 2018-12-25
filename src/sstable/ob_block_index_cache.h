/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_block_index_cache.h for block index cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_BLOCK_INDEX_CACHE_H_ 
#define OCEANBASE_SSTABLE_BLOCK_INDEX_CACHE_H_

#include "common/ob_define.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_fileinfo_manager.h"
#include "common/ob_range2.h"
#include "ob_sstable_block_index_v2.h"

namespace oceanbase
{
  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct CSBlockIndexCacheValueDeepCopyTag {};

      template <>
      struct traits<sstable::ObSSTableBlockIndexV2>
      {
        typedef CSBlockIndexCacheValueDeepCopyTag Tag;
      };

      inline sstable::ObSSTableBlockIndexV2* do_copy(
        const sstable::ObSSTableBlockIndexV2& other,
        char* buffer, CSBlockIndexCacheValueDeepCopyTag)
      {
        return other.deserialize_copy(buffer);
      }

      inline int32_t do_size(const sstable::ObSSTableBlockIndexV2& data,
                             CSBlockIndexCacheValueDeepCopyTag)
      {
        return static_cast<int32_t>((const_cast<sstable::ObSSTableBlockIndexV2&>(data).get_deserialize_size()));
      }

      inline void do_destroy(sstable::ObSSTableBlockIndexV2* data,
                             CSBlockIndexCacheValueDeepCopyTag)
      {
        UNUSED(data);
      }
    }
  }

  namespace sstable
  {
    class ObBlockIndexCache
    {
      /**
       * for chunkserver, generately, the sstable file size is about
       * 256MB, in most cases, the sstable size is greater than 128MB,
       * so there is a block index with about 300KB, usually the block
       * index size is greater than 150KB. here we define the block 
       * index size of each sstable is 8KB, it can ensure there are 
       * enough meta item in one memblock. if KVCACHE_ITEM_SIZE is too 
       * big, maybe the memblock has memory to store more block index, 
       * but there is no meta item left. it will waste memory. 
       *  
       * for UPS, the sstable file is very large but this 
       * configuration also fit, because the kv cache has supported 
       * large and variable memblock. 
       */
      static const int64_t KVCACHE_ITEM_SIZE = 8 * 1024;         //8k
      static const int64_t KVCACHE_BLOCK_SIZE = 2 * 1024 * 1024;  //2M

    public:
      typedef common::KeyValueCache<ObBlockIndexPositionInfo, ObSSTableBlockIndexV2, 
        KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObBlockIndexCache();
      explicit ObBlockIndexCache(common::IFileInfoMgr& fileinfo_cache);
      ~ObBlockIndexCache();

      int init(const int64_t cache_mem_size);
      int enlarg_cache_size(const int64_t cache_mem_size);

      /**
       * search block position info by key, return block position info 
       * array. 
       * 
       * @param block_index_info block index pos(offset, size) in 
       *                         sstable, represent by sstable
       *                         trailer.
       * @param table_id table id to search
       * @param column_group_id column group id to search
       * @param key key to search
       * @param search_mode search mode
       * @param pos_info search result, block position array
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_BEYOND_THE_RANGE or OB_IO_ERROR
       */
      int get_block_position_info(const ObBlockIndexPositionInfo& block_index_info,
                                  const uint64_t table_id,
                                  const uint64_t column_group_id,
                                  const common::ObRowkey& key,
                                  const SearchMode search_mode,
                                  ObBlockPositionInfos& pos_info);

      /**
       * search block position info by key range, return block 
       * position info array. 
       * 
       * @param block_index_info block index pos(offset, size) in 
       *                         sstable, represent by sstable
       *                         trailer.
       * @param table_id table id to search
       * @param column_group_id column group id to search
       * @param range key range to search
       * @param is_reverse_scan whether reverse scan
       * @param pos_info search result, block position array
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_BEYOND_THE_RANGE or OB_IO_ERROR
       */
      int get_block_position_info(const ObBlockIndexPositionInfo& block_index_info,
                                  const uint64_t table_id,
                                  const uint64_t column_group_id,
                                  const common::ObNewRange& range,
                                  const bool is_reverse_scan,
                                  ObBlockPositionInfos& pos_info);

      /**
       * search block position info by key, only return one block 
       * position info. 
       * 
       * @param block_index_info block index pos(offset, size) in 
       *                         sstable, represent by sstable
       *                         trailer.
       * @param table_id table id of block to search
       * @param column_group_id column group id of block to search
       * @param key key to search
       * @param search_mode search mode
       * @param pos_info search result, block position array
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_BEYOND_THE_RANGE or OB_IO_ERROR
       */
      int get_single_block_pos_info(const ObBlockIndexPositionInfo& block_index_info,
                                    const uint64_t table_id,
                                    const uint64_t column_group_id,
                                    const common::ObRowkey &key,
                                    const SearchMode search_mode,
                                    ObBlockPositionInfo& pos_info);
      
      /**
       * search next blocks in sstable, return block position info 
       * array.
       * 
       * @param block_index_info block index pos(offset, size) in 
       *                         sstable, represent by sstable
       *                         trailer.
       * @param table_id table id of block to search
       * @param column_group_id column group id of block to search
       * @param cur_offset current block offset
       * @param search_mode search mode
       * @param pos_info search result
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_BEYOND_THE_RANGE or OB_IO_ERROR
       */
      int next_offset(const ObBlockIndexPositionInfo& block_index_info,
                      const uint64_t table_id,
                      const uint64_t column_group_id,
                      const int64_t cur_offset,
                      const SearchMode search_mode, 
                      ObBlockPositionInfos& pos_info);

      /**
       * if using the default constructor, must call this function to 
       * set the file info cache 
       * 
       * @param fileinfo_cache finle info cache to set 
       */
      inline void set_fileinfo_cache(common::IFileInfoMgr& fileinfo_cache)
      {
        fileinfo_cache_ = &fileinfo_cache;
      }

      //clear all items in block index cache
      int clear();

      //destroy this cache in order to reuse it
      int destroy();

      inline int64_t get_cache_mem_size() { return kv_cache_.size(); }

      int get_end_key(const ObBlockIndexPositionInfo& block_index_info,
                      const uint64_t table_id, common::ObRowkey& row_key);

    private:
      int read_record(common::IFileInfoMgr& fileinfo_cache, 
                      const uint64_t sstable_id, 
                      const int64_t offset, 
                      const int64_t size, 
                      const char*& out_buffer);

      int check_param(const ObBlockIndexPositionInfo& block_index_info,
                      const uint64_t table_id,
                      const uint64_t column_group_id);

      /**
       * read and put block index into cache, then return block index
       * 
       * @param block_index_info block index pos(offset, size) in 
       *                         sstable, represent by sstable
       *                         trailer.
       * @param table_id table id of block to search 
       * @param block_index block index to return 
       * @param handle handle which holds the refference count
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int read_sstable_block_index(const ObBlockIndexPositionInfo& block_index_info,
                                   ObSSTableBlockIndexV2& block_index, 
                                   const uint64_t table_id,
                                   Handle& handle);

      int load_block_index(const ObBlockIndexPositionInfo& block_index_info,
                           ObSSTableBlockIndexV2& block_index,
                           const uint64_t table_id,
                           Handle& handle);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObBlockIndexCache);

      bool inited_;
      common::IFileInfoMgr* fileinfo_cache_;
      KVCache kv_cache_;
    };
  }
}

#endif //OCEANBASE_SSTABLE_BLOCK_INDEX_CACHE_H_
