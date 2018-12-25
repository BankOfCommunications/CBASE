#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_INDEX_CACHE_H_ 
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_INDEX_CACHE_H_

#include "common/ob_define.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_fileinfo_manager.h"
#include "common/ob_range2.h"
#include "common/ob_record_header_v2.h"
#include "ob_sstable_block_index_mgr.h"

class TestSSTableBlockIndexCache_construct_Test;
class TestSSTableBlockIndexCache_init_Test;

namespace oceanbase
{
  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct CSBlockIndexCacheValueDeepCopyTagV2
      {
      };

      template <>
      struct traits<compactsstablev2::ObSSTableBlockIndexMgr>
      {
        typedef CSBlockIndexCacheValueDeepCopyTagV2 Tag;
      };
      
      inline compactsstablev2::ObSSTableBlockIndexMgr* do_copy(
          const compactsstablev2::ObSSTableBlockIndexMgr& other,
          char* buffer, CSBlockIndexCacheValueDeepCopyTagV2)
      {
        return other.copy(buffer);
      }

      inline int32_t do_size(
          const compactsstablev2::ObSSTableBlockIndexMgr& data,
          CSBlockIndexCacheValueDeepCopyTagV2)
      {
        return static_cast<int32_t>((
              const_cast<compactsstablev2::ObSSTableBlockIndexMgr&>(data)
              .get_size()));
      }

      inline void do_destroy(compactsstablev2::ObSSTableBlockIndexMgr* data,
          CSBlockIndexCacheValueDeepCopyTagV2)
      {
        UNUSED(data);
      }
    }//end namesapce KVStoreCacheComponent
  }//end namespace common

  namespace compactsstablev2
  {
    class ObSSTableBlockIndexCache
    {
    public:
      friend class ::TestSSTableBlockIndexCache_construct_Test;
      friend class ::TestSSTableBlockIndexCache_init_Test;

    private:
      static const int64_t KVCACHE_ITEM_SIZE = 8 * 1024;         //8k
      static const int64_t KVCACHE_BLOCK_SIZE = 2 * 1024 * 1024;  //2M

    public:
      typedef common::KeyValueCache<ObBlockIndexPositionInfo, 
              ObSSTableBlockIndexMgr, KVCACHE_ITEM_SIZE, 
              KVCACHE_BLOCK_SIZE> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObSSTableBlockIndexCache()
        : inited_(false),
          fileinfo_cache_(NULL)
      {
      }

      ObSSTableBlockIndexCache(common::IFileInfoMgr& fileinfo_cache)
        : inited_(false),
          fileinfo_cache_(&fileinfo_cache)
      {
      }

      ~ObSSTableBlockIndexCache()
      {
      }

      int init(const int64_t cache_mem_size);

      inline int clear()
      {
        return kv_cache_.clear();
      }

      inline int destroy()
      {
        int ret = common::OB_SUCCESS;
        ret = kv_cache_.destroy();
        if (common::OB_SUCCESS == ret)
        {
          inited_ = false;
          fileinfo_cache_ = NULL;
        }
        return ret;
      }

      inline void set_fileinfo_cache(common::IFileInfoMgr& fileinfo_cache)
      {
        fileinfo_cache_ = &fileinfo_cache;
      }

      int get_block_position_info(
          const ObBlockIndexPositionInfo& block_index_info,
          const uint64_t table_id, const common::ObRowkey& key,
          const SearchMode search_mode, ObBlockPositionInfos& pos_info);

      int get_block_position_info(
          const ObBlockIndexPositionInfo& block_index_info,
          const uint64_t table_id, const common::ObNewRange& range,
          const bool is_reverse_scan, ObBlockPositionInfos& pos_info);

      int get_single_block_pos_info(
          const ObBlockIndexPositionInfo& block_index_info,
          const uint64_t table_id, const common::ObRowkey &key,
          const SearchMode search_mode, ObBlockPositionInfo& pos_info);

      int next_offset(const ObBlockIndexPositionInfo& block_index_info,
          const uint64_t table_id, const int64_t cur_offset,
          const SearchMode search_mode, ObBlockPositionInfos& pos_info);

    private:
      int read_index_record(common::IFileInfoMgr& fileinfo_cache, 
                      const uint64_t sstable_id, 
                      const int64_t offset, 
                      const int64_t size, 
                      const char*& out_buffer);

      int read_endkey_record(common::IFileInfoMgr& fileinfo_cache, 
                      const uint64_t sstable_id, 
                      const int64_t offset, 
                      const int64_t size, 
                      const char*& out_buffer);

      int check_param(const ObBlockIndexPositionInfo& block_index_info,
                      const uint64_t table_id);

      int read_sstable_block_index(
          const ObBlockIndexPositionInfo& block_index_info,
          ObSSTableBlockIndexMgr& block_index, 
          const uint64_t table_id, Handle& handle);

      int load_block_index(const ObBlockIndexPositionInfo& block_index_info,
          ObSSTableBlockIndexMgr& block_index,
          const uint64_t table_id,
          Handle& handle);

    private:
      bool inited_;
      common::IFileInfoMgr* fileinfo_cache_;
      KVCache kv_cache_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase

#endif
