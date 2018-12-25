#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_CACHE_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_CACHE_H_

#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_fileinfo_manager.h"
#include "ob_sstable_aio_buffer_mgr.h"
#include "common/ob_record_header_v2.h"
#include "compactsstablev2/ob_sstable_block_index_mgr.h"
#include "common/ob_file.h"

class TestSSTableBlockCache_construction1_Test;
class TestSSTableBlockCache_construction2_Test;

namespace oceanbase
{
  namespace compactsstablev2
  {
    struct ObSSTableBlockCacheKey
    {
      uint64_t sstable_id_;
      int64_t offset_;
      int64_t size_;

      inline int64_t hash() const
      {
        return common::murmurhash2(this, sizeof(ObSSTableBlockCacheKey), 0);
      }

      inline bool operator == (const ObSSTableBlockCacheKey& other) const
      {
        return (sstable_id_ == other.sstable_id_
            && offset_ == other.offset_
            && size_ == other.size_);
      }
    };

    struct ObSSTableBlockCacheValue
    {
      ObSSTableBlockCacheValue()
        : buffer_(NULL),
          nbyte_(0)
      {
      }
      char* buffer_;
      int64_t nbyte_;
    };
  }//end namespace compactsstablev2

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct CSBlockCacheValueDeepCopyTagV2
      {
      };

      template <>
      struct traits<compactsstablev2::ObSSTableBlockCacheValue>
      {
        typedef CSBlockCacheValueDeepCopyTagV2 Tag;
      };

      inline compactsstablev2::ObSSTableBlockCacheValue* do_copy(
          const compactsstablev2::ObSSTableBlockCacheValue& other,
          char* buffer, CSBlockCacheValueDeepCopyTagV2)
      {
        compactsstablev2::ObSSTableBlockCacheValue* ret
          = (compactsstablev2::ObSSTableBlockCacheValue*)buffer;
        if (NULL != ret)
        {
          ret->nbyte_ = other.nbyte_;
          ret->buffer_ = buffer 
            + sizeof(compactsstablev2::ObSSTableBlockCacheValue);
          memcpy(ret->buffer_, other.buffer_, other.nbyte_);
        }
        return ret;
      }

      inline int32_t do_size(
          const compactsstablev2::ObSSTableBlockCacheValue& data,
          CSBlockCacheValueDeepCopyTagV2)
      {
        return static_cast<int32_t>(
            sizeof(compactsstablev2::ObSSTableBlockCacheValue) + data.nbyte_);
      }

      inline void do_destroy(compactsstablev2::ObSSTableBlockCacheValue* data,
                             CSBlockCacheValueDeepCopyTagV2)
      {
        UNUSED(data);
      }
    }//end namespace KVStroeCacheComponent
  }//end namespace common

  namespace compactsstablev2
  {
    class ObSSTableBlockBufferHandle;

    class ObSSTableBlockCache
    {
    public:
      friend class ::TestSSTableBlockCache_construction1_Test;
      friend class ::TestSSTableBlockCache_construction2_Test;
      friend class ObSSTableBlockBufferHandle;

      static const int64_t KVCACHE_ITEM_SIZE = 1 * 1024;
      static const int64_t KVCACHE_BLOCK_SIZE = 1024 * 1024L;
      static const int64_t MAX_READ_AHEAD_SIZE = 1024 * 1024L;

    public:
      typedef common::KeyValueCache<ObSSTableBlockCacheKey, 
              ObSSTableBlockCacheValue, 
              KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObSSTableBlockCache()
        : inited_(false),
          fileinfo_cache_(NULL)
      {
      }

      ObSSTableBlockCache(common::IFileInfoMgr& fileinfo_cache)
        : inited_(false),
          fileinfo_cache_(&fileinfo_cache)
      {
      }

      ~ObSSTableBlockCache()
      {
      }

      int init(const int64_t cache_mem_size);

      inline int destroy()
      {
        int ret = common::OB_SUCCESS;

        if (inited_)
        {
          if (common::OB_SUCCESS != (ret = kv_cache_.destroy()))
          {
            TBSYS_LOG(WARN, "kv cache destroy error:ret=%d", ret);
          }
          else
          {
            inited_ = false;
            fileinfo_cache_ = NULL;
          }
        }

        return ret;
      }

      inline const int64_t size() const
      {
        return kv_cache_.size();
      }

      inline int clear()
      {
        int ret = common::OB_SUCCESS;
        if (!inited_)
        {
          TBSYS_LOG(WARN, "class not inited");
          ret = common::OB_NOT_INIT;
        }
        else if (common::OB_SUCCESS != (ret = kv_cache_.clear()))
        {
          TBSYS_LOG(WARN, "kv cache clear error:ret=%d", ret);
          ret = common::OB_ERROR;
        }
        else
        {
        }
        return ret;
      }

      int advise(const uint64_t sstable_id,
          const ObBlockPositionInfos& block_infos,
          const uint64_t table_id,
          const bool copy2cache = false,
          const bool reverse_scan = false);

      int get_cache_block(const uint64_t sstable_id,
          const int64_t offset,
          const int64_t nbyte,
          ObSSTableBlockBufferHandle& buffer_handle);

      int get_block(const uint64_t sstable_id,
          const int64_t offset,
          const int64_t nbyte,
          ObSSTableBlockBufferHandle& buffer_handle,
          const uint64_t table_id,
          const bool check_crc = true);

      int get_block_readahead(const uint64_t sstable_id,
          const uint64_t table_id,
          const ObBlockPositionInfos& block_infos,
          const int64_t cursor,
          const bool is_reverse,
          ObSSTableBlockBufferHandle& buffer_handle,
          const bool check_crc = true);

      int get_block_aio(const uint64_t sstable_id,
          const uint64_t offset,
          const int64_t nbyte,
          ObSSTableBlockBufferHandle& buffer_handle,
          const int64_t timeout_us,
          const uint64_t table_id,
          const bool check_crc = true);

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

      inline KVCache& get_kv_cache()
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
          const bool free_mgr = false);

    private:
      bool inited_;
      common::IFileInfoMgr* fileinfo_cache_;
      KVCache kv_cache_;
    };

    class ObSSTableBlockBufferHandle
    {
      public:
        friend class ObSSTableBlockCache;

      public:
        ObSSTableBlockBufferHandle()
          : block_cache_(NULL),
          buffer_(NULL)
      {
      }

      ObSSTableBlockBufferHandle(const char* buffer)
        : block_cache_(NULL),
          buffer_(buffer)
      {
      }

      ~ObSSTableBlockBufferHandle()
      {
        if (NULL != block_cache_)
        {
          block_cache_->kv_cache_.revert(handle_);
        }
        block_cache_ = NULL;
        buffer_ = NULL;
      }

      ObSSTableBlockBufferHandle(const ObSSTableBlockBufferHandle& other)
      {
        *this = other;
      }

      inline void reset()
      {
        ObSSTableBlockBufferHandle tmp_handle;
        *this = tmp_handle;
      }
    
    public:
      const char* get_buffer() const
      {
        return buffer_;
      }

      ObSSTableBlockBufferHandle& operator = (const ObSSTableBlockBufferHandle& other)
      {
        ObSSTableBlockCache* blockcache_tmp = block_cache_;
        ObSSTableBlockCache::Handle handle_tmp = handle_;

        if (NULL != other.block_cache_)
        {
          if (common::OB_SUCCESS == 
              other.block_cache_->kv_cache_.dup_handle(other.handle_, handle_))
          {
            block_cache_ = other.block_cache_;
            buffer_= other.buffer_;
          }
          else
          {
            TBSYS_LOG(WARN, "kv cache dup handle error");
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
      }
      
    private:
      ObSSTableBlockCache* block_cache_;
      ObSSTableBlockCache::Handle handle_;
      const char* buffer_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif
