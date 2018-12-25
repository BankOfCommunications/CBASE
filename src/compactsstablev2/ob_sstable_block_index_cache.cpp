#include "common/ob_file.h"
#include "ob_sstable_block_index_cache.h"
#include "common/ob_common_stat.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockIndexCache::init(const int64_t cache_mem_size)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        TBSYS_LOG(INFO, "have inited");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = kv_cache_.init(cache_mem_size)))
      {
        TBSYS_LOG(WARN, "init kv cache fail");
      }
      else
      {
        inited_ = true;
      }

      return ret;
    }
    
    int ObSSTableBlockIndexCache::get_block_position_info(
        const ObBlockIndexPositionInfo& block_index_info,
        const uint64_t table_id,
        const ObRowkey& key,
        const SearchMode search_mode,
        ObBlockPositionInfos& pos_info)
    {
      int ret = OB_SUCCESS;
      bool revert_handle = false;
      ObSSTableBlockIndexMgr block_index;
      Handle handle;
    
      if (OB_SUCCESS != (ret = check_param(block_index_info, table_id)))
      {
        TBSYS_LOG(ERROR, "check param error");
      }
      else if (is_regular_mode(search_mode) 
          && (NULL == key.ptr() || 0 >= key.length()) )
      {
        TBSYS_LOG(WARN, "invalid argumen");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = load_block_index(
              block_index_info, block_index, table_id, handle)) )
      {
        TBSYS_LOG(ERROR, "load block index error");
      }
      else
      {
        revert_handle = true;
        ret = block_index.search_batch_blocks_by_key(
            key, search_mode, pos_info);
      }
      
      if (revert_handle && OB_SUCCESS != kv_cache_.revert(handle))
      {
        TBSYS_LOG(WARN, "failed to revert  block index cache handle");
      }

      return ret;
    }

    int ObSSTableBlockIndexCache::get_block_position_info(
        const ObBlockIndexPositionInfo& block_index_info,
        const uint64_t table_id,
        const common::ObNewRange& range,
        const bool is_reverse_scan,
        ObBlockPositionInfos& pos_info)
    {
      int ret = OB_SUCCESS;
      bool revert_handle = false;
      ObSSTableBlockIndexMgr block_index;
      Handle handle;
    
      if ( OB_SUCCESS != (ret = check_param(block_index_info, table_id)))
      {
        TBSYS_LOG(ERROR, "check param error");
      }
      else if (OB_SUCCESS != (ret = load_block_index(
              block_index_info, block_index, table_id, handle)) )
      {
        TBSYS_LOG(ERROR, "load block index error");
      }
      else
      {
        revert_handle = true;
        ret = block_index.search_batch_blocks_by_range(
            range, is_reverse_scan, pos_info);
      }

      if (revert_handle && OB_SUCCESS != kv_cache_.revert(handle))
      {
        TBSYS_LOG(WARN, "kv cache revert error");
      }

      return ret;
    }

    int ObSSTableBlockIndexCache::get_single_block_pos_info(
        const ObBlockIndexPositionInfo& block_index_info,
        const uint64_t table_id,
        const ObRowkey& key,
        const SearchMode search_mode,
        ObBlockPositionInfo& pos_info)
    {
      int ret = OB_SUCCESS;
      bool revert_handle = false;
      ObSSTableBlockIndexMgr block_index;
      Handle handle;

      if (OB_SUCCESS != (ret = check_param(block_index_info, table_id)))
      {
        TBSYS_LOG(ERROR, "check param error");
      }
      else if (OB_SUCCESS != (ret = load_block_index(block_index_info, 
              block_index, table_id, handle)))
      {
        TBSYS_LOG(ERROR, "load block index error");
      }
      else
      {
        revert_handle = true;
        ret = block_index.search_one_block_by_key(key, search_mode, pos_info);
      }

      if (revert_handle && OB_SUCCESS != kv_cache_.revert(handle))
      {
        TBSYS_LOG(WARN, "kv cache revert error");
      }

      return ret;
    }

    int ObSSTableBlockIndexCache::next_offset(
        const ObBlockIndexPositionInfo& block_index_info,
        const uint64_t table_id,
        const int64_t cur_offset,
        const SearchMode search_mode, 
        ObBlockPositionInfos& pos_info)
    {
      int ret = OB_SUCCESS;
      bool revert_handle = false;
      ObSSTableBlockIndexMgr block_index;
      Handle handle;
    
      if (OB_SUCCESS != (ret = check_param(block_index_info, table_id)))
      {
        TBSYS_LOG(ERROR, "check param error");
      }
      else if (OB_SUCCESS != (ret = load_block_index(block_index_info, 
              block_index, table_id, handle)))
      {
        TBSYS_LOG(ERROR, "load block index error");
      }
      else
      {
        revert_handle = true;
        ret = block_index.search_batch_blocks_by_offset(cur_offset, 
            search_mode, pos_info);
      }

      if (revert_handle && OB_SUCCESS != kv_cache_.revert(handle))
      {
        TBSYS_LOG(WARN, "kv cache revert error");
      }

      return ret;
    }


    int ObSSTableBlockIndexCache::read_index_record(IFileInfoMgr& fileinfo_cache, 
        const uint64_t sstable_id, const int64_t offset, 
        const int64_t size, const char*& out_buffer)
    {
      int ret = OB_SUCCESS;
      ObFileBuffer* file_buf = GET_TSI_MULT(ObFileBuffer, 
          TSI_COMPACTSSTABLEV2_FILE_BUFFER_1);
      out_buffer = NULL;

      if (NULL == file_buf)
      {
        TBSYS_LOG(WARN, "get tsi mult error");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = ObFileReader::read_record(
              fileinfo_cache, sstable_id, offset, size, *file_buf)))
      {
        TBSYS_LOG(WARN, "read record error");
      }
      else
      {
        out_buffer = file_buf->get_buffer() + file_buf->get_base_pos();
      }

      return ret;
    }

    int ObSSTableBlockIndexCache::read_endkey_record(IFileInfoMgr& fileinfo_cache, 
        const uint64_t sstable_id, const int64_t offset, 
        const int64_t size, const char*& out_buffer)
    {
      int ret = OB_SUCCESS;
      ObFileBuffer* file_buf = GET_TSI_MULT(ObFileBuffer, 
          TSI_COMPACTSSTABLEV2_FILE_BUFFER_2);
      out_buffer = NULL;

      if (NULL == file_buf)
      {
        TBSYS_LOG(WARN, "get tsi mult error");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = ObFileReader::read_record(
              fileinfo_cache, sstable_id, offset, size, *file_buf)))
      {
        TBSYS_LOG(WARN, "read record error");
      }
      else
      {
        out_buffer = file_buf->get_buffer() + file_buf->get_base_pos();
      }

      return ret;
    }

    int ObSSTableBlockIndexCache::read_sstable_block_index(
        const ObBlockIndexPositionInfo& block_index_info,
        ObSSTableBlockIndexMgr& block_index, 
        const uint64_t table_id, Handle& handle)
    {
      int ret = OB_SUCCESS;
      int64_t read_index_offset = 0;
      int64_t read_index_size = 0;
      int64_t read_endkey_offset = 0;
      int64_t read_endkey_size = 0;
      int64_t block_count = 0;
      const char* record_index_buf = NULL;
      const char* record_endkey_buf = NULL;
      const char* index_payload_ptr = NULL;
      const char* endkey_payload_ptr = NULL;
      int64_t index_payload_size = 0;
      int64_t endkey_payload_size = 0;
      ObRecordHeaderV2 index_header;
      ObRecordHeaderV2 endkey_header;
      int64_t total_size = 0;
      ObSSTableBlockIndexMgr* tmp_buf = NULL;

      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "is not inited");
        ret = OB_NOT_INIT;
      }
      else if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "table id error:table_id=%lu", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        read_index_offset = block_index_info.index_offset_;
        read_index_size = block_index_info.index_size_;
        read_endkey_offset = block_index_info.endkey_offset_;
        read_endkey_size = block_index_info.endkey_size_;
        block_count = block_index_info.block_count_;
      }

      if (OB_SUCCESS == ret)
      {
        ret = read_index_record(*fileinfo_cache_, 
            block_index_info.sstable_file_id_,
            read_index_offset, read_index_size, record_index_buf);
        if (OB_SUCCESS != ret || NULL == record_index_buf)
        {
          TBSYS_LOG(WARN, "read index record error");
        }
        else 
        {
#ifndef _SSTABLE_NO_STAT_
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_NUM, 1);
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_BYTES, read_index_size);
#endif
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = ObRecordHeaderV2::check_record(record_index_buf, read_index_size,
            OB_SSTABLE_BLOCK_INDEX_MAGIC, index_header, index_payload_ptr,
            index_payload_size);
        if (OB_SUCCESS == ret)
        {
          if (index_header.is_compress())
          {
            TBSYS_LOG(ERROR, "dont support compress");
            ret = OB_ERROR;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "check record error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = read_endkey_record(*fileinfo_cache_, 
            block_index_info.sstable_file_id_,
            read_endkey_offset, read_endkey_size, record_endkey_buf);
        if (OB_SUCCESS != ret || NULL == record_endkey_buf)
        {
          TBSYS_LOG(WARN, "read endkey recored error");
        }
        else 
        {
#ifndef _SSTABLE_NO_STAT_
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_NUM, 1);
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_BYTES, read_endkey_size);
#endif
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = ObRecordHeaderV2::check_record(record_endkey_buf, read_endkey_size,
            OB_SSTABLE_BLOCK_ENDKEY_MAGIC, endkey_header, endkey_payload_ptr,
            endkey_payload_size);
        if (OB_SUCCESS == ret)
        {
          if (endkey_header.is_compress())
          {
            TBSYS_LOG(ERROR, "don not support compress");
            ret = OB_ERROR;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "check record error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        total_size = index_payload_size + endkey_payload_size
          + sizeof(ObSSTableBlockIndexMgr);
        ModuleArena* arena = GET_TSI_MULT(ModuleArena, 
            TSI_COMPACTSSTABLEV2_FILE_BUFFER_2);
        char* buffer = NULL;

        if (NULL == arena || NULL == (buffer = arena->alloc(total_size)))
        {
          TBSYS_LOG(WARN, "faile to alloc memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          memcpy(buffer + sizeof(ObSSTableBlockIndexMgr), index_payload_ptr,
              index_payload_size);
          memcpy(buffer + sizeof(ObSSTableBlockIndexMgr) + index_payload_size,
              endkey_payload_ptr, endkey_payload_size);
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == (tmp_buf = new(buffer)ObSSTableBlockIndexMgr(
                  index_payload_size, endkey_payload_size, block_count)))
          {
            TBSYS_LOG(WARN, "new ObSSTableBlockIndexMgr error");
            ret = OB_MEM_OVERFLOW;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = kv_cache_.put_and_fetch(
                block_index_info, *tmp_buf, block_index, 
                handle, false, false)))
        {
          TBSYS_LOG(WARN, "kv cache put and fetch error");
        }
      }

      return ret;
    }
    
    int ObSSTableBlockIndexCache::load_block_index(
        const ObBlockIndexPositionInfo& block_index_info,
        ObSSTableBlockIndexMgr& block_index,
        const uint64_t table_id, Handle& handle)
    {
      int ret = OB_SUCCESS;

      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "is not inited");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = kv_cache_.get(block_index_info, block_index, handle, false);
        if (OB_SUCCESS == ret)
        {
#ifndef _SSTABLE_NO_STAT_
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_INDEX_CACHE_HIT, 1);
#endif
        }
        else
        {
          if (OB_SUCCESS != (ret = read_sstable_block_index(
                  block_index_info, block_index, table_id, handle)))
          {
            TBSYS_LOG(WARN, "read sstable block index error");
          }
          else
          {
#ifndef _SSTABLE_NO_STAT_
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_INDEX_CACHE_MISS, 1);
#endif
          }
        }
      }
      
      return ret;
    }

    int ObSSTableBlockIndexCache::check_param(
        const ObBlockIndexPositionInfo& block_index_info,
        const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "ObSSTableBlockIndexCache is not init");
        ret = OB_NOT_INIT;
      }
      else if (NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "fileinfo_cache_ is NULL");
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == block_index_info.sstable_file_id_ 
          || 0 > block_index_info.index_offset_
          || 0 > block_index_info.index_size_ 
          || 0 > block_index_info.endkey_offset_
          || 0 > block_index_info.endkey_size_
          || 0 > block_index_info.block_count_
          || OB_INVALID_ID == table_id 
          || 0 == table_id)
      {
        TBSYS_LOG(WARN, "block index_info error");
        ret = OB_ERROR;
      }

      return ret;
    }
  } //end namespace sstable
} //end namespace oceanbase
