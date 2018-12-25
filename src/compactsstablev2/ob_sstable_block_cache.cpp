#include "common/ob_common_stat.h"
#include "ob_sstable_block_cache.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockCache::init(const int64_t cache_mem_size)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        TBSYS_LOG(INFO, "have inited");
      }
      else if (OB_SUCCESS != kv_cache_.init(cache_mem_size))
      {
        TBSYS_LOG(WARN, "kv cache init error:ret=%d,"
            "cache_mem_size=%ld", ret, cache_mem_size);
        ret = OB_ERROR;
      }
      else
      {
        inited_ = true;
        TBSYS_LOG_US(DEBUG, "init blockcache succ cache_mem_size=%ld, ", cache_mem_size);
      }

      return ret;
    }

    int ObSSTableBlockCache::advise(const uint64_t sstable_id,
        const ObBlockPositionInfos& block_infos,
        const uint64_t table_id,
        const bool copy2cache/*=false*/,
        const bool reverse_scan/*=false*/)
    {
      int ret = OB_SUCCESS;
      ObAIOBufferMgr* aio_buf_mgr = NULL;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "block cache is not inited");
        ret = OB_NOT_INIT;
      }
      else if (NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "fileinfo_cache_ is null");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_INVALID_ID == sstable_id || OB_INVALID_ID == table_id
          || 0 == table_id)
      {
        TBSYS_LOG(WARN, "table id is invalid:sstable_id=%lu, table_id=%lu", 
            sstable_id, table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (aio_buf_mgr = get_aio_buf_mgr(sstable_id, 
              table_id, true)))
      {
        TBSYS_LOG(WARN, "get aio buf mgr error:sstable_id=%ld,table_id=%ld",
            sstable_id, table_id);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = aio_buf_mgr->advise(*this, 
              sstable_id, block_infos, copy2cache, reverse_scan)))
      {
        TBSYS_LOG(WARN, "aio buf mgr advise error:ret=%d,sstable_id=%ld,"
            "block_infos.block_count_=%ld,copy2cache=%d,reverse_scan=%d",
            ret, sstable_id, block_infos.block_count_, 
            copy2cache, reverse_scan);
      }
      else
      {
      }

      return ret;
    }

    int32_t ObSSTableBlockCache::get_cache_block(const uint64_t sstable_id,
        const int64_t offset,
        const int64_t nbyte,
        ObSSTableBlockBufferHandle& buffer_handle)
    {
      int32_t ret = -1;
      ObSSTableBlockCacheKey data_index;
      ObSSTableBlockCacheValue value;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "not inited");
        ret = OB_NOT_INIT;
      }
      else if (NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "fileinfo_cache_ is null");
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == sstable_id || 0 > offset || 0 >= nbyte)
      {
        TBSYS_LOG(WARN, "invalid argument:sstable_id=%lu, "
            "offset=%lu, nbyte=%lu", sstable_id, offset, nbyte);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        data_index.sstable_id_ = sstable_id;
        data_index.offset_ = offset;
        data_index.size_ = nbyte;

        if (OB_SUCCESS == kv_cache_.get(data_index, value, 
              buffer_handle.handle_, true))
        {
          buffer_handle.block_cache_ = this;
          buffer_handle.buffer_ = value.buffer_;
          ret = static_cast<int32_t>(nbyte);
        }
      }

      return ret;
    }

    int ObSSTableBlockCache::get_block(const uint64_t sstable_id,
        const int64_t offset,
        const int64_t nbyte,
        ObSSTableBlockBufferHandle& buffer_handle,
        const uint64_t table_id,
        const bool check_crc/*=true*/)
    {
      int ret = -1;
      int status = OB_SUCCESS;
      const char* buffer = NULL;
      ObSSTableBlockCacheKey key;
      ObSSTableBlockCacheValue input_value;
      ObSSTableBlockCacheValue output_value;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "not inited");
        ret = OB_NOT_INIT;
      }
      else if (NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "file info cache is null");
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == sstable_id || offset < 0 || nbyte <= 0
          || OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid param:sstable_id=%lu, "
            "offset=%lu, nbyte=%lu, table_id=%lu",
            sstable_id, offset, nbyte, table_id);
      }
      else
      {
        key.sstable_id_ = sstable_id;
        key.offset_ = offset;
        key.size_ = nbyte;
        
        if (OB_SUCCESS == kv_cache_.get(key, output_value, 
                buffer_handle.handle_, false))
        {
          buffer_handle.block_cache_ = this;
          buffer_handle.buffer_ = output_value.buffer_;
          ret = static_cast<int>(nbyte);
#ifndef _SSTABLE_NO_STAT_
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_CACHE_HIT, 1);
#endif
        }
        else
        {
#ifndef _SSTABLE_NO_STAT_
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_CACHE_MISS, 1);
#endif
          status = read_record(*fileinfo_cache_, sstable_id, offset,
              nbyte, buffer);
          if (OB_SUCCESS == status && NULL != buffer)
          {
#ifndef _SSTABLE_NO_STAT_
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_NUM, 1);
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_BYTES, nbyte);
#endif
            input_value.nbyte_ = nbyte;
            input_value.buffer_ = const_cast<char*>(buffer);

            if (check_crc)
            {
              status = ObRecordHeaderV2::check_record(input_value.buffer_,
                  input_value.nbyte_, OB_SSTABLE_BLOCK_DATA_MAGIC);
              if (OB_SUCCESS != status)
              {
                TBSYS_LOG(WARN, "check record error:input_value.buffer_=%p,"
                    "input_value.nbyte_=%ld,OB_SSTABLE_BLOCK_DATA_MAGIC",
                    input_value.buffer_, input_value.nbyte_);
              }
            }

            if (OB_SUCCESS == status)
            {
              status = kv_cache_.put_and_fetch(key, input_value, output_value,
                  buffer_handle.handle_, false, false);
              if (OB_SUCCESS == status)
              {
                buffer_handle.block_cache_ = this;
                buffer_handle.buffer_ = output_value.buffer_;
                ret = static_cast<int>(nbyte);
              }
              else
              {
                TBSYS_LOG(WARN, "put and fetch error:key.sstable_id_=%lu,"
                    "key.offset_=%ld,key.size_=%ld,status=%d", 
                    key.sstable_id_, key.offset_, key.size_, status);
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "read record error:status=%d,fileinfo_cache=%p,"
                "sstable_id=%lu,offset=%ld,nbyte=%ld,buffer=%p", 
                status, fileinfo_cache_,
                sstable_id, offset, nbyte, buffer);
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockCache::get_block_readahead(const uint64_t sstable_id,
        const uint64_t table_id,
        const ObBlockPositionInfos& block_infos,
        const int64_t cursor,
        const bool is_reverse,
        ObSSTableBlockBufferHandle& buffer_handle,
        const bool check_crc/* =true */)
    {
      UNUSED(table_id);
      int ret = -1;
      int status = OB_SUCCESS;

      const char* record_buf = NULL;
      ObSSTableBlockCacheKey key;
      ObSSTableBlockCacheValue input_value;
      ObSSTableBlockCacheValue output_value;

      if (cursor < 0 || cursor >= block_infos.block_count_)
      {
        TBSYS_LOG(WARN, "INVALID argument:cursor=%ld,"
            "block_infos.block_count_=%ld", cursor, block_infos.block_count_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        const ObBlockPositionInfo& current_block 
          = block_infos.position_info_[cursor];
        key.sstable_id_ = sstable_id;
        key.offset_ = current_block.offset_;
        key.size_ = current_block.size_;

        if (OB_SUCCESS == kv_cache_.get(key, output_value, 
                buffer_handle.handle_, true))
        {
          buffer_handle.block_cache_ = this;
          buffer_handle.buffer_ = output_value.buffer_;
          ret = static_cast<int>(key.size_);
#ifndef _SSTABLE_NO_STAT_
          OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_CACHE_HIT, 1);
#endif
        }
        else
        {
          int64_t start_cursor = cursor;
          int64_t end_cursor = cursor;
          int64_t readahead_size = 0;
          int64_t inner_offset = 0;
          int64_t readahead_offset = 0;
          int64_t pos = 0;
          int64_t next_offset = block_infos.position_info_[cursor].offset_;

          if (!is_reverse)
          {
            for (pos = cursor; pos < block_infos.block_count_ 
                && readahead_size < MAX_READ_AHEAD_SIZE; ++ pos)
            {
              readahead_size += block_infos.position_info_[pos].size_;
              if (next_offset != block_infos.position_info_[pos].offset_)
              {
                TBSYS_LOG(WARN, "next offset error:next_offset=%ld,"
                    "pos=%ld,offset=%ld", next_offset, pos,
                    block_infos.position_info_[pos].offset_);
                ret= OB_ERROR;
                break;
              }
              else
              {
                next_offset += block_infos.position_info_[pos].size_;
              }
            }

            end_cursor = pos - 1;
          }
          else
          {
            for (pos = cursor; pos >= 0 
                && readahead_size < MAX_READ_AHEAD_SIZE; -- pos)
            {
              readahead_size += block_infos.position_info_[pos].size_;
              if (next_offset != block_infos.position_info_[pos].offset_)
              {
                TBSYS_LOG(WARN, "next offset error:next_offset=%ld"
                    "pos=%ld,offset=%ld", next_offset, pos,
                    block_infos.position_info_[pos].offset_);
                break;
              }
              else if (pos > 0)
              {
                next_offset -= block_infos.position_info_[pos - 1].size_;
              }
            }

            start_cursor = pos + 1;
          }

          if (OB_SUCCESS == status)
          {
            readahead_offset = block_infos.position_info_[start_cursor].offset_;
            status = read_record(*fileinfo_cache_, sstable_id,
                readahead_offset, readahead_size, record_buf);
#ifndef _SSTABLE_NO_STAT_
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_CACHE_MISS, 1);
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_NUM, 1);
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_BYTES, readahead_size);
#endif
            ObIOStat stat;
            stat.total_read_size_ = readahead_size;
            stat.total_read_times_ = 1;
            stat.total_read_blocks_ = end_cursor - start_cursor + 1;
            add_io_stat(stat);
          }

          if (OB_SUCCESS == status && NULL != record_buf)
          {
            for (int64_t i = start_cursor; i <= end_cursor; ++ i)
            {
              key.sstable_id_ = sstable_id;
              key.offset_ = block_infos.position_info_[i].offset_;
              key.size_ = block_infos.position_info_[i].size_;

              input_value.nbyte_ = block_infos.position_info_[i].size_;
              input_value.buffer_ = const_cast<char*>(record_buf) + inner_offset;

              if (check_crc)
              {
                status = ObRecordHeaderV2::check_record(input_value.buffer_,
                    input_value.nbyte_, OB_SSTABLE_BLOCK_DATA_MAGIC);
                if (OB_SUCCESS != status)
                {
                  TBSYS_LOG(WARN, "check record error:status=%d,"
                      "input_value.buffer_=%p, input_value.nbyte=%ld,"
                      "OB_SSTABLE_BLOCK_DATA_MAGIC", status,
                      input_value.buffer_, input_value.nbyte_);
                  break;
                }
              }

              if (cursor == i)
              {
                status = kv_cache_.put_and_fetch(key, input_value,
                    output_value, buffer_handle.handle_, false, false);
                if (OB_SUCCESS == status)
                {
                  buffer_handle.block_cache_ = this;
                  buffer_handle.buffer_ = output_value.buffer_;
                  ret = static_cast<int>(key.size_);
                }
                else
                {
                  TBSYS_LOG(WARN, "put and fetch error:status=%d,"
                      "key.sstable_id_=%lu,key.offset_=%ld,key.size_=%ld"
                      "input_value.buffer_=%p,input_value.nbyte_=%ld",
                      status, key.sstable_id_, key.offset_, key.size_,
                      input_value.buffer_, input_value.nbyte_);
                  break;
                }
              }
              else
              {
                kv_cache_.put(key, input_value, false);
              }
              inner_offset += block_infos.position_info_[i].size_;
            }//end for
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockCache::get_block_aio(const uint64_t sstable_id,
        const uint64_t offset,
        const int64_t nbyte,
        ObSSTableBlockBufferHandle& buffer_handle,
        const int64_t timeout_us,
        const uint64_t table_id,
        const bool check_crc/*ret= true*/)
    {
      int status = OB_SUCCESS;
      int32_t ret_size = -1;
      ObAIOBufferMgr* aio_buf_mgr = NULL;
      char* buffer = NULL;
      bool from_cache = false;
      
      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "invalid param:inited_=%d, fileinfo_cache_=%p",
            inited_, fileinfo_cache_);
      }
      else if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "table id error:table_id=%lu", table_id);
      }
      else if (NULL == (aio_buf_mgr = get_aio_buf_mgr(sstable_id, table_id)))
      {
        TBSYS_LOG(WARN, "get aio buf mgr error:aio_buf_mgr=%p,"
            "sstable_id=%lu,table_id=%lu", aio_buf_mgr, sstable_id, table_id);
      }
      else
      {
        status = aio_buf_mgr->get_block(*this, sstable_id, offset, nbyte,
            timeout_us, buffer, from_cache, check_crc);
        if (OB_SUCCESS == status && NULL != buffer)
        {
          ObSSTableBlockBufferHandle handle_tmp(buffer);
          buffer_handle = handle_tmp;
          ret_size = static_cast<int32_t>(nbyte);
          if (from_cache)
          {
#ifndef _SSTABLE_NO_STAT_
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_CACHE_HIT, 1);
#endif          
          }
          else
          {
#ifndef _SSTABLE_NO_STAT_
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_BLOCK_CACHE_MISS, 1);
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_NUM, 1);
            OB_STAT_TABLE_INC(SSTABLE, table_id, INDEX_DISK_IO_BYTES, nbyte);
#endif
          }
        }
        else
        {
          TBSYS_LOG(WARN, "get block error:status=%d,sstable_id=%lu,"
              "offset=%ld,nbyte=%ld,timeout_us=%ld,buffer=%p,from_cache=%d,"
              "check_crc=%d", status, sstable_id, offset, nbyte,
              timeout_us, buffer, from_cache, check_crc);
        }
      }
      
      return ret_size;
    }
    
    int ObSSTableBlockCache::read_record(IFileInfoMgr& fileinfo_cache,
        const uint64_t sstable_id,
        const int64_t offset,
        const int64_t size,
        const char*& out_buffer)
    {
      int ret = OB_SUCCESS;
      ObFileBuffer* file_buf = GET_TSI_MULT(ObFileBuffer, 
          TSI_COMPACTSSTABLEV2_FILE_BUFFER_1);
      out_buffer = NULL;

      if (NULL == file_buf)
      {
        TBSYS_LOG(WARN, "NULL==file_buf");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = ObFileReader::read_record(fileinfo_cache, 
              sstable_id, offset, size, *file_buf)))
      {
        TBSYS_LOG(WARN, "read record error:ret=%d"
            "sstable_id=%lu,offset=%ld,size=%ld,fiel_buf=%p",
            ret, sstable_id, offset, size, file_buf);
      }
      else
      {
        out_buffer = file_buf->get_buffer() + file_buf->get_base_pos();
      }

      return ret;
    }


    ObAIOBufferMgr* ObSSTableBlockCache::get_aio_buf_mgr(
        const uint64_t sstable_id,
        const uint64_t table_id,
        const bool free_mgr/*=false*/)
    {
      ObAIOBufferMgr* aio_buf_mgr = NULL;
      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = NULL;

      if (NULL == (aio_buf_mgr_array = GET_TSI_MULT(ObThreadAIOBufferMgrArray,
              TSI_COMPACTSSTABLEV2_THREAD_AIO_BUFFER_MGR_ARRAY_1)))
      {
        TBSYS_LOG(WARN, "GET_TSI_MULT error");
      }
      else if (NULL == (aio_buf_mgr = aio_buf_mgr_array->get_aio_buf_mgr(
              sstable_id, table_id, free_mgr)))
      {
        TBSYS_LOG(WARN, "get aio buf mgr error:sstable_id=%lu,"
            "table_id=%lu,free_mgr=%d", sstable_id, table_id, free_mgr);
      }

      return aio_buf_mgr;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
