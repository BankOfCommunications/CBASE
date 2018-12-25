#include "ob_compact_sstable_reader.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObCompactSSTableReader::ObFileInfo::init(
        const char* sstable_fname, const bool use_dio)
    {
      int ret = OB_SUCCESS;

      destroy();

      if (NULL == sstable_fname)
      {
        TBSYS_LOG(WARN, "argument error:NULL==sstable_fname");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!use_dio && -1 == (fd_ = ::open(sstable_fname,
              FILE_OPEN_NORMAL_RFLAG)))
      {
        TBSYS_LOG(ERROR, "open file error:sstable_fname=%s", sstable_fname);
        ret = OB_IO_ERROR;
      }
      else if (use_dio && -1 == (fd_ = ::open(sstable_fname,
              FILE_OPEN_DIRECT_RFLAG)))
      {
        TBSYS_LOG(ERROR, "open file error:sstable_fname=%s", sstable_fname);
        ret = OB_IO_ERROR;
      }
      else
      {
        TBSYS_LOG(INFO, "open file:fd_=%d", fd_);
      }

      return ret;
    }

    int ObCompactSSTableReader::init(const uint64_t sstable_id)
    {
      int ret = OB_SUCCESS;
      const IFileInfo* file_info = NULL;
      ObSSTableTrailerOffset trailer_offset;
      sstable_id_ = sstable_id;

      if (is_inited_)
      {
        TBSYS_LOG(WARN, "init twice");
        ret = OB_INIT_TWICE;
      }
      else if (OB_INVALID_ID == sstable_id)
      {
        TBSYS_LOG(WARN, "OB_INVALID_ID == sstable_id");
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == (file_info = fileinfo_cache_.get_fileinfo(sstable_id)))
        {
          TBSYS_LOG(WARN, "get fileinfo error");
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = fetch_sstable_size(
                *file_info, sstable_size_)))
        {
          TBSYS_LOG(WARN, "fectch sstable size error:ret=%d", ret);
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = load_trailer_offset(
                *file_info, sstable_size_, trailer_offset)))
        {
          TBSYS_LOG(WARN, "load trailer offset error:ret=%d", ret);
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = load_trailer(*file_info,
                sstable_header_, trailer_offset,
                sstable_size_)))
        {
          TBSYS_LOG(WARN, "load trailer error:ret=%d", ret);
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = load_schema(*file_info,
                sstable_header_, sstable_schema_)))
        {
          TBSYS_LOG(WARN, "load schema error:ret=%d", ret);
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = load_table_index(
                *file_info, sstable_header_, table_index_)))
        {
          TBSYS_LOG(WARN, "load table index error:ret=%d", ret);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = load_table_bloom_filter(
                *file_info, sstable_header_, table_index_, bloom_filter_)))
        {
          TBSYS_LOG(WARN, "load table bloom filter error:ret=%d", ret);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = load_table_range(
                *file_info, sstable_header_, table_index_, table_range_)))
        {
          TBSYS_LOG(WARN, "load table range error:ret=%d", ret);
          ret = OB_ERROR;
        }
        else
        {
          is_inited_ = true;
        }

        fileinfo_cache_.revert_fileinfo(file_info);
      }

      return ret;
    }

    void ObCompactSSTableReader::reset()
    {
      is_inited_ = false;
      sstable_size_ = 0;
      sstable_id_ = 0;

      if (NULL != table_index_)
      {
        ObSSTableTableIndex* ptr = table_index_;
        for (int64_t i = 0; i < sstable_header_.table_count_; i ++)
        {
          ptr->~ObSSTableTableIndex();
          ptr ++;
        }
        table_index_ = NULL;
      }

      if (NULL != table_range_)
      {
        ObNewRange* ptr = table_range_;
        for (int64_t i = 0; i < sstable_header_.table_count_; i ++)
        {
          ptr->~ObNewRange();
          ptr ++;
        }
        table_range_ = NULL;
      }

      if (NULL != bloom_filter_)
      {
        ObBloomFilterV1* ptr = bloom_filter_;
        for (int64_t i = 0; i < sstable_header_.table_count_; i ++)
        {
          ptr->~ObBloomFilterV1();
          ptr ++;
        }
        bloom_filter_ = NULL;
      }

      if (NULL != sstable_schema_)
      {
        get_sstable_schema_cache().revert_schema(
            sstable_header_.first_table_id_, sstable_header_.major_version_);
      }

      memset(&sstable_header_, 0, sizeof(sstable_header_));
      enable_bloomfilter_ = false;

      if (NULL != compressor_)
      {
        destroy_compressor(compressor_);
        compressor_ = NULL;
      }

      arena_.free();
    }

    ObCompressor* ObCompactSSTableReader::get_decompressor()
    {
      const char* compressor_name = sstable_header_.compressor_name_;

      if (NULL == compressor_ && strlen(compressor_name) > 0)
      {
        compressor_ = create_compressor(compressor_name);
        if (NULL == compressor_)
        {
          TBSYS_LOG(WARN, "create compressor error:NULL==compressor_");
        }
      }
      return compressor_;
    }

    int ObCompactSSTableReader::fetch_sstable_size(const IFileInfo& file_info,
        int64_t& sstable_size)
    {
      int ret = OB_SUCCESS;

      sstable_size = get_file_size(file_info.get_fd());
      if (sstable_size < 0)
      {
        TBSYS_LOG(WARN, "get file size error:ret=%ld", sstable_size);
        ret = OB_IO_ERROR;
      }

      return ret;
    }

    int ObCompactSSTableReader::load_trailer_offset(
        const IFileInfo& file_info,
        const int64_t sstable_size,
        ObSSTableTrailerOffset& trailer_offset)
    {
      int ret = OB_SUCCESS;
      const char* record_buf = NULL;
      int64_t size = sizeof(ObSSTableTrailerOffset);
      int64_t offset = sstable_size - size;

      if (OB_SUCCESS != (ret = read_record(file_info,
              offset, size, record_buf)))
      {
        TBSYS_LOG(WARN, "read recrod error:ret=%d", ret);
      }
      else if (NULL != record_buf)
      {
        memcpy(&trailer_offset, record_buf, size);
      }

      return ret;
    }

    int ObCompactSSTableReader::load_trailer(const IFileInfo& file_info,
        ObSSTableHeader& sstable_header,
        const ObSSTableTrailerOffset& trailer_offset,
        const int64_t sstable_size)
    {
      int ret = OB_SUCCESS;
      const int64_t offset = trailer_offset.offset_;
      const int64_t size = sstable_size - trailer_offset.offset_
        - sizeof(ObSSTableTrailerOffset);
      const char* record_buf = NULL;
      const char* payload_ptr = NULL;
      int64_t payload_size = 0;
      ObRecordHeaderV2 record_header;

      if (offset < 0 || size <= 0)
      {
        TBSYS_LOG(WARN, "argument error:offset=%ld,size=%ld", offset, size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = read_record(file_info,
              offset, size, record_buf)))
      {
        TBSYS_LOG(WARN, "read record error:offset=%ld, size=%ld", offset, size);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = ObRecordHeaderV2::check_record(
              record_buf, size, OB_SSTABLE_HEADER_MAGIC,
              record_header, payload_ptr, payload_size)))
      {
        TBSYS_LOG(ERROR, "check record error:ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        if (!record_header.is_compress())
        {
          memcpy(&sstable_header, payload_ptr, payload_size);
        }
        else
        {
          TBSYS_LOG(WARN, "don not allow compress");
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObCompactSSTableReader::load_schema(const IFileInfo& file_info,
        const ObSSTableHeader& sstable_header,
        ObSSTableSchema*& schema)
    {
      int ret = OB_SUCCESS;
      int64_t record_offset = sstable_header.schema_record_offset_;
      int64_t record_size = sstable_header.schema_array_unit_count_
        * sstable_header.schema_array_unit_size_
        + sizeof(ObRecordHeaderV2);
      ObRecordHeaderV2 record_header;
      const char* payload_ptr = NULL;
      int64_t payload_size = 0;
      ObSSTableSchema* result_schema;
      const char* record_buf = NULL;

      uint64_t first_table_id = sstable_header.first_table_id_;
      int64_t version = sstable_header.major_version_;
      ObSSTableSchemaCache& schema_cache = get_sstable_schema_cache();

      if (record_offset < 0 || record_size <=0 || version < 0)
      {
        TBSYS_LOG(WARN, "ARGUMENT ERROR:record_offset=%ld, record_size=%ld, version=%ld",
            record_offset, record_size, version);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (NULL != (result_schema = schema_cache.get_schema(
                first_table_id, version)))
        {
          //TBSYS_LOG(WARN, "schema cache get schema error");
          //ret = OB_ERROR;
        }
        else
        {
          char* result_buf = static_cast<char*>(
            ob_malloc(sizeof(ObSSTableSchema), ObModIds::OB_SSTABLE_SCHEMA));
          if (NULL == result_buf)
          {
            TBSYS_LOG(WARN, "NULL == result_buf");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            result_schema = new(result_buf)ObSSTableSchema();
            if (NULL == result_schema)
            {
              ob_free(result_buf);
              TBSYS_LOG(WARN, "new ObSSTableSchema error");
              ret = OB_MEM_OVERFLOW;
            }
          }

          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = read_record(file_info,
                    record_offset, record_size, record_buf)))
            {
              TBSYS_LOG(WARN, "read record error:ret=%d", ret);
            }
            else if (NULL == record_buf)
            {
              TBSYS_LOG(WARN, "NULL == record_buf");
              ret = OB_ERROR;
            }
          }

          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = ObRecordHeaderV2::check_record(
                    record_buf, record_size, OB_SSTABLE_SCHEMA_MAGIC,
                    record_header, payload_ptr, payload_size)))
            {
              TBSYS_LOG(WARN, "check record error");
            }
            else
            {
              if (!record_header.is_compress())
              {
                const ObSSTableTableSchemaItem* tmp_buf =
                  reinterpret_cast<const ObSSTableTableSchemaItem*>(payload_ptr);
                int64_t tmp_count = sstable_header.schema_array_unit_count_;

                if (OB_SUCCESS != (ret = trans_to_sstable_schema(*result_schema,
                        tmp_buf, tmp_count)))
                {
                  TBSYS_LOG(WARN, "trans to sstable schema error");
                }
              }
              else
              {
                TBSYS_LOG(WARN, "do not support compress");
                ret = OB_ERROR;
              }
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = schema_cache.add_schema(result_schema, first_table_id,
                version);
            if (OB_ENTRY_EXIST == ret)
            {
              result_schema = schema_cache.get_schema(first_table_id, version);
              if (NULL == sstable_schema_)
              {
                TBSYS_LOG(WARN, "get schema error");
                ret = OB_ERROR;
              }
              else
              {
                ret = OB_SUCCESS;
              }
            }
            else if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "schema cache add schema error:ret=%d", ret);
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        schema = result_schema;
      }

      return ret;
    }//end function load_schema

    int ObCompactSSTableReader::load_table_index(const IFileInfo& file_info,
        const ObSSTableHeader& sstable_header,
        ObSSTableTableIndex*& table_index)
    {
      int ret = OB_SUCCESS;
      int64_t record_offset = sstable_header.table_index_offset_;
      int64_t record_size = sstable_header.table_index_unit_size_
        * sstable_header.table_count_ + sizeof(ObRecordHeaderV2);
      const char* payload_ptr = NULL;
      int64_t payload_size = 0;
      const char* record_buf = NULL;
      ObRecordHeaderV2 record_header;
      ObSSTableTableIndex* result_index = NULL;

      if (record_offset < 0 || record_size <= 0)
      {
        TBSYS_LOG(WARN, "argument error:record_offset=%ld, record_size=%ld",
            record_offset, record_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = read_record(file_info,
              record_offset, record_size, record_buf)))
      {
        TBSYS_LOG(WARN, "read record error:ret=%d", ret);
        ret = OB_ERROR;
      }
      else if (NULL != record_buf)
      {
        if (OB_SUCCESS != (ret = ObRecordHeaderV2::check_record(
                record_buf, record_size, OB_SSTABLE_TABLE_INDEX_MAGIC,
                record_header, payload_ptr, payload_size)))
        {
          TBSYS_LOG(WARN, "check record error:ret=%d,record_buf=%p,"
              "record_size=%ld,payload_ptr=%p,payload_size=%ld",
              ret, record_buf, record_size, payload_ptr, payload_size);
          ret = OB_ERROR;
        }
        else
        {
          if (!record_header.is_compress())
          {
            char* tmp_buf = arena_.alloc(payload_size);
            if (NULL == tmp_buf)
            {
              TBSYS_LOG(WARN, "failed to alloc memory for arena");
              ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
              result_index = new(tmp_buf)ObSSTableTableIndex();
              if (NULL == result_index)
              {
                TBSYS_LOG(WARN, "faile to new ObSSTableTableIndex");
                ret = OB_MEM_OVERFLOW;
              }
              else
              {
                memcpy(result_index, payload_ptr, payload_size);
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "do not support compress");
            ret = OB_ERROR;
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "can not go to here");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        table_index = result_index;
      }

      return ret;
    }


    int ObCompactSSTableReader::load_table_bloom_filter(
        const IFileInfo& file_info,
        const ObSSTableHeader& sstable_header,
        const ObSSTableTableIndex* table_index,
        ObBloomFilterV1*& bloom_filter)
    {
      int ret = OB_SUCCESS;
      int64_t record_offset = 0;
      int64_t record_size = 0;
      const char* payload_ptr = NULL;
      int64_t payload_size = 0;
      const char* record_buf = NULL;
      ObRecordHeaderV2 record_header;
      int64_t table_count = sstable_header.table_count_;
      ObBloomFilterV1* result_bloomfilter = NULL;

      if (NULL == table_index)
      {
        TBSYS_LOG(WARN, "table_index==NULL");
        ret = OB_ERROR;
      }
      else
      {
        int64_t tmp_size = table_count * sizeof(ObBloomFilterV1);
        ObBloomFilterV1* bloomfilter_ptr = NULL;
        char* tmp_buf = arena_.alloc(tmp_size);
        char* tmp_ptr = tmp_buf;
        if (NULL == tmp_buf)
        {
          TBSYS_LOG(WARN, "failed to alloce memory for arena");
        }
        else
        {
          for (int64_t i = 0; i < table_count; i ++)
          {
            bloomfilter_ptr = new(tmp_ptr)ObBloomFilterV1();
            if (NULL == bloomfilter_ptr)
            {
              TBSYS_LOG(WARN, "failed to new ObBloomFilterV1");
              ret = OB_ERROR;
              break;
            }
            else
            {
              tmp_ptr += sizeof(ObBloomFilterV1);
            }
          }

          if (OB_SUCCESS == ret)
          {
            result_bloomfilter = reinterpret_cast<ObBloomFilterV1*>(tmp_buf);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObSSTableTableIndex* table_index_ptr
          = const_cast<ObSSTableTableIndex*>(table_index);
        ObBloomFilterV1* bloom_filter_ptr = result_bloomfilter;
        for (int64_t i = 0; i < table_count; i ++)
        {
          record_offset = table_index_ptr->bloom_filter_offset_;
          record_size = table_index_ptr->bloom_filter_size_;
          if (OB_SUCCESS != (ret = read_record(file_info,
                  record_offset, record_size, record_buf)))
          {
            TBSYS_LOG(WARN, "read record error:ret=%d", ret);
            break;
          }
          else
          {
            ret = ObRecordHeaderV2::check_record(record_buf, record_size,
                OB_SSTABLE_TABLE_BLOOMFILTER_MAGIC, record_header,
                payload_ptr, payload_size);
            if (OB_SUCCESS == ret)
            {
              if (!record_header.is_compress())
              {
                if (OB_SUCCESS != (ret = bloom_filter_ptr->init(
                    table_index_ptr->bloom_filter_hash_count_,
                    payload_size)))
                {
                  TBSYS_LOG(WARN, "bloomfilter init error:ret=%d", ret);
                  break;
                }

                if (OB_SUCCESS == ret)
                {
                  const uint8_t* tmp_ptr
                    = reinterpret_cast<const uint8_t*>(payload_ptr);
                  if (OB_SUCCESS != (ret = bloom_filter_ptr->set_buffer(tmp_ptr,
                          payload_size)))
                  {
                    TBSYS_LOG(WARN, "bloomfilter reinit error:ret=%d", ret);
                    break;
                  }
                }
              }
              else
              {
                TBSYS_LOG(WARN, "check record error:ret=%d", ret);
                break;
              }
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        bloom_filter = result_bloomfilter;
      }

      return ret;
    }

    int ObCompactSSTableReader::load_table_range(
        const IFileInfo& file_info,
        const ObSSTableHeader& sstable_header,
        const ObSSTableTableIndex* table_index,
        ObNewRange*& table_range)
    {
      int ret = OB_SUCCESS;
      int64_t record_offset = 0;
      int64_t record_size = 0;
      const char* payload_ptr = NULL;
      int64_t payload_size = 0;
      const char* record_buf = NULL;
      ObRecordHeaderV2 record_header;
      int64_t table_count = sstable_header.table_count_;
      ObCompactStoreType store_type = static_cast<ObCompactStoreType>(
          sstable_header.row_store_type_);
      ObNewRange* result_range = NULL;

      if (NULL == table_index)
      {
        TBSYS_LOG(WARN, "invalid argument:NULL==table_index");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t tmp_size = table_count * sizeof(ObNewRange);
        char* tmp_buf = arena_.alloc(tmp_size);
        char* tmp_ptr = tmp_buf;
        ObNewRange* range_ptr;
        if (NULL == tmp_buf)
        {
          TBSYS_LOG(WARN, "faile to alloc memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          for (int64_t i = 0; i < table_count; i ++)
          {
            range_ptr = new(tmp_ptr)ObNewRange();
            if (NULL == range_ptr)
            {
              TBSYS_LOG(WARN, "new ObNewRange error");
              ret = OB_MEM_OVERFLOW;
              break;
            }
            else
            {
              tmp_ptr += sizeof(ObNewRange);
            }
          }

          if (OB_SUCCESS == ret)
          {
            result_range = reinterpret_cast<ObNewRange*>(tmp_buf);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        const ObSSTableTableIndex* table_index_ptr = table_index;
        ObNewRange* range_ptr = result_range;
        for (int64_t i = 0; i < table_count; i ++)
        {
          range_ptr->table_id_ = table_index_ptr->table_id_;
          //range_ptr->border_flag_.unset_inclusive_start();
          //range_ptr->border_flag_.set_inclusive_end();
          record_offset = table_index_ptr->range_keys_offset_;
          record_size = sizeof(int8_t)
            + table_index_ptr->range_start_key_length_
            + table_index_ptr->range_end_key_length_
            + sizeof(ObRecordHeaderV2);
          if (OB_SUCCESS != (ret = read_record(file_info,
                  record_offset, record_size, record_buf)))
          {
            TBSYS_LOG(WARN, "read record error");
            break;
          }
          else
          {
            ret = ObRecordHeaderV2::check_record(record_buf,
                record_size, OB_SSTABLE_TABLE_RANGE_MAGIC,
                record_header, payload_ptr, payload_size);
            if (OB_SUCCESS == ret)
            {
              if (!record_header.is_compress())
              {
                //border flag + start key string + end key string
                //+ start key obj array + end key obj array
                int64_t tmp_size2 = sizeof(int8_t)
                  + table_index_ptr->range_start_key_length_
                  + table_index_ptr->range_end_key_length_
                  + 2 * sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER;
                char* tmp_buf2 = NULL;
                tmp_buf2 = arena_.alloc(tmp_size2);
                if (NULL == tmp_buf2)
                {
                  TBSYS_LOG(WARN, "faile to alloc memory");
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                }
                else
                {
                  memcpy(tmp_buf2, payload_ptr, payload_size);
                  if (OB_SUCCESS != (ret = trans_to_table_range(
                          *range_ptr, store_type, tmp_buf2,
                          table_index_ptr->range_start_key_length_,
                          table_index_ptr->range_end_key_length_)))
                  {
                    TBSYS_LOG(WARN, "trans_to_table_range error");
                    break;
                  }
                  else
                  {
                    range_ptr ++;
                  }
                }
              }
            }
            else
            {
              TBSYS_LOG(WARN, "check record error");
              break;
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        table_range = result_range;
      }

      return ret;
    }

    int ObCompactSSTableReader::read_record(const IFileInfo& file_info,
        const int64_t offset, const int64_t size, const char*& out_buf)
    {
      int ret = OB_SUCCESS;
      ObFileBuffer* file_buf = GET_TSI_MULT(ObFileBuffer,
          TSI_COMPACTSSTABLEV2_FILE_BUFFER_1);
      out_buf = NULL;

      if (NULL == file_buf)
      {
        TBSYS_LOG(WARN, "get tsi mult error");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = ObFileReader::read_record(
              file_info, offset, size, *file_buf)))
      {
        TBSYS_LOG(WARN, "read record error");
        ret = OB_ERROR;
      }
      else
      {
        out_buf = file_buf->get_buffer() + file_buf->get_base_pos();
      }

      return ret;
    }

    int ObCompactSSTableReader::trans_to_sstable_schema(
        ObSSTableSchema& schema, const ObSSTableTableSchemaItem* item_array,
        int64_t item_count)
    {
      int ret = OB_SUCCESS;
      ObSSTableSchemaColumnDef def;

      if (NULL == item_array || 0 == item_count)
      {
        TBSYS_LOG(WARN, "invalid argument:item_array=%p, item_count=%ld", item_array, item_count);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        for (int64_t i = 0; i < item_count; i ++)
        {
          def.table_id_ = item_array[i].table_id_;
          def.column_id_ = item_array[i].column_id_;
          def.column_value_type_ = item_array[i].column_value_type_;
          def.offset_ = 0;
          def.rowkey_seq_ = item_array[i].rowkey_seq_;

          /*
          TBSYS_LOG(ERROR, "%lu,%lu,%d,%ld,%ld", def.table_id_,
              def.column_id_,def.column_value_type_,def.offset_,
              def.rowkey_seq_);
          */
          if (OB_SUCCESS != (ret = schema.add_column_def(def)))
          {
            TBSYS_LOG(WARN, "add column def error");
            break;
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableReader::trans_to_table_range(ObNewRange& range,
          const common::ObCompactStoreType store_type, const char* buf,
          const int64_t start_key_length, const int64_t end_key_length)
    {
      int ret = OB_SUCCESS;
      const ObObj* obj = NULL;
      ObCompactCellIterator row;
      bool is_row_finished = false;

      const char* start_key_ptr = buf + sizeof(int8_t);
      const char* end_key_ptr = buf + sizeof(int8_t) + start_key_length;

      const ObObj* tmp_buf = reinterpret_cast<const ObObj*>(
          buf + sizeof(int8_t) + start_key_length + end_key_length);
      ObObj* start_key_obj_array = const_cast<ObObj*>(tmp_buf);
      ObObj* end_key_obj_array = start_key_obj_array
        + sizeof(const ObObj) * sizeof(OB_MAX_ROWKEY_COLUMN_NUMBER);

      int64_t obj_cnt = 0;

      if (NULL == buf)
      {
        TBSYS_LOG(WARN, "invald argument:buf==NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = row.init(start_key_ptr, store_type)))
        {
          TBSYS_LOG(WARN, "row init error");
        }
        else
        {
          obj_cnt = 0;
          while(true)
          {
            if (OB_SUCCESS != (ret = row.next_cell()))
            {
              TBSYS_LOG(WARN, "row next cell error");
              break;
            }
            else
            {
              if (DENSE_SPARSE == store_type || DENSE_DENSE == store_type)
              {
                if (OB_SUCCESS != (ret = row.get_cell(obj, &is_row_finished)))
                {
                  TBSYS_LOG(WARN, "row get cell error");
                  break;
                }
                else
                {
                  if (is_row_finished)
                  {
                    range.start_key_.assign(start_key_obj_array, obj_cnt);
                    break;
                  }
                  else
                  {
                    start_key_obj_array[obj_cnt] = *obj;
                    obj_cnt ++;
                  }
                }
              }
              else
              {
                TBSYS_LOG(WARN, "can not go here");
                break;
              }
            }
          }
        }

        if (OB_SUCCESS != ret)
        {
        }
        else if (OB_SUCCESS != (ret = row.init(end_key_ptr, store_type)))
        {
          TBSYS_LOG(WARN, "row init error");
        }
        else
        {
          obj_cnt = 0;
          while(true)
          {
            if (OB_SUCCESS != (ret = row.next_cell()))
            {
              TBSYS_LOG(WARN, "row next cell error");
              break;
            }
            else
            {
              if (DENSE_SPARSE == store_type || DENSE_DENSE == store_type)
              {
                if (OB_SUCCESS != (ret = row.get_cell(obj, &is_row_finished)))
                {
                  TBSYS_LOG(WARN, "row get cell error");
                  break;
                }
                else
                {
                  if (is_row_finished)
                  {
                    range.end_key_.assign(end_key_obj_array, obj_cnt);
                    break;
                  }
                  else
                  {
                    end_key_obj_array[obj_cnt] = *obj;
                    obj_cnt ++;
                  }
                }
              }
              else
              {
                TBSYS_LOG(WARN, "can not go here");
                break;
              }
            }
          }
        }
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
