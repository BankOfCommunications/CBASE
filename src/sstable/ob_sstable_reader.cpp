/**
 *  (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *     ObSSTableReader hold a sstable file object.
 */
#include "ob_sstable_reader.h"
#include "tbsys.h"
#include "common/ob_tsi_factory.h"
#include "common/utility.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/ob_record_header.h"
#include "common/ob_mod_define.h"
#include "common/ob_file.h"
#include "ob_sstable_writer.h" // for magic
#include "ob_disk_path.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

namespace oceanbase
{
  namespace sstable
  {
    ObSSTableReader::ObFileInfo::ObFileInfo()
    {
      fd_ = -1;
    }

    ObSSTableReader::ObFileInfo::~ObFileInfo()
    {
      destroy();
    }

    int ObSSTableReader::ObFileInfo::init(const char* sstable_fname, const bool use_dio)
    {
      int ret = OB_SUCCESS;

      destroy();
      if (NULL == sstable_fname)
      {
        TBSYS_LOG(WARN, "file name is NULL");
        ret = OB_ERROR;
      }
      else if (!use_dio && -1 == (fd_ = ::open(sstable_fname, FILE_OPEN_NORMAL_RFLAG)))
      {
        TBSYS_LOG(WARN, "normal open sstable file fail errno=%u, error_str=%s, "
                        "filename=%s",
                  errno, strerror(errno), sstable_fname);
        ret = OB_ERROR;
      }
      else if (use_dio && -1 == (fd_ = ::open(sstable_fname, FILE_OPEN_DIRECT_RFLAG)))
      {
        TBSYS_LOG(WARN, "direct open sstable file fail errno=%u, error_str=%s, "
                        "filename=%s",
                  errno, strerror(errno), sstable_fname);
        ret = OB_ERROR;
      }
      else
      {
        TBSYS_LOG(INFO, "open file succ fd=%d, sstable_filename=%s",
                  fd_, sstable_fname);
      }

      return ret;
    }

    void ObSSTableReader::ObFileInfo::destroy()
    {
      if (-1 != fd_)
      {
        close(fd_);
        fd_ = -1;
      }
    }

    int ObSSTableReader::ObFileInfo::get_fd() const
    {
      return fd_;
    }

    ObSSTableReader::ObSSTableReader(common::ModuleArena &arena,
                                     common::IFileInfoMgr& fileinfo_cache,
                                     tbsys::CThreadMutex* external_arena_mutex)
      : is_opened_(false), enable_bloom_filter_(true), use_external_arena_(true),
      sstable_size_(0), schema_(NULL), compressor_(NULL), bloom_filter_(NULL), sstable_id_(),
      mod_(ObModIds::OB_CS_SSTABLE_READER),
      own_arena_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
      external_arena_(arena), external_arena_mutex_(external_arena_mutex), fileinfo_cache_(fileinfo_cache)
    {
    }

    ObSSTableReader::~ObSSTableReader()
    {
      reset();
    }

    void ObSSTableReader::reset()
    {
      is_opened_ = false;
      sstable_id_.sstable_file_id_ = 0;
      sstable_id_.sstable_file_offset_ = 0;

      if (NULL != schema_)
      {
        get_sstable_schema_cache().revert_schema(
          trailer_.get_first_table_id(), trailer_.get_table_version());
        schema_ = NULL;
      }

      trailer_.reset();

      if (!use_external_arena_) own_arena_.free();
      external_arena_mutex_ = NULL;

      sstable_size_ = 0;

      if (NULL != compressor_)
      {
        destroy_compressor(compressor_);
        compressor_ = NULL;
      }

      if (NULL != bloom_filter_)
      {
        common::destroy_bloom_filter(bloom_filter_);
        bloom_filter_ = NULL;
      }

    }

    int ObSSTableReader::open(const int64_t sstable_id, const int64_t version)
    {
      ObSSTableId id;
      id.sstable_file_id_ = sstable_id;
      id.sstable_file_offset_ = 0; // not used for now.
      return open(id, version);
    }

    int ObSSTableReader::open(const ObSSTableId& sstable_id, const int64_t version)
    {
      int ret                     = OB_SUCCESS;
      const IFileInfo* file_info  = NULL;
      const char* trailer_buf     = NULL;
      int64_t trailer_buf_size    = 0;
      int64_t trailer_offset      = 0;
      int64_t trailer_size        = 0;

      if (is_opened_)
      {
        TBSYS_LOG(WARN, "SSTableReader already opened");
        ret = OB_INIT_TWICE;
      }
      else if (OB_INVALID_ID == sstable_id.sstable_file_id_
               || sstable_id.sstable_file_offset_ < 0)
      {
        TBSYS_LOG(WARN, "open sstable file error. sstable_id=%lu, offset pos(%ld) < 0",
                  sstable_id.sstable_file_id_, sstable_id.sstable_file_offset_);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        file_info = fileinfo_cache_.get_fileinfo(sstable_id.sstable_file_id_);
        if (NULL == file_info)
        {
          TBSYS_LOG(WARN, "get file info fail sstable_id=%lu",
                    sstable_id.sstable_file_id_);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && NULL != file_info)
      {
        ret = fetch_sstable_size(*file_info, sstable_size_);

        // read trailer offset
        if (OB_SUCCESS == ret)
        {
          ret = load_trailer_offset(*file_info, trailer_, sstable_size_,
                                    trailer_buf, trailer_buf_size,
                                    trailer_offset, trailer_size);
        }

        if (OB_SUCCESS == ret)
        {
          ret = load_trailer(*file_info, trailer_, trailer_buf, trailer_buf_size,
                             trailer_offset, trailer_size);
        }

        if (OB_SUCCESS == ret)
        {
          if (!trailer_.is_valid())
          {
            trailer_.dump();
            ret = OB_ERROR;
          }
        }

        /**
         * for bypass sstable, the tabelt version is 0, when we load
         * these sstable, we need set the sstable with right tablet
         * version. otherwise the sstable schema is changed, but all the
         * bypass sstable with version 0, maybe the sstable schema in
         * sstable schema cache is existed, new sstable will use the
         * old sstable schema, it's wrong.
         */
        if (OB_SUCCESS == ret && 0 == trailer_.get_table_version()
            && version > 0)
        {
          trailer_.set_table_version(version);
        }

        // load schema;
        if (OB_SUCCESS == ret)
        {
          ret = load_schema(*file_info);
        }

        if (OB_SUCCESS == ret)
        {
          ret = load_range(*file_info);
        }

        fileinfo_cache_.revert_fileinfo(file_info);
      }

      if (OB_SUCCESS ==ret)
      {
        is_opened_ = true;
        sstable_id_ = sstable_id;
      }

      return ret;
    }

    int ObSSTableReader::fetch_sstable_size(const IFileInfo& file_info,
                                            int64_t& sstable_size)
    {
      int ret = OB_SUCCESS;

      sstable_size = get_file_size(file_info.get_fd());
      if (sstable_size < 0)
      {
        TBSYS_LOG(ERROR, "cannot get sstable size, sstable_size=%ld, fd=%d",
                  sstable_size, file_info.get_fd());
        ret = OB_IO_ERROR;
      }

      return ret;
    }

    int ObSSTableReader::read_record(const IFileInfo& file_info,
                                     const int64_t offset,
                                     const int64_t size,
                                     const char*& out_buffer)
    {
      int ret                 = OB_SUCCESS;
      ObFileBuffer* file_buf  = GET_TSI_MULT(ObFileBuffer, TSI_SSTABLE_FILE_BUFFER_1);
      out_buffer = NULL;

      if (NULL == file_buf)
      {
        TBSYS_LOG(WARN, "get thread file read buffer failed, file_buf=NULL");
        ret = OB_ERROR;
      }
      else
      {
        ret = ObFileReader::read_record(file_info, offset, size, *file_buf);
        if (OB_SUCCESS == ret)
        {
          out_buffer = file_buf->get_buffer() + file_buf->get_base_pos();
        }
      }

      return ret;
    }

    int ObSSTableReader::load_trailer_offset(
        const IFileInfo& file_info,
        ObSSTableTrailer& trailer,
        int64_t sstable_size,
        const char*& trailer_buf,
        int64_t& trailer_buf_size,
        int64_t& trailer_offset,
        int64_t& trailer_size)
    {
      int ret                         = OB_SUCCESS;
      int64_t read_tail_size          = 0;
      const char* trailer_offset_buf  = NULL;
      int64_t pos                     = 0;
      const char* record_buf          = NULL;

      //init return value
      trailer_buf = NULL;
      trailer_buf_size = 0;
      trailer_offset = 0;
      trailer_size = 0;

      read_tail_size = sizeof(ObRecordHeader) + trailer.get_serialize_size()
                       + TRAILER_OFFSET_SIZE;

      // read trailer offset
      if (OB_SUCCESS == ret)
      {
        ret = read_record(file_info, sstable_size - read_tail_size,
                          read_tail_size, record_buf);
        if (OB_SUCCESS == ret && NULL != record_buf)
        {
          trailer_offset_buf = record_buf + read_tail_size - TRAILER_OFFSET_SIZE;
          ret = decode_i64(trailer_offset_buf, TRAILER_OFFSET_SIZE,
                           pos, &trailer_offset);
        }

        if (OB_SUCCESS == ret)
        {
          trailer_size = sstable_size - TRAILER_OFFSET_SIZE - trailer_offset;
          if (read_tail_size - TRAILER_OFFSET_SIZE >= trailer_size)
          {
            //the trailer is in the buffer, we needn't to read it again
            trailer_buf_size = trailer_size;
            trailer_buf = record_buf + read_tail_size - TRAILER_OFFSET_SIZE - trailer_size;
          }
        }
      }

      return ret;
    }

    int ObSSTableReader::load_trailer(
        const IFileInfo& file_info,
        ObSSTableTrailer& trailer,
        const char* trailer_buf,
        const int64_t trailer_buf_size,
        const int64_t trailer_offset,
        const int64_t trailer_size)
    {
      int ret                 = OB_SUCCESS;
      const char* data_buf    = NULL;
      int64_t data_size       = 0;
      int64_t pos             = 0;
      int64_t payload_size    = 0;
      const char* payload_ptr = NULL;
      ObRecordHeader header;

      if (trailer_offset < 0 || trailer_size <= 0)
      {
        TBSYS_LOG(ERROR, "trailer_offset:%ld, trailer_size:%ld is illegal",
            trailer_offset, trailer_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (NULL == trailer_buf || trailer_buf_size <= 0)
        {
          ret = read_record(file_info, trailer_offset, trailer_size, data_buf);
          if (OB_SUCCESS == ret && NULL != data_buf)
          {
            data_size = trailer_size;
          }
        }
        else if (NULL != trailer_buf && trailer_buf_size == trailer_size)
        {
          data_buf = trailer_buf;
          data_size = trailer_buf_size;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid trailer buffer size, trailer_buf=%p, trailer_buf_size=%ld,"
                          "trailer_size=%ld",
                    trailer_buf, trailer_buf_size, trailer_size);
          ret = OB_INVALID_ARGUMENT;
        }

        if (OB_SUCCESS == ret)
        {
          ret = ObRecordHeader::check_record(data_buf, data_size,
              ObSSTableWriter::TRAILER_MAGIC, header, payload_ptr, payload_size);
          if (OB_SUCCESS == ret)
          {
            if (!header.is_compress())
            {
              ret = trailer.deserialize(payload_ptr, payload_size, pos);
            }
            else
            {
              // trailer must be uncompressed
              TBSYS_LOG(ERROR, "Trailer is compressed, don't know how to uncompress it");
              ret = OB_ERROR;
            }
          }
        }
      }

      return ret;
    }


    int ObSSTableReader::load_bloom_filter(const IFileInfo& file_info)
    {
      int ret                 = OB_SUCCESS;
      int64_t filter_offset   = trailer_.get_bloom_filter_record_offset();
      int64_t filter_size     = trailer_.get_bloom_filter_record_size();
      int64_t payload_size    = 0;
      const char* payload_ptr = NULL;
      const char* record_buf  = NULL;
      ObRecordHeader header;

      if (filter_offset < 0 || filter_size < 0)
      {
        TBSYS_LOG(ERROR, "filter_offset:%ld, filter_size:%ld is illegal",
            filter_offset, filter_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (filter_offset == 0 && filter_size == 0)
      {
        //no bloom filter data in sstable file, disable floom filter
        enable_bloom_filter_ = false;
      }
      else if (NULL != bloom_filter_)
      {
        // already loaded before.
        ret = OB_SUCCESS;
      }
      else if (NULL == (bloom_filter_ = dynamic_cast<ObBloomFilterV1*>(
              create_bloom_filter<ObTCMalloc>(
                ObBasicBloomFilter<ObTCMalloc>::BLOOM_FILTER_VERSION))))
      {
        TBSYS_LOG(WARN, "cannot create bloom filter.");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        enable_bloom_filter_ = true;

        ret = read_record(file_info, filter_offset, filter_size, record_buf);
        if (OB_SUCCESS == ret && NULL != record_buf)
        {
          ret = ObRecordHeader::check_record(record_buf, filter_size,
              ObSSTableWriter::BLOOM_FILTER_MAGIC, header, payload_ptr, payload_size);
          if (OB_SUCCESS == ret)
          {
            if (!header.is_compress())
            {
              bloom_filter_->get_allocator()->set_mod_id(ObModIds::OB_BLOOM_FILTER);
              bloom_filter_->init(trailer_.get_bloom_filter_hash_count(), payload_size);
              bloom_filter_->set_buffer(reinterpret_cast<const uint8_t*>(payload_ptr), payload_size);
            }
            else
            {
              // compressed bloom filter
              TBSYS_LOG(ERROR, "bloom filter is in compressed format, "
                               "but expect uncompressed format");
              ret = OB_ERROR;
            }
          }
        }
      }

      return ret;
    }

    int ObSSTableReader::load_schema(const IFileInfo& file_info)
    {
      int ret                             = OB_SUCCESS;
      int64_t schema_offset               = trailer_.get_schema_record_offset();
      int64_t schema_size                 = trailer_.get_schema_record_size();
      uint64_t first_table_id             = trailer_.get_first_table_id();
      int64_t version                     = trailer_.get_table_version();
      ObSSTableSchemaCache& schema_cache  = get_sstable_schema_cache();
      ObSSTableSchema* schema_tmp         = NULL;
      char* schema_buf                    = NULL;
      int64_t pos                         = 0;
      int64_t payload_size                = 0;
      const char* payload_ptr             = NULL;
      const char* record_buf              = NULL;
      ObRecordHeader header;

      if (schema_offset < 0 || schema_size <= 0 || version < 0)
      {
        TBSYS_LOG(ERROR, "schema_offset:%ld, schema_size:%ld, version:%ld is illegal",
            schema_offset, schema_size, version);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (NULL == (schema_ = schema_cache.get_schema(first_table_id, version)))
        {
          schema_buf = static_cast<char*>(ob_malloc(sizeof(ObSSTableSchema), ObModIds::OB_SSTABLE_READER));
          if (NULL == schema_buf)
          {
            TBSYS_LOG(ERROR,"alloc schema buf error");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }

          if (OB_SUCCESS == ret && NULL != schema_buf)
          {
            schema_ = new (schema_buf) ObSSTableSchema();
            if (NULL == schema_)
            {
              TBSYS_LOG(WARN,"new sstable schema instance error");
              ret = OB_ERROR;
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = read_record(file_info, schema_offset, schema_size, record_buf);
          }

          if (OB_SUCCESS == ret && NULL != record_buf)
          {
            ret = ObRecordHeader::check_record(record_buf, schema_size,
                ObSSTableWriter::SCHEMA_MAGIC, header, payload_ptr, payload_size);
            if (OB_SUCCESS == ret)
            {
              if (!header.is_compress())
              {
                ret = schema_->deserialize(payload_ptr, payload_size, pos);
              }
              else
              {
                // compressed schema
                TBSYS_LOG(ERROR, "schema is in compressed format, "
                                 "but expect uncompressed format");
                ret = OB_ERROR;
              }
            }
          }

          if (OB_SUCCESS == ret && NULL != schema_)
          {
            schema_tmp  = schema_;
            ret = schema_cache.add_schema(schema_tmp, first_table_id, version);
            if (OB_ENTRY_EXIST == ret)
            {
              ob_free(schema_tmp);
              schema_tmp = NULL;

              schema_ = schema_cache.get_schema(first_table_id, version);
              TBSYS_LOG(INFO, "schema exist with table id =%ld, version=%ld, and refetch from cache:%p",
                  first_table_id, version, schema_);
              if (NULL == schema_)
              {
                TBSYS_LOG(WARN, "get schema from sstable schema cache failed, version=%ld",
                          version);
                ret = OB_ERROR;
              }
              else
              {
                ret = OB_SUCCESS;
              }
            }
            else if (OB_SUCCESS != ret)
            {
              ob_free(schema_tmp);
              schema_tmp = NULL;

              TBSYS_LOG(WARN, "add schema to sstable schema cache failed, version=%ld",
                        version);
            }
          }
        }
      }

      return ret;
    }

    int ObSSTableReader::load_range_compatible(const char* payload_ptr, const int64_t payload_size,
        common::ObNewRange& range, common::ObObj* start_key_obj_array, common::ObObj* end_key_obj_array)
    {
      // load range for sstableV2 format, which is the sstable format of oceanbase 0.3.x
      //start_key_obj_array and end_key_obj_array's length should be OB_MAX_ROWKEY_COLUMN_NUMBER
      int ret = OB_SUCCESS;
      ObRowkeyInfo rowkey_info;
      ObString start_key;
      ObString end_key;
      uint64_t table_id = OB_INVALID_ID;
      int8_t flag = 0;
      int64_t pos = 0;

      if (OB_SUCCESS !=
          (ret = get_global_schema_rowkey_info(trailer_.get_first_table_id(), rowkey_info)))
      {
        TBSYS_LOG(ERROR, "failed to get_global_schema_rowkey_info, ret=%d", ret);
      }
      else if (OB_SUCCESS !=
          (ret = serialization::decode_vi64(payload_ptr, payload_size,
                                            pos, reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(ERROR, "failed to decode table id, payload_ptr=%p, payload_size=%ld, pos=%ld,"
            " ret=%d", payload_ptr, payload_size, pos, ret);
      }
      else if (OB_SUCCESS !=
          (ret = serialization::decode_i8(payload_ptr, payload_size, pos, &flag)))
      {
        TBSYS_LOG(ERROR, "failed to decode flag, payload_ptr=%p, payload_size=%ld, pos=%ld,"
            " ret=%d", payload_ptr, payload_size, pos, ret);
      }
      else if (OB_SUCCESS !=
          (ret = start_key.deserialize(payload_ptr, payload_size, pos)))
      {
        TBSYS_LOG(ERROR, "failed to deserialize start key, payload_ptr=%p, payload_size=%ld, pos=%ld,"
            " ret=%d", payload_ptr, payload_size, pos, ret);
      }
      else if (OB_SUCCESS !=
          (ret = end_key.deserialize(payload_ptr, payload_size, pos)))
      {
        TBSYS_LOG(ERROR, "failed to deserialize end key, payload_ptr=%p, payload_size=%ld, pos=%ld,"
            " ret=%d", payload_ptr, payload_size, pos, ret);
      }
      else
      {
        range.table_id_ = table_id;
        range.border_flag_.set_data(flag);
        if(range.border_flag_.is_min_value())
        {
          range.border_flag_.unset_min_value();
          range.border_flag_.unset_inclusive_start();

          range.start_key_.set_min_row();
        }
        else
        {
          range.border_flag_.unset_min_value();
          range.border_flag_.unset_inclusive_start();

          ret = ObRowkeyHelper::binary_rowkey_to_obj_array(
              rowkey_info, start_key, start_key_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "failed to trans start binary rowkey to obj array, ret=%d", ret);
          }
          else
          {
            range.start_key_.assign(start_key_obj_array, rowkey_info.get_size());
          }
        }

        if(range.border_flag_.is_max_value())
        {
          range.border_flag_.unset_max_value();
          range.border_flag_.unset_inclusive_end();

          range.end_key_.set_max_row();
        }
        else
        {
          range.border_flag_.unset_max_value();
          range.border_flag_.set_inclusive_end();

          ret = ObRowkeyHelper::binary_rowkey_to_obj_array(
              rowkey_info, end_key, end_key_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "failed to trans end binary rowkey to obj array, ret=%d", ret);
          }
          else
          {
            range.end_key_.assign(end_key_obj_array, rowkey_info.get_size());
          }
        }
      }

      return ret;
    }

    int ObSSTableReader::load_range(const IFileInfo& file_info)
    {
      int ret = OB_SUCCESS;
      int64_t range_offset = trailer_.get_range_record_offset();
      int64_t range_size = trailer_.get_range_record_size();
      int64_t payload_size    = 0;
      int64_t pos = 0;
      const char* payload_ptr = NULL;
      const char* record_buf  = NULL;
      ObRecordHeader header;
      ObNewRange range;
      ObObj start_key_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      ObObj end_key_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];

      if (0 == range_offset && 0 == range_size)
      { // compat for SSTABLEV2
        // no range set in sstable
        // set range as (MIN, MAX)
        range.reset();
        range.set_whole_range();
        range.table_id_ = trailer_.get_first_table_id();
        ret = trailer_.set_range(range);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to set range of trailer, ret=%d", ret);
        }
      }
      else if (range_offset < 0 || range_size <= 0)
      {
        TBSYS_LOG(ERROR, "range_offset:%ld, range_size:%ld is illegal",
                  range_offset, range_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = read_record(file_info, range_offset, range_size, record_buf);
        if (OB_SUCCESS == ret && NULL != record_buf)
        {
          ret = ObRecordHeader::check_record(record_buf, range_size,
              ObSSTableWriter::RANGE_MAGIC, header, payload_ptr, payload_size);
          if (OB_SUCCESS == ret)
          {
            if (header.is_compress())
            {
              // compressed range buf
              TBSYS_LOG(ERROR, "range buf is in compressed format, "
                        "but expect uncompressed format");
              ret = OB_ERROR;
            }
            else
            {
              if (trailer_.get_trailer_version() == ObSSTableTrailer::SSTABLEV2)
              { // compat for SSTABLEV2
                ret = load_range_compatible(payload_ptr, payload_size,
                    range, start_key_obj_array, end_key_obj_array);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "failed to deserialize range of sstable v2: payload_ptr %p,"
                      " payload_size %ld, pos %ld", payload_ptr, payload_size, pos);
                }
              }
              else
              {
                range.start_key_.assign(start_key_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
                range.end_key_.assign(end_key_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);

                ret = range.deserialize(payload_ptr, payload_size, pos);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "failed to deserialize range: payload_ptr %p,"
                      " payload_size %ld, pos %ld", payload_ptr, payload_size, pos);
                }
              }

              // deep copy range
              if (OB_SUCCESS == ret)
              {
                ObNewRange tmp_range;
                if(use_external_arena_)
                {
                  if (NULL != external_arena_mutex_)
                  {
                    external_arena_mutex_->lock();
                  }
                  ret = common::deep_copy_range(external_arena_, range, tmp_range);
                  if (NULL != external_arena_mutex_)
                  {
                    external_arena_mutex_->unlock();
                  }
                }
                else
                {
                  ret = common::deep_copy_range(own_arena_, range, tmp_range);
                }

                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "failed to deep copy range,ret=%d", ret);
                }
                else
                {
                  ret = trailer_.set_range(tmp_range);
                  if (OB_SUCCESS != ret)
                  {
                    TBSYS_LOG(ERROR, "failed to set range of trailer, ret=%d", ret);
                  }
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "failed to check record");
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "failed to read_record of range");
        }
      }

      return ret;
    }

    /**
     * total row count in trailer
     */
    int64_t ObSSTableReader::get_row_count() const
    {
      return trailer_.get_row_count();
    }

    int64_t ObSSTableReader::get_sstable_size() const
    {
      int64_t size = 0;
      if (is_opened_) size = sstable_size_;
      return size;
    }

    int64_t ObSSTableReader::get_sstable_checksum() const
    {
      int64_t sum = 0;
      if (is_opened_) sum = trailer_.get_sstable_checksum();
      return sum;
    }

    const ObSSTableSchema* ObSSTableReader::get_schema() const
    {
      return schema_;
    }

    const BloomFilter* ObSSTableReader::get_bloom_filter() const
    {
      bf_mutex_.lock();
      if (NULL == bloom_filter_)
      {
        const IFileInfo* file_info  = fileinfo_cache_.get_fileinfo(sstable_id_.sstable_file_id_);
        if (NULL != file_info)
        {
          const_cast<ObSSTableReader*>(this)->load_bloom_filter(*file_info);
          fileinfo_cache_.revert_fileinfo(file_info);
        }
      }
      bf_mutex_.unlock();
      return bloom_filter_;
    }

    int ObSSTableReader::serialize_bloom_filter(const int64_t bf_version, char* &bf_buffer, int64_t &bf_serialize_size)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      bf_mutex_.lock();
      if (NULL == bloom_filter_)
      {
        const IFileInfo* file_info  = fileinfo_cache_.get_fileinfo(sstable_id_.sstable_file_id_);
        if (NULL != file_info)
        {
          const_cast<ObSSTableReader*>(this)->load_bloom_filter(*file_info);
          fileinfo_cache_.revert_fileinfo(file_info);
        }
      }
      if (NULL == bloom_filter_  || (bf_serialize_size = bloom_filter_->get_serialize_size()) <= 0)
      {
        ret = OB_CS_TABLET_NOT_EXIST;
      }
      else if ( bf_version != (bloom_filter_->get_version()))
      {
        TBSYS_LOG(WARN, "need bf version=%ld <> filter's version=%ld", 
            bf_version, bloom_filter_->get_version());
        ret = OB_NOT_SUPPORTED;
      }
      else if ( NULL == (bf_buffer = reinterpret_cast<char*>(ob_tc_malloc(
                bf_serialize_size, ObModIds::OB_BLOOM_FILTER))))
      {
        TBSYS_LOG(WARN, "allocator memory failed. size=%ld", bf_serialize_size);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = bloom_filter_->serialize(bf_buffer, bf_serialize_size, pos)))
      {
        TBSYS_LOG(WARN, "serialize filter ret=%d, size=%ld, pos=%ld", ret, bf_serialize_size, pos);
      }
      bf_mutex_.unlock();
      return ret;
    }

    void ObSSTableReader::destroy_bloom_filter() 
    {
      bf_mutex_.lock();
      if (NULL != bloom_filter_)
      {
        common::destroy_bloom_filter(bloom_filter_);
        bloom_filter_ = NULL;
      }
      bf_mutex_.unlock();
    }

    ObCompressor* ObSSTableReader::get_decompressor()
    {
      const char *compressor_name = trailer_.get_compressor_name();

      //create compressor
      if (NULL == compressor_ && strlen(compressor_name) > 0)
      {
        compressor_ = create_compressor(compressor_name);
        if (NULL == compressor_)
        {
          TBSYS_LOG(ERROR, "Problem create compressor");
        }
      }

      return compressor_;
    }

    // check sstable if may contain %key, check by bloomfilter.
    bool ObSSTableReader::may_contain(const ObRowkey& key) const
    {
      bool ret = false;
      if (is_opened_)
      {
        if (NULL == bloom_filter_)
        {
          const IFileInfo* file_info  = fileinfo_cache_.get_fileinfo(sstable_id_.sstable_file_id_);
          if (NULL != file_info)
          {
            const_cast<ObSSTableReader*>(this)->load_bloom_filter(*file_info);
            fileinfo_cache_.revert_fileinfo(file_info);
          }
        }

        if (enable_bloom_filter_ && NULL != bloom_filter_)
        {
          ret = bloom_filter_->may_contain(key);
        }
        else
        {
          //disabled bloom filter, default return true
          ret = true;
        }
      }
      return ret;
    }

    bool ObSSTableReader::check_sstable(const char* sstable_fname, uint64_t *sstable_checksum)
    {
      bool ret                  = false;
      int64_t sstable_size      = 0;
      const char* trailer_buf   = NULL;
      int64_t trailer_buf_size  = 0;
      int64_t trailer_offset    = 0;
      int64_t trailer_size      = 0;
      const ObSSTableId sstable_id;
      ObFileInfo file_info;
      ObSSTableTrailer trailer;

      if (NULL == sstable_fname)
      {
        TBSYS_LOG(WARN, "file name is NULL");
      }
      else if (OB_SUCCESS != file_info.init(sstable_fname))
      {
        TBSYS_LOG(WARN, "failed to init file info, sstable_fname=%s", sstable_fname);
      }
      else if (OB_SUCCESS != fetch_sstable_size(file_info, sstable_size))
      {
        TBSYS_LOG(WARN, "failed to fetch sstable size, sstable_fname=%s", sstable_fname);
      }
      else if (OB_SUCCESS != load_trailer_offset(file_info, trailer, sstable_size,
               trailer_buf, trailer_buf_size, trailer_offset, trailer_size))
      {
        TBSYS_LOG(WARN, "failed to load trailer offset, sstable_fname=%s", sstable_fname);
      }
      else if (OB_SUCCESS != load_trailer(file_info, trailer, trailer_buf,
               trailer_buf_size, trailer_offset, trailer_size))
      {
        TBSYS_LOG(WARN, "failed to load trailer, sstable_fname=%s", sstable_fname);
      }
      else
      {
        ret = trailer.is_valid();
        if (!ret)
        {
          trailer.dump();
        }
        else
        {
          if (NULL != sstable_checksum)
          {
            *sstable_checksum = trailer.get_sstable_checksum();
          }
        }
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
