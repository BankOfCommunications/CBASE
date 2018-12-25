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
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_READER_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_READER_H_

#include "common/bloom_filter.h"
#include "common/compress/ob_compressor.h"
#include "common/page_arena.h"
#include "common/ob_fileinfo_manager.h"
#include "ob_sstable_reader_i.h"
#include "ob_sstable_schema.h"
#include "ob_sstable_schema_cache.h"
#include "ob_sstable_trailer.h"
#include "ob_disk_path.h" // for ObSSTableId;

namespace oceanbase
{
  namespace common
  {
    class FileUtils;
    class ObRecordHeader;
  }
  namespace sstable
  {
    class ObSSTableReader : public SSTableReader
    {
      class ObFileInfo : public common::IFileInfo
      {
        static const int FILE_OPEN_NORMAL_RFLAG = O_RDONLY;
        static const int FILE_OPEN_DIRECT_RFLAG = O_RDONLY | O_DIRECT;
  
        public:
          ObFileInfo();
          virtual ~ObFileInfo();
  
        public:
          int init(const char* sstable_fname, const bool use_dio = true);
          void destroy();
  
        public:
          virtual int get_fd() const;
  
        private:
          DISALLOW_COPY_AND_ASSIGN(ObFileInfo);
          int fd_;
      };

    public:
      static const int64_t TRAILER_OFFSET_SIZE = sizeof(ObTrailerOffset);

      /**
       * try to read the tail of sstable, usually, the toatal size of
       * trailer and trailer offset are less than 1024 bytes, so we
       * can first read 1024 bytes from the end of sstable, it may
       * include the trailer and trailer offset.
       */
      static const int64_t READ_TAIL_SIZE = 2 * common::OB_DIRECT_IO_ALIGN; //1024; 

    public:
      ObSSTableReader(common::ModuleArena &arena, common::IFileInfoMgr& fileinfo_cache,
        tbsys::CThreadMutex* external_arena_mutex = NULL);
      virtual ~ObSSTableReader();

      int open(const int64_t sstable_id, const int64_t version = 0);
      int open(const ObSSTableId& sstable_id, const int64_t version = 0);
      void reset();
      inline bool is_opened() const { return is_opened_; }
      inline const ObSSTableId& get_sstable_id() const { return sstable_id_; }
      inline const common::ObNewRange& get_range() const { return trailer_.get_range(); }

      int64_t get_row_count() const;
      int64_t get_sstable_size() const;
      int64_t get_sstable_checksum() const;

      const ObSSTableSchema* get_schema() const;
      const common::BloomFilter* get_bloom_filter() const; 
      int serialize_bloom_filter(const int64_t bf_version, char* &buffer, int64_t &size);
      void destroy_bloom_filter();
      inline const ObSSTableTrailer& get_trailer() const { return trailer_; }
      ObCompressor* get_decompressor();
      bool may_contain(const common::ObRowkey& key) const;
      inline bool empty() const { return (trailer_.get_row_count() == 0); }

      static bool check_sstable(const char* sstable_fname, uint64_t *sstable_checksum);

    private:
      static int read_record(const common::IFileInfo& file_info,
                             const int64_t offset, 
                             const int64_t size,
                             const char*& out_buffer);
      static int fetch_sstable_size(const common::IFileInfo& file_info, 
                                    int64_t& sstable_size);
      static int load_trailer_offset(const common::IFileInfo& file_info,
                                     ObSSTableTrailer& trailer,
                                     int64_t sstable_size,
                                     const char*& trailer_buf,
                                     int64_t& trailer_buf_size, 
                                     int64_t& trailer_offset, 
                                     int64_t& trailer_size);
      static int load_trailer(const common::IFileInfo& file_info,
                              ObSSTableTrailer& trailer,
                              const char* trailer_buf,
                              const int64_t trailer_buf_size,
                              const int64_t trailer_offset, 
                              const int64_t trailer_size);
      int load_bloom_filter(const common::IFileInfo& file_info);
      int load_schema(const common::IFileInfo& file_info);
      int load_range(const common::IFileInfo& file_info);

      // load range for sstableV2 format, which is the sstable format of oceanbase 0.3.x
      int load_range_compatible(const char* payload_ptr, const int64_t payload_size,
          common::ObNewRange& range, common::ObObj* start_key_obj_array, 
          common::ObObj* end_key_obj_array);

      static inline ObSSTableSchemaCache& get_sstable_schema_cache()
      {
        static ObSSTableSchemaCache s_schema_cache;

        return s_schema_cache;
      }

    private:
      bool is_opened_;
      bool enable_bloom_filter_;        //if enable bloom filter
      bool use_external_arena_;
      int64_t sstable_size_;
      ObSSTableSchema* schema_;
      ObCompressor* compressor_;
      common::ObBloomFilterV1* bloom_filter_;
      ObSSTableId sstable_id_;
      ObSSTableTrailer trailer_;

      common::ModulePageAllocator mod_;
      common::ModuleArena own_arena_;
      common::ModuleArena &external_arena_;
      tbsys::CThreadMutex* external_arena_mutex_;
      mutable tbsys::CThreadMutex bf_mutex_;

      common::IFileInfoMgr& fileinfo_cache_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif

