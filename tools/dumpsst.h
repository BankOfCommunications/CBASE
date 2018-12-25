/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * dumpsst.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "common/ob_define.h"
#include "common/page_arena.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_trailer.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "common/file_utils.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_sstable_schema.h"
#include "common/compress/ob_compressor.h"

namespace oceanbase
{
  namespace sstable
  {
    class DumpSSTable
    {
      public:
        DumpSSTable() 
          : opened_(false), reader_(arena_, file_info_cache_),block_index_(NULL),schema_() { file_info_cache_.init(10); }
        ~DumpSSTable();
        int open(const int64_t sstable_file_id);
        void display_trailer_info();
        void display_bloom_filter();
        void display_block_index();
        void display_schema_info();
        ObSSTableReader& get_reader();
        void load_block(int32_t block_id);
		void load_blocks(int32_t block_id , int32_t block_n);
		void dump_another_sstable(const char * file_dir_ ,const char *compressor_);
		void dump_another_block(int32_t block_id);
private:
        int load_block_index();
        int read_and_decompress_record(const int16_t magic,const int64_t offset,
                                       const int64_t zsize,common::ObRecordHeader& record_header,
                                       char *& data_buf,int64_t& dsize);
        int load_schema();
        bool opened_;
        common::ModuleArena arena_;
        chunkserver::FileInfoCache file_info_cache_;
        ObSSTableReader reader_;
        ObSSTableWriter writer_;
        common::FileUtils util_;
        sstable::ObSSTableBlockIndexV2 *block_index_;
        //ObCompressor* compressor_;
        //ObSSTableBlockIndexV2::ObSSTableBlockIndexHeader block_index_header_;
        //ObSSTableBlockIndexV2::IndexEntryType *block_index_entry_;
        ObSSTableSchema schema_;

public:
        static int compare_block(
            const char* src_file_name,
            const char* dst_file_name,
            ObSSTableReader &src_reader,
            ObSSTableReader &dst_reader,
            ObSSTableBlockIndexV2::const_iterator src_block_pos, 
            ObSSTableBlockIndexV2::const_iterator dst_block_pos, 
            bool ignore_size);

        static int compare_block_index(
            const char* src_file_name,
            const char* dst_file_name,
            ObSSTableReader &src_reader,
            ObSSTableReader &dst_reader,
            const ObSSTableBlockIndexV2 & src_index, 
            const ObSSTableBlockIndexV2 & dst_index, 
            bool ignore_size);

        static int compare_sstable(
            const int64_t src_file_id,
            const int64_t dst_file_id,
            ObSSTableReader &src_reader,
            ObSSTableReader &dst_reader,
            bool ignore_size);

        static int get_block_index(
            const char* file_name, 
            const ObSSTableTrailer &trailer, 
            ObSSTableBlockIndexV2* &index_object);

        static int64_t get_block_index_entry_size(
            const ObSSTableBlockIndexV2 & index_object);
    };

  } /* chunkserver */

} /* oceanbase */

