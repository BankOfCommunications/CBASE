#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_WRITER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_WRITER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_file.h"
#include "common/ob_bloomfilter.h"
#include "common/ob_list.h"
#include "common/compress/ob_compressor.h"
#include "common/ob_record_header_v2.h"
#include "common/ob_range2.h"
#include "common/ob_compact_store_type.h"
#include "ob_sstable_block.h"
#include "ob_sstable_table.h"
#include "ob_sstable.h"
#include "ob_compact_sstable_writer_buffer.h"
#include "ob_sstable_store_struct.h"

class TestCompactSSTableWriter_construct_Test;
class TestCompactSSTableWriter_set_sstable_param1_Test;
class TestCompactSSTableWriter_set_sstable_param2_Test;
class TestCompactSSTableWriter_set_sstable_param3_Test;
class TestCompactSSTableWriter_set_sstable_param4_Test;
class TestCompactSSTableWriter_set_table_info1_Test;
class TestCompactSSTableWriter_set_table_info2_Test;
class TestCompactSSTableWriter_set_sstable_filepath1_Test;

//#define OB_COMPACT_SSTABLE_ALLOW_SCHEMA_CHECK_ROWKEY_
//#define OB_COMPACT_SSTABLE_ALLOW_TABLE_RANGE_CHECK_ROWKEY_
//#define OB_COMPACT_SSTABLE_ALLOW_BLOOMFILTER_
//#define OB_COMPACT_SSTABLE_ALLOW_LAST_CHECK_ROWKEY_

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObCompactSSTableWriter
    {
    public:
      friend class ::TestCompactSSTableWriter_construct_Test;
      friend class ::TestCompactSSTableWriter_set_sstable_param1_Test;
      friend class ::TestCompactSSTableWriter_set_sstable_param2_Test;
      friend class ::TestCompactSSTableWriter_set_sstable_param3_Test;
      friend class ::TestCompactSSTableWriter_set_sstable_param4_Test;
      friend class ::TestCompactSSTableWriter_set_table_info1_Test;
      friend class ::TestCompactSSTableWriter_set_table_info2_Test;
      friend class ::TestCompactSSTableWriter_set_sstable_filepath1_Test;

    public:
      static const int64_t OB_MAX_COMPRESSOR_NAME_LENGTH = 1024;

    public:
      struct QueryStruct
      {
        int64_t sstable_size_;

        QueryStruct()
        {
          memset(this, 0, sizeof(QueryStruct));
        }

        void reset()
        {
          memset(this, 0, sizeof(QueryStruct));
        }
      };

    public:
      ObCompactSSTableWriter()
        : sstable_inited_(false), table_inited_(false),
          sstable_file_inited_(false), sstable_first_table_(false),
          not_table_first_row_(false), compressor_(NULL),
          dio_flag_(true), filesys_(&default_filesys_),
          split_buf_inited_(false), split_flag_(false),
          def_sstable_size_(0), min_split_sstable_size_(0),
          cur_offset_(0), cur_table_offset_(0),
          sstable_checksum_(0), sstable_table_init_count_(0)
      {
        //default_filesys_(construct)
        sstable_writer_buffer_.reset();
        //sstable_writer_buffer_(construct)
        //sstable_(construct)
        sstable_.set_bloomfilter_hashcount(SSTABLE_BLOOMFILTER_HASH_COUNT);
        //table_(construct)
        table_.init(SSTABLE_BLOOMFILTER_HASH_COUNT, SSTABLE_BLOOMFILTER_SIZE);
        //block_(construct)
        //sstable_trailer_offset_(construct)
        //query_struct_(construct)
      }

      ~ObCompactSSTableWriter()
      {
        if (NULL != compressor_)
        {
          destroy_compressor(compressor_);
          compressor_ = NULL;
        }
        //default_filesys_(deconstruct)
        filesys_->close();
        filesys_ = NULL;
        //sstable_writer_buffer_(deconstruct)
        //sstable_(deconstruct)
        //table_(deconstruct)
        //block_(deconstruct)
        //sstable_trailer_offset(deconstruct)
        //query_struct_(deconstruct)
      }

      /**
       * set sstable param
       * --must call the reset() before reuse the writer
       * @param version_range: version range
       * @param row_stroe_type: row store type
       * @param table_count: table count
       * @param blocksize: block size
       * @param compressor_name: not compress
       *        --none: none compressor
       *        --    : not compressor(no need compress buffer)
       *        --lzo_1.0: lzo
       *        --snappy: snappy
       * @param def_sstable_size: default sstable size
       * @param min_split_sstable_size: min split sstable size
       *        --def_sstable_size == 0 (not split)
       *        --def_sstable_size != 0 (split)
       */
      int set_sstable_param(
          const ObFrozenMinorVersionRange& version_range,
          const common::ObCompactStoreType& row_store_type,
          const int64_t table_count,
          const int64_t blocksize,
          const common::ObString& compressor_name,
          const int64_t def_sstable_size,
          const int64_t min_split_sstable_size = 0);

      /**
       * set table info
       * --init the table
       * --switch table
       * @param table id: table id
       * @param schema: schema
       * @param table_range: table range
       */
      int set_table_info(const uint64_t table_id,
          const ObSSTableSchema& schema,
          const common::ObNewRange& table_range);

      /**
       * set sstable file name
       * @param filepath: file name
       */
      int set_sstable_filepath(const common::ObString& filepath);

      /**
       * this function is not already used
       */
      int append_row(const common::ObRowkey& row_key,
          const common::ObRow& row_value, bool& is_sstable_split);

      /**
       * append row
       * @param row: inserted row
       * @param is_sstable_split: sstable is or not split
       */
      int append_row(const common::ObRow& row, bool& is_sstable_split);

      /**
       * finish current sstable
       */
      int finish();

      /**
       * get table range
       * @param table_range: table range
       * @param flag: 0(get old sstable range), 1(get cur sstable range)
       */
      int get_table_range(const common::ObNewRange*& table_range,
          const int64_t flag);

      /**
       * get sstable checksum
       * @param flag: 0(get old sstable checksum),
       *              1(get cur sstable checksum)
       */
      inline uint64_t get_sstable_checksum(const int64_t flag) const
      {
        return sstable_.get_sstable_check_sum(flag);
      }

      /**
       * get sstable row count
       * @param flag: 0(get old sstable row count)
       *              1(get cur sstable row count)
       */
      inline int64_t get_sstable_row_count(const int64_t flag) const
      {
        return sstable_.get_sstable_row_count(flag);
      }

      /**
       * get sstable size
       * @param flag: 0(get old sstable size)
       *              1(get cur sstable size)
       */
      inline int64_t get_sstable_size(const int64_t flag) const
      {
        int64_t ret = 0;
        if (0 == flag)
        {
          ret = query_struct_.sstable_size_;
        }
        else if (1 == flag)
        {
          ret = cur_offset_;
        }
        return ret;
      }

      /**
       * reuse
       */
      void reset();

    private:
      /**
       * switch sstable
       */
      void switch_sstable_reset();

      /**
       * flush the block in split list
       */
      int flush_mem_block_list();

      //block_
      /**
       * finish current block
       * @param is_sstable_split: sstable is or not split
       */
      int finish_current_block(bool& is_sstable_split);

      /**
       * finish current block(use for DENSE_DENSE)
       * @param is_sstable_split: sstable is or not split
       */
      int finish_current_block_split(bool& is_sstable_split);

      /**
       * finish current block(use for DENSE_SPARSE)
       */
      int finish_current_block_nosplit();

      /**
       * need switch block(current block is full)
       */
      bool need_switch_block();

      //table_
      /**
       * finish curent table
       * @param finish_flag:
       * ---true(finish one sstable,use for sstable split,DENSE_DENSE)
       * ---false(the current sstable is not finished,
       *          use for DENSE_SPARSE, one sstable contain mult table)
       */
      int finish_current_table(const bool finish_flag);

      /**
       * finish current block index
       */
      int finish_current_block_index();

      /**
       * finish current block endkey
       */
      int finish_current_block_endkey();

      /**
       * finish current table range
       * @param finish_flag:
       * ---true(finish one sstable,use for sstable split,DENSE_DENSE)
       * ---false(the current sstable is not finished,
       *          use for DENSE_SPARSE, one sstable contain mult table)
       */
      int finish_current_table_range(const bool finish_flag);

      /**
       * finish current table bloomfilter
       */
      int finish_current_table_bloomfilter();

      //sstable_
      /**
       * finish current sstable
       */
      int finish_current_sstable();

      /**
       * finish current table index
       */
      int finish_current_table_index();

      /**
       * finish current table schema
       */
      int finish_current_table_schema();

      /**
       * finish current sstable header
       */
      int finish_current_sstable_header();

      //trailer
      /**
       * finish current sstable trailer
       */
      int finish_current_sstable_trailer_offset();

      //record write
      /**
       * write record header
       * @param record_header: record header
       */
      int write_record_header(const common::ObRecordHeaderV2& record_header);

      /**
       * write record bodky
       * @param record_body: record body
       */
      int write_record_body(const char* block_buf, const int64_t block_buf_len);

    private:
      //status
      bool sstable_inited_;
      bool table_inited_;
      bool sstable_file_inited_;
      bool sstable_first_table_;
      bool not_table_first_row_;

      //compressor, fileappender
      ObCompressor* compressor_;
      bool dio_flag_; //true: direct io  false:not direct io
      common::ObFileAppender default_filesys_; //default fileappender
      common::ObIFileAppender* filesys_; //file appender

      //split
      bool split_buf_inited_; //true:started   false:not started
      bool split_flag_; //true:allow split  false:don't allow split
      int64_t def_sstable_size_;
      int64_t min_split_sstable_size_;

      //file
      int64_t cur_offset_;
      int64_t cur_table_offset_;
      uint64_t sstable_checksum_;
      int64_t sstable_table_init_count_;

      //buffer
      ObCompactSSTableWriterBuffer sstable_writer_buffer_;

      //store
      ObSSTable sstable_;
      ObSSTableTable table_;
      ObSSTableBlock block_;
      ObSSTableTrailerOffset sstable_trailer_offset_;

      QueryStruct query_struct_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif

