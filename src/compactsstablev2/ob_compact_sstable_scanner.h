#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_SCANNER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_SCANNER_H_

#include "common/ob_iterator.h"
#include "common/ob_scan_param.h"
#include "common/ob_rowkey.h"
#include "common/ob_row.h"
#include "ob_sstable_scan_column_indexes.h"
#include "ob_compact_sstable_reader.h"
#include "ob_sstable_block_cache.h"
#include "ob_sstable_block_index_cache.h"
#include "sstable/ob_sstable_scan_param.h"
#include "sql/ob_multi_cg_scanner.h"
#include "ob_sstable_block_index_mgr.h"
#include "ob_sstable_block_scanner.h"
#include "ob_sstable_block_cache.h"

class TestCompactSSTableScanner_init_Test;
class TestCompactSSTableScanner_construct_Test;
class TestCompactSSTableScanner_set_scan_param1_Test;

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObCompactSSTableScanner : public sql::ObRowkeyIterator
    {
    public:
      friend class ::TestCompactSSTableScanner_init_Test;
      friend class ::TestCompactSSTableScanner_construct_Test;
      friend class ::TestCompactSSTableScanner_set_scan_param1_Test;

    public:
      static const int64_t UNCOMP_BUF_SIZE = 1024 * 1024;
      static const int64_t INTERNAL_BUF_SIZE = 1024 * 1024;
      static const int32_t TIME_OUT_US = 10 * 1000 * 1000;
      static const int64_t INVALID_CURSOR = -1;

    public:
      struct ScanContext
      {
        ObCompactSSTableReader* sstable_reader_;
        ObSSTableBlockCache* block_cache_;
        ObSSTableBlockIndexCache* block_index_cache_;

        ScanContext()
          : sstable_reader_(NULL),
            block_cache_(NULL),
            block_index_cache_(NULL)
        {
        }

        inline bool is_valid() const
        {
          return ((NULL != block_cache_) && (NULL != block_index_cache_));
        }
      };

    private:
      enum
      {
        ITERATE_NOT_INITIALIZED = 1,
        ITERATE_NOT_START,
        ITERATE_IN_PROGRESS,
        ITERATE_LAST_BLOCK,
        ITERATE_NEED_FORWARD,
        ITERATE_IN_ERROR,
        ITERATE_END
      };

      enum ScanFlag
      {
        DENSE_DENSE_FULL_ROW_SCAN = 1,
        DENSE_DENSE_NORMAL_ROW_SCAN,
        DENSE_SPARSE_FULL_ROW_SCAN,
        DENSE_SPARSE_NORMAL_ROW_SCAN,
        INVALID_SCAN_FLAG
      };

      enum RowCountFlag
      {
        DENSE_DENSE_READER_NULL = 1, //sstable_reader_ == NULL
        DENSE_SPARSE_TABLE_NULL,     //table is not exist
        DENSE_DENSE_ROW_NULL,        //row count==0
        NORMAL_ROW_COUNT_FLAG        //normal
      };

    public:
      ObCompactSSTableScanner();

      ~ObCompactSSTableScanner();

      /**
       * set the scan param
       * --init the ObCompactSSTableScanner
       * --reuse the ObCompactSSTableScanner
       * @param scan_param: scan param
       * @param scan_context: sstable_reader + block_cache + block_index_cache
       */
      int set_scan_param(const sstable::ObSSTableScanParam* scan_param, const ScanContext* scan_context);

      /**
       * get next row
       * @param row_key: rowkey
       * @param row_value: rowvalue
       * @return
       */
      int get_next_row(const common::ObRowkey*& row_key,
          const common::ObRow*& row_value);


      /**
       * get row desc
       * --it must be used after the get next row
       * --it's the row desc of row_value of get_next_row
       * --if scan param set daily_merge_scan and DENSE_DENSE，must set rowkey column count
       * --if FULL_ROW_SCAN,must set rowkey column count
       * --if NORMAL_ROW_SCAN, must use the schema to check the rowkey column of scan param
       * @param row_desc: row_desc of row_value
       */
      inline int get_row_desc(const common::ObRowDesc*& row_desc) const
      {
        int ret = common::OB_SUCCESS;

        if (NULL == row_desc)
        {
          row_desc = &row_desc_;
        }
        else
        {
          ret = common::OB_ERROR;
        }

        return ret;
      }

    private:
      /**
       * initialize the sstable scanner
       * --use for reuse the ObSSTableScanParam
       * @param scan_context: sstable reader+block cache+block_index_cache
       * @param scan_param: sstable scan param
       */
      int initialize(const ScanContext* scan_context,
          const sstable::ObSSTableScanParam* scan_param);

      /**
       * alloc buffer from TSI_COMPACTSSTABLEV2_MODULE_ARENA_2
       * @param buf: buf
       * @param buf_size: buf_size
       */
      int alloc_buffer(char*& buf, const int64_t buf_size);

      /**
       * extract the scan column id from the sstable scan param
       * @param table_id: table id
       */
      int convert_column_id(const uint64_t table_id);

      /**
       * check input columns if inclusive rowkey columns
       * --row_count_flag: RowCountFlag
       * --talbe_id: table id
       */
      int match_rowkey_desc(const RowCountFlag row_count_flag, const uint64_t table_id);

      /**
       * build row desc
       */
      int build_row_desc(const common::ObCompactStoreType row_store_type, const uint64_t table_id);

      /**
       * build column index
       */
      int build_column_index(const common::ObCompactStoreType row_store_type, const uint64_t table_id);

      /**
       * deserialize row
       */
      int deserialize_row(common::ObCompactCellIterator& row);

      /**
       * search block index(the block index of mult block count)
       * @param first_time: is first time load?(MAX_BLOCK_COUNT)
       */
      int load_block_index(const bool first_time);

      /**
       * fetch next block
       */
      int fetch_next_block();

      /**
       * decompress current block
       * @param buf: block buf
       * @param buf_size: buf size
       */
      int read_current_block_data(const char*& buf, int64_t& buf_size);

      /**
       * read current block and move the block cursor of block index
       */
      int load_current_block_and_advance();

      /**
       * current batch block scan over
       */
      inline bool is_end_of_block() const
      {
        bool ret = true;
        if (is_valid_cursor())
        {
          if (sstable_scan_param_->is_reverse_scan())
          {
            ret = (index_array_cursor_ < 0);
          }
          else
          {
            ret = (index_array_cursor_ >= index_array_.block_count_);
          }
        }
        return ret;
      }

      /**
       * cursor is valid
       */
      inline bool is_valid_cursor() const
      {
        return ((index_array_cursor_ >= 0)
            && (index_array_cursor_ < index_array_.block_count_)
            && (index_array_cursor_ != INVALID_CURSOR));
      }

      /**
       * reset block index array(array + cursor);
       */
      inline void reset_block_index_array()
      {
        index_array_cursor_ = INVALID_CURSOR;
        index_array_.reset();
        index_array_.block_count_
          = ObBlockPositionInfos::NUMBER_OF_BATCH_BLOCK_INFO;
      }

      /**
       * prepare read blocks
       * --async(advise)
       * --decide the start and end pos
       */
      int prepare_read_blocks();

      /**
       * need scan forward
       */
      inline bool is_forward_status() const
      {
        return ITERATE_IN_PROGRESS == iterate_status_
          || ITERATE_LAST_BLOCK == iterate_status_
          || ITERATE_NEED_FORWARD == iterate_status_;
      }

      /**
       * move the cursor to the next block
       */
      inline void advance_to_next_block()
      {
        if (sstable_scan_param_->is_reverse_scan())
        {
          index_array_cursor_ --;
        }
        else
        {
          index_array_cursor_ ++;
        }
      }

      /**
       * the iteartor status
       */
      int check_status() const;

      /**
       * store the current row according to the scan param
       */
      int store_and_advance_row();

      /**
       * search the column in current row according to the cursor
       */
      int get_column_index(const int64_t cursor, ObSSTableScanColumnIndexes::Column& column) const;

      int dense_dense_store_row(const int64_t scan_column_cnt);

      int dense_sparse_store_row(const int64_t scan_column_cnt);

    private:
      //init
      const ScanContext* scan_context_;
      const sstable::ObSSTableScanParam* sstable_scan_param_;

      //index
      ObSSTableScanColumnIndexes scan_column_indexes_;
      ObBlockPositionInfos index_array_;
      int64_t index_array_cursor_;

      //status
      int64_t iterate_status_;
      bool end_of_data_;

      //block scanner
      ObSSTableBlockScanner block_scanner_;

      //rowkey rowvalue
      common::ObRowkey row_key_;
      common::ObRow row_value_;
      common::ObRowDesc row_desc_;

      common::ObObj rowkey_buf_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      int64_t rowkey_column_cnt_;

      //temp row
      uint64_t column_ids_[common::OB_MAX_COLUMN_NUMBER];
      common::ObObj column_objs_[common::OB_MAX_COLUMN_NUMBER];
      int64_t rowvalue_column_cnt_;
      int64_t max_column_offset_;

      //temp buffer
      char* uncomp_buf_;
      int64_t uncomp_buf_size_;
      char* internal_buf_;
      int64_t internal_buf_size_;

      ScanFlag scan_flag_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif
