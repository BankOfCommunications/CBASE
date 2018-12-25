#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_GETTER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_GETTER_H_

#include "common/ob_iterator.h"
#include "common/ob_rowkey.h"
#include "common/ob_row.h"
#include "common/ob_get_param.h"
#include "common/ob_common_param.h"
#include "ob_compact_sstable_reader.h"
#include "ob_sstable_block_index_cache.h"
#include "ob_sstable_block_cache.h"
#include "ob_sstable_block_index_mgr.h"
#include "ob_sstable_scan_column_indexes.h"
#include "ob_sstable_block_getter.h"
#include "sstable/ob_sstable_row_cache.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObCompactSSTableGetter
    {
    public:
      static const int64_t UNCOMP_BUF_SIZE = 1024 * 1024;
      static const int64_t INTERNAL_BUF_SIZE = 1024 * 1024;
      static const int64_t INVALID_CURSOR = 0xFFFFFFFFFFFFFFFF;

    private:
      enum
      {
        ROW_NOT_FOUND = 1,
        GET_NEXT_ROW,
        ITERATE_NORMAL,
        ITERATE_END
      };

    public:
      ObCompactSSTableGetter()
        : inited_(false),
          get_param_(NULL),
          sstable_reader_(NULL),
          table_id_(0),
          not_exit_col_ret_nop_(false),
          block_cache_(NULL),
          block_index_cache_(NULL),
          row_cache_(NULL),
          iterate_status_(INVALID_CURSOR),
          cur_row_index_(0),
          column_cursor_(0),
          handled_del_row_(false),
          bloomfilter_hit_(false),
          column_cnt_(0),
          uncomp_buf_(NULL),
          uncomp_buf_size_(UNCOMP_BUF_SIZE),
          internal_buf_(NULL),
          internal_buf_size_(INTERNAL_BUF_SIZE)
      {
        memset(rowkey_buf_array_, 0, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
        memset(column_ids_, 0, common::OB_MAX_COLUMN_NUMBER);
        memset(column_objs_, 0, common::OB_MAX_COLUMN_NUMBER);
      }

      ~ObCompactSSTableGetter()
      {
      }

      inline void reset()
      {
        inited_ = false;
        get_param_ = NULL;
        sstable_reader_ = NULL;
        get_column_indexes_.reset();
        table_id_ = 0;
        not_exit_col_ret_nop_ = false;
        block_cache_ = NULL;
        block_index_cache_ = NULL;
        row_cache_ = NULL;
        iterate_status_ = ITERATE_NORMAL;
        cur_row_index_ = 0;
        column_cursor_ = 0;
        column_cnt_ = 0;
        handled_del_row_ = false;
        bloomfilter_hit_ = false;
      }

      int next_row();

      int get_row(common::ObRowkey*& rowkey, common::ObRow*& rowvalue);

      int init(const common::ObGetParam& get_param,
          const ObCompactSSTableReader& sstable_reader,
          ObSSTableBlockCache& block_cache, 
          ObSSTableBlockIndexCache& block_index_cache, 
          sstable::ObSSTableRowCache& row_cache,
          bool not_exit_col_ret_nop = false);

    private:
      int initialize(ObSSTableBlockCache& block_cache,
          ObSSTableBlockIndexCache& block_index_cache,
          sstable::ObSSTableRowCache& row_cache,
          const ObCompactSSTableReader& sstable_reader);
    
      int alloc_buffer(char*& buf, const int64_t buf_size);

      int load_cur_block();

      int fetch_block();

      int init_column_mask();

      int switch_row();

      inline const common::ObCellInfo* get_cur_param_cell() const
      {
        const common::ObCellInfo* cell = NULL;
        const common::ObGetParam::ObRowIndex* row_index = NULL;
        row_index = get_param_->get_row_index();
        cell = (*get_param_)[row_index[cur_row_index_].offset_];
        if (NULL == cell)
        {
          TBSYS_LOG(WARN, "get param error");
        }
        return cell;
      }

      int get_column_index(const int64_t cursor, 
          ObSSTableScanColumnIndexes::Column& column) const;

      int store_current_cell(const ObSSTableScanColumnIndexes::Column& column);

      int store_dense_column(const ObSSTableScanColumnIndexes::Column& column);

      int store_sparse_column(const ObSSTableScanColumnIndexes::Column& column);

      int get_block_data(const char*& buf, int64_t& buf_size);

    private:
      bool inited_;
      const common::ObGetParam* get_param_;
      const ObCompactSSTableReader* sstable_reader_;
      ObSSTableScanColumnIndexes get_column_indexes_;
      uint64_t table_id_;
      bool not_exit_col_ret_nop_;

      ObSSTableBlockCache* block_cache_;
      ObSSTableBlockIndexCache* block_index_cache_;
      sstable::ObSSTableRowCache* row_cache_;

      int64_t iterate_status_;
      int64_t cur_row_index_;
      int64_t column_cursor_;
      bool handled_del_row_;
      bool bloomfilter_hit_;

      ObBlockPositionInfo block_pos_;
      ObSSTableBlockGetter getter_;

      common::ObRowkey row_key_;
      common::ObRow row_value_;
      common::ObRowDesc row_desc_;
      common::ObObj rowkey_buf_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];

      uint64_t column_ids_[common::OB_MAX_COLUMN_NUMBER];
      common::ObObj column_objs_[common::OB_MAX_COLUMN_NUMBER];
      int64_t column_cnt_;

      char* uncomp_buf_;
      int64_t uncomp_buf_size_;
      char* internal_buf_;
      int64_t internal_buf_size_;
      common::ObMemBuf row_buf_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif  // OCEANBASE_SSTABLE_OB_SSTABLE_GETTER_H_
