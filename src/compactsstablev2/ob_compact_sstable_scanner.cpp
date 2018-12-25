#include "ob_compact_sstable_scanner.h"
#include "common/ob_object.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace compactsstablev2
  {
    ObCompactSSTableScanner::ObCompactSSTableScanner()
      : scan_context_(NULL),
        sstable_scan_param_(NULL),
        index_array_cursor_(INVALID_CURSOR),
        iterate_status_(ITERATE_NOT_INITIALIZED),
        end_of_data_(false),
        rowkey_column_cnt_(0),
        rowvalue_column_cnt_(0),
        max_column_offset_(0),
        uncomp_buf_(NULL),
        uncomp_buf_size_(UNCOMP_BUF_SIZE),
        internal_buf_(NULL),
        internal_buf_size_(INTERNAL_BUF_SIZE),
        scan_flag_(INVALID_SCAN_FLAG)
    {
      //scan_column_indexes_(construct)
      //index_array_(construct)
      //block_scanner_(construct)
      //row_key_(construct)
      //row_value_(construct)
      //row_desc_(construct)
      memset(rowkey_buf_array_, 0, sizeof(common::ObObj) * common::OB_MAX_ROWKEY_COLUMN_NUMBER);
      memset(column_ids_, 0, sizeof(uint64_t) * common::OB_MAX_COLUMN_NUMBER);
      memset(column_objs_, 0, sizeof(uint64_t) * common::OB_MAX_COLUMN_NUMBER);
    }

    ObCompactSSTableScanner::~ObCompactSSTableScanner()
    {
      TBSYS_LOG(DEBUG, "destruct ObCompactSSTableScanner");
    }

    int ObCompactSSTableScanner::set_scan_param(const ObSSTableScanParam* scan_param, const ScanContext* scan_context)
    {
      int ret = OB_SUCCESS;

      ObCompactStoreType row_store_type = INVALID_COMPACT_STORE_TYPE;
      uint64_t table_id = OB_INVALID_ID;
      RowCountFlag row_count_flag = NORMAL_ROW_COUNT_FLAG;

      if (NULL == scan_param)
      {
        TBSYS_LOG(WARN, "scan_param is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == scan_context)
      {
        TBSYS_LOG(WARN, "scan_context is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!scan_context->is_valid())
      {
        TBSYS_LOG(WARN, "invalid scan_context: block_cache_=[%p], block_index_cache_=[%p]",
            scan_context->block_cache_, scan_context->block_index_cache_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        table_id = scan_param->get_table_id();
        if (0 == table_id || OB_INVALID_ID == table_id)
        {
          TBSYS_LOG(WARN, "table id is invalid: table_id=[%lu]", table_id);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = initialize(scan_context, scan_param)))
        {
          TBSYS_LOG(WARN, "initialize error: ret=[%d], scan_param=[%s]", ret, to_cstring(*scan_param));
        }
        else if (NULL == scan_context_->sstable_reader_)
        {
          row_store_type = DENSE_DENSE;
          iterate_status_ = ITERATE_END;
          end_of_data_ = true;
          row_count_flag = DENSE_DENSE_READER_NULL;
        }
        else if (!scan_context_->sstable_reader_->get_schema()->is_table_exist(table_id))
        {
          row_store_type = DENSE_SPARSE;
          iterate_status_ = ITERATE_END;
          end_of_data_ = true;
          row_count_flag = DENSE_SPARSE_TABLE_NULL;
        }
        else if (0 == scan_context_->sstable_reader_->get_row_count(table_id))
        {
          row_store_type = DENSE_SPARSE;
          iterate_status_ = ITERATE_END;
          end_of_data_ = true;
          row_count_flag = DENSE_DENSE_ROW_NULL;
        }
        else
        {
          row_store_type = scan_context_->sstable_reader_->get_row_store_type();
          if (DENSE_DENSE != row_store_type && DENSE_SPARSE != row_store_type)
          {
            TBSYS_LOG(WARN, "invalid row store type: row_store_type=[%d]", row_store_type);
            ret = OB_INVALID_ARGUMENT;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = build_row_desc(row_store_type, table_id)))
        {
          TBSYS_LOG(WARN, "build row desc error: ret=[%d], row_store_type=[%d], table_id=[%lu]", ret, row_store_type, table_id);
        }
        else if (OB_SUCCESS != (ret = build_column_index(row_store_type, table_id)))
        {
          TBSYS_LOG(WARN, "build column index error: ret=[%d], row_store_type=[%d]", ret, row_store_type);
        }
        else if (OB_SUCCESS != (ret = match_rowkey_desc(row_count_flag, table_id)))
        {
          TBSYS_LOG(WARN, "match rowkey desc error: ret=[%d], row_count_flag=[%d]", ret, row_count_flag);
        }
      }

      if (OB_SUCCESS == ret && ITERATE_END != iterate_status_)
      {
        if (OB_SUCCESS != (ret = load_block_index(true)))
        {
          TBSYS_LOG(WARN, "serarh block index error: ret=[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = fetch_next_block()))
        {
          TBSYS_LOG(WARN, "fetch next block error: ret=[%d]", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "sstable scanner set scan param success");
      }

      return ret;
    }

    int ObCompactSSTableScanner::get_next_row(const common::ObRowkey*& row_key, const common::ObRow*& row_value)
    {
      int ret = OB_SUCCESS;

      ObCompactCellIterator* row = NULL;
      ObObj row_obj;

      ret = check_status();

      if (OB_SUCCESS == ret)
      {
        if (end_of_data_)
        {
          iterate_status_ = ITERATE_END;
          ret = OB_ITER_END;
        }
        else
        {
          ret = block_scanner_.get_next_row(row);

          if (OB_SUCCESS != ret &&  OB_BEYOND_THE_RANGE != ret)
          {
            iterate_status_ = ITERATE_END;
            ret = OB_ITER_END;
          }
          else
          {
            do
            {
              if (OB_BEYOND_THE_RANGE == ret
                  && (OB_SUCCESS == (ret = fetch_next_block()))
                  && is_forward_status())
              {
                ret = block_scanner_.get_next_row(row);
              }
            }while (OB_BEYOND_THE_RANGE == ret);
          }
        }
      }

      ret = check_status();

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = deserialize_row(*row)))
        {
          TBSYS_LOG(WARN, "deserialize row error: ret=[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = store_and_advance_row()))
        {
          TBSYS_LOG(WARN, "store and advance row error: ret=[%d]", ret);
        }
        else
        {
          row_key = &row_key_;
          row_value = &row_value_;
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::initialize(const ScanContext* scan_context, const ObSSTableScanParam* scan_param)
    {
      int ret = OB_SUCCESS;

      scan_context_ = scan_context;
      sstable_scan_param_ = scan_param;
      scan_column_indexes_.reset();
      index_array_.reset();
      index_array_cursor_ = 0;
      iterate_status_ = ITERATE_NOT_START;
      end_of_data_ = false;
      //block_scanner.set_scan_param()
      row_desc_.reset();
      memset(rowkey_buf_array_, 0, sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER);
      rowkey_column_cnt_ = 0;
      memset(column_ids_, 0, sizeof(uint64_t) * OB_MAX_COLUMN_NUMBER);
      memset(column_objs_, 0, sizeof(ObObj) * OB_MAX_COLUMN_NUMBER);
      rowvalue_column_cnt_ = 0;
      max_column_offset_ = 0;
      uncomp_buf_ = NULL;
      //uncomp_buf_size_(not re inited)
      internal_buf_ = NULL;
      //internal_buf_size_(not re inited)
      scan_flag_ = INVALID_SCAN_FLAG;

      if (OB_SUCCESS != (ret = alloc_buffer(uncomp_buf_, uncomp_buf_size_)))
      {
        TBSYS_LOG(WARN, "alloc buffer error:ret=[%d], uncomp_buf_size_=[%ld]", ret, uncomp_buf_size_);
      }
      else if (OB_SUCCESS != (ret = alloc_buffer(internal_buf_, internal_buf_size_)))
      {
        TBSYS_LOG(WARN, "alloc buffer error:ret=[%d], internal_buf_size_=[%ld]", ret, internal_buf_size_);
      }
      else
      {
        TBSYS_LOG(DEBUG, "sstable scanner initialize success");
      }

      return ret;
    }

    int ObCompactSSTableScanner::alloc_buffer(char*& buf,
        const int64_t buf_size)
    {
      int ret = OB_SUCCESS;

      ModuleArena* arena = GET_TSI_MULT(ModuleArena, TSI_COMPACTSSTABLEV2_MODULE_ARENA_2);

      if (buf_size <= 0)
      {
        TBSYS_LOG(ERROR, "invalid buf size: buf_size=[%ld]", buf_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == arena)
      {
        TBSYS_LOG(ERROR, "failed to get tsi mult arena");
        ret = OB_ERROR;
      }
      else if (NULL == (buf = arena->alloc(buf_size)))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory for buf:buf_size=%ld", buf_size);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        TBSYS_LOG(DEBUG, "alloc buf success: buf_size=%ld", buf_size);
      }

      return ret;
    }

    int ObCompactSSTableScanner::convert_column_id(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      const ObSSTableSchema* const schema = scan_context_->sstable_reader_->get_schema();
      const uint64_t* const scan_column_array_ptr = sstable_scan_param_->get_column_id();
      const int64_t scan_column_array_size = sstable_scan_param_->get_column_id_size();
      const ObSSTableSchemaColumnDef* def = NULL;

      if (NULL == schema)
      {
        TBSYS_LOG(WARN, "schema ptr is NULL");
        ret = OB_ERROR;
      }
      else if (NULL == scan_column_array_ptr)
      {
        TBSYS_LOG(WARN, "scan_column_array_ptr is NULL");
        ret = OB_ERROR;
      }
      else if (scan_column_array_size <= 0)
      {
        TBSYS_LOG(WARN, "scan_column_array_size <= 0");
        ret = OB_ERROR;
      }

      uint64_t cur_column_id = 0;
      for (int64_t i = 0; OB_SUCCESS == ret && i < scan_column_array_size; i ++)
      {
        cur_column_id = scan_column_array_ptr[i];
        if (0 == cur_column_id || OB_INVALID_ID == cur_column_id)
        {
          TBSYS_LOG(WARN, "invalid column id: column_id=[%lu]", cur_column_id);
          ret = OB_ERROR;
        }
        else if (NULL != (def = schema->get_column_def(table_id, cur_column_id)))
        {
          if (def->is_rowkey_column())
          {//rowkey
            if (OB_SUCCESS != (ret = scan_column_indexes_.add_column_id(ObSSTableScanColumnIndexes::Rowkey, def->offset_, cur_column_id)))
            {
              TBSYS_LOG(WARN, "scan column indexes add column id error: i=[%ld], ret=[%d], scan_column_indexes_=[%s], "
                  "offset=[%ld], cur_column_id=[%lu]", i, ret, to_cstring(scan_column_indexes_), def->offset_, cur_column_id);
            }
          }
          else
          {//row value
            if (OB_SUCCESS != (ret = scan_column_indexes_.add_column_id(ObSSTableScanColumnIndexes::Normal, def->offset_, cur_column_id)))
            {
              TBSYS_LOG(WARN, "scan column indexes add column id error: i=[%ld], ret=[%d], scan_column_indexes_=[%s], "
                  "offset=[%ld], cur_column_id=[%lu]", i, ret, to_cstring(scan_column_indexes_), def->offset_, cur_column_id);
            }
            else
            {
              if (def->offset_ > max_column_offset_)
              {
                max_column_offset_ = def->offset_;
              }
            }
          }
        }
        else
        {//not exist;
          if (OB_SUCCESS != (ret = scan_column_indexes_.add_column_id(ObSSTableScanColumnIndexes::NotExist, 0, cur_column_id)))
          {
            TBSYS_LOG(WARN, "scan column indexes add column id error: i=[%ld], ret=[%d], scan_column_indexes_=[%s], cur_column_id=[%lu]",
                i, ret, to_cstring(scan_column_indexes_), cur_column_id);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::match_rowkey_desc(const RowCountFlag row_count_flag, const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      const uint64_t* const scan_column_array_ptr = sstable_scan_param_->get_column_id();
      const int64_t scan_rowkey_column_cnt = sstable_scan_param_->get_rowkey_column_count();
      const bool is_daily_merge_scan = sstable_scan_param_->is_daily_merge_scan();

      const ObSSTableSchema* schema = NULL;
      const ObSSTableSchemaColumnDef* def_array_ptr = NULL;
      int64_t def_array_size = 0;

      if (NULL == scan_column_array_ptr)
      {
        TBSYS_LOG(WARN, "scan column array ptr is NULL");
        ret = OB_ERROR;
      }
      else if (scan_rowkey_column_cnt < 0)
      {
        TBSYS_LOG(WARN, "scan rowkey column cnt < 0: scan_rowkey_column_cnt=[%ld]", scan_rowkey_column_cnt);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (is_daily_merge_scan
            || DENSE_DENSE_FULL_ROW_SCAN == scan_flag_
            || DENSE_SPARSE_FULL_ROW_SCAN == scan_flag_)
        {
          row_desc_.set_rowkey_cell_count(scan_rowkey_column_cnt);
        }
        else if (NORMAL_ROW_COUNT_FLAG == row_count_flag
            || DENSE_DENSE_ROW_NULL == row_count_flag)
        {
          if (NULL == (schema = scan_context_->sstable_reader_->get_schema()))
          {
            TBSYS_LOG(WARN, "schema is null");
            ret = OB_ERROR;
          }
          else if (NULL == (def_array_ptr = schema->get_table_schema(table_id, true, def_array_size)))
          {
            TBSYS_LOG(WARN, "schema get table schema error: schema=[%s], table_id=[%lu]", to_cstring(*schema), table_id);
            ret = OB_ERROR;
          }
          else if (0 == def_array_size)
          {
            TBSYS_LOG(WARN, "def_array_size must > 0: def_array_size=[%lu]", def_array_size);
            ret = OB_ERROR;
          }
          else if (scan_rowkey_column_cnt != def_array_size)
          {
            //do_nothing
          }
          else
          {
            int64_t i = 0;
            for (i = 0; i < def_array_size; i ++)
            {
              if (def_array_ptr[i].column_id_ != scan_column_array_ptr[i])
              {
                break;
              }
            }

            if (i == def_array_size)
            {
              row_desc_.set_rowkey_cell_count(scan_rowkey_column_cnt);
            }
          }
        }
        else if (DENSE_SPARSE_TABLE_NULL == row_count_flag)
        {
          //do nothing
        }
        else
        {
          TBSYS_LOG(WARN, "can not go here: scan_flag_=[%d], row_count_flag=[%d], is_daily_merge_scan=[%d]",
              scan_flag_, row_count_flag, is_daily_merge_scan);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_value_.set_row_desc(row_desc_);
      }

      return ret;
    }

    int ObCompactSSTableScanner::build_row_desc(const ObCompactStoreType row_store_type, const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      const uint64_t* const column_id_array_ptr = sstable_scan_param_->get_column_id();
      const int64_t column_id_array_size = sstable_scan_param_->get_column_id_size();

      if (NULL == column_id_array_ptr)
      {
        TBSYS_LOG(WARN, "column_id_array_ptr is NULL");
        ret = OB_ERROR;
      }
      else if (0 == column_id_array_size)
      {
        TBSYS_LOG(WARN, "column_id_array_size==0");
        ret = OB_ERROR;
      }

      //DENSE_DENSE:normal column
      //DENSE_SPARSE:normal column + action column
      if (OB_SUCCESS == ret)
      {
        if (DENSE_DENSE == row_store_type || DENSE_SPARSE == row_store_type)
        {
          for (int64_t i = 0; OB_SUCCESS == ret && i < column_id_array_size; i ++)
          {
            if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id, column_id_array_ptr[i])))
            {
              TBSYS_LOG(WARN, "row desc add column desc error: ret=[%d], table_id=[%lu], column_id=[%lu]",
                  ret, table_id, column_id_array_ptr[i]);
            }
          }
        }

        if (DENSE_SPARSE == row_store_type)
        {
          if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
          {
            TBSYS_LOG(WARN, "row desc add column desc error: ret=[%d], table_id=[%lu]", ret, table_id);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::build_column_index(const ObCompactStoreType row_store_type, const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      const uint64_t* const column_id_array_ptr = sstable_scan_param_->get_column_id();
      const int64_t column_id_array_size = sstable_scan_param_->get_column_id_size();

      if (NULL == column_id_array_ptr)
      {
        TBSYS_LOG(WARN, "column_id_array_ptr is NULL");
        ret = OB_ERROR;
      }
      else if (0 == column_id_array_size)
      {
        TBSYS_LOG(WARN, "column_id_array_size==0");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (DENSE_DENSE == row_store_type)
        {
          if (sstable_scan_param_->is_full_row_scan())
          {
            scan_flag_ = DENSE_DENSE_FULL_ROW_SCAN;
            max_column_offset_ = OB_MAX_COLUMN_NUMBER;
          }
          else
          {
            if (OB_SUCCESS != (ret = convert_column_id(table_id)))
            {
              TBSYS_LOG(WARN, "convert column id error: ret=[%d]", ret);
            }
            else
            {
              scan_flag_ = DENSE_DENSE_NORMAL_ROW_SCAN;
            }
          }
        }
        else if (DENSE_SPARSE == row_store_type)
        {
          if (sstable_scan_param_->is_full_row_scan())
          {
            scan_flag_ = DENSE_SPARSE_FULL_ROW_SCAN;
          }
          else
          {
            scan_flag_ = DENSE_SPARSE_NORMAL_ROW_SCAN;
          }
        }
      }


      return ret;
    }

    int ObCompactSSTableScanner::deserialize_row(ObCompactCellIterator& row)
    {
      int ret = OB_SUCCESS;

      int64_t rowkey_cnt = 0;
      int64_t rowvalue_cnt = 0;
      bool is_row_finished = false;
      const ObCompactStoreType row_store_type = scan_context_->sstable_reader_->get_row_store_type();

      for (; OB_SUCCESS == ret; )
      {//rowkey
        if (OB_SUCCESS != (ret = row.get_next_cell(rowkey_buf_array_[rowkey_cnt], is_row_finished)))
        {
          TBSYS_LOG(WARN, "row get next cell error: i=[%ld], ret=[%d]", rowkey_cnt, ret);
        }
        else
        {
          if (is_row_finished)
          {
            is_row_finished = false;
            rowkey_column_cnt_ = rowkey_cnt;
            row_key_.assign(rowkey_buf_array_, rowkey_column_cnt_);
            break;
          }
          else
          {
            rowkey_cnt ++;
          }
        }
      }

      //row value
      if (DENSE_SPARSE == row_store_type)
      {
        for (int64_t i = 0; OB_SUCCESS == ret; i ++)
        {
          if (OB_SUCCESS != (ret = row.get_next_cell(column_objs_[rowvalue_cnt], column_ids_[rowvalue_cnt], is_row_finished)))
          {
            TBSYS_LOG(WARN, "row get next cell error: i=[%ld], ret=[%d]", i, ret);
          }
          else
          {
            if (is_row_finished)
            {
              rowvalue_column_cnt_ = rowvalue_cnt;
              break;
            }
            else
            {
              rowvalue_cnt ++;
            }
          }
        }
      }
      else if (DENSE_DENSE == row_store_type)
      {
        for (int64_t i = 0; OB_SUCCESS == ret; i ++)
        {
          if (OB_SUCCESS != (ret = row.get_next_cell(column_objs_[rowvalue_cnt], is_row_finished)))
          {
            TBSYS_LOG(WARN, "row get next cell error: i=[%ld], ret=[%d]", i, ret);
          }
          else
          {
            if (is_row_finished || i >= max_column_offset_)
            {
              rowvalue_column_cnt_ = rowvalue_cnt;
              break;
            }
            else
            {
              rowvalue_cnt ++;
            }
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::load_block_index(
        const bool first_time)
    {
      int ret = OB_SUCCESS;

      SearchMode mode = OB_SEARCH_MODE_LESS_THAN;
      ObBlockIndexPositionInfo info;
      info.reset();
      const uint64_t table_id = sstable_scan_param_->get_table_id();
      const ObSSTableHeader* sstable_header = scan_context_->sstable_reader_->get_sstable_header();
      const ObSSTableTableIndex* table_index = scan_context_->sstable_reader_->get_table_index(table_id);
      int64_t end_offset = 0;

      if (NULL == sstable_header)
      {
        TBSYS_LOG(WARN, "sstable header is NULL");
        ret = OB_ERROR;
      }
      else if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid table id: table_id=[%lu]", table_id);
        ret = OB_ERROR;
      }
      else if (NULL == table_index)
      {
        TBSYS_LOG(WARN, "table index is null: table_id=[%lu]", table_id);
        ret = OB_ERROR;
      }
      else
      {
        info.sstable_file_id_ = scan_context_->sstable_reader_->get_sstable_id();
        info.index_offset_ = table_index->block_index_offset_;
        info.index_size_ = table_index->block_index_size_;
        info.endkey_offset_ = table_index->block_endkey_offset_;
        info.endkey_size_ = table_index->block_endkey_size_;
        info.block_count_ = table_index->block_count_;
      }

      if (OB_SUCCESS == ret)
      {//prepare for next offset
        if (!first_time)
        {
          if (sstable_scan_param_->is_reverse_scan())
          {
            end_offset = index_array_.position_info_[0].offset_;
            mode = OB_SEARCH_MODE_LESS_THAN;
          }
          else
          {
            end_offset = index_array_.position_info_[index_array_.block_count_ - 1].offset_;
            mode = OB_SEARCH_MODE_GREATER_THAN;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {//block index array(if not find, return OB_BEYOND_THE_RANGE)
        reset_block_index_array();
        if (first_time)
        {
          if (OB_SUCCESS != (ret = scan_context_->block_index_cache_
                ->get_block_position_info(
                  info, table_id, sstable_scan_param_->get_range(),
                  sstable_scan_param_->is_reverse_scan(),
                  index_array_)))
          {
            TBSYS_LOG(DEBUG, "block index cache get block position info"
                "error:ret=%d,info=%s,table_id=%lu,range=%s,"
                "is_reverse_scan=%d", ret, to_cstring(info), table_id,
                to_cstring(sstable_scan_param_->get_range()),
                sstable_scan_param_->is_reverse_scan());
          }
        }
        else
        {
          if (OB_SUCCESS != (ret
                = scan_context_->block_index_cache_->next_offset(
                 info, table_id, end_offset, mode, index_array_)))
          {
            TBSYS_LOG(DEBUG, "block index cache next offset:"
                "ret=%d,info=%s,table_id=%lu, end_offset=%ld,mode=%d",
                ret, to_cstring(info), table_id, end_offset, mode);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = prepare_read_blocks()))
        {
          TBSYS_LOG(WARN, "prepare read blocks error:ret=%d", ret);
        }
      }

      if (OB_BEYOND_THE_RANGE == ret)
      {
        iterate_status_ = ITERATE_END;
        TBSYS_LOG(DEBUG, "OB_BEYOND_THE_RANGE");
        ret = OB_SUCCESS;
      }
      else if (OB_SUCCESS != ret)
      {
        iterate_status_ = ITERATE_IN_ERROR;
        TBSYS_LOG(WARN, "ITERATE ERROR:ret=%d", ret);
      }

      return ret;
    }

    int ObCompactSSTableScanner::fetch_next_block()
    {
      int ret = OB_SUCCESS;

      do
      {
        if (OB_SUCCESS != (ret = load_current_block_and_advance()))
        {
          TBSYS_LOG(WARN, "load current block and advance error:ret=%d",
              ret);
          break;
        }
      }
      while (ITERATE_NEED_FORWARD == iterate_status_);

      return ret;
    }

    int ObCompactSSTableScanner::read_current_block_data(
        const char*& buf, int64_t& buf_size)
    {
      int ret = OB_SUCCESS;

      ObSSTableBlockBufferHandle handler;
      const ObBlockPositionInfo& pos
        = index_array_.position_info_[index_array_cursor_];

      const char* comp_buf = NULL;
      int64_t comp_buf_size = 0;

      uint64_t sstable_file_id = scan_context_->sstable_reader_->get_sstable_id();
      bool is_reverse_scan = sstable_scan_param_->is_reverse_scan();
      const char* block_data_ptr = NULL;
      int64_t block_data_size = 0;
      const uint64_t table_id = sstable_scan_param_->get_table_id();
      ObRecordHeaderV2 header;

      if (OB_INVALID_ID == sstable_file_id)
      {
        TBSYS_LOG(WARN, "invalid sstable file id");
        ret = OB_ERROR;
      }
      else if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid table id:table_id=%lu", table_id);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (sstable_scan_param_->is_sync_read())
        {//sync read
          ret = scan_context_->block_cache_->get_block_readahead(
              sstable_file_id,
              table_id, index_array_, index_array_cursor_,
              is_reverse_scan, handler);
          if (pos.size_ == ret)
          {
            block_data_ptr = handler.get_buffer();
            block_data_size = pos.size_;
            ret = OB_SUCCESS;
          }
          else
          {
            TBSYS_LOG(ERROR, "IO ERROR:ret=%d,sstable_file_id=%lu,"
                "table_id=%lu,index_array_=%s,index_array_cursor_=%ld,"
                "is_reverse_scan=%d", ret, sstable_file_id, table_id,
                to_cstring(index_array_), index_array_cursor_,
                is_reverse_scan);
            ret = OB_IO_ERROR;
          }
        }
        else
        {//async read
          ret = scan_context_->block_cache_->get_block_aio(
              sstable_file_id, pos.offset_, pos.size_,
              handler, TIME_OUT_US, table_id);
          if (pos.size_ == ret)
          {
            block_data_ptr = handler.get_buffer();
            block_data_size = pos.size_;
            ret = OB_SUCCESS;
          }
          else
          {
            TBSYS_LOG(ERROR, "IO ERROR:ret=%d,sstable_file_id=%lu,"
                "offset_=%ld,size_=%ld,table_id=%lu", ret,
                sstable_file_id, pos.offset_, pos.size_, table_id);
            ret = OB_IO_ERROR;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = ObRecordHeaderV2::get_record_header(
                block_data_ptr, block_data_size, header,
                comp_buf, comp_buf_size)))
        {
          TBSYS_LOG(WARN, "get record header error:ret=%d,"
              "block_data_ptr=%p,block_data_size=%ld",
              ret, block_data_ptr, block_data_size);
        }
        else if (header.data_length_ > uncomp_buf_size_)
        {
          uncomp_buf_size_ = header.data_length_;
          if (OB_SUCCESS != (ret = alloc_buffer(uncomp_buf_,
                  uncomp_buf_size_)))
          {
            TBSYS_LOG(ERROR, "failed to alloc buffer:ret=%d,"
                "uncomp_buf_size=%ld", ret, uncomp_buf_size_);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t real_size = 0;
        if (header.is_compress())
        {
          ObCompressor* dec = const_cast<ObCompactSSTableReader*>(
              scan_context_->sstable_reader_)->get_decompressor();
          if (NULL != dec)
          {
            if (OB_SUCCESS != (ret = dec->decompress(
                    comp_buf, comp_buf_size,
                    uncomp_buf_, header.data_length_, real_size)))
            {
                TBSYS_LOG(WARN, "decompress error:ret=%d,"
                    "comp_buf=%p,comp_buf_size=%ld,data_length_=%ld",
                    ret, comp_buf, comp_buf_size, header.data_length_);
            }
            else
            {
              buf = uncomp_buf_;
              buf_size = real_size;
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "compressor is NULL");
          }
        }
        else
        {
          memcpy(uncomp_buf_, comp_buf, comp_buf_size);
          buf = uncomp_buf_;
          buf_size = comp_buf_size;
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::load_current_block_and_advance()
    {
      int ret = OB_SUCCESS;

      ObCompactStoreType row_store_type
        = scan_context_->sstable_reader_->get_row_store_type();

      if (DENSE_DENSE != row_store_type
          && DENSE_SPARSE != row_store_type)
      {
        TBSYS_LOG(WARN, "invalid row store type:row_stroe_type=%d",
            row_store_type);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && is_forward_status())
      {
        if (ITERATE_LAST_BLOCK == iterate_status_)
        {//last block is the end, no need to looking forward
          iterate_status_ = ITERATE_END;
          TBSYS_LOG(DEBUG, "iterate_status==ITERATE_END");
        }
        else if (is_end_of_block())
        {//current batch block scan over, begin fetch next batch blocks
          if (OB_SUCCESS != (ret = load_block_index(false)))
          {
            TBSYS_LOG(WARN, "load block index error:ret=%d", ret);
          }
        }
      }

      if (OB_SUCCESS == ret && is_forward_status())
      {
        if (is_end_of_block())
        {//search block index, but get noting
          iterate_status_ = ITERATE_END;
          TBSYS_LOG(DEBUG, "iterate_status==ITERATE_END");
        }
        else
        {
          const char* block_data_ptr = NULL;
          int64_t block_data_size = 0;

          ret = read_current_block_data(block_data_ptr,
              block_data_size);
          if (OB_SUCCESS == ret && NULL != block_data_ptr &&
              block_data_size > 0)
          {
            bool need_looking_forward = false;
            ObSSTableBlockReader::BlockData block_data(internal_buf_, internal_buf_size_, block_data_ptr, block_data_size);

            ret = block_scanner_.set_scan_param(sstable_scan_param_->get_range(), sstable_scan_param_->is_reverse_scan(),
                block_data, row_store_type, need_looking_forward);
            if (OB_SUCCESS == ret)
            {
              advance_to_next_block();
              iterate_status_ = need_looking_forward ? ITERATE_IN_PROGRESS : ITERATE_LAST_BLOCK;
            }
            else if (OB_BEYOND_THE_RANGE == ret)
            {
              if (!need_looking_forward)
              {
                iterate_status_ = ITERATE_END;
                TBSYS_LOG(DEBUG, "iterate_status==ITERATE_END");
              }
              else
              {
                //current block is not we wanted, has no data in query range and either not the end of scan,set
                //stataus to NEED_FORWARD tell fetch_next_block call this function again.
                //it happens only in reverse scan and query_range.end_key in
                //(prev_block.end_key < query_range.end_key <current_block.start_key)
                advance_to_next_block();
                iterate_status_ = ITERATE_NEED_FORWARD;
              }

              ret = OB_SUCCESS;
            }
            else
            {
              TBSYS_LOG(WARN, "block scanner set scan param error: ret=[%d], range=[%s], is_reverse_scan=[%d], "
                  "row_store_type=[%d], need_looking_forward=[%d]", ret, to_cstring(sstable_scan_param_->get_range()),
                  sstable_scan_param_->is_reverse_scan(), row_store_type, need_looking_forward);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "read current block data error: ret=[%d], block_data_ptr=[%p], block_data_size=[%ld]",
                ret, block_data_ptr, block_data_size);
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        iterate_status_ = ITERATE_IN_ERROR;
        TBSYS_LOG(ERROR, "iterate_staus_ == ITERATE_IN_ERROR");
      }

      return ret;
    }

    int ObCompactSSTableScanner::prepare_read_blocks()
    {
      int ret = OB_SUCCESS;

      const uint64_t sstable_file_id
        = scan_context_->sstable_reader_->get_sstable_id();
      bool is_result_cached
        = sstable_scan_param_->get_is_result_cached();
      bool is_reverse_scan
        = sstable_scan_param_->is_reverse_scan();
      const uint64_t table_id = sstable_scan_param_->get_table_id();

      if (OB_INVALID_ID == sstable_file_id)
      {
        TBSYS_LOG(WARN, "invalid sstable file id");
        ret = OB_ERROR;
      }
      else if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid table id:table_id=%lu", table_id);
        ret = OB_ERROR;
      }
      else
      {
        if (is_reverse_scan)
        {
          index_array_cursor_ = index_array_.block_count_ - 1;
        }
        else
        {
          index_array_cursor_ = 0;
        }
      }

      if (OB_SUCCESS == ret)
      {
        iterate_status_ = ITERATE_IN_PROGRESS;

        if (!sstable_scan_param_->is_sync_read())
        {
          if (OB_SUCCESS != (ret = scan_context_->block_cache_->advise(
                  sstable_file_id, index_array_, table_id,
                  is_result_cached, is_reverse_scan)))
          {
            TBSYS_LOG(WARN, "block cache advise error:ret=%d,"
                "sstable_file_id=%lu, index_array_=%s, table_id=%lu,"
                "is_result_cached=%d, is_reverse_scan=%d",
                ret, sstable_file_id, to_cstring(index_array_), table_id,
                is_result_cached, is_reverse_scan);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::check_status() const
    {
      int ret = OB_SUCCESS;

      switch (iterate_status_)
      {
        case ITERATE_IN_PROGRESS:
        case ITERATE_LAST_BLOCK:
        case ITERATE_NEED_FORWARD:
          ret = OB_SUCCESS;
          break;
        case ITERATE_NOT_INITIALIZED:
        case ITERATE_NOT_START:
          ret = OB_NOT_INIT;
          break;
        case ITERATE_END:
          ret = OB_ITER_END;
          break;
        default:
          ret = OB_ERROR;
      }

      return ret;
    }

    int ObCompactSSTableScanner::get_column_index(const int64_t cursor,
        ObSSTableScanColumnIndexes::Column& column) const
    {
      int ret = OB_SUCCESS;

      const ObCompactStoreType row_store_type = scan_context_->sstable_reader_->get_row_store_type();

      if (DENSE_DENSE != row_store_type
          && DENSE_SPARSE != row_store_type)
      {
        TBSYS_LOG(ERROR, "invalid row store type:row_store_type=%d",
            row_store_type);
      }
      else if (cursor < 0 || INVALID_CURSOR == cursor)
      {
        TBSYS_LOG(ERROR, "invalid cursor:cursor=%ld", cursor);
      }
      else if (OB_SUCCESS != (ret = scan_column_indexes_.get_column(cursor, column)))
      {
        TBSYS_LOG(ERROR, "scan column indexes get clumn error: ret=[%d], scan_column_indexes_=[%s], cursor=[%ld]",
            ret, to_cstring(scan_column_indexes_), cursor);
      }
      else if (DENSE_SPARSE == row_store_type
          && ObSSTableScanColumnIndexes::Normal == column.type_)
      {
        column.type_ = ObSSTableScanColumnIndexes::NotExist;
        for (int64_t i = 0; i < rowvalue_column_cnt_; i ++)
        {
          if (column.id_ == column_ids_[i])
          {
            column.type_ = ObSSTableScanColumnIndexes::Normal;
            column.index_ = i;
            break;
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::store_and_advance_row()
    {
      int ret = OB_SUCCESS;

      const int64_t scan_column_cnt = sstable_scan_param_->get_column_id_size();
      ObSSTableScanColumnIndexes::Column column;
      const ObCompactStoreType row_store_type = scan_context_->sstable_reader_->get_row_store_type();

      if (scan_column_cnt <= 0)
      {
        TBSYS_LOG(ERROR, "invalid column cnt: column_cnt=[%ld]", scan_column_cnt);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (DENSE_DENSE == row_store_type)
        {
          if (OB_SUCCESS != (ret = dense_dense_store_row(scan_column_cnt)))
          {
            TBSYS_LOG(WARN, "dense dense store row error: ret=[%d], scan_column_cnt=[%ld]", ret, scan_column_cnt);
          }
        }
        else if (DENSE_SPARSE == row_store_type)
        {
          if (OB_SUCCESS != (ret = dense_sparse_store_row(scan_column_cnt)))
          {
            TBSYS_LOG(WARN, "dense sparse store row error: ret=[%d], scan_column_cnt=[%ld]", ret, scan_column_cnt);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::dense_dense_store_row(const int64_t scan_column_cnt)
    {
      int ret = OB_SUCCESS;

      static const ObObj nop_obj(ObExtendType, 0, 0, ObActionFlag::OP_NOP);
      static const ObObj null_obj(ObNullType, 0, 0, 0);
      static const ObObj valid_obj(ObExtendType, 0, 0, ObActionFlag::OP_VALID);

      int64_t index = 0;
      const bool not_exit_col_ret_nop = sstable_scan_param_->is_not_exit_col_ret_nop();
      ObSSTableScanColumnIndexes::Column column;

      if (DENSE_DENSE_FULL_ROW_SCAN == scan_flag_)
      {
        for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_column_cnt_; i ++, index ++)
        {
          if (OB_SUCCESS != (ret = row_value_.raw_set_cell(index, rowkey_buf_array_[i])))
          {
            TBSYS_LOG(WARN, "row value raw set cell error: i=[%ld], ret=[%d], index=[%ld], obj=[%s]",
                i, ret, index, to_cstring(rowkey_buf_array_[i]));
          }
        }

        for (int64_t i = 0; OB_SUCCESS == ret && i < rowvalue_column_cnt_; i ++, index ++)
        {
          if (OB_SUCCESS != (ret = row_value_.raw_set_cell(index, column_objs_[i])))
          {
            TBSYS_LOG(WARN, "row value raw set cell error: i=[%ld], ret=[%d], index=[%ld], obj=[%s]",
                i, ret, index, to_cstring(rowkey_buf_array_[i]));
          }
        }
      }
      else if (DENSE_DENSE_NORMAL_ROW_SCAN == scan_flag_)
      {
        for (; OB_SUCCESS == ret && index < scan_column_cnt; index ++)
        {
          if (OB_SUCCESS != (ret = get_column_index(index, column)))
          {
              TBSYS_LOG(WARN, "get column index:ret=[%d], index=[%ld]", ret, index);
          }
          else if (ObSSTableScanColumnIndexes::Rowkey == column.type_)
          {
            if (OB_SUCCESS != (ret = row_value_.raw_set_cell(index, rowkey_buf_array_[column.index_])))
            {
              TBSYS_LOG(WARN, "row value set cell error:ret=[%d], index=[%ld], column.index_=[%ld], obj=[%s], row_value=[%s]",
                  ret, index, column.index_, to_cstring(rowkey_buf_array_[column.index_]), to_cstring(row_value_));
            }
          }
          else if (ObSSTableScanColumnIndexes::NotExist == column.type_)
          {
            if (not_exit_col_ret_nop)
            {
              if (OB_SUCCESS != (ret = row_value_.raw_set_cell(index, nop_obj)))
              {
                TBSYS_LOG(WARN, "row value raw set cell error: ret=[%d], index=[%ld], row_value=[%s]",
                    ret, index, to_cstring(row_value_));
              }
            }
            else
            {
              if (OB_SUCCESS != (ret = row_value_.raw_set_cell(index, null_obj)))
              {
                TBSYS_LOG(WARN, "row value raw set cell error: ret=[%d], index=[%ld], row_value_=[%s]",
                    ret, index, to_cstring(row_value_));
              }
            }
          }
          else
          {
            if (OB_SUCCESS != (ret = row_value_.raw_set_cell(index, column_objs_[column.index_])))
            {
              TBSYS_LOG(WARN, "row value raw set cell error:ret=[%d], row_value=[%s], index=[%ld], column.index=[%ld], obj=[%s]",
                  ret, to_cstring(row_value_), index, column.index_, to_cstring(column_objs_[column.index_]));
            }
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableScanner::dense_sparse_store_row(const int64_t scan_column_cnt)
    {
      int ret = OB_SUCCESS;

      static const ObObj nop_obj(ObExtendType, 0, 0, ObActionFlag::OP_NOP);
      static const ObObj null_obj(ObNullType, 0, 0, 0);
      static const ObObj valid_obj(ObExtendType, 0, 0, ObActionFlag::OP_VALID);

      int64_t size = 0;
      int64_t cell_idx = 0;
      const ObRowDesc* row_desc = row_value_.get_row_desc();
      const ObSSTableSchema* schema = NULL;
      const ObSSTableSchemaColumnDef* def_array = NULL;
      const uint64_t table_id = sstable_scan_param_->get_table_id();

      if (NULL == (schema = scan_context_->sstable_reader_->get_schema()))
      {
        TBSYS_LOG(WARN, "schema is null");
        ret = OB_ERROR;
      }
      else if (NULL == (def_array = schema->get_table_schema(table_id, true, size)))
      {
        TBSYS_LOG(WARN, "def array is NULL");
        ret = OB_ERROR;
      }
      else if (0 == size)
      {
        TBSYS_LOG(WARN, "the rowkey count of table is 0");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        //reset for DENSE_SPARSE
        if (OB_SUCCESS != (ret = row_value_.reset(false, ObRow::DEFAULT_NOP)))
        {
          TBSYS_LOG(WARN, "row value reset error: ret=[%d], row_value_=[%s]", ret, to_cstring(row_value_));
        }
        else if (OB_ACTION_FLAG_COLUMN_ID == column_ids_[0])
        {
          if (OB_SUCCESS != (ret = row_value_.raw_set_cell(scan_column_cnt, column_objs_[0])))
          {
            TBSYS_LOG(WARN, "row value raw set cell error: ret=[%d], scan_column_cnt=[%ld], obj=[%s]",
                ret, scan_column_cnt, to_cstring(column_objs_[0]));
          }
        }
        else if (OB_SUCCESS != (ret = row_value_.raw_set_cell(scan_column_cnt, valid_obj)))
        {
          TBSYS_LOG(WARN, "row value raw set cell error: ret=[%d], scan_column_cnt=[%ld]", ret, scan_column_cnt);
        }
      }

      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_column_cnt_; i ++)
      {
        if (OB_INVALID_INDEX != (cell_idx = row_desc->get_idx(table_id, def_array[i].column_id_)))
        {
          if (OB_SUCCESS != (ret = row_value_.raw_set_cell(cell_idx, rowkey_buf_array_[i])))
          {
            TBSYS_LOG(WARN, "row value raw set cell error: ret=[%d], cell_idx=[%ld], obj=[%s]",
                ret, cell_idx, to_cstring(rowkey_buf_array_[i]));
          }
        }
      }

      for (int64_t i = 0; OB_SUCCESS == ret && i < rowvalue_column_cnt_; i ++)
      {
        if (OB_INVALID_INDEX != (cell_idx = row_desc->get_idx(table_id, column_ids_[i])))
        {
          if (OB_SUCCESS != (ret = row_value_.raw_set_cell(cell_idx, column_objs_[i])))
          {
            TBSYS_LOG(WARN, "row value raw set cell error: ret=[%d], cell_idx=[%ld], obj=[%s]",
                ret, cell_idx, to_cstring(column_objs_[i]));
          }
        }
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
