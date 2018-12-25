#include "common/ob_common_stat.h"
#include "ob_compact_sstable_getter.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObCompactSSTableGetter::next_row()
    {
      int ret = OB_SUCCESS;
      handled_del_row_ = false;

      if (OB_SUCCESS == ret)
      {
        ret = switch_row();
      }

      return ret;
    }

    int ObCompactSSTableGetter::init(const common::ObGetParam& get_param,
        const ObCompactSSTableReader& sstable_reader,
        ObSSTableBlockCache& block_cache, 
        ObSSTableBlockIndexCache& block_index_cache, 
        sstable::ObSSTableRowCache& row_cache,
        bool not_exit_col_ret_nop/*=false*/)
    {
      int ret = OB_SUCCESS;

      reset();

      if (NULL == get_param.get_row_index()
          || get_param.get_row_size() <= 0)
      {
        TBSYS_LOG(WARN, "get_param error:%p, %ld", 
            get_param.get_row_index(), get_param.get_row_size());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = initialize(block_cache, block_index_cache,
              row_cache, sstable_reader)))
      {
        TBSYS_LOG(WARN, "initialize fail:ret=%d", ret);
      }
      else
      {
        get_param_ = &get_param;
        not_exit_col_ret_nop_ = not_exit_col_ret_nop;
        inited_ = true;
        const ObCellInfo* cell = NULL;

        if (NULL == (cell = get_cur_param_cell()))
        {
          TBSYS_LOG(WARN, "get cur param cell error:cell==NULL");
          ret = OB_ERROR;
        }
        else
        {
          table_id_ = cell->table_id_;
        }

        const ObGetParam::ObRowIndex* row_index = NULL;
        row_index = get_param_->get_row_index();
        const ObBloomFilterV1* bloom_filter 
          = sstable_reader_->get_table_bloomfilter(table_id_);
        cell = (*get_param_)[row_index[cur_row_index_].offset_];
        if (!bloom_filter->may_contain(cell->row_key_))
        {
          bloomfilter_hit_ = false;
        }
        else
        {
          bloomfilter_hit_ = true;
          ret = load_cur_block();
          if (OB_SEARCH_NOT_FOUND == ret)
          {
            iterate_status_ = ROW_NOT_FOUND;
            ret = OB_SUCCESS;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "load current block error:ret=%d", ret);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableGetter::initialize(ObSSTableBlockCache& block_cache,
          ObSSTableBlockIndexCache& block_index_cache,
          sstable::ObSSTableRowCache& row_cache,
          const ObCompactSSTableReader& sstable_reader)
    {
      int ret = OB_SUCCESS;

      block_cache_ = &block_cache;
      block_index_cache_ = &block_index_cache;
      row_cache_ = &row_cache;
      sstable_reader_ = &sstable_reader;

      if (OB_SUCCESS != (ret = alloc_buffer(uncomp_buf_, uncomp_buf_size_)))
      {
        TBSYS_LOG(WARN, "alloc uncomp buffer error:uncomp_buf_=%p, uncomp_buf_size_=%ld",
            uncomp_buf_, uncomp_buf_size_);
      }
      else if (OB_SUCCESS != (ret = alloc_buffer(internal_buf_, 
              internal_buf_size_)))
      {
        TBSYS_LOG(WARN, "alloc uncomp buffer error:internal_buf_=%p, internal_buf_size_=%ld",
            internal_buf_, internal_buf_size_);
      }

      return ret;
    }

    int ObCompactSSTableGetter::alloc_buffer(char*& buf, 
        const int64_t buf_size)
    {
      int ret = OB_SUCCESS;

      ModuleArena* arena = GET_TSI_MULT(ModuleArena, 
          TSI_COMPACTSSTABLEV2_MODULE_ARENA_2);

      if (NULL == arena)
      {
        TBSYS_LOG(WARN, "GET TSI MULT:ModuleArena, " 
            "TSI_COMPACTSSTABLEV2_MODULE_ARENA_2");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == (buf = arena->alloc(buf_size)))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory for buf:buf_size=%ld", buf_size);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        //do nothing
      }

      return ret;
    }

    int ObCompactSSTableGetter::load_cur_block()
    {
      int ret = OB_SUCCESS;
      const ObCellInfo* cell = NULL;
      uint64_t sstable_id = sstable_reader_->get_sstable_id();
      ObRowkey look_key;
      sstable::ObSSTableRowCacheValue row_cache_val;
      bool is_row_cache_hit = false;
      ObBlockIndexPositionInfo info;
      SearchMode mode = OB_SEARCH_MODE_GREATER_EQUAL;

      if (NULL == (cell = get_cur_param_cell()))
      {
        TBSYS_LOG(WARN, "get cur param cell error:cell==NULL");
        ret = OB_ERROR;
      }
      else
      {
        look_key = cell->row_key_;
        table_id_ = cell->table_id_;
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL != row_cache_)
        {
          sstable::ObSSTableRowCacheKey row_cache_key(sstable_id,
              table_id_, look_key);
          ret = row_cache_->get_row(row_cache_key, row_cache_val, row_buf_);
          if (OB_SUCCESS == ret)
          {
#ifndef _SSTABLE_NO_STAT_
            OB_STAT_TABLE_INC(SSTABLE, table_id_, INDEX_SSTABLE_ROW_CACHE_HIT, 1);
#endif
            is_row_cache_hit = true;
          }
          else
          {
#ifndef _SSTABLE_NO_STAT_
            OB_STAT_TABLE_INC(SSTABLE, table_id_, INDEX_SSTABLE_ROW_CACHE_MISS, 1);
#endif
          }
        }

        if (NULL == row_cache_ || !is_row_cache_hit)
        {
          const ObSSTableTableIndex* table_index 
            = sstable_reader_->get_table_index(table_id_);
          info.sstable_file_id_ = sstable_id;
          info.index_offset_ = table_index->block_index_offset_;
          info.index_size_ = table_index->block_index_size_;
          info.endkey_offset_ = table_index->block_endkey_offset_;
          info.endkey_size_ = table_index->block_endkey_size_;
          info.block_count_ = table_index->block_count_;
          ret = block_index_cache_->get_single_block_pos_info(
              info, table_id_, look_key, mode, block_pos_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = init_column_mask();
        if (OB_SUCCESS == ret)
        {
          if (is_row_cache_hit)
          {
          }
          else
          {
            ret = fetch_block();
          }

          if (OB_SUCCESS != ret && OB_SEARCH_NOT_FOUND != ret)
          {
            TBSYS_LOG(WARN, "fectch block error:ret=%d", ret);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "init column mask error:ret=%d", ret);
        }
      }
      else if (OB_BEYOND_THE_RANGE == ret)
      {
      }
      else
      {
        TBSYS_LOG(WARN, "can not reach here:ret=%d", ret);
      }

      return ret;
    }

    int ObCompactSSTableGetter::fetch_block()
    {
      int ret = OB_SUCCESS;
      const ObCellInfo* cell = NULL;
      uint64_t sstable_id = sstable_reader_->get_sstable_id();
      const char* buf = NULL;
      int64_t buf_size = 0;
      ObCompactStoreType store_type = sstable_reader_->get_row_store_type();
      int64_t rowkey_column_cnt = 0;

      cell = get_cur_param_cell();
      if (NULL == cell)
      {
        TBSYS_LOG(WARN, "cell==NULL");
        ret = OB_ERROR;
      }
      else
      {
        table_id_ = cell->table_id_;
      }

      if (OB_SUCCESS == ret)
      {
        ret = get_block_data(buf, buf_size);
        sstable_reader_->get_schema()->get_rowkey_column_count(table_id_,
            rowkey_column_cnt);
        ObSSTableBlockReader::BlockData block_data(
            internal_buf_, internal_buf_size_, buf, buf_size);
        ret = getter_.init(cell->row_key_, block_data, store_type);
        if (OB_SUCCESS != ret && OB_SEARCH_NOT_FOUND != ret)
        {
          TBSYS_LOG(WARN, "block getter init error:ret=%d", ret);
        }
      }

      if (NULL != row_cache_)
      {
        sstable::ObSSTableRowCacheKey row_cache_key(sstable_id, table_id_,
            const_cast<ObCellInfo*>(cell)->row_key_);
        sstable::ObSSTableRowCacheValue row_cache_val;
        ret = getter_.get_cache_row_value(row_cache_val);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get caceh row value error:ret=%d", ret);
        }
        else if (row_cache_val.size_ <= 0 || NULL == row_cache_val.buf_)
        {
          TBSYS_LOG(WARN, "row_cache_val error:row_cache_val.size_=%ld,"
              "row_cache_val.buf_=%p",
              row_cache_val.size_, row_cache_val.buf_);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          ret = row_cache_->put_row(row_cache_key, row_cache_val);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "row cache put row error:ret=%d", ret);
          }
          ret = OB_SUCCESS;
        }

        if (OB_SEARCH_NOT_FOUND == ret)
        {
          ret = row_cache_->put_row(row_cache_key, row_cache_val);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "row cache put row error:ret=%d", ret);
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableGetter::init_column_mask()
    {
      int ret = OB_SUCCESS;
      int64_t cell_size = 0;
      int64_t start = 0;
      int64_t end = 0;
      const ObGetParam::ObRowIndex* row_index = NULL;

      const ObSSTableSchema* schema = sstable_reader_->get_schema();
      uint64_t sstable_file_id = sstable_reader_->get_sstable_id();
      cell_size = get_param_->get_cell_size();
      row_index = get_param_->get_row_index();
      start = row_index[cur_row_index_].offset_;
      end = row_index[cur_row_index_].offset_ 
        + row_index[cur_row_index_].size_;

      if (NULL == schema)
      {
        TBSYS_LOG(WARN, "schema==NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_INVALID_ID == sstable_file_id)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "sstable_file_id==OB_INVALID_ID");
      }
      else if (start >= cell_size || end > cell_size)
      {
        TBSYS_LOG(WARN, "cell size error:cell_size=%ld, start=%ld, end=%ld", 
            cell_size, start, end);
        ret = OB_ERROR;
      }
      else
      {
        table_id_ = (*get_param_)[start]->table_id_;
        schema = sstable_reader_->get_schema();
      }

      if (OB_SUCCESS == ret)
      {
        //reset scan column indexes
        get_column_indexes_.reset();

        //schema array
        const ObSSTableSchemaColumnDef* def_array = NULL;
        int64_t column_cnt = 0;
        bool is_rowkey = true;

        //rowkey columns
        if (NULL == (def_array = schema->get_table_schema(
                table_id_, is_rowkey, column_cnt)))
        {
          TBSYS_LOG(WARN, "get table schema error:table_id=%lu, is_rowkey=%d, column_cnt=%ld",
              table_id_, is_rowkey, column_cnt);
        }
        else
        {
          for (int64_t i = 0; i < column_cnt; i ++)
          {//add rowkey column id
            if (OB_SUCCESS != (ret = get_column_indexes_.add_column_id(
                    ObSSTableScanColumnIndexes::Rowkey, 
                    def_array[i].offset_, 
                    def_array[i].column_id_)))
            {
              TBSYS_LOG(WARN, "get column indexes error:ret=%d", ret);
              break;
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t get_column_cnt = row_index[cur_row_index_].size_;
        const ObCellInfo* cell = NULL;
        const ObGetParam::ObRowIndex* row_index = NULL;
        row_index = get_param_->get_row_index();
        cell = (*get_param_)[row_index[cur_row_index_].offset_];
        uint64_t column_id = cell->column_id_;
        const ObSSTableSchemaColumnDef* def_array = NULL;

        if (1 == get_column_cnt 
            && OB_FULL_ROW_COLUMN_ID == column_id)
        {//query whole row
          bool is_rowkey = false;
          if (NULL != (def_array = schema->get_table_schema(
                  table_id_, is_rowkey, column_cnt_)))
          {
            TBSYS_LOG(WARN, "get table schema error:table_id_=%lu, is_rowkey=%d, column_cnt_=%ld",
                table_id_, is_rowkey, column_cnt_);
          }
          else
          {
            for (int64_t i = 0; i < column_cnt_; i ++)
            {
              if (OB_SUCCESS != (ret = get_column_indexes_.add_column_id(
                      ObSSTableScanColumnIndexes::Normal, 
                      def_array[i].offset_, 
                      def_array[i].column_id_)))
              {
                TBSYS_LOG(WARN, "add column id error:i=%ld, offset=%ld, clumn_id_=%lu",
                    i, def_array[i].offset_, def_array[i].column_id_);
                break;
              }
            }//end for
          }
        }
        else
        {//not query whole row
          uint64_t current_column_id = OB_INVALID_ID;
          const ObSSTableSchemaColumnDef* def = NULL;
          cell = (*get_param_)[row_index[cur_row_index_].offset_];

          for (int64_t i = 0; i < get_column_cnt; i ++)
          {
            current_column_id = cell->column_id_;
            if (0 == current_column_id || OB_INVALID_ID == current_column_id)
            {
              TBSYS_LOG(WARN, "invalid pram:current_column_id=%lu", current_column_id);
              ret = OB_INVALID_ARGUMENT;
            }
            else if (NULL != (def = schema->get_column_def(table_id_, 
                    current_column_id)))
            {//exist
              if (def->is_rowkey_column())
              {//rowkey column
                if (OB_SUCCESS != (ret = get_column_indexes_.add_column_id(
                        ObSSTableScanColumnIndexes::Rowkey,
                        def->offset_, current_column_id)))
                {
                  TBSYS_LOG(WARN, "add rowkey column id error:offset=%ld, current_column_id=%lu",
                      def->offset_, current_column_id);
                }
              }
              else
              {//rowvalue column
                if (OB_SUCCESS != (ret = get_column_indexes_.add_column_id(
                        ObSSTableScanColumnIndexes::Normal,
                        def->offset_, current_column_id)))
                {
                  TBSYS_LOG(WARN, "add rowvalue column id error:offset=%ld, current_column_id=%lu",
                      def->offset_, current_column_id);
                }
              }
            }
            else
            {//not exist
              if (OB_SUCCESS != (ret = get_column_indexes_.add_column_id(
                      ObSSTableScanColumnIndexes::NotExist,
                      0, current_column_id)))
              {
                TBSYS_LOG(ERROR, "add noexist column id error:offset=0, current_column_id=%lu",
                    current_column_id);
              }
            }

            if (OB_SUCCESS == ret)
            {
              cell ++;
            }
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableGetter::switch_row()
    {
      int ret = OB_SUCCESS;
      get_column_indexes_.reset();

      iterate_status_ = ITERATE_NORMAL;
      if (cur_row_index_ + 1 >= get_param_->get_row_size())
      {
        iterate_status_ = ITERATE_END;
        ret = OB_ITER_END;
      }
      else
      {
        cur_row_index_ ++;
        if (OB_SUCCESS == ret)
        {
          ObCellInfo* cell = NULL;
          const ObGetParam::ObRowIndex* row_index = NULL;
          row_index = get_param_->get_row_index();
          const ObBloomFilterV1* bloom_filter 
            = sstable_reader_->get_table_bloomfilter(table_id_);
          cell = (*get_param_)[row_index[cur_row_index_].offset_];
          if (!bloom_filter->may_contain(cell->row_key_))
          {
            bloomfilter_hit_ = false;
            if (OB_SUCCESS != (ret = init_column_mask()))
            {
              TBSYS_LOG(WARN, "init column mask error");
            }
          }
          else
          {
            ret = load_cur_block();
            if (OB_SEARCH_NOT_FOUND == ret)
            {
              iterate_status_ = ROW_NOT_FOUND;
              ret = OB_SUCCESS;
            }
            else if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "load cur block error:ret=%d", ret);
            }
            bloomfilter_hit_ = true;
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableGetter::get_row(ObRowkey*& rowkey, ObRow*& rowvalue)
    {
      int ret = OB_SUCCESS;
      ObCompactCellIterator* row = NULL;
      const ObObj* row_obj = NULL;
      int64_t rowkey_cnt = 0;
      int64_t rowvalue_cnt = 0;
      bool is_row_finished = false;
      
      if (bloomfilter_hit_)
      {
        if (OB_SUCCESS != (ret = getter_.get_row(row)))
        {
          TBSYS_LOG(ERROR, "block getter get row error:ret=%d", ret);
        }
        else
        {
          while(true)
          {//解析,得到obj数组
            if (OB_SUCCESS != (ret = row->next_cell()))
            {
              TBSYS_LOG(WARN, "row next cell error:ret=%d", ret);
              break;
            }
            else if (OB_SUCCESS != (ret = row->get_cell(row_obj, 
                    &is_row_finished)))
            {
              TBSYS_LOG(WARN, "row next cell error:ret=%d", ret);
              break;
            }
            else
            {
              if (is_row_finished)
              {
                row_key_.assign(rowkey_buf_array_, rowkey_cnt);
                break;
              }
              else
              {
                rowkey_buf_array_[rowkey_cnt] = *row_obj;
                rowkey_cnt ++;
              }
            }
          }//end while

          if (OB_SUCCESS == ret)
          {
            ObCompactStoreType row_store_type 
              = sstable_reader_->get_row_store_type();
            if (DENSE_SPARSE == row_store_type)
            {
              while(true)
              {
                if (OB_SUCCESS != (ret = row->next_cell()))
                {
                  TBSYS_LOG(WARN, "row next cell error:ret=%d", ret);
                  break;
                }
                else
                {
                  uint64_t column_id;
                  if (OB_SUCCESS != (ret = row->get_cell(column_id, 
                          row_obj, &is_row_finished)))
                  {
                    TBSYS_LOG(WARN, "row next cell error:ret=%d", ret);
                    break;
                  }
                  else
                  {
                    if (is_row_finished)
                    {
                      column_cnt_ = rowvalue_cnt;
                      break;
                    }
                    else
                    {
                      column_ids_[rowvalue_cnt] = column_id;
                      column_objs_[rowvalue_cnt] = *row_obj;
                      rowvalue_cnt ++;
                    }
                  }
                }
              }
            }
            else if (DENSE_DENSE == row_store_type)
            {
              while(true)
              {
                if (OB_SUCCESS != (ret = row->next_cell()))
                {
                  TBSYS_LOG(WARN, "row next cell error:ret=%d", ret);
                  break;
                }
                else if (OB_SUCCESS != (ret = row->get_cell(
                        row_obj, &is_row_finished)))
                {
                  TBSYS_LOG(WARN, "row get cell error:ret=%d", ret);
                }
                else
                {
                  column_objs_[rowvalue_cnt] = *row_obj;
                  rowvalue_cnt ++;

                  if (is_row_finished)
                  {
                    column_cnt_ = rowvalue_cnt;
                  }
                }
              }
            }
          }//end while
        }

        if (OB_SUCCESS == ret)
        {
          column_cursor_ = rowkey_cnt;
          row_desc_.reset();
          row_value_.set_row_desc(row_desc_);
          ObSSTableScanColumnIndexes::Column column;
          while (column_cursor_ < get_column_indexes_.get_column_count())
          {
            if (OB_SUCCESS != (ret = get_column_index(column_cursor_,
                    column)))
            {
              TBSYS_LOG(WARN, "get column index error:ret=%d, column_cursor_=%lu", 
                  ret, column_cursor_);
              break;
            }
            else if (OB_SUCCESS != (ret = store_current_cell(column)))
            {
              TBSYS_LOG(WARN, "store current cell error:ret=%d", ret);
              break;
            }
            else
            {
              column_cursor_ ++;
            }
          }
        }

        if (OB_SUCCESS == ret)
        {
          rowkey = &row_key_;
          rowvalue = &row_value_;
        }
      }
      else
      {
        ObCellInfo* cell = NULL;
        const ObGetParam::ObRowIndex* row_index = NULL;
        row_index = get_param_->get_row_index();
        cell = (*get_param_)[row_index[cur_row_index_].offset_];
        rowkey = &cell->row_key_;
        rowkey_cnt = rowkey->get_obj_cnt();

        column_cursor_ = rowkey_cnt;
        row_desc_.reset();
        row_value_.set_row_desc(row_desc_);
        ObSSTableScanColumnIndexes::Column column;
        while (column_cursor_ < get_column_indexes_.get_column_count())
        {
          if (OB_SUCCESS != (ret = get_column_index(column_cursor_,
                  column)))
          {
            TBSYS_LOG(WARN, "get column index error:ret=%d, column_cursor_=%lu", 
                ret, column_cursor_);
            break;
          }
          else
          {
            column.type_ = ObSSTableScanColumnIndexes::NotExist;
          }

          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = store_current_cell(column)))
            {
              TBSYS_LOG(WARN, "store current cell error:ret=%d", ret);
              break;
            }
            else
            {
              column_cursor_ ++;
            }
          }
        }

        if (OB_SUCCESS == ret)
        {
          rowvalue = &row_value_;
        }
      }

      return ret;
    }

    int ObCompactSSTableGetter::get_column_index(const int64_t cursor, 
        ObSSTableScanColumnIndexes::Column& column) const
    {
      int ret = OB_SUCCESS;

      const ObCompactStoreType row_store_type 
        = sstable_reader_->get_row_store_type();
      if (OB_SUCCESS != (ret = get_column_indexes_.get_column(cursor, column)))
      {
        TBSYS_LOG(ERROR, "get column indexes get column error:ret=%d", ret);
      }
      else if (DENSE_SPARSE == row_store_type 
          && ObSSTableScanColumnIndexes::Normal == column.type_)
      {
        column.type_ = ObSSTableScanColumnIndexes::NotExist;
        for (int64_t i = 0; i < column_cnt_; i ++)
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

    int ObCompactSSTableGetter::store_current_cell(
        const ObSSTableScanColumnIndexes::Column& column)
    {
      int ret = OB_SUCCESS;
      const ObCompactStoreType row_store_type 
        = sstable_reader_->get_row_store_type();

      if (DENSE_DENSE == row_store_type)
      {
        if (OB_SUCCESS != (ret = store_dense_column(column)))
        {
          TBSYS_LOG(WARN, "store dense column error:ret=%d", ret);
        }
      }
      else if (DENSE_SPARSE == row_store_type)
      {
        if (OB_SUCCESS != (ret = store_sparse_column(column)))
        {
          TBSYS_LOG(WARN, "store sparse column error:ret=%d", ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "can not reach here");
      }

      return ret;
    }

    int ObCompactSSTableGetter::store_dense_column(
        const ObSSTableScanColumnIndexes::Column& column)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id_,
              column.id_)))
      {
        TBSYS_LOG(WARN, "row desc add column desc error:ret=%d", ret);
      }
      else if (ObSSTableScanColumnIndexes::Rowkey == column.type_)
      {
        if (OB_SUCCESS != (ret = row_value_.set_cell(
                table_id_, column.id_, rowkey_buf_array_[column.index_])))
        {
          TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
        }
      }
      else if (ObSSTableScanColumnIndexes::NotExist == column.type_)
      {
        ObObj obj;
        if (not_exit_col_ret_nop_)
        {
          obj.set_ext(ObActionFlag::OP_NOP);
          if (OB_SUCCESS != (row_value_.set_cell(
                  table_id_, column.id_, obj)))
          {
            TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
          }
        }
        else
        {
          obj.set_null();
          if (OB_SUCCESS != (row_value_.set_cell(
                  table_id_, column.id_, obj)))
          {
            TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
          }
        }
      }
      else if (column.index_ >= column_cnt_)
      {
        TBSYS_LOG(WARN, "column.index >= column_cnt_:column.index_=%ld, column_cnt_=%ld",
            column.index_, column_cnt_);
        ret = OB_ERROR;
      }
      else
      {
        if (OB_SUCCESS != (row_value_.set_cell(
                table_id_, column.id_, column_objs_[column.index_])))
        {
          TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObCompactSSTableGetter::store_sparse_column(
        const ObSSTableScanColumnIndexes::Column& column)
    {
      int ret = OB_SUCCESS;

      bool is_del_row = false;

      if (0 == column_cursor_ && !handled_del_row_)
      {
        if (OB_DELETE_ROW_COLUMN_ID == column_ids_[column_cursor_])
        {
          is_del_row = true;
        }
      }
      else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id_,
              column.id_)))
      {
        TBSYS_LOG(WARN, "row desc add column desc error:ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        if (is_del_row)
        {
          if (OB_SUCCESS != (ret = row_value_.set_cell(table_id_,
                  column_ids_[column.index_], column_objs_[column.index_])))
          {
            TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
          }
          handled_del_row_ = true;

          if (1 == column_cnt_)
          {
            column_cursor_ = get_column_indexes_.get_column_count();
          }
        }
        else
        {
          handled_del_row_ = true;
          if (ObSSTableScanColumnIndexes::Rowkey == column.type_)
          {
            if (OB_SUCCESS != (ret = row_value_.set_cell(table_id_,
                    column.id_, rowkey_buf_array_[column.index_])))
            {
              TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
            }
          }
          else if (ObSSTableScanColumnIndexes::NotExist == column.type_)
          {
            ObObj obj;
            obj.set_ext(ObActionFlag::OP_NOP);
            if (OB_SUCCESS != (ret = row_value_.set_cell(table_id_,
                    column.id_, obj)))
            {
              TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
            }
          }
          else
          {
            if (OB_SUCCESS != (ret = row_value_.set_cell(table_id_,
                    column.id_, column_objs_[column.index_])))
            {
              TBSYS_LOG(WARN, "row value set cell error:ret=%d", ret);
            }
          }
        }
      }

      return ret;
    }

    int ObCompactSSTableGetter::get_block_data(const char*& buf, 
        int64_t& buf_size)
    {
      int ret = OB_SUCCESS;
      ObSSTableBlockBufferHandle handler;
      const char* comp_buf = NULL;
      int64_t comp_buf_size = 0;
      uint64_t sstable_id = sstable_reader_->get_sstable_id();
      const char* block_data_ptr = NULL;
      int64_t block_data_size = 0;
      ObRecordHeaderV2 header;

      ret = block_cache_->get_block(
              sstable_id, block_pos_.offset_,
              block_pos_.size_, handler, table_id_);
      if (-1 == ret || block_pos_.size_ != ret)
      {
        TBSYS_LOG(WARN, "block cache get block error:ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        block_data_ptr = handler.get_buffer();
        block_data_size = block_pos_.size_;
        ret = OB_SUCCESS;
      }

      if (OB_SUCCESS == ret)
      {
        ret = ObRecordHeaderV2::get_record_header(
                block_data_ptr, block_data_size, header,
                comp_buf, comp_buf_size);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get record header error:ret=%d", ret);
        }
        else
        {
          if (header.data_length_ > uncomp_buf_size_)
          {
            TBSYS_LOG(WARN, "header.data_length_ > uncomp_buf_size_");
            uncomp_buf_size_ = header.data_length_;
            ret = alloc_buffer(uncomp_buf_, 
                uncomp_buf_size_);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t real_size = 0;
        if (header.is_compress())
        {
          ObCompressor* dec = const_cast<ObCompactSSTableReader*>(
              sstable_reader_)->get_decompressor();
          if (NULL != dec)
          {
            if (OB_SUCCESS != (ret = dec->decompress(
                    comp_buf, comp_buf_size,
                    uncomp_buf_, header.data_length_, real_size)))
            {
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "decompress error:ret=%d", ret);
              }
              else
              {
                buf = uncomp_buf_;
                buf_size = real_size;
              }
            }
          }
        }
        else
        {
          memcpy(uncomp_buf_, comp_buf, comp_buf_size);
          buf = uncomp_buf_;
          buf_size = comp_buf_size;
        }
      }

      if (OB_SUCCESS != ret && NULL != sstable_reader_)
      {
        TBSYS_LOG(WARN, "ret!=OB_SUCCESS, NULL!=sstable_reader_,ret=%d,sstable_reader_=%p",
            ret, sstable_reader_);
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
