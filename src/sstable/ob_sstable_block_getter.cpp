/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_block_getter.cpp for get one or more columns from 
 * ssatable block. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include "ob_sstable_getter.h"
#include "ob_sstable_block_getter.h"
#include "ob_sstable_block_index_v2.h"
#include "ob_sstable_trailer.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace common;

    ObSSTableBlockGetter::ObSSTableBlockGetter(const ObScanColumnIndexes& column_index)
    : inited_(false), handled_del_row_(false), not_exit_col_ret_nop_(false), 
      is_row_cache_data_(false), is_row_finished_(false), 
      sstable_data_store_style_(OB_SSTABLE_STORE_DENSE), 
      column_cursor_(0), current_column_count_(0), query_column_indexes_(column_index),
      row_cursor_(NULL), sstable_row_cache_(NULL), index_buf_(DEFAULT_INDEX_BUF_SIZE)
    {
      memset(current_columns_, 0 , sizeof(current_columns_));
    }

    ObSSTableBlockGetter::~ObSSTableBlockGetter()
    {

    }

    inline int ObSSTableBlockGetter::get_current_column_index(
        const int64_t cursor, ObScanColumnIndexes::Column& column) const
    {
      int ret = query_column_indexes_.get_column(cursor, column);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "query_column_indexes_ cursor=%ld not exist.", cursor);
      }
      else if (sstable_data_store_style_ == OB_SSTABLE_STORE_SPARSE && column.type_ == ObScanColumnIndexes::Normal)
      {
        // for sparse sstable, column offset search at current_ids_
        uint64_t data_column_id = OB_INVALID_ID;
        column.type_ =  ObScanColumnIndexes::NotExist;
        for (int64_t i = 0; i < current_column_count_ && OB_SUCCESS == ret; ++i)
        {
          ret = current_ids_[i].get_int(reinterpret_cast<int64_t&>(data_column_id));
          if (OB_SUCCESS == ret && data_column_id == column.id_)
          {
            column.type_ = ObScanColumnIndexes::Normal;
            column.index_ = static_cast<int32_t>(i);
            break;
          }
        }
      }
      return ret;
    }

    int ObSSTableBlockGetter::load_current_row(const_iterator row_index)
    {
      int ret               = OB_SUCCESS;
      current_column_count_ = OB_MAX_COLUMN_NUMBER;

      ret = reader_.get_row(static_cast<int>(sstable_data_store_style_), row_index, 
            current_rowkey_, current_ids_, current_columns_, current_column_count_, false);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "read current row error, store style=%ld, current row cursor=%d,%d",
            sstable_data_store_style_, row_index->offset_, row_index->size_);
      }

      return ret;
    }

    int ObSSTableBlockGetter::store_sparse_column(const ObScanColumnIndexes::Column &column)
    {
      int ret                 = OB_SUCCESS;
      bool is_del_row         = false;
      uint64_t data_column_id = OB_INVALID_ID;

      if (0 == column_cursor_ && !handled_del_row_)
      {
        ret = current_ids_[column_cursor_].get_int(reinterpret_cast<int64_t&>(data_column_id));
        if (OB_SUCCESS == ret)
        {
          if (OB_DELETE_ROW_COLUMN_ID == data_column_id)
          {
            is_del_row = true;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to get column id from obj");
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (is_del_row)
        {
          /**
           * delete row is a special column with column_id == 0 and only 
           * exists sparse format sstable, we could check whether it's 
           * delete row op first, if it's delete row op, we only return 
           * the cell with delete row op, ignore the other column.
           */
          current_cell_info_.column_id_ = data_column_id;
          current_cell_info_.value_ = current_columns_[column_cursor_];
          handled_del_row_ = true;

          if (1 == current_column_count_)
          {
            /**
             * if there is only one delete row op in sstable for current 
             * row, skip the next columns, in this case we don't fill the 
             * extra NOP for non-existent columns. 
             */
            column_cursor_ = query_column_indexes_.get_column_count();
          }
        }
        else
        {
          ++column_cursor_;
          handled_del_row_ = true;
          if (column.type_ == ObScanColumnIndexes::Rowkey)
          {
            current_cell_info_.value_ = *(current_rowkey_.ptr() + column.index_);
          }
          else if (column.type_ == ObScanColumnIndexes::NotExist)
          {
            current_cell_info_.value_.set_ext(ObActionFlag::OP_NOP);
          }
          else
          {
            current_cell_info_.value_ = current_columns_[column.index_];
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockGetter::store_current_cell(const ObScanColumnIndexes::Column& column)
    {
      int ret = OB_SUCCESS;


      if (OB_SUCCESS == ret)
      {
        current_cell_info_.table_id_ = 0;
        current_cell_info_.row_key_ = current_rowkey_;
        current_cell_info_.column_id_ = column.id_;
      }

      if (sstable_data_store_style_ == OB_SSTABLE_STORE_DENSE)
      {
        ret = store_dense_column(column);
      }
      else
      {
        ret = store_sparse_column(column);
      }

      return ret;
    }

    int ObSSTableBlockGetter::store_dense_column(const ObScanColumnIndexes::Column & column)
    {
      int ret = OB_SUCCESS;
      if (column.type_ == ObScanColumnIndexes::Rowkey)
      {
        current_cell_info_.value_ = *(current_rowkey_.ptr() + column.index_);
      }
      else if (column.type_ == ObScanColumnIndexes::NotExist)
      {
        //only dense format return null cell if column non-existent
        if (not_exit_col_ret_nop_)
        {
          current_cell_info_.value_.set_ext(ObActionFlag::OP_NOP);
        }
        else
        {
          current_cell_info_.value_.set_null();
        }
      }
      else if (column.index_ >= current_column_count_)
      {
          TBSYS_LOG(ERROR, "column_index=%d > current_column_count_=%ld", 
              column.index_, current_column_count_);
          ret = OB_ERROR;
      }
      else
      {
        current_cell_info_.value_ = current_columns_[column.index_];
      }

      if (OB_SUCCESS == ret) 
      {
        ++column_cursor_;
      }

      return ret;
    }

    inline int ObSSTableBlockGetter::store_and_advance_column()
    {
      int ret               = OB_SUCCESS;
      ObScanColumnIndexes::Column column;

      if (OB_SUCCESS != (ret = get_current_column_index(column_cursor_, column)))
      {
        TBSYS_LOG(ERROR, "get column index error, ret = %d, cursor=%ld, id= %lu, index=%d", 
            ret, column_cursor_, column.id_, column.index_);
      }
      else if (OB_SUCCESS != (ret = store_current_cell(column)))
      {
        TBSYS_LOG(ERROR, "store current cell error, ret = %d, cursor=%ld, id=%lu, index=%d", 
            ret, column_cursor_, column.id_, column.index_);
      }

      return ret;
    }

    /**
     * NOTE: column data pair is like this: 
     *    --------------------------
     *    |column_id_obj|column_obj|
     *    --------------------------
     * sstable sparse format is like this:
     *     -------------------------------------------------------
     *     |row_key|column data pair|column data pair|   ...     |
     *     -------------------------------------------------------
     * sparse format is used for ups, currently we assumpt that
     * there is only one column group in sparse format sstable, and 
     * we only get full row once. 
     */
    int ObSSTableBlockGetter::next_cell()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "ObSSTableBlockGetter doesn't initialize");
        ret = OB_NOT_INIT;      
      }
      else if ((!is_row_cache_data_ && NULL == row_cursor_) 
               || query_column_indexes_.get_column_count() == 0)
      {
        TBSYS_LOG(WARN, "not initialized, is_row_cache_data_=%d, "
                        "cursor=%p, column_count=%ld",
            is_row_cache_data_, row_cursor_, query_column_indexes_.get_column_count());
        ret = OB_NOT_INIT;
      }
      else
      {
        is_row_finished_ = false;
        if (column_cursor_ < query_column_indexes_.get_column_count())
        {
          ret = store_and_advance_column();
          if (OB_SUCCESS == ret 
              && column_cursor_ >= query_column_indexes_.get_column_count())
          {
            is_row_finished_ = true;
          }
        }
        else
        {
          // we iterator over current row
          ret = OB_ITER_END;
        }
      }

      return ret;
    }

    int ObSSTableBlockGetter::get_cell(ObCellInfo** cell)
    {
      int ret = OB_SUCCESS;

      if (NULL == cell)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!inited_)
      {
        TBSYS_LOG(WARN, "ObSSTableBlockGetter doesn't initialize");
        ret = OB_ERROR;      
      }
      else if (OB_SUCCESS == ret)
      {
        *cell = &current_cell_info_;
      }

      return ret; 
    }

    void ObSSTableBlockGetter::clear()
    {
      inited_ = false;
      handled_del_row_ = false;
      not_exit_col_ret_nop_ = false;
      is_row_cache_data_ = false;
      is_row_finished_ = false;
      sstable_data_store_style_ = OB_SSTABLE_STORE_DENSE;
      row_cursor_ = NULL;
      sstable_row_cache_ = NULL;
      column_cursor_ = 0;
      current_column_count_ = 0;
      current_cell_info_.reset();
      reader_.reset();
    }

    int ObSSTableBlockGetter::init(const ObRowkey& row_key, 
                                   const char* buf, 
                                   const int64_t data_len, 
                                   const ObSSTableBlockReader::BlockDataDesc& data_desc,
                                   ObSSTableRowCache* row_cache,
                                   const bool is_row_cache_data, 
                                   bool not_exit_col_ret_nop)
    {
      int32_t ret = OB_SUCCESS;

      if (NULL == buf || data_len <= 0 
          || NULL == row_key.ptr() || 0 == row_key.length()
          || (is_row_cache_data && NULL == row_cache))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, "
                        "row_key_ptr=%p, row_key_len=%ld, "
                        "is_row_cache_data=%d, sstable_row_cache=%p", 
                  buf, data_len, row_key.ptr(), row_key.length(),
                  is_row_cache_data, row_cache);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        clear();
        current_rowkey_ = row_key;
        sstable_data_store_style_ = data_desc.store_style_;
        sstable_row_cache_ = row_cache;
        not_exit_col_ret_nop_ = not_exit_col_ret_nop;
        is_row_cache_data_ = is_row_cache_data;
      }

      if (is_row_cache_data && NULL != row_cache)
      {
        ret = read_row_columns(sstable_data_store_style_, buf, data_len);
        if (OB_SUCCESS == ret)
        {
          inited_ = true;
        }
      }
      else 
      {
        ret = index_buf_.ensure_space(DEFAULT_INDEX_BUF_SIZE, ObModIds::OB_SSTABLE_GET_SCAN);
        if (OB_SUCCESS == ret)
        {
          ObSSTableBlockReader::BlockData block_data(index_buf_.get_buffer(), index_buf_.get_buffer_size(), buf, data_len);

          ret = reader_.deserialize(data_desc, block_data);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize error, ret=%d", ret);
            ret = OB_DESERIALIZE_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          inited_ = true;
          row_cursor_ = reader_.find(row_key);
          if (row_cursor_ != reader_.end())
          {
            //just the rowkey, load it
            ret = load_current_row(row_cursor_);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "load row error, ret=%d", ret);
              inited_ = false;
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "not find the row key:, ret=%d, rowkey:%s", ret, to_cstring(row_key));
            ret = OB_SEARCH_NOT_FOUND;
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockGetter::read_row_columns(
      const int64_t format, const char* row_buf, 
      const int64_t data_len)
    {
      int ret = OB_SUCCESS;
      int64_t column_index = 0;
      int64_t pos = 0;

      if (NULL == row_buf || data_len <= 0)
      {
        TBSYS_LOG(WARN, "invalide row data, row_buf=%p, data_len=%ld",
          row_buf, data_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        while (pos < data_len)
        {
          if (column_index >= OB_MAX_COLUMN_NUMBER) 
          {
            ret = OB_SIZE_OVERFLOW;
            break;
          }

          if (format == OB_SSTABLE_STORE_SPARSE)
          {
            ret = current_ids_[column_index].deserialize(row_buf, data_len, pos);
            if (OB_SUCCESS != ret || current_ids_[column_index].get_type() != ObIntType)
            {
              TBSYS_LOG(ERROR, "deserialize id object error, ret=%d, "
                  "column_index=%ld, row_buf=%p, pos=%ld, size=%ld",
                  ret, column_index, row_buf, pos, data_len);
              ret = OB_DESERIALIZE_ERROR;
              break;
            }
          }

          if (OB_SUCCESS != (ret = 
                current_columns_[column_index].deserialize(row_buf, data_len, pos)) )
          {
            TBSYS_LOG(ERROR, "deserialize column value object error, ret=%d, "
                "column_index=%ld, row_buf=%p, pos=%ld, size=%ld",
                ret, column_index, row_buf, pos, data_len);
            break;
          }
          else
          { 
            ++column_index; 
          }
        }

        if (OB_SUCCESS == ret) 
        {
          current_column_count_ = column_index;
        }
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
