/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_scanner.cpp is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#include "ob_sstable_block_scanner.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_action_flag.h"
#include "common/ob_rowkey_helper.h"
#include "ob_sstable_scanner.h"
#include "ob_sstable_reader.h"
#include "ob_sstable_scan_param.h"
#include "ob_sstable_block_index_v2.h"
#include "ob_sstable_trailer.h" 

using namespace oceanbase::common;

namespace oceanbase
{
  namespace sstable
  {
    ObSSTableBlockScanner::ObSSTableBlockScanner(const ObScanColumnIndexes& column_indexes)
      : initialize_status_(OB_NOT_INIT), sstable_data_store_style_(OB_SSTABLE_STORE_DENSE), 
      is_reverse_scan_(false), is_row_changed_(false), is_row_finished_(false), 
      handled_del_row_(false), not_exit_col_ret_nop_(false),
      column_cursor_(0),current_column_count_(0), query_column_indexes_(column_indexes), 
      row_cursor_(NULL), row_start_index_(NULL), row_last_index_(NULL)
    {
      memset(current_columns_, 0 , sizeof(current_columns_));
    }

    ObSSTableBlockScanner::~ObSSTableBlockScanner()
    {
    }

    inline void ObSSTableBlockScanner::next_row()
    {
      handled_del_row_ = false;
      if (!is_reverse_scan_)
      {
        ++row_cursor_;
      }
      else
      {
        --row_cursor_;
      }
    }

    inline bool ObSSTableBlockScanner::start_of_block()
    {
      bool ret = false;
      if (0 == column_cursor_ && !handled_del_row_)
      {
        if ((!is_reverse_scan_) && row_cursor_ == row_start_index_)
        {
          ret = true;
        }
        else if (is_reverse_scan_ && row_cursor_ == row_last_index_)
        {
          ret = true;
        }
      }
      return ret;
    }

    inline bool ObSSTableBlockScanner::end_of_block()
    {
      bool ret = false;
      if ((!is_reverse_scan_) && row_cursor_ > row_last_index_)
      {
        ret = true;
      }
      else if (is_reverse_scan_ && row_cursor_ < row_start_index_)
      {
        ret = true;
      }
      return ret;
    }

    int ObSSTableBlockScanner::store_and_advance_column()
    {
      int ret = OB_SUCCESS;
      ObScanColumnIndexes::Column column;

      if (OB_SUCCESS != (ret = get_current_column_index(column_cursor_, column)))
      {
        TBSYS_LOG(ERROR, "get column index error, ret = %d, cursor=%ld, id= %lu, index=%d", 
            ret, column_cursor_, column.id_, column.index_);
      }
      else if (OB_SUCCESS != (ret = store_current_cell(column)))
      {
        TBSYS_LOG(ERROR, "store current cell error, ret = %d, cursor=%ld, type=%d, index=%d, id=%lu", 
            ret, column_cursor_, column.type_, column.index_, column.id_);
      }
      return ret;
    }

    int ObSSTableBlockScanner::next_cell()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != initialize_status_)
      {
        ret = static_cast<int>(initialize_status_);
      }
      else if (NULL == row_cursor_ 
            || NULL == row_start_index_ 
            || NULL == row_last_index_
            || row_cursor_ < row_start_index_
            || row_cursor_ > row_last_index_)
      {
        TBSYS_LOG(ERROR, "not initialized, cursor=%p, start=%p, last=%p",
            row_cursor_, row_start_index_, row_last_index_);
        ret = OB_NOT_INIT;
      }
      else if (query_column_indexes_.get_column_count() == 0 || end_of_block())
      {
        ret = OB_BEYOND_THE_RANGE;
      }
      else
      {
        is_row_changed_ = start_of_block();
        is_row_finished_ = false;
        if (column_cursor_ < query_column_indexes_.get_column_count())
        {
          ret = store_and_advance_column();
        }
        else
        {
          // we iterator over current column, step to next row
          next_row();
          if (end_of_block())
          {
            ret = OB_BEYOND_THE_RANGE;
          }
          else if (OB_SUCCESS != (ret = load_current_row(row_cursor_)))
          {
            TBSYS_LOG(ERROR, "load current row error, ret=%d , cursor=%d,%d",
                ret, row_cursor_->offset_, row_cursor_->size_);
          }
          // if we load next row successfully, reset %column_cursor_
          else
          { 
            is_row_changed_ = true;
            column_cursor_ = 0; 
            // retry to get cell info of column 0
            ret = store_and_advance_column();
          }
        }

        if (OB_SUCCESS == ret 
            && column_cursor_ >= query_column_indexes_.get_column_count())
        {
          is_row_finished_ = true;
        }
      }

      return ret;
    }

    inline int ObSSTableBlockScanner::get_current_column_index(
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

    int ObSSTableBlockScanner::load_current_row(const_iterator row_index)
    {
      int ret = OB_SUCCESS;
      current_column_count_ = OB_MAX_COLUMN_NUMBER;

      if (row_index < row_start_index_ || row_index > row_last_index_)
      {
        TBSYS_LOG(ERROR, "internal error, current row cursor=%p < start=%p, >last=%p.",
            row_index, row_start_index_, row_last_index_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = 
         reader_.get_row(static_cast<int>(sstable_data_store_style_), row_index, 
            current_rowkey_, current_ids_, current_columns_, current_column_count_) ))
      {
        TBSYS_LOG(ERROR, "read current row error, store style=%ld, current row cursor=%d,%d",
            sstable_data_store_style_, row_index->offset_, row_index->size_);
      }
      return ret;
    }

    int ObSSTableBlockScanner::store_sparse_column(const ObScanColumnIndexes::Column &column)
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

    int ObSSTableBlockScanner::store_dense_column(const ObScanColumnIndexes::Column & column)
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

      TBSYS_LOG(DEBUG, "store dense column:(type:%d, index:%d, value:%s)", 
          column.type_, column.index_, to_cstring(current_cell_info_.value_));

      if (OB_SUCCESS == ret) 
      {
        ++column_cursor_;
      }

      return ret;
    }

    int ObSSTableBlockScanner::store_current_cell(const ObScanColumnIndexes::Column& column)
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

    int ObSSTableBlockScanner::initialize(const bool is_reverse_scan, 
      const int64_t store_style, const bool not_exit_col_ret_nop)
    {
      sstable_data_store_style_ = store_style;
      is_reverse_scan_ = is_reverse_scan;
      is_row_changed_ = false;
      is_row_finished_ = false;
      handled_del_row_ = false;
      not_exit_col_ret_nop_ = not_exit_col_ret_nop;

      row_cursor_ = NULL;
      row_start_index_ = NULL;
      row_last_index_ = NULL;
      column_cursor_ = 0;
      initialize_status_ = OB_NOT_INIT;
      current_column_count_ = 0;
      reader_.reset();

      return OB_SUCCESS;
    }

    /**
     * initialize block object with block data
     * @param [in] range input scan param
     * @param [in] block_data_buf  block data buffer from sstable files or blockcache
     * @param [in] block_data_len size of block data
     * @return 
     *    OB_SUCCESS on success 
     *    OB_BEYOND_THE_RANGE , scan out of range, caller should look forward
     *    on next blocks
     *    OB_ITER_END, scan out of range, caller should not look forward on
     *    next blocks.
     *    others, internal error code.
     */
    int ObSSTableBlockScanner::set_scan_param( 
        const common::ObNewRange& range,
        const bool is_reverse_scan,
        const ObSSTableBlockReader::BlockDataDesc& data_desc,  
        const ObSSTableBlockReader::BlockData& block_data, 
        bool &need_looking_forward, 
        bool not_exit_col_ret_nop) 
    {
      int32_t ret = OB_SUCCESS; 

      need_looking_forward = true;
      reader_.reset();  //reset reader before assign start_iterator and last_iterator
      const_iterator start_iterator = reader_.end();
      const_iterator last_iterator = reader_.end();


      if (!block_data.available() || !data_desc.available())
      {
        TBSYS_LOG(ERROR, "block_data,desc invalid, bd=(%p,%ld, %p, %ld), desc=(%p,%ld,%ld) ",
            block_data.data_buffer_, block_data.data_bufsiz_, block_data.internal_buffer_, block_data.internal_bufsiz_,
            data_desc.rowkey_info_, data_desc.rowkey_column_count_, data_desc.store_style_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = initialize(is_reverse_scan, 
        data_desc.store_style_, not_exit_col_ret_nop)))
      {
        // cannot happen
      }
      else if (OB_SUCCESS != (ret =  reader_.deserialize(data_desc, block_data)) )
      {
        TBSYS_LOG(ERROR, "deserialize error, ret=%d, data bufsiz=%ld", 
            ret, block_data.data_bufsiz_);
      }
      else if (OB_SUCCESS != (ret = 
            locate_start_pos(range, start_iterator, need_looking_forward)))
      {
        TBSYS_LOG(DEBUG, "locate start key out of range or error=%d", ret);
      }
      else if (OB_SUCCESS != (ret =
            locate_end_pos(range, last_iterator, need_looking_forward)))
      {
        TBSYS_LOG(DEBUG, "locate end key out of range or error=%d", ret);
      }
      else  if (start_iterator > last_iterator)
      {
        TBSYS_LOG(DEBUG, "query key not exist in this block, start_iterator > last_iterator."
            " pls check input parameters, start_key(%s) and end_key(%s).", 
            to_cstring(range.start_key_), to_cstring(range.end_key_));
        ret = OB_BEYOND_THE_RANGE;
        need_looking_forward = false;
      }
      else
      {
        row_start_index_ = start_iterator;
        row_last_index_ = last_iterator;

        if (!is_reverse_scan_)
        {
          row_cursor_ = row_start_index_;
        }
        else
        {
          row_cursor_ = row_last_index_;
        }

        column_cursor_ = 0;
        ret = load_current_row(row_cursor_);
      }

      // error log
      if (OB_SUCCESS != ret && OB_BEYOND_THE_RANGE != ret) 
      {
        char range_buf[OB_RANGE_STR_BUFSIZ];
        range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(ERROR, "set_scan_param error, ret = %d, cursor=%p,%p,%p, range=%s", 
            ret , row_cursor_, row_start_index_, row_last_index_, range_buf);
      }

      initialize_status_ = ret;

      return ret;
    }


    int ObSSTableBlockScanner::locate_start_pos(const common::ObNewRange& range,
        const_iterator& start_iterator, bool& need_looking_forward)
    {
      int ret = OB_SUCCESS;
      start_iterator = reader_.end();
      ObRowkey query_start_key = range.start_key_;
      ObRowkey find_start_key;

      if (range.start_key_.is_min_row())
      {
        start_iterator = reader_.begin();
      }
      else
      {
        // lookup scan %start_key_ in block
        start_iterator = reader_.lower_bound(query_start_key);
        if (start_iterator == reader_.end())
        {
          // scan %start_key_ not in this block. query failed
          TBSYS_LOG(DEBUG, "query start_key not in block's range.");
          ret = OB_BEYOND_THE_RANGE;
          if (is_reverse_scan_) need_looking_forward = false;
        }
        else if (OB_SUCCESS != (ret = 
              reader_.get_row_key(start_iterator, find_start_key)) )
        {
          TBSYS_LOG(ERROR, "get start key of block error, start_iterator=%d,%d",
              start_iterator->offset_, start_iterator->size_);
        }
        else
        {
          if (query_start_key.compare(find_start_key) == 0
              && (!range.border_flag_.inclusive_start()))
          {
            ++start_iterator;
          }
          if (start_iterator == reader_.end())
          {
            TBSYS_LOG(DEBUG, "query start_key equal last key in block, but query"
                " request donot want it.");
            ret = OB_BEYOND_THE_RANGE;
          }

          // only query_start_key less than all of rowkey in this block,
          // that need be continue lookup(in previous blocks).
          if (is_reverse_scan_)
          {
             if (start_iterator == reader_.begin() 
                 && query_start_key.compare(find_start_key) < 0)
             {
               need_looking_forward = true;
             }
             else
             {
               need_looking_forward = false;
             }
          }
          else
          {
            need_looking_forward  = true;
          }

        }
      }
      return ret;
    }

    int ObSSTableBlockScanner::locate_end_pos(const common::ObNewRange& range,
        const_iterator& last_iterator, bool& need_looking_forward)
    {
      int ret = OB_SUCCESS;
      last_iterator = reader_.end();
      ObRowkey query_end_key = range.end_key_;

      if (range.end_key_.is_max_row())
      {
        last_iterator = reader_.end();
        --last_iterator;
        if (last_iterator < reader_.begin())
        {
          // empty block?
          ret = OB_BEYOND_THE_RANGE;
        }
      }
      else
      {
        // lookup scan %end_key_ in block
        last_iterator = reader_.lower_bound(query_end_key);
        if (last_iterator == reader_.end())
        {
          // scan %end_key_ larger than any rowkeys in block.
          // so we got the last key in it.
          --last_iterator;
        }
        else
        {
          // there 's a key >= query_end_key in this block,
          // so we do not need looking forward anymore.
          if (!is_reverse_scan_) need_looking_forward = false;
        }

        // now %last_iterator >= begin() && %last_iterator < end();
        // check if scan %end_key_ less than %last_iterator.row_key
        ObRowkey find_end_key;
        ret = reader_.get_row_key(last_iterator, find_end_key);
        if (OB_SUCCESS == ret)
        {
          if (last_iterator == reader_.begin())
          {
            if (query_end_key.compare(find_end_key) < 0)
            {
              ret = OB_BEYOND_THE_RANGE;
            }

            if (query_end_key.compare(find_end_key) == 0
                && (!range.border_flag_.inclusive_end()))
            {
              ret = OB_BEYOND_THE_RANGE;
            }
          }
          else
          {
            if (query_end_key.compare(find_end_key) < 0)
            {
              --last_iterator;
            }

            // now last_iterator > reader_.begin() && last_iterator < reader_.end();
            // check border_flag_ if inclusive %end_key_
            if (query_end_key.compare(find_end_key) == 0)
            {
              if (!range.border_flag_.inclusive_end())
              {
                --last_iterator;
              }
            }

            if (last_iterator < reader_.begin())
            {
              ret = OB_BEYOND_THE_RANGE;
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "get last key of block error, last_iterator=%d,%d",
              last_iterator->offset_, last_iterator->size_);
        }
      }

      return ret;
    }


  }//end namespace sstable
} // end namespace oceanbase




