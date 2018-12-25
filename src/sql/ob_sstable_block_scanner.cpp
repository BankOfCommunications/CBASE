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
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_action_flag.h"
#include "common/ob_rowkey_helper.h"
#include "sstable/ob_scan_column_indexes.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_trailer.h" 
#include "sstable/ob_sstable_scan_param.h"
#include "ob_sstable_block_scanner.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace sql
  {
    ObSSTableBlockScanner::ObSSTableBlockScanner(const ObSimpleColumnIndexes& column_indexes)
      : initialize_status_(OB_NOT_INIT), sstable_data_store_style_(OB_SSTABLE_STORE_DENSE), 
      is_reverse_scan_(false), is_full_row_scan_(false), not_exit_col_ret_nop_(false),
      current_column_count_(0), row_cursor_(NULL), row_start_index_(NULL), row_last_index_(NULL),
      query_column_indexes_(column_indexes)
    {
      memset(current_columns_, 0 , sizeof(current_columns_));
    }

    ObSSTableBlockScanner::~ObSSTableBlockScanner()
    {
    }

    inline void ObSSTableBlockScanner::next_row()
    {
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
      if ((!is_reverse_scan_) && row_cursor_ == row_start_index_)
      {
        ret = true;
      }
      else if (is_reverse_scan_ && row_cursor_ == row_last_index_)
      {
        ret = true;
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

    int ObSSTableBlockScanner::store_and_advance_row()
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(row_cursor_ < row_start_index_ || row_cursor_ > row_last_index_))
      {
        TBSYS_LOG(ERROR, "internal error, current row cursor=%d,%d < start=%p, >last=%p.",
            row_cursor_->offset_, row_cursor_->size_, row_start_index_, row_last_index_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = 
            reader_.get_row(static_cast<int>(sstable_data_store_style_),
              row_cursor_, is_full_row_scan_, query_column_indexes_, 
              current_rowkey_, current_row_))) 
      {
        TBSYS_LOG(ERROR, "read current row error, store style=%ld, current row cursor=%d,%d",
            sstable_data_store_style_, row_cursor_->offset_, row_cursor_->size_);
      }
      else
      {
        next_row();
      }
      return ret;
    }

    int ObSSTableBlockScanner::get_next_row(const ObRowkey* &row_key, const ObRow *&row_value)
    {
      int ret = OB_SUCCESS;
      if (end_of_block())
      {
        ret = OB_BEYOND_THE_RANGE;
      }
      else if (OB_SUCCESS != (ret = store_and_advance_row()))
      {
        TBSYS_LOG(ERROR, "store_and_advance_row error, ret= %d", ret);
      }
      else
      {
        row_key = &current_rowkey_;
        row_value = &current_row_;
      }
      return ret;
    }

    int ObSSTableBlockScanner::initialize(const bool is_reverse_scan, 
        const bool is_full_row_scan, const int64_t store_style, const bool not_exit_col_ret_nop)
    {
      sstable_data_store_style_ = store_style;
      is_reverse_scan_ = is_reverse_scan;
      is_full_row_scan_ = is_full_row_scan;
      not_exit_col_ret_nop_ = not_exit_col_ret_nop;

      row_cursor_ = NULL;
      row_start_index_ = NULL;
      row_last_index_ = NULL;
      initialize_status_ = OB_NOT_INIT;
      current_column_count_ = 0;
      reader_.reset();

      // fill null value;
      not_null_column_count_ = query_column_indexes_.get_not_null_column_count();

      return OB_SUCCESS;
    }

    /**
     * initialize block object with block data
     * @param [in] scan_param input scan param
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
        const common::ObRowDesc& row_desc,
        const ObSSTableScanParam & scan_param,
        const ObSSTableBlockReader::BlockDataDesc& data_desc,  
        const ObSSTableBlockReader::BlockData& block_data, 
        bool &need_looking_forward) 
    {
      int32_t ret = OB_SUCCESS; 

      need_looking_forward = true;
      reader_.reset();  //reset reader before assign start_iterator and last_iterator
      const_iterator start_iterator = reader_.end();
      const_iterator last_iterator = reader_.end();
      current_row_.set_row_desc(row_desc);
      current_row_.reset(false, scan_param.is_not_exit_col_ret_nop() ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);

      const ObNewRange & range = scan_param.get_range();


      if (!block_data.available() || !data_desc.available())
      {
        TBSYS_LOG(ERROR, "block_data,desc invalid, bd=(%p,%ld, %p, %ld), desc=(%p,%ld,%ld) ",
            block_data.data_buffer_, block_data.data_bufsiz_, block_data.internal_buffer_, block_data.internal_bufsiz_,
            data_desc.rowkey_info_, data_desc.rowkey_column_count_, data_desc.store_style_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = initialize(scan_param.is_reverse_scan(), 
        scan_param.is_full_row_scan(), data_desc.store_style_, 
        scan_param.is_not_exit_col_ret_nop())))
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

    int ObSSTableBlockScanner::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      row_desc = current_row_.get_row_desc();
      return ret;
    }

  }//end namespace sstable
} // end namespace oceanbase





