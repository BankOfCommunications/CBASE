/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_block_getter.h for get one or more columns from 
 * ssatable block. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_GETTER_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_GETTER_H_

#include "common/ob_string.h"
#include "common/ob_common_param.h"
#include "common/ob_rowkey.h"
#include "ob_sstable_block_reader.h"
#include "ob_sstable_row_cache.h"

namespace oceanbase
{
  namespace sstable
  {
    class ObScanColumnIndexes;
  
    class ObSSTableBlockGetter
    {
    public:
      explicit ObSSTableBlockGetter(const ObScanColumnIndexes& column_index);
      ~ObSSTableBlockGetter();
  
      /**
       * WARNING: this function must be called before function 
       * get_cell(), next_cell() -> get_cell() -> next_cell() -> 
       * get_cell() 
       * 
       * @return int 
       * 1. success 
       *    OB_SUCCESS
       *    OB_ITER_END finish to traverse the current row
       * 2. fail 
       *     OB_ERROR
       *     OB_DESERIALIZE_ERROR failed to deserialize obj
       */
      int next_cell();
  
      /**
       * get current cell. must be called after next_cell()
       * 
       * @param cell_info [out] store the cell result 
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_INVALID_ARGUMENT
       */
      int get_cell(common::ObCellInfo** cell_info);
  
      /**
       * initialize block getter
       * 
       * @param row_key row key which the block getter to find 
       * @param buf block buffer which include the block data
       * @param data_len block data length 
       * @param row_cache sstable row cache 
       * @param is_row_cache_data if the buf includes data from row 
       *                          cache
       * @param store_style sstable store style 
       * @param not_exit_col_ret_nop whether return nop if column 
       *                             doesn't exit
       *  
       * @return int 
       * 1. success 
       *    OB_SUCCESS
       *    OB_SEARCH_NOT_FOUND not find the row key in block
       * 2. fail 
       *     OB_ERROR
       *     OB_INVALID_ARGUMENT invalid arguments
       *     OB_DESERIALIZE_ERROR failed to deserialize obj
       */
      int init(const common::ObRowkey& row_key, const char* buf, 
               const int64_t data_len, const ObSSTableBlockReader::BlockDataDesc& data_desc,
               ObSSTableRowCache* row_cache, const bool is_row_cache_data,
               bool not_exit_col_ret_nop = false);

      // WARNING: this function must be called after init()
      inline int get_cache_row_value(ObSSTableRowCacheValue& row_value)
      {
        return reader_.get_cache_row_value(row_cursor_, row_value);
      }

      inline int is_row_finished(bool* is_row_finished)
      {
        if (NULL != is_row_finished) 
        {
          *is_row_finished = is_row_finished_;
        }
        return common::OB_SUCCESS;
      }
    
    private:
      typedef ObSSTableBlockReader::const_iterator const_iterator;
      typedef ObSSTableBlockReader::iterator iterator;

    private:
      int load_current_row(const_iterator row_index);
      int store_sparse_column(const ObScanColumnIndexes::Column &column);
      int store_dense_column(const ObScanColumnIndexes::Column &column);
      int store_current_cell(const ObScanColumnIndexes::Column &column);
      int store_and_advance_column();
      int get_current_column_index(const int64_t cursor, ObScanColumnIndexes::Column& column) const;
      int read_row_columns(const int64_t format, const char* row_buf, 
        const int64_t data_len);
      void clear();
      
    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableBlockGetter);
  
    private:
      static const int64_t DEFAULT_INDEX_BUF_SIZE = 256 * 1024; // 256K
  
      bool inited_;                     //whether the block getter is initialized
      bool handled_del_row_;            //whether handled the first delete row op
      bool not_exit_col_ret_nop_;       //whether return nop if columns doesn't exit
      bool is_row_cache_data_;          //whether row value cached in sstable row cache
      bool is_row_finished_;            //whether the row is end
      int64_t sstable_data_store_style_;//sstable store style
      int64_t column_cursor_;           //current column cursor
      int64_t current_column_count_;    //current column count
      const ObScanColumnIndexes& query_column_indexes_;
      const_iterator row_cursor_;       //current row cursor
      ObSSTableRowCache* sstable_row_cache_; //sstable row cache

      common::ObMemBuf index_buf_;      //row position index buffer

      common::ObCellInfo current_cell_info_;//current cell info to return for get_cell()
      common::ObRowkey current_rowkey_;  //current row key
      common::ObObj current_ids_[common::OB_MAX_COLUMN_NUMBER];
      common::ObObj current_columns_[common::OB_MAX_COLUMN_NUMBER];

      ObSSTableBlockReader reader_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif  //OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_GETTER_H_
