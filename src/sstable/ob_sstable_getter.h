/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_getter.h for get one or more columns from 
 * sstables. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_GETTER_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_GETTER_H_

#include <tbsys.h>
#include "common/ob_iterator.h"
#include "common/ob_read_common_data.h"
#include "ob_sstable_reader.h"
#include "ob_sstable_block_index_v2.h"
#include "ob_block_index_cache.h"
#include "ob_blockcache.h"
#include "ob_scan_column_indexes.h"
#include "ob_sstable_block_getter.h"
#include "ob_sstable_row_cache.h"

namespace oceanbase
{
  namespace sstable
  {
    class ObSSTableGetter : public common::ObIterator
    {
    public:
      ObSSTableGetter();
      virtual ~ObSSTableGetter();

      /**
       * WARNING: this function must be called before function 
       * get_cell(), next_cell() -> get_cell() -> next_cell() -> 
       * get_cell() 
       * 
       * @return int 
       * 1. success 
       *    OB_SUCCESS
       *    OB_ITER_END finish to traverse all the blocks
       * 2. fail 
       *     OB_ERROR
       *     OB_INVALID_ARGUMENT
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
      int get_cell(common::ObCellInfo** cell);
      int get_cell(common::ObCellInfo** cell, bool* is_row_changed);

      /**
       * initialize sstable getter
       * 
       * @param block_cache block cache 
       * @param block_index_cache block index cache
       * @param get_param get parameter
       * @param readers readers for each row
       * @param reader_size readers count 
       * @param not_exit_col_ret_nop whether return nop if column 
       *                             doesn't exit
       * @param row_cahe sstable row cache to store static row data 
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_INVALID_ARGUMENT
       */
      int init(ObBlockCache& block_cache, ObBlockIndexCache& block_index_cache, 
               const common::ObGetParam& get_param, 
               const ObSSTableReader* const readers[], const int64_t reader_size,
               bool not_exit_col_ret_nop = false, ObSSTableRowCache* row_cache = NULL);

      inline int64_t get_handled_cells_in_param()
      {
        return handled_cells_;
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
      /**
       * check whether the internal members are legal
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_internal_member();

      /**
       * switch to next row if next row exists
       * 
       * @return int 
       * 1. success 
       *    OB_ITER_END finish to traverse all the blocks
       * 2. fail 
       *     OB_ERROR
       *     OB_GET_NEXT_ROW 
       */
      int switch_row();

      int handle_row_not_found();

      /**
       * switch to the next column 
       * 
       * @return int 
       * 1. success 
       *    OB_SUCCESS
       *    OB_GET_NEXT_ROW
       * 2. fail 
       *     OB_ERROR
       */
      int switch_column();

      int get_block_data(const uint64_t sstable_id, const uint64_t table_id,
                         const char*& block_data, int64_t &block_data_size);

      /**
       *  fetch block data from  block cache, decompress and
       *  initialize block getter
       * 
       * @return int 
       * 1. success 
       *    OB_SUCCESS
       *    OB_SEARCH_NOT_FOUND not found current row key 
       * 2. fail 
       *     OB_ERROR 
       */
      int fetch_block();

      int fetch_cache_row(const common::ObRowkey& row_key, 
        const int64_t store_style, ObSSTableRowCacheValue& row_cache_val);

      /**
       * filter columns of current row key, set flag for expected 
       * column 
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR 
       */
      int init_column_mask();

      /**
       * load current block for current row key, find the row key 
       * belong to which block, if find the block, load it 
       * 
       * @return int 
       * 1. success 
       *    OB_SUCCESS
       *    OB_SEARCH_NOT_FOUND not found current row key 
       * 2. fail 
       *     OB_ERROR  
       */
      int load_cur_block();

      void reset();

      const common::ObCellInfo* get_cur_param_cell() const;
      bool is_column_group_id_existent(const uint64_t column_group_id);
      int add_column_group_id(const ObSSTableSchema& schema, 
                              const uint64_t table_id,
                              const uint64_t column_group_id);
      int filter_column_group();

    private:
      enum ObGetterIterState
      {
        ROW_NOT_FOUND,
        GET_NEXT_ROW,
        ITERATE_NORMAL,
        ITERATE_END
      };

    private:
      static const int64_t DEFAULT_UNCOMP_BUF_SIZE  = 128 * 1024; //128k
      static const int64_t DEFAULT_ROW_BUF_SIZE     = 64 * 1024; //64k
      static const int64_t MAX_GET_COLUMN_COUNT_PRE_ROW = common::ObGetParam::MAX_CELLS_PER_ROW;

      DISALLOW_COPY_AND_ASSIGN(ObSSTableGetter);

      bool inited_;                     //whether sstable getter is initialized
      ObGetterIterState cur_state_;     //current getter state
      bool not_exit_col_ret_nop_;       //whether return nop if columns doesn't exit
      bool is_row_finished_;            //whether the row is end

      const ObSSTableReader* const* readers_; //sstable readers
      int64_t readers_size_;            //sstable readers count
      int64_t cur_reader_idx_;          //current reader index
      int64_t prev_reader_idx_;         //previous reader index
      int64_t handled_cells_;           //handled cells in get param

      uint64_t column_group_[common::OB_MAX_COLUMN_GROUP_NUMBER]; //column group to read
      int64_t column_group_num_;        //how many column group to read
      int64_t cur_column_group_idx_;    //current reading column group index

      const common::ObGetParam* get_param_;//get parameter
      ObBlockPositionInfo block_pos_;   //current block position for current row key
      sstable::ObBlockPositionInfos index_array_;

      ObScanColumnIndexes cur_column_mask_;//current column mask to show which columns to get
      common::ObCellInfo null_cell_info_;//null cell info for return null cell
      ObSSTableBlockGetter getter_;     //block getter

      ObBlockCache* block_cache_;       //block cache
      ObBlockIndexCache* block_index_cache_; //block index cache
      ObSSTableRowCache* sstable_row_cache_; //sstable row cache

      common::ObMemBuf uncomp_buf_;     //uncompressed buffer
      common::ObMemBuf row_buf_;     //sstable row data buffer
      common::ObObj rowkey_obj_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
      common::ObRowkeyInfo rowkey_info_; // for deserialize old fashion binary rowkey
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif  // OCEANBASE_SSTABLE_OB_SSTABLE_GETTER_H_
