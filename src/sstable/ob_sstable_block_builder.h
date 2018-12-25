/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_sstable_block_builder.h for persistent ssatable block and 
 * store inner block index. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_BLOCK_BUILDER_H_
#define OCEANBASE_SSTABLE_OB_BLOCK_BUILDER_H_

#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_row.h"
#include "ob_sstable_row.h"

namespace oceanbase 
{
  namespace sstable 
  {
    struct ObSSTableBlockHeader
    {
      int32_t row_index_array_offset_;  // offset of row index array in payload
      int32_t row_count_;               // row count
      int16_t reserved_;                // must be 0  V0.2.0
      uint16_t column_group_id_;        // column group id of this block
      uint32_t table_id_;               // table id of this block

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    class ObSSTableBlockBuilder 
    {
    public:
      static const int64_t SSTABLE_BLOCK_SIZE     = 64 * 1024;  //64k
      static const int64_t BLOCK_DATA_BUFFER_SIZE = 2 * 1024 * 1024; //2M
      static const int64_t ROW_INDEX_BUFFER_SIZE  = 64 * 1024;  //64K
      static const int64_t SSTABLE_BLOCK_BUF_ALIGN_SIZE = 1024; //1k
      static const int64_t MIN_ROW_INDEX_BUF_SIZE = sizeof(int32_t) * 2;  //8bytes
      static const int64_t ROW_INDEX_ITEM_SIZE    = sizeof(int32_t);      //4bytes
      static const int64_t TABLE_ID_MASK          = 0xFFFF;
      static const int64_t COLUMN_GROUP_ID_MASK   = 0xFF;

    public:
      ObSSTableBlockBuilder();
      ~ObSSTableBlockBuilder();

      /**
       * return row index buffer 
       *  
       * WARNING: the buffer stores the serialized row offset(int32_t) 
       * array. Please don't cast it to int32_t array, you need 
       * deserialize it then use it. If user have called function 
       * build_block(), the row index buffer is unavailable.
       *  
       * @return const char*
       */
      inline const char* row_index_buf() const
      { 
        return row_index_buf_; 
      }

      /**
       * return the block buffer 
       *  
       * WARNING: before running function build_block(), block buffer 
       * only store the serialized rows, includes row key and row 
       * data, after running build_block(), the block buffer stores 
       * data using below format: 
       *    block header + serialized rows + row index
       * 
       * @return const char* 
       */
      inline const char* block_buf() const
      { 
        return block_buf_; 
      }

      /**
       * reset all the members
       */
      void reset();

      /**
       * return the offset of row index array in block. 
       *  
       * WARNING: this function only can be called after running 
       * build_block(), otherwise it return 0. 
       * 
       * @return int32_t 
       */
      inline const int64_t get_row_index_array_offset() const
      {
        return block_header_.row_index_array_offset_;
      }

      /**
       * return row count in the block, if there is no row in block, 
       * return 0. 
       * 
       * @return int32_t 
       */
      inline const int64_t get_row_count() const
      {
        return block_header_.row_count_;
      }

      /**
       * return how much space to used to store this block in disk  
       * 
       * @return int64_t return block size
       */
      inline const int64_t get_block_size() const
      {
        return ((block_buf_cur_ - block_buf_) 
                + (row_index_buf_cur_ - row_index_buf_));
      }

      /**
       * return the size of row index array 
       * 
       * @return int64_t 
       */
      inline const int64_t get_row_index_size() const
      {
        return (row_index_buf_cur_ - row_index_buf_);
      }

      /**
       * return the row data size in the block, not include row index, 
       * but include the block header size 
       * 
       * @return int64_t 
       */
      inline const int64_t get_block_data_size() const
      {
        return (block_buf_cur_ - block_buf_);
      }

      /**
       * initialize the index buffer and block buffer, user must run 
       * this function first. 
       *  
       * @param block_size block size of sstable
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR  
       */
      int init(const int64_t block_size = SSTABLE_BLOCK_SIZE);

      /**
       * ensure the index buffer and block buffer have enough space to 
       * store a new serialized row 
       * 
       * @param size serialization size of row
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int ensure_space(const int64_t size);

      /**
       * add a new row and serialize the row in the block buffer and 
       * add a serialized row index in the index buffer 
       *  
       * WARNING: because this class use static block buffer, the 
       * buffer size is 256k, if the row size is larger than 256k, 
       * this function always return OB_ERROR. it can't work in this 
       * case. 
       * 
       * @param row the row to add
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int add_row(const ObSSTableRow &row, uint64_t &row_checksum, const int64_t row_serialize_size = 0);
      int add_row(const uint64_t table_id, const uint64_t column_group_id, 
          const common::ObRow &row, const int64_t row_serialize_size = 0);

      /**
       * merge block header, block data and row index into one block 
       * based on the block format 
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int build_block();

      /**
       * get table id of this block
       *
       * @return uint64_t table id
       */
      const uint64_t get_table_id() const;

      /**
       * get column group id of this block
       *
       * @return uint64_t column group id
       */
      const uint64_t get_column_group_id() const;
      
      /**
       * set table id of this block
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int set_table_id(const uint64_t table_id);

      /**
       * set column group id of this block
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int set_column_group_id(const uint64_t column_group_id);

    private:
      /**
       * return the remain space in row index buffer 
       * 
       * @return int64_t 
       */
      const int64_t get_row_index_remain() const;

      /**
       * return the remain space of block buffer 
       * 
       * @return int64_t 
       */
      const int64_t get_block_data_remain() const;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableBlockBuilder);
      char* row_index_buf_;     //int32_t array, store the row index
      char* row_index_buf_cur_; //current pointer of row index buffer 
      int64_t row_index_buf_size_;//size of row index buffer                    
      char* block_buf_;         //buf to store serialized block data 
      char* block_buf_cur_;     //current pointer of block buffer
      int64_t block_buf_size_;  //size of block buffer
      ObSSTableBlockHeader block_header_;   //block header
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_BLOCK_BUILDER_H_
