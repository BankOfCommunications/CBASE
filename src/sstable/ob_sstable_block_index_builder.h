/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_sstable_block_index_builder.h for persistent ssatable 
 * index and store index. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_BLOCK_INDEX_BUILDER_H_
#define OCEANBASE_SSTABLE_OB_BLOCK_INDEX_BUILDER_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "common/ob_list.h"
#include "common/ob_rowkey.h"
#include "ob_sstable_block_index_buffer.h"

namespace oceanbase 
{
  namespace sstable 
  {
    struct ObSSTableBlockIndexHeader
    {
      int64_t sstable_block_count_;         //block count of sstable
      int32_t end_key_char_stream_offset_;  //offset of end keys array
      int16_t rowkey_flag_;                 // v2.1 rowkey obj array format, set to 1
      int16_t reserved16_;                  // reserved, must be 0
      int64_t reserved64_[2];               //reserved, must be 0

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    class ObSSTableBlockIndexBuilder
    {
      
    public:
      ObSSTableBlockIndexBuilder();
      ~ObSSTableBlockIndexBuilder();

      
      /**
       * return how many blocks in sstable  
       * 
       * @return int64_t 
       */
      const int64_t get_block_count() const;

      /**
       * WARNING: only call this function after call function
       * build_block_index(), or after deserialize
       * ObSSTableBlockIndexHeader. otherwise it return 0.
       * 
       */
      const int64_t get_end_key_char_stream_offset() const;

      /**
       * return total index size, includes index header, index items 
       * and end keys char stream 
       * 
       * @return int64_t 
       */
      const int64_t get_index_block_size() const;

      /**
       * return how much data in index items array 
       * 
       * @return int64_t 
       */
      const int64_t get_index_items_size() const;

      /**
       * return how much data in end keys buffer 
       * 
       * @return int64_t 
       */
      const int64_t get_end_keys_size() const;

      /**
       * get index item buffer
       *
       * @return ObSSTableBlockIndexBuffer* index item buffer pointer
       */
      const ObSSTableBlockIndexBuffer* get_index_items_buffer() const;
      
      /**
       * get end keys buffer
       *
       * @return ObSSTableBlockIndexBuffer* end key buffer pointer
       */
      const ObSSTableBlockIndexBuffer* get_end_keys_buffer() const;

      /**
       * reset this instance be convenient for reuse it.
       */
      void reset();

      int init();
      
      /**
       * add new index entry(index item and end key char stream), when 
       * a block is written to sstable, call this function to add the 
       * end key and size of the record into the block index buffer. 
       *  
       * @param table_id table id 
       * @param column_group_id column group id 
       * @param key end key of block
       * @param record_size size of the record in sstable
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int add_entry(const uint64_t table_id, const uint64_t column_group_id, const common::ObRowkey &key, 
                    const int32_t record_size);

      int add_entry(const uint64_t table_id, const uint64_t column_group_id, const common::ObString &key, 
                    const int32_t record_size);
      /**
       * before user write the block index into sstable, call this 
       * function to serialize and merge block index header, index 
       * items array and end key stream into one block, the block like 
       * below format 
       *  ------------------------------------------------------------
       *  | index block header | index items array | end keys stream |
       *  ------------------------------------------------------------
       *  
       *  WARNING: the application must ensure the index block has
       *  enough memory to store the block index data.
       * 
       * @param index_block the new block buffer to store the result
       * @param index_size the length of new block buffer
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int build_block_index(const bool use_binary_rowkey, char* index_block, const int64_t buffer_size, int64_t& index_size);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableBlockIndexBuilder);
    
      ObSSTableBlockIndexBuffer index_items_buf_;     //buffer to store index item
      ObSSTableBlockIndexBuffer end_keys_buf_;        //buffer to stroe end keys
      ObSSTableBlockIndexHeader index_block_header_;  //block index header
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_BLOCK_INDEX_BUILDER_H_
