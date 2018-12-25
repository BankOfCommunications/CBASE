/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_sstable_block_index_buffer.h is for what ...
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *
 */

#ifndef OCEANBASE_SSTABLE_SSTABLE_BLOCK_INDEX_BUFFER_H_
#define OCEANBASE_SSTABLE_SSTABLE_BLOCK_INDEX_BUFFER_H_

#include "tblog.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "common/ob_rowkey.h"

namespace oceanbase
{
  namespace sstable
  {
    struct ObSSTableBlockIndexItem
    {
      int16_t  rowkey_column_count_; //rowkey column count with table, used by v0.2.1
      uint16_t column_group_id_;     //column group id       
      uint32_t table_id_;            //table id
      int32_t  block_record_size_;   //block record size includes record header
      int16_t  block_end_key_size_;  //end key size
      int16_t  reserved_;            //must be 0

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    class ObSSTableBlockIndexBuffer
    {
    public:
      ObSSTableBlockIndexBuffer();
      ~ObSSTableBlockIndexBuffer();
      
      /**
       * Free all memory 
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int clear();

      /**
       * Free all mem block except head mem block
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int reset();
      
    public:
      /**
       * copy key to MemBlock
       *
       * @param ObString key  the key to be copied
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int add_key(const common::ObRowkey& key);
      int add_key(const common::ObString& key);

      /**
       * add an block index item to MemBlock
       *
       * @param ObSSTableBlockIndexItem  item to be added
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int add_index_item(const ObSSTableBlockIndexItem& item);


      /**
       * WARNING ptr must holds more buffer than block index size
       * copy data from mem block to ptr
       *
       * @return  int if success, return OB_SUCCESS, else return OB_ERROR
       *
       */
      int get_data(char*& ptr, const int64_t ptr_size);

      /**
       * get size of real data
       *
       * @return int64_t size of real date in bytes
       */
      const int64_t get_data_size() const;
      
      /**
       * get sieze of buffer
       *
       * @return int64_t size of whole buffer
       */
      const int64_t get_total_size() const;
    
    public:
      const static int64_t DEFAULT_MEM_BLOCK_SIZE;    
    private:
      int alloc_block();
      int alloc_mem(const int64_t size, void*& ptr);
      int free_mem(void* ptr);
      
    public:
      struct MemBlock
      {
        MemBlock* next_;
        int32_t cur_pos_;
        int32_t block_size_;
        char data_[0];
      };

    public:
      inline const MemBlock* get_block_head() const
      {
        return block_head_;
      }
      
      inline const MemBlock* get_block_tail() const
      {
        return block_tail_;
      }
      
    private:
      MemBlock* block_head_;
      MemBlock* block_tail_;
      int64_t total_size_;
      int64_t data_size_; 
    };
  }//namespace sstable
}//namespace oceanbase
#endif
