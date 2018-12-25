/**
 * (C) 2010-2011 Taobao Inc. 
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_sstable_block_index_buffer.cpp is for what ...
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *
 */

#include <tbsys.h>
#include "common/serialization.h"
#include "ob_sstable_block_index_buffer.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace common;
    using namespace common::serialization;

    const int64_t ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE = 2 * 1024 * 1024; //2M;    
    ObSSTableBlockIndexBuffer::ObSSTableBlockIndexBuffer():block_head_(NULL),
                                                           block_tail_(NULL),
                                                           total_size_(0),
                                                           data_size_(0)
    {
    }
    
    ObSSTableBlockIndexBuffer::~ObSSTableBlockIndexBuffer()
    {
      clear();
      block_head_ = NULL;
      block_tail_ = NULL;
    }

     DEFINE_SERIALIZE(ObSSTableBlockIndexItem)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len)) 
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret 
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, rowkey_column_count_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_group_id_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, table_id_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, block_record_size_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, block_end_key_size_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved_))))
      { 
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie block index item, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      return ret;
    }
    
    DEFINE_DESERIALIZE(ObSSTableBlockIndexItem)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size + pos > data_len) 
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, data_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &rowkey_column_count_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos,
                                       reinterpret_cast<int16_t*>(&column_group_id_))))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, 
                                       reinterpret_cast<int32_t*>(&table_id_))))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &block_record_size_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &block_end_key_size_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie block index item, buf=%p, "
                        "data_len=%ld, pos=%ld", buf, data_len, pos);
      }        
    
      return ret;
    }
    
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableBlockIndexItem)
    {
      return (encoded_length_i16(rowkey_column_count_)
              + encoded_length_i16(column_group_id_)
              + encoded_length_i32(table_id_)
              + encoded_length_i32(block_record_size_) 
              + encoded_length_i16(block_end_key_size_) 
              + encoded_length_i16(reserved_));
    }

    int ObSSTableBlockIndexBuffer::clear()
    {
      int ret = OB_SUCCESS;
      MemBlock* tmp = NULL;
      while (NULL != block_head_)
      {
        tmp = block_head_->next_;
        ret = free_mem(block_head_);
        if (OB_ERROR == ret)
        {
          TBSYS_LOG(WARN, "failed to free mem, ptr=%p", block_head_);
        }
        block_head_ = tmp;
      }
      block_head_ = NULL;
      block_tail_ = NULL;
      total_size_ = 0;
      data_size_  = 0;

      return ret;
    }

    int ObSSTableBlockIndexBuffer::reset()
    {
      int ret = OB_SUCCESS;
      if (NULL != block_head_)
      {
        MemBlock* tmp = NULL;
        while (NULL != block_head_->next_)
        {
          tmp = block_head_->next_;
          block_head_->next_ = tmp->next_;
          ret = free_mem(tmp);
          if (OB_ERROR == ret)
          {
            TBSYS_LOG(WARN, "failed to free mem, ptr=%p", tmp);
          }
        }
        block_tail_ = block_head_;
        block_head_->next_ = NULL;
        block_head_->cur_pos_ = 0;
        block_head_->block_size_ = DEFAULT_MEM_BLOCK_SIZE - sizeof(MemBlock);
        total_size_ = DEFAULT_MEM_BLOCK_SIZE;
        data_size_  = 0;
      }
      else
      {
        block_head_ = NULL;
        block_tail_ = NULL;
        total_size_ = 0;
        data_size_  = 0;
      }

      return ret;
    }

    
    int ObSSTableBlockIndexBuffer::add_key(const ObRowkey& key)
    {
      int ret = OB_SUCCESS;

      int64_t rowkey_len = key.get_serialize_objs_size();
     
      if (0 >= key.get_obj_cnt() || NULL == key.get_obj_ptr())
      {
        TBSYS_LOG(WARN, "invalid argument key_len=%ld, key_ptr=%p", 
            key.get_obj_cnt(), key.get_obj_ptr());
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        if (NULL == block_tail_ || 
            (NULL != block_tail_ && block_tail_->block_size_ - block_tail_->cur_pos_ <= rowkey_len))
        {
          ret = alloc_block();
          if (OB_ERROR == ret)
          {
            TBSYS_LOG(WARN, "failed to alloc mem block ret=%d",ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == block_tail_ || 
              (NULL != block_tail_ && block_tail_->block_size_ - block_tail_->cur_pos_ <= rowkey_len))
          {
            ret = OB_ERROR;
          }
          else
          {
            int64_t pos = block_tail_->cur_pos_;
            int64_t size = block_tail_->block_size_;
            key.serialize_objs(block_tail_->data_, size, pos);
            data_size_ += rowkey_len;
            block_tail_->cur_pos_ = static_cast<int32_t>(pos);
          }
        }
      }
      return ret;
    }

    int ObSSTableBlockIndexBuffer::add_key(const ObString& key)
    {
      int ret = OB_SUCCESS;
     
      if (0 >= key.length() || NULL == key.ptr())
      {
        TBSYS_LOG(WARN, "invalid argument key_len=%d, key_ptr=%p", key.length(), key.ptr());
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        if (NULL == block_tail_ || 
            (NULL != block_tail_ && block_tail_->block_size_ - block_tail_->cur_pos_ <= key.length()))
        {
          ret = alloc_block();
          if (OB_ERROR == ret)
          {
            TBSYS_LOG(WARN, "failed to alloc mem block ret=%d",ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == block_tail_ || 
              (NULL != block_tail_ && block_tail_->block_size_ - block_tail_->cur_pos_ <= key.length()))
          {
            ret = OB_ERROR;
          }
          else
          {
            memcpy(block_tail_->data_ + block_tail_->cur_pos_, key.ptr(), key.length());
            data_size_ += key.length();
            block_tail_->cur_pos_ += key.length();
          }
        }
      }
      return ret;
    }
    
    int ObSSTableBlockIndexBuffer::add_index_item(const ObSSTableBlockIndexItem& item)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size  = item.get_serialize_size();
      int64_t pos = 0;

      if (0 >= serialize_size)
      {
        TBSYS_LOG(WARN, "invalid argument item seriallize_size=%ld", serialize_size);
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        if (NULL == block_tail_ || 
            (NULL != block_tail_ && block_tail_->block_size_ - block_tail_->cur_pos_ <= serialize_size))
        {
          ret = alloc_block();
          if (OB_ERROR == ret)
          {
            TBSYS_LOG(WARN, "failed to alloc mem block ret=%d",ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == block_tail_ || 
              (NULL != block_tail_ && block_tail_->block_size_ - block_tail_->cur_pos_ <= serialize_size))
          {
            ret = OB_ERROR;
          }
          else
          {
            ret = item.serialize(block_tail_->data_ + block_tail_->cur_pos_,
                                 static_cast<uint64_t>(sizeof(ObSSTableBlockIndexItem)), pos);
            if (OB_ERROR == ret)
            {
              TBSYS_LOG(WARN, "failed to serialize item to mem block,");
            }

            if (OB_SUCCESS == ret)
            {
              data_size_ += serialize_size;
              block_tail_->cur_pos_ = static_cast<int32_t>(block_tail_->cur_pos_ + serialize_size);
            }
          }
        }
      }
      return ret;
    }

    const int64_t ObSSTableBlockIndexBuffer::get_data_size() const
    {
      return data_size_;
    }

    const int64_t ObSSTableBlockIndexBuffer::get_total_size() const
    {
      return total_size_;
    }
    
    int ObSSTableBlockIndexBuffer::get_data(char*& ptr, const int64_t ptr_size)
    {
      int ret = OB_SUCCESS;
      
      if (NULL == ptr || data_size_ > ptr_size)
      {
        TBSYS_LOG(WARN, "invalid argument ptr=%p, buf_size=%ld, data_size_=%ld", ptr, ptr_size, data_size_);
        ret = OB_ERROR;
      }
      int64_t pos = 0;
      MemBlock* tmp = block_head_;
      while(NULL != tmp)
      {
        memcpy(ptr + pos, tmp->data_, tmp->cur_pos_);
        pos += tmp->cur_pos_;
        tmp = tmp->next_;
      }
      return ret;
    }
    
    int ObSSTableBlockIndexBuffer::alloc_block()
    {
      int ret = OB_SUCCESS;
      void* ptr = NULL;
      MemBlock* new_block = NULL;
      ret = alloc_mem(DEFAULT_MEM_BLOCK_SIZE, ptr);
      if (OB_SUCCESS != ret || NULL == ptr)
      {
        TBSYS_LOG(WARN, "failed to alloc mem size=%ld, ret=%d", DEFAULT_MEM_BLOCK_SIZE, ret);
        ret = OB_ERROR;
      }
      else
      {
        new_block = static_cast<MemBlock*>(ptr);
        new_block->next_ = NULL;
        new_block->cur_pos_ = 0;
        new_block->block_size_ = DEFAULT_MEM_BLOCK_SIZE - sizeof(MemBlock);
        if (NULL == block_tail_)
        {
          block_tail_ = new_block;
          block_head_ = new_block;
        }
        else
        {
          block_tail_->next_ = new_block;
          block_tail_ = new_block;
        }
      }
      return ret;
    }

    int ObSSTableBlockIndexBuffer::alloc_mem(const int64_t size, void*& ptr)
    {
      int ret = OB_SUCCESS;
      if (0 >= size)
      {
        TBSYS_LOG(WARN, "invalid argument size=%ld",size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ptr = ob_malloc(size, ObModIds::OB_SSTABLE_WRITER);
        if (NULL == ptr)
        {
          TBSYS_LOG(WARN, "failed to alloc memory, size=%ld", size);
          ret = OB_ERROR;
        }
        else
        {
          total_size_ += size;
        }
      }
      
      return ret;
    }
    
    int ObSSTableBlockIndexBuffer::free_mem(void* ptr)
    {
      int ret = OB_SUCCESS;
      
      if (NULL == ptr)
      {
        TBSYS_LOG(WARN, "invalid argument ptr=%p", ptr);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ob_free(ptr);
      }
      return ret;
    }
  }
}
