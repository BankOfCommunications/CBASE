/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_block_membuf.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#include <tbsys.h>
#include "ob_block_membuf.h"
//#include "common/ob_malloc.h"

namespace oceanbase
{
  using namespace oceanbase::common;
  
  namespace compactsstable
  {
    ObBlockMembuf::ObBlockMembuf(const int64_t block_size,
                                 const int32_t mod_id) : mod_id_(mod_id),
                                                         block_size_(block_size),
                                                         block_data_size_(block_size_ - BLOCK_META_SIZE),
                                                         total_data_size_(0L),
                                                         block_num_(0L),
                                                         buf_head_(NULL),
                                                         current_block_(NULL)
    {}

    ObBlockMembuf::~ObBlockMembuf()
    {
      clear();
    }

    int ObBlockMembuf::write(const char* buf,const int64_t size,const char** ret_addr /*=NULL*/)
    {
      int ret = OB_SUCCESS;

      if ((NULL == buf) || (size <= 0))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"invalid argument");
      }

      if (OB_SUCCESS == ret)
      {
        if (((NULL == current_block_) && (NULL == buf_head_)) ||
            ((block_data_size_ - current_block_->data_size_) < size))
        {
          if ((ret = alloc_block()) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"alloc block failed");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        char* tmp = current_block_->buf_ + current_block_->data_size_;
        memcpy(tmp,buf,size);
        current_block_->data_size_ += size;
        total_data_size_ += size;

        if (ret_addr != NULL)
        {
          *ret_addr = tmp;
        }
      }
      
      return ret;
    }

    const char* ObBlockMembuf::read(const int64_t offset,const int64_t size) const
    {
      int ret = OB_SUCCESS;
      const char* buf = NULL;
      if (offset < 0 || size <= 0 || ((offset + size) > total_data_size_))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"invalid argument,offset(%ld),size(%ld)",offset,size);
      }

      if (OB_SUCCESS == ret)
      {
        BlockMem* tmp   = buf_head_;
        BlockMem* block = buf_head_;
        int64_t block_offset   = 0;
        while(tmp != NULL)
        {
          if((block_offset + tmp->data_size_) > offset)
          {
            break;
          }
          else
          {
            block = tmp;
            block_offset += tmp->data_size_;
            tmp   = tmp->next_;
          }
        }

        if (block != NULL)
        {
          buf = block->buf_ + (offset - block_offset);
        }
      }
      return buf;
    }

    int ObBlockMembuf::dump(ObFileAppender& filesys,int64_t& size)
    {
      int       ret   = OB_SUCCESS;
      BlockMem* block = buf_head_;
      while(block != NULL)
      {
        if ((ret = filesys.append(block->buf_,block->data_size_,false)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"write buf to file failed,ret = %d",ret);
        }
        block = block->next_;
      }

      if (OB_SUCCESS == ret)
      {
        size = total_data_size_;
      }
      return ret;
    }
    
    int64_t ObBlockMembuf::get_data_size() const
    {
      return total_data_size_;
    }

    const char* ObBlockMembuf::get_current_pos() const
    {
      const char* buf = NULL;
      if (current_block_ != NULL)
      {
        buf = current_block_->buf_ + current_block_->data_size_;
      }
      return buf;
    }

    int ObBlockMembuf::clear()
    {
      BlockMem* block = NULL;
      while(buf_head_ != NULL)
      {
        block = buf_head_->next_;
        ob_free(buf_head_);
        buf_head_ = block;
      }
      current_block_ = NULL;
      buf_head_      = NULL;
      block_num_     = 0;
      total_data_size_ = 0;
      return OB_SUCCESS;
    }

    int ObBlockMembuf::alloc_block()
    {
      int ret = OB_SUCCESS;

      char* tmp = static_cast<char*>(ob_malloc(block_size_,mod_id_));

      if (NULL == tmp)
      {
        TBSYS_LOG(ERROR,"alloc mem failed");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        BlockMem* block   = reinterpret_cast<BlockMem*>(tmp);
        block->next_      = NULL;
        block->data_size_ = 0;
        
        if ((NULL == buf_head_) && (NULL == current_block_))
        {
          buf_head_       = block;
          current_block_  = block;
        }
        else
        {
          current_block_->next_ = block;
          current_block_        = block;
        }
        ++block_num_;
      }
      return ret;
    }
  }
}
