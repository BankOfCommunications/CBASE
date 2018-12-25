#include "ob_sstable_buffer.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    ObSSTableBuffer::ObSSTableBuffer(const int64_t block_size/*=DEFAULT_MEM_BLOCK_SIZE*/)
      : block_size_(block_size),
        size_(0),
        length_(0)
    {
    }

    ObSSTableBuffer::~ObSSTableBuffer()
    {
      clear();
    }

    void ObSSTableBuffer::reset()
    {//只留下一个block
      ObList<MemBlock*>::iterator start_iter = mem_block_list_.begin();
      ObList<MemBlock*>::iterator end_iter = mem_block_list_.end();
      ObList<MemBlock*>::iterator cur_iter = start_iter;
      ObList<MemBlock*>::iterator tmp_iter;

      //跳过第一个block
      cur_iter ++;
      //释放block
      while (cur_iter != end_iter)
      {
        free_mem(*cur_iter);
        tmp_iter = cur_iter;
        cur_iter ++;
        mem_block_list_.erase(tmp_iter);
      }

      start_iter = mem_block_list_.begin();
      end_iter = mem_block_list_.end();
      if (start_iter != end_iter)
      {//有一个block
        cur_iter = start_iter;
        (*cur_iter)->cur_pos_ = 0;
        (*cur_iter)->block_size_ = block_size_ - sizeof(MemBlock);
        size_ = block_size_;
        length_ = 0;
      }
      else
      {//没有block
        size_ = 0;
        length_ = 0;
      }
    }

    void ObSSTableBuffer::clear()
    {
      ObList<MemBlock*>::iterator cur_iter = mem_block_list_.begin();
      ObList<MemBlock*>::iterator end_iter = mem_block_list_.end();
      ObList<MemBlock*>::iterator tmp_iter = cur_iter;

      while (cur_iter != end_iter)
      {
        free_mem(*cur_iter);
        tmp_iter = cur_iter;
        cur_iter ++;
        mem_block_list_.erase(tmp_iter);
      }

      size_ = 0;
      length_ = 0;
    }

    int ObSSTableBuffer::add_item(const void* buf, const int64_t length)
    {
      int ret = OB_SUCCESS;

      ObList<MemBlock*>::iterator end_iter = mem_block_list_.end();
      ObList<MemBlock*>::iterator last_iter = end_iter;

      if (0 >= length)
      {
        TBSYS_LOG(WARN, "invalid argument:item size=%ld", length);
        ret = OB_ERROR;
      }
      else
      {
        end_iter = mem_block_list_.end();
        last_iter = end_iter;
        last_iter --;
        if (last_iter == end_iter
            || ((last_iter != end_iter)
              && ((*last_iter)->block_size_ - (*last_iter)->cur_pos_ < length)))
        {
          if (OB_SUCCESS != (ret = alloc_block()))
          {
            TBSYS_LOG(WARN, "failed to alloc mem block ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          end_iter = mem_block_list_.end();
          last_iter = end_iter;
          last_iter --;
          if (last_iter == end_iter
              || ((last_iter != end_iter)
                && ((*last_iter)->block_size_ - (*last_iter)->cur_pos_ < length)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "data is larger than the block size ret=%d", ret);
          }
          else
          {
            memcpy((*last_iter)->data_ + (*last_iter)->cur_pos_, buf, length);
            (*last_iter)->cur_pos_ += length;
            length_ += length;
          }
        }
      }

      return ret;
    }

    int ObSSTableBuffer::get_data(char* const buf, const int64_t buf_size, 
        int64_t& length) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ObList<MemBlock*>::const_iterator cur_iter = mem_block_list_.begin();
      ObList<MemBlock*>::const_iterator end_iter = mem_block_list_.end();

      if (NULL == buf)
      {
        TBSYS_LOG(WARN, "invalid argument");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (length_ > buf_size)
      {
        TBSYS_LOG(WARN, "buf is no enought:length_=%ld,buf_size=%ld",
            length_, buf_size);
        ret = OB_ERROR;
      }
      else
      {
        while(cur_iter != end_iter)
        {
          memcpy(buf + pos, (*cur_iter)->data_, (*cur_iter)->cur_pos_);
          pos += (*cur_iter)->cur_pos_;
          cur_iter ++;
        }

        length = length_;
      }

      return ret;
    }

    int ObSSTableBuffer::get_data(const char*& buf, int64_t& length) const
    {
      int ret = OB_SUCCESS;

      ObList<MemBlock*>::const_iterator cur_iter = mem_block_list_.begin();

      if (1 != get_block_count())
      {
        TBSYS_LOG(WARN, "the block count must be 1");
        ret = OB_ERROR;
      }
      else
      {
        buf = (*cur_iter)->data_;
        length = get_length();
      }
      
      return ret;
    }

    int ObSSTableBuffer::get_free_buf(char*&buf, int64_t& size)
    {
      int ret = OB_SUCCESS;
      
      ObList<MemBlock*>::const_iterator end_iter = mem_block_list_.end();

      if (mem_block_list_.empty())
      {
        if (OB_SUCCESS != (ret = alloc_block()))
        {
          TBSYS_LOG(ERROR, "alloc block error:ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObList<MemBlock*>::const_iterator last_iter = end_iter;
        last_iter --;
        buf = (*last_iter)->data_ + (*last_iter)->cur_pos_;
        size = block_size_ - (*last_iter)->cur_pos_ - sizeof(MemBlock);
      }

      return ret;
    }

    int ObSSTableBuffer::set_write_size(const int64_t length)
    {
      int ret = OB_SUCCESS;

      ObList<MemBlock*>::iterator end_iter = mem_block_list_.end();
      ObList<MemBlock*>::iterator last_iter = end_iter;
      last_iter --;

      (*last_iter)->cur_pos_ += length;
      length_ += length;

      return ret;
    }


    /*
    int64_t ObSSTableBuffer::get_block_count() const
    {
      ObList<MemBlock*>::const_iterator cur_iter = mem_block_list_.begin();
      ObList<MemBlock*>::const_iterator end_iter = mem_block_list_.end();

      int64_t i = 0;
      while (cur_iter != end_iter)
      {
        i ++;
        cur_iter ++;
      }

      return i;
    }
    */

    int ObSSTableBuffer::alloc_block()
    {
      int ret = OB_SUCCESS;

      void* buf = NULL;
      MemBlock* new_block = NULL;

      if (OB_SUCCESS != (ret = alloc_mem(buf, block_size_)))
      {
        TBSYS_LOG(WARN, "alloc mem error:ret=%d,buf=%p,block_size_=%ld",
            ret, buf, block_size_);
      }
      else
      {
        new_block = reinterpret_cast<MemBlock*>(buf);
        new_block->cur_pos_ = 0;
        new_block->block_size_ = block_size_ - sizeof(MemBlock);
        mem_block_list_.push_back(new_block);
      }
     
      return ret;
    }

    int ObSSTableBuffer::alloc_mem(void*& buf, const int64_t size)
    {
      int ret = OB_SUCCESS;
      
      if (0 >= size)
      {
        TBSYS_LOG(WARN, "invalid argument size=%ld", size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (NULL == (buf = ob_malloc(size, ObModIds::OB_SSTABLE_WRITER)))
        {
          TBSYS_LOG(WARN, "failed to alloc memory, size=%ld", size);
          ret = OB_ERROR;
        }
        else
        {
          size_ += size;
        }
      }
      
      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
