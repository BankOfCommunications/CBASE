#include "ob_sstable_block_builder.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockBuilder::reset()
    {
      int ret = OB_SUCCESS;

      block_header_.reset();

      if (NULL == row_buf_)
      {
        if (OB_SUCCESS != (ret = alloc_mem(row_buf_, BLOCK_ROW_BUFFER_SIZE)))
        {
          TBSYS_LOG(ERROR, "faild to alloc memory:ret=%d,row_buf_=%p," \
              "buffer_size=%ld", ret, row_buf_, BLOCK_ROW_BUFFER_SIZE);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          //contain the block header
          row_length_ = SSTABLE_BLOCK_HEADER_SIZE;
          row_buf_size_ = BLOCK_ROW_BUFFER_SIZE;
        }
      }
      else
      {
        row_length_ = SSTABLE_BLOCK_HEADER_SIZE;
        //use the last row_buf_size, don not reset
      }

      if (NULL == row_index_buf_)
      {
        if (OB_SUCCESS != (ret = alloc_mem(row_index_buf_, 
                BLOCK_ROW_INDEX_BUFFER_SIZE)))
        {
          TBSYS_LOG(ERROR, "faild to alloc memory:ret=%d,row_index_buf_=%p," \
              "buffer_size=%ld", ret, row_buf_, BLOCK_ROW_INDEX_BUFFER_SIZE);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          row_index_length_ = 0;
          row_index_buf_size_ = BLOCK_ROW_INDEX_BUFFER_SIZE;
        }
      }
      else
      {
        row_index_length_ = 0;
        //use the last row_buf_size, don not reset
      }

      return ret;
    }

    int ObSSTableBlockBuilder::add_row(const ObRowkey& row_key, 
        const ObRow& row_value)
    {
      int ret = OB_SUCCESS;
      char* cur_row_ptr = NULL;
      int64_t cur_row_offset = 0;
      int64_t row_remain_size = 0;
      ObCompactCellWriter row;

      //append row
      while(true)
      {
        cur_row_ptr = get_cur_row_ptr();
        row_remain_size = get_row_remain();

        if (NULL == cur_row_ptr)
        {
          TBSYS_LOG(WARN, "context error:cur_row_ptr=NULL");
          ret = OB_ERROR;
        }
        if (row_remain_size <= 0)
        {
          TBSYS_LOG(WARN, "context error:row_remain_size=%ld", row_remain_size);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = row.init(cur_row_ptr, row_remain_size,
                row_store_type_)))
        {
          TBSYS_LOG(WARN, "row init error:ret=%d,cur_row_ptr=%p," \
              "row_remain_size=%ld,row_store_type_=%d", 
              ret, cur_row_ptr, row_remain_size, row_store_type_);
        }
        else if (OB_SUCCESS != (ret = row.append_row(row_key, row_value)))
        {
          int64_t new_size = row_buf_size_ * 2;
          char* new_buf = NULL;
          if (OB_SUCCESS != (ret = alloc_mem(new_buf, new_size)))
          {
            TBSYS_LOG(WARN, "alloc mem error:ret=%d,new_buf=%p,new_size=%ld",
                ret, new_buf, new_size);
          }
          else
          {
            memcpy(new_buf, row_buf_, row_length_);
            free_mem(row_buf_);
            row_buf_ = new_buf;
            row_buf_size_ = new_size;
            ret = OB_SUCCESS;
          }
        }
        else
        {
          cur_row_offset = row_length_;
          row_length_ += row.size();
          break;
        }
      }

      //update row index
      if (OB_SUCCESS == ret)
      {
        ObSSTableBlockRowIndex index;
        index.row_offset_ = static_cast<int32_t>(cur_row_offset);
        if (OB_SUCCESS != (ret = add_row_index(index)))
        {
          TBSYS_LOG(WARN, "add row index error:ret=%d, index.row_offset_=%d", 
              ret, index.row_offset_);
        }
      }

      //update block header
      if (OB_SUCCESS == ret)
      {
        block_header_.row_count_ ++;
      }

      return ret;
    }

    int ObSSTableBlockBuilder::add_row(const common::ObRow& row)
    {
      int ret = OB_SUCCESS;
      char* cur_row_ptr = NULL;
      int64_t cur_row_offset = 0;
      int64_t row_remain_size = 0;
      ObCompactCellWriter row_writer;

      //append row
      while(true)
      {
        cur_row_ptr = get_cur_row_ptr();
        row_remain_size = get_row_remain();

        if (NULL == cur_row_ptr)
        {
          TBSYS_LOG(WARN, "context error:cur_row_ptr=NULL");
          ret = OB_ERROR;
        }
        if (row_remain_size <= 0)
        {
          TBSYS_LOG(WARN, "context error:row_remain_size=%ld", row_remain_size);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = row_writer.init(cur_row_ptr, 
                row_remain_size, row_store_type_)))
        {
          TBSYS_LOG(WARN, "row init error:ret=%d,cur_row_ptr=%p," \
              "row_remain_size=%ld,row_store_type_=%d", 
              ret, cur_row_ptr, row_remain_size, row_store_type_);
        }
        else if (OB_SUCCESS != (ret = row_writer.append_row(row)))
        {
          int64_t new_size = row_buf_size_ * 2;
          char* new_buf = NULL;
          if (OB_SUCCESS != (ret = alloc_mem(new_buf, new_size)))
          {
            TBSYS_LOG(WARN, "alloc mem error:ret=%d,new_buf=%p,new_size=%ld",
                ret, new_buf, new_size);
          }
          else
          {
            memcpy(new_buf, row_buf_, row_length_);
            free_mem(row_buf_);
            row_buf_ = new_buf;
            row_buf_size_ = new_size;
            ret = OB_SUCCESS;
          }
        }
        else
        {
          cur_row_offset = row_length_;
          row_length_ += row_writer.size();
          break;
        }
      }

      //update row index
      if (OB_SUCCESS == ret)
      {
        ObSSTableBlockRowIndex index;
        index.row_offset_ = static_cast<int32_t>(cur_row_offset);
        if (OB_SUCCESS != (ret = add_row_index(index)))
        {
          TBSYS_LOG(WARN, "add row index error:ret=%d, index.row_offset_=%d", 
              ret, index.row_offset_);
        }
      }

      //update block header
      if (OB_SUCCESS == ret)
      {
        block_header_.row_count_ ++;
      }

      return ret;
    }

    int ObSSTableBlockBuilder::build_block(char*& buf, int64_t& size)
    {
      int ret = OB_SUCCESS;
      ObSSTableBlockRowIndex index;

      //the last row index
      index.row_offset_ = static_cast<int32_t>(row_length_);
      block_header_.row_index_offset_  = static_cast<int32_t>(row_length_);

      if (OB_SUCCESS != (ret = add_row_index(index)))
      {
        TBSYS_LOG(WARN, "add row index error:ret=%d, index.row_offset_=%d",
            ret, index.row_offset_);
      }
      else if (NULL == row_buf_ || NULL == row_index_buf_)
      {
        TBSYS_LOG(WARN, "row_buf_==NULL or row_index_buf_==NULL:row_buf_=%p" \
            "row_index_buf_=%p", row_buf_, row_index_buf_);
      }
      else
      {
        //block header--->buf
        memcpy(row_buf_, &block_header_, SSTABLE_BLOCK_HEADER_SIZE);

        char* new_buf = NULL;
        int64_t new_size = row_buf_size_;
        bool use_new_buf = false;
        int cnt = 0;
        while (true)
        {
          if (new_size - row_length_ < row_index_length_)
          {
            new_size *= 2;
            use_new_buf = true;
            cnt ++;
            if (10 == cnt)
            {
              TBSYS_LOG(WARN, "error? while cnt is so large");
              ret = OB_ERROR;
            }
          }
          else
          {
            break;
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (use_new_buf)
          {
            if (OB_SUCCESS != (ret = alloc_mem(new_buf, new_size)))
            {
              TBSYS_LOG(ERROR, "alloc mem error:ret=%d,new_buf=%p,new_size=%ld",
                  ret, new_buf, new_size);
            }
            else
            {
              memcpy(new_buf, row_buf_, row_length_);
              free_mem(row_buf_);
              row_buf_ = new_buf;
              row_buf_size_ = new_size;
            }
          }
          else
          {
            char* buf_ptr = get_cur_row_ptr();
            if (NULL == buf_ptr)
            {
              TBSYS_LOG(WARN, "context error:buf_ptr==NULL");
              ret = OB_ERROR;
            }
            else
            {
              memcpy(buf_ptr, row_index_buf_, row_index_length_);
              row_length_ += row_index_length_;
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf = row_buf_;
        size = row_length_;
      }

      return ret;
    }

    int ObSSTableBlockBuilder::add_row_index(
        const ObSSTableBlockRowIndex& row_index)
    {
      int ret = OB_SUCCESS;

      if (NULL == row_buf_ || NULL == row_index_buf_)
      {
        TBSYS_LOG(WARN, "row_buf_==NULL or row_index_buf_==NULL:row_buf_=%p" \
            "row_index_buf_=%p", row_buf_, row_index_buf_);
      }
      else
      {
        char* new_buf = NULL;
        int64_t new_size = row_index_buf_size_;
        bool use_new_buf = false;
        int cnt = 0;

        while (true)
        {
          if (new_size - row_index_length_ < BLOCK_ROW_INDEX_SIZE)
          {
            new_size *= 2;
            use_new_buf = true;
            cnt ++;
            if (10 == cnt)
            {
              TBSYS_LOG(WARN, "error? while cnt is so large");
              ret = OB_ERROR;
              break;
            }
          }
          else
          {
            break;
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (use_new_buf)
          {
            if (OB_SUCCESS != (ret = alloc_mem(new_buf, new_size)))
            {
              TBSYS_LOG(ERROR, "alloc mem error:ret=%d,new_buf=%p,new_size=%ld",
                  ret, new_buf, new_size);
            }
            else
            {
              memcpy(new_buf, row_index_buf_, row_index_length_);
              free_mem(row_index_buf_);
              row_index_buf_ = new_buf;
              row_index_buf_size_ = new_size;
            }
          }
          
          if (OB_SUCCESS == ret)
          {
            char* buf_ptr = get_cur_row_index_ptr();
            if (NULL == buf_ptr)
            {
              TBSYS_LOG(WARN, "context error:buf_ptr==NULL");
              ret = OB_ERROR;
            }
            else
            {
              memcpy(buf_ptr, &row_index, BLOCK_ROW_INDEX_SIZE);
              row_index_length_ += BLOCK_ROW_INDEX_SIZE;
            }
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockBuilder::alloc_mem(char*& buf, const int64_t size)
    {
      int ret = OB_SUCCESS;
      
      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid argument:size=%ld", size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (buf = reinterpret_cast<char*>(
              ob_malloc(size, ObModIds::OB_SSTABLE_WRITER))))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory,size=%ld," \
            "ObModIds::OB_SSTABLE_WRITER", size);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      
      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase

