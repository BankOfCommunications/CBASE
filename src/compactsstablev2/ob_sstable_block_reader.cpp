#include "ob_sstable_block_reader.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockReader::init(const BlockData& data, 
        const common::ObCompactStoreType& row_store_type)
    {
      int ret = OB_SUCCESS;

      if (!data.available())
      {
        TBSYS_LOG(WARN, "invalid BlockData:data.internal_buf_=%p," \
            "data.internal_buf_size_=%ld,data.data_buf_=%p" \
            "data.data_buf_size_=%ld", data.internal_buf_, 
            data.internal_buf_size_, data.data_buf_, data.data_buf_size_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (DENSE_SPARSE != row_store_type
          && DENSE_DENSE != row_store_type)
      {
        TBSYS_LOG(WARN, "invalid row_store_type:row_store_type=%d",
            row_store_type);
      }
      else
      {
        row_store_type_ = row_store_type;

        //block header
        memcpy(&block_header_, data.data_buf_, BLOCK_HEADER_SIZE);

        //block data
        data_begin_ = data.data_buf_;
        data_end_ = data.data_buf_ + block_header_.row_index_offset_;

        //block row index
        char* internal_buf_ptr = data.internal_buf_;
        int64_t index_item_length = INTERNAL_ROW_INDEX_ITEM_SIZE 
          * (block_header_.row_count_ + 1);

        //传过来的row index预留空间不够
        if (index_item_length > data.internal_buf_size_)
        {
          ModuleArena* arena = GET_TSI_MULT(ModuleArena,
              TSI_COMPACTSSTABLEV2_MODULE_ARENA_1);
          if (arena == NULL)
          {
            TBSYS_LOG(WARN, "GET_TSI_MULT error:ModuleArena, " \
                "TSI_COMPACTSSTABLEV2_MODULE_ARENA_1");
          }
          else
          {
            internal_buf_ptr = arena->alloc_aligned(index_item_length);
            if (NULL == internal_buf_ptr)
            {
              TBSYS_LOG(ERROR, "failed to alloc memrory for " \
                  "internal_buf_ptr:index_item_length=%ld", 
                  index_item_length);
              ret = OB_ALLOCATE_MEMORY_FAILED;
            }
          }
        }

        if (NULL != internal_buf_ptr)
        {
          //row index
          char* row_index = const_cast<char*>(data_end_);
          iterator index_ptr = reinterpret_cast<iterator>(internal_buf_ptr);
          index_begin_ = index_ptr;
          index_end_ = index_begin_ + block_header_.row_count_ + 1;

          int32_t offset = 0;
          int32_t pos = 0;

          //row offset--->row offset, row size
          for (int i = 0; i < (block_header_.row_count_ + 1); i ++)
          {
            memcpy(&offset, row_index + pos, ROW_INDEX_ITEM_SIZE);
            pos = pos + ROW_INDEX_ITEM_SIZE;

            index_ptr[i].offset_ = offset;
            if (i > 0)
            {
              index_ptr[i - 1].size_ = offset - index_ptr[i - 1].offset_;
            }
          }
          index_end_ --;
        }
      }

      return ret;
    }
  
    int ObSSTableBlockReader::get_row(const_iterator index, 
        common::ObCompactCellIterator& row) const
    {
      int ret = OB_SUCCESS;

      const char* row_buf = find_row(index);
      if (NULL == row_buf)
      {
        ret = OB_SEARCH_NOT_FOUND;
      }
      else if (OB_SUCCESS != (ret = row.init(row_buf, row_store_type_)))
      {
        TBSYS_LOG(WARN, "row init error:ret=%d, row_buf=%p,row_store_type_=%d",
            ret, row_buf, row_store_type_);
      }

      return ret;
    }

    int ObSSTableBlockReader::get_row_key(const_iterator index, 
        common::ObRowkey& key) const
    {
      int ret = OB_SUCCESS;
      ObCompactCellIterator row;
      
      if (OB_SUCCESS != (ret = get_row(index, row)))
      {
        TBSYS_LOG(WARN, "get row error:ret=%d,index.offset_=%d," \
            "index.size_=%d", ret, index->offset_, index->size_);
      }
      else if (OB_SUCCESS != (ret = get_row_key(row, key)))
      {
        TBSYS_LOG(WARN, "ger row key error:ret=%d", ret); 
      }
      else
      {
        //do nothing
      }

      return ret;
    }

    int ObSSTableBlockReader::get_cache_row_value(const_iterator index,
          sstable::ObSSTableRowCacheValue& row_value) const
    {
      int ret = OB_SUCCESS;
      ObRowkey rowkey;

      if (NULL == index)
      {
        TBSYS_LOG(WARN, "argument error:NULL==index");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == data_begin_)
      {
        TBSYS_LOG(WARN, "context error:NULL==data_begin_");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = get_row_key(index, rowkey)))
      {
        TBSYS_LOG(WARN, "get row key error:ret=%d,index.offset_=%d" \
            "index.size_=%d", ret, index->offset_, index->size_);
      }
      else
      {
        row_value.buf_ = const_cast<char*>(data_begin_ + index->offset_);
        row_value.size_ = index->size_;
      }

      return ret;
    }
    
    int ObSSTableBlockReader::get_row_key(ObCompactCellIterator& row, 
        ObRowkey& key) const
    {
      int ret = OB_SUCCESS;
      int rowkey_obj_count = 0;
      bool is_row_finished = false;
      const ObObj* obj = NULL;
      
      do
      {
        if (OB_SUCCESS != (ret = row.next_cell()))
        {
          TBSYS_LOG(WARN, "row next cell error:ret=%d", ret);
          break;
        }
        else
        {
          if (DENSE_SPARSE == row_store_type_ || DENSE_DENSE == row_store_type_)
          {
            if (OB_SUCCESS != (ret = row.get_cell(obj, &is_row_finished)))
            {
              TBSYS_LOG(WARN, "row get cell error:ret=%d", ret);
            }
            else
            {
              rowkey_buf_array_[rowkey_obj_count] = *obj;
              if (is_row_finished)
              {//rowkey行结束
                break;
              }
              else
              {
                rowkey_obj_count ++;
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "row store type error:row_store_type_=%d",
                row_store_type_);
            ret = OB_ERROR;
            break;
          }
        }
      }while(1);

      if (OB_SUCCESS == ret)
      {
        key.assign(rowkey_buf_array_, rowkey_obj_count);
      }

      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
