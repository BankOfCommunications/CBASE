/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_reader.cpp is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#include "ob_sstable_block_reader.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/ob_tsi_factory.h"
#include "common/page_arena.h"
#include "common/ob_rowkey.h"
#include "common/ob_schema.h"
#include "common/utility.h"
#include "common/ob_row.h"
#include "common/ob_common_stat.h"
#include "ob_sstable_trailer.h" 
#include "ob_sstable_schema.h" 
#include "ob_scan_column_indexes.h" 

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

namespace oceanbase 
{ 
  namespace sstable 
  {
    ObSSTableBlockReader::ObSSTableBlockReader()
      : index_begin_(NULL), index_end_(NULL),
        data_begin_(NULL), data_end_(NULL)
    {
      memset(&header_, 0, sizeof(header_));
    }

    ObSSTableBlockReader::~ObSSTableBlockReader()
    {
      reset();
    }

    int ObSSTableBlockReader::reset()
    {
      index_begin_ = NULL;
      index_end_ = NULL;
      data_begin_ = NULL;
      data_end_ = NULL;
      return OB_SUCCESS;
    }

    int ObSSTableBlockReader::deserialize_sstable_rowkey(const char* buf, 
      const int64_t data_len, ObString& key)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int16_t key_length = 0;
      int64_t rowkey_stream_len = data_len - sizeof(int16_t);

      if ((NULL == buf) || (data_len <= 0))
      {
        TBSYS_LOG(ERROR, "invalid argument, data_len=%ld", data_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = 
            serialization::decode_i16(buf, data_len, pos, &key_length)))
      {
        TBSYS_LOG(ERROR, "decode key length error, "
            "data_len=%ld, pos=%ld, key_length=%d",
            data_len, pos, key_length);
      }
      else if ((key_length <= 0) || (key_length > rowkey_stream_len))
      {
        TBSYS_LOG(ERROR, "error parsing rowkey, key_length(%d) <= 0 "
            "or key_length(%d) > data_len(%ld)\n", 
            key_length, key_length, rowkey_stream_len);
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        key.assign_ptr(const_cast<char*>(buf + sizeof(int16_t)), key_length); 
      }
      return ret;
    }

    int ObSSTableBlockReader::deserialize(const BlockDataDesc& desc, const BlockData& data)
    {
      int ret = OB_SUCCESS; 
      const char* base_ptr = data.data_buffer_;
      int64_t pos = 0;

      if (!data.available() || !desc.available())
      {
        TBSYS_LOG(ERROR, "invalid argument, input block data, desc error.");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = header_.deserialize(data.data_buffer_, data.data_bufsiz_, pos))) 
      {
        TBSYS_LOG(ERROR, "deserialize header error, databuf=%p,bufsiz=%ld", 
            data.data_buffer_, data.data_bufsiz_);
      }
      else
      {
        block_data_desc_ = desc;

        data_begin_  = base_ptr;
        data_end_    = (base_ptr + header_.row_index_array_offset_);

        int64_t index_entry_length = sizeof(IndexEntryType) * (header_.row_count_ + 1);
        char* internal_buffer_ptr = data.internal_buffer_;

        // if fixed length buffer not enough, reallocate memory for use;
        // in case some block size larger than normal block.
        if (index_entry_length > data.internal_bufsiz_)
        {
          TBSYS_LOG(WARN, "block need size = %ld > input internal bufsiz=%ld, row count=%d, realloc", 
              index_entry_length, data.internal_bufsiz_, header_.row_count_);
          internal_buffer_ptr = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1)->alloc_aligned(index_entry_length);
        }

        if (NULL != internal_buffer_ptr)
        {
          iterator index_entry_ptr = reinterpret_cast<iterator>(internal_buffer_ptr);
          index_begin_ = index_entry_ptr;
          index_end_ = index_begin_ + header_.row_count_ + 1;

          // okay, start parse index entry(row start offset relative to payload buffer)
          pos +=  header_.row_index_array_offset_ - sizeof(header_);
          int32_t offset = 0;
          iterator index = index_entry_ptr;
          for (int i = 0; i < (header_.row_count_ + 1) && OB_SUCCESS == ret; ++i)
          {
            ret = decode_i32(data.data_buffer_, data.data_bufsiz_, pos, &offset); 
            if ((OB_SUCCESS == ret) && ((index_begin_ + i) < index_end_))
            {
              index[i].offset_ = offset;
              if (i > 0)
              {
                index[i - 1].size_ = offset - index[i - 1].offset_;
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "analysis of the index data failed,"
                  "i=%d, ret=%d, row count=%d\n", i, ret, header_.row_count_);
              ret = OB_DESERIALIZE_ERROR;
            }
          } 


          --index_end_; // last index entry not used.

        }
        else
        {
          TBSYS_LOG(ERROR, "block internal buffer allocate error,sz=%ld", index_entry_length);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
      return ret;
    }

    inline bool ObSSTableBlockReader::Compare::operator()
      (const IndexEntryType& index, const common::ObRowkey& key)
    {
      ObRowkey row_key;
      reader_.get_row_key(&index, row_key);
      return row_key.compare(key) < 0;
    }

    ObSSTableBlockReader::const_iterator ObSSTableBlockReader::lower_bound(const common::ObRowkey& key)
    {
      return std::lower_bound(index_begin_, index_end_, key, Compare(*this));
    }

    ObSSTableBlockReader::const_iterator ObSSTableBlockReader::find(const common::ObRowkey& key)
    {
      ObSSTableBlockReader::const_iterator iter = end();
      ObRowkey row_key;

      iter = std::lower_bound(index_begin_, index_end_, key, Compare(*this));
      if (end() != iter)
      {
        if (OB_SUCCESS != get_row_key(iter, row_key))
        {
          iter = end();
        }
        else
        {
          if (row_key != key)
          {
            iter = end();
          }
        }
      }

      return iter;
    }

    int ObSSTableBlockReader::get_row_key(const_iterator index, common::ObRowkey& key) const
    {
      int64_t pos = 0;  
      return get_row_key(index, key, pos);
    }

    int ObSSTableBlockReader::get_row_key(const_iterator index, common::ObRowkey& key, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(!block_data_desc_.available()))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        pos = 0;
        int64_t rowkey_count = block_data_desc_.rowkey_column_count_;
        if (rowkey_count == 0)
        {
          rowkey_count = block_data_desc_.rowkey_info_->get_size();
          // old version sstable store binary rowkey
          ObString binary_rowkey;
          if (OB_SUCCESS != ( ret = deserialize_sstable_rowkey(
                  data_begin_ + index->offset_, index->size_, binary_rowkey)))
          {
            TBSYS_LOG(ERROR, "deserialize binary rowkey error, offset=%d, size=%d",
                index->offset_, index->size_);
          }
          else if (OB_SUCCESS != (ret = ObRowkeyHelper::binary_rowkey_to_obj_array(
                  *block_data_desc_.rowkey_info_, binary_rowkey, rowkey_buf_array_, rowkey_count)))
          {
            TBSYS_LOG(ERROR, "cannot translate binary rowkey into ObObj array.");
          }
          else
          {
            pos = sizeof(int16_t) + binary_rowkey.length();
          }
        }
        else
        {
          int64_t count = rowkey_count;
          ret = get_objs(OB_SSTABLE_STORE_DENSE, index, pos, NULL, rowkey_buf_array_, count);
          if (OB_SUCCESS != ret || count != rowkey_count)
          {
            TBSYS_LOG(ERROR, "cannot get rowkey columns ret =%d, count=%ld, rowkey_count=%ld",
                ret , count, rowkey_count);
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          key.assign(rowkey_buf_array_, rowkey_count);
        }
      }
      return ret;
    }

    int ObSSTableBlockReader::get_row(
        const RowFormat format, const_iterator index,
        common::ObRowkey& key, common::ObObj* ids, 
        common::ObObj* values, int64_t& column_count, bool is_scan /*=true*/) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      if (OB_SUCCESS != (ret = get_row_key(index, key, pos)))
      {
        TBSYS_LOG(ERROR, "get row key error, ret=%d, format=%d, index=%d,%d", 
            ret, format, index->offset_, index->size_);
      }
      else
      {
        ret = get_objs(format, index, pos, ids, values, column_count);
      }
#ifndef _SSTABLE_NO_STAT_
      int32_t stat_index = is_scan ? INDEX_SSTABLE_SCAN_ROWS : INDEX_SSTABLE_GET_ROWS;
      OB_STAT_TABLE_INC(SSTABLE, header_.table_id_, stat_index, 1);
#endif
      return ret;
    }

    int ObSSTableBlockReader::get_row(
        const RowFormat format, const_iterator index,
        const bool is_full_row_columns,
        const ObSimpleColumnIndexes& query_columns, 
        common::ObRowkey& key, common::ObRow& value) const
    {
      int ret = OB_SUCCESS;
      if (format == OB_SSTABLE_STORE_SPARSE)
      {
        ret = get_sparse_row(index, query_columns, key, value);
      }
      else
      {
        if (is_full_row_columns)
        {
          ret = get_dense_full_row(index, key, value);
        }
        else
        {
          ret = get_dense_row(index, query_columns, key, value);
        }
      }
#ifndef _SSTABLE_NO_STAT_
      OB_STAT_TABLE_INC(SSTABLE, header_.table_id_, INDEX_SSTABLE_SCAN_ROWS, 1);
#endif
      return ret;
    }

    int ObSSTableBlockReader::get_dense_full_row(const_iterator index,
        common::ObRowkey& key, common::ObRow& value) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t result_index = 0;

      const char* row_start = data_begin_ + index->offset_;
      const char* row_end = row_start + index->size_;
      ObObj obj;
      ObObj *first_obj = NULL;

      int64_t rowkey_count = block_data_desc_.rowkey_column_count_;
      if (rowkey_count == 0)
      {
        // binary rowkey;
        if (OB_SUCCESS != (ret = get_row_key(index, key, pos)))
        {
          TBSYS_LOG(WARN, "get_binary_rowkey error, pos=%ld, ret=%d", pos, ret);
        }
        else
        {
          rowkey_count = block_data_desc_.rowkey_info_->get_size();
          while (result_index < rowkey_count)
          {
            value.raw_set_cell(result_index, *(key.get_obj_ptr() + result_index));
            ++result_index;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        // traverse rowvalue;
        while (OB_SUCCESS == ret && row_start + pos < row_end)
        {

          if (OB_SUCCESS != (ret = obj.deserialize(row_start, index->size_, pos)))
          {
            TBSYS_LOG(ERROR, "deserialize column value object error, ret=%d, "
                "result_index=%ld, row_start=%p, pos=%ld, size=%d",
                ret, result_index, row_start, pos, index->size_);
            break;
          }
          else if (OB_SUCCESS != (ret = value.raw_set_cell(result_index, obj)))
          {
            TBSYS_LOG(ERROR, "cannot get obj at cell:%ld", result_index);
          }
          else
          {
            // dense row, rowkey assign to ObRawRow::objs_ array directly. 
            if (result_index == 0) 
            { 
              value.raw_get_cell_for_update(result_index, first_obj);
              key.assign(first_obj, rowkey_count); 
            }
            ++result_index;
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockReader::get_dense_row(const_iterator index,
        const ObSimpleColumnIndexes& query_columns, 
        common::ObRowkey& key, common::ObRow& value) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t value_index = 0;
      int64_t filled = 0;
      int64_t result_index = 0;
      int64_t not_null_col_num = query_columns.get_not_null_column_count();

      const char* row_start = data_begin_ + index->offset_;
      const char* row_end = row_start + index->size_;
      ObObj obj ;

      if (OB_SUCCESS != (ret = get_row_key(index, key, pos)))
      {
        TBSYS_LOG(ERROR, "get row key error, ret=%d, index=%d,%d", 
            ret, index->offset_, index->size_);
      }

      // traverse rowkey first;
      while (OB_SUCCESS == ret && value_index < key.get_obj_cnt() 
          && filled < not_null_col_num)
      {
        if ((result_index = query_columns.find_by_offset(value_index)) >= 0)
        {
          value.raw_set_cell(result_index, *(key.get_obj_ptr() + value_index));
          ++filled;
        }
        ++value_index;
      }

      // traverse rowvalue;
      while (OB_SUCCESS == ret && row_start + pos < row_end
          && filled < not_null_col_num)
      {
        if (OB_SUCCESS != (ret = obj.deserialize(row_start, index->size_, pos)))
        {
          TBSYS_LOG(ERROR, "deserialize column value object error, ret=%d, "
              "object_index=%ld, row_start=%p, pos=%ld, size=%d",
              ret, value_index, row_start, pos, index->size_);
          break;
        }
        else if ((result_index = query_columns.find_by_offset(value_index)) < 0)
        {
          // this column not in query columns, ignore;
        }
        else if (OB_SUCCESS != (ret = value.raw_set_cell(result_index, obj)))
        {
          TBSYS_LOG(ERROR, "cannot get obj at cell:%ld", result_index);
        }
        else 
        {
          ++filled;
        }
        ++value_index;
      }

      return ret;

    }

    int ObSSTableBlockReader::get_sparse_row(const_iterator index,
        const ObSimpleColumnIndexes& query_columns, 
        common::ObRowkey& key, common::ObRow& value) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t column_id = 0;
      int64_t filled = 0;
      int64_t result_index = 0;
      int64_t value_index = 0;
      int64_t not_null_col_num = query_columns.get_not_null_column_count();
      uint64_t table_id = query_columns.get_table_id();
      bool  has_delete_row = false;

      const char* row_start = data_begin_ + index->offset_;
      const char* row_end = row_start + index->size_;
      const ObRowDesc * row_desc = value.get_row_desc();
      ObObj obj;

      if (OB_SUCCESS != (ret = get_row_key(index, key, pos)))
      {
        TBSYS_LOG(ERROR, "get row key error, ret=%d, index=%d,%d", 
            ret, index->offset_, index->size_);
      }

      // traverse rowkey in case ;
      while (OB_SUCCESS == ret && value_index < key.get_obj_cnt() 
          && filled < not_null_col_num)
      {
        if ((result_index = query_columns.find_by_offset(value_index)) >= 0)
        {
          value.raw_set_cell(result_index, *(key.get_obj_ptr() + value_index));
          ++filled;
        }
        ++value_index;
      }

      value_index = 0;

      // traverse rowvalue;
      while (OB_SUCCESS == ret && row_start + pos < row_end
          && filled < not_null_col_num)
      {
        if (OB_SUCCESS != (ret = obj.deserialize(row_start, index->size_, pos)))
        {
          TBSYS_LOG(ERROR, "deserialize column value object error, ret=%d, "
              "object_index=%ld, row_start=%p, pos=%ld, size=%d",
              ret, value_index, row_start, pos, index->size_);
        }
        else if (obj.get_type() != ObIntType || OB_SUCCESS != (ret = obj.get_int(column_id)))
        {
          TBSYS_LOG(ERROR, "sparse format, obj(%s) not column_id", to_cstring(obj));
          ret = OB_UNKNOWN_OBJ;
        }
        else if (OB_SUCCESS != (ret = obj.deserialize(row_start, index->size_, pos)))
        {
          TBSYS_LOG(ERROR, "deserialize column value object error, ret=%d, "
              "object_index=%ld, row_start=%p, pos=%ld, size=%d",
              ret, value_index, row_start, pos, index->size_);
        }
        else if (value_index == 0 && static_cast<uint64_t>(column_id) == OB_ACTION_FLAG_COLUMN_ID)
        {
          // delete row? store first
          has_delete_row = true;
        }
        else if (OB_INVALID_INDEX == (result_index = row_desc->get_idx(table_id, column_id)))
        {
          // do nothing when user do ask it
        }
        else if (OB_SUCCESS != (ret = value.raw_set_cell(result_index, obj)))
        {
        }
        else
        {
          ++filled;
        }
        ++value_index;
      }

      // set sparse row action flag column;
      if (OB_SUCCESS == ret)
      {
        // normal row
        obj.set_ext(ObActionFlag::OP_VALID);
        // delete row
        if (has_delete_row)
        {
          // delete row without any columns;
          obj.set_ext(ObActionFlag::OP_DEL_ROW);
          // delete row with new columns;
          if (value_index > 1)
          {
            obj.set_ext(ObActionFlag::OP_NEW_ADD);
          }
        }
        value.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj);
      }


      return ret;

    }

    int ObSSTableBlockReader::get_objs(const RowFormat format, 
        const_iterator index, int64_t & pos,
        common::ObObj* ids, common::ObObj* values, int64_t& count) const
    {
      int ret = OB_SUCCESS;
      int64_t object_index = 0;
      const char* row_start = data_begin_ + index->offset_;
      const char* row_end = row_start + index->size_;
      int64_t size = row_end - row_start;

      if (OB_UNLIKELY(NULL == row_start || NULL == row_end  
          || NULL == values || count <= 0))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_UNLIKELY(OB_SSTABLE_STORE_SPARSE == format && NULL == ids))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        while (row_start + pos < row_end && object_index < count)
        {
          if (format == OB_SSTABLE_STORE_SPARSE)
          {
            ret = (ids + object_index)->deserialize(row_start, size, pos);
            if (OB_SUCCESS != ret || ids[object_index].get_type() != ObIntType)
            {
              ret = OB_DESERIALIZE_ERROR;
            }
          }

          if (OB_SUCCESS != ret) 
          {
            TBSYS_LOG(ERROR, "deserialize id object error, ret=%d, "
                "object_index=%ld, row_start=%p, pos=%ld, size=%ld",
                ret, object_index, row_start, pos, size);
            break;
          }
          else if (OB_SUCCESS != (ret = 
                (values + object_index)->deserialize(row_start, size, pos)) )
          {
            TBSYS_LOG(ERROR, "deserialize column value object error, ret=%d, "
                "object_index=%ld, row_start=%p, pos=%ld, size=%ld",
                ret, object_index, row_start, pos, size);
            break;
          }
          else
          { 
            ++object_index; 
          }
        }
        if (OB_SUCCESS == ret) count = object_index;
      }

      return ret;
    }

    int ObSSTableBlockReader::get_cache_row_value(const_iterator index, 
      ObSSTableRowCacheValue& row_value) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ObRowkey rowkey;

      if (NULL == index || NULL == data_begin_)
      {
        TBSYS_LOG(WARN, "invalid param, row_index=%p, row_begin=%p",
          index, data_begin_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_row_key(index, rowkey, pos)))
      {
        TBSYS_LOG(ERROR, "erase key part error, ret=%d, "
            "pos=%ld, index->offset_=%d, index->size_=%d", 
            ret, pos, index->offset_, index->size_);
      }
      else if (pos > index->size_)
      {
        TBSYS_LOG(ERROR, "erase key part error, "
            "pos=%ld, index->offset_=%d, index->size_=%d, rowkey=%s", 
            pos, index->offset_, index->size_, to_cstring(rowkey));
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        row_value.buf_ = const_cast<char*>(data_begin_ + index->offset_ + pos);
        row_value.size_ = index->size_ - pos;
      }

      return ret;
    }
  } // end namespace sstable
} // end namespace oceanbase
