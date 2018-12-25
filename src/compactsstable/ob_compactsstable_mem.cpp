
/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compactsstable_mem.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#include "tbsys.h"
#include "common/ob_define.h"
#include "common/utility.h"
#include "ob_compactsstable_mem.h"
#include "ob_compact_row.h"

namespace oceanbase
{
  using namespace common;

  namespace compactsstable
  {

    bool is_invalid_version_range(const ObFrozenVersionRange& range)
    {
      int ret = false;
      if ((range.major_version_ < 0) || (range.minor_version_start_ < 0) || (range.minor_version_end_ < 0))
      {
        ret = true;
      }
      return ret;
    }

    ObCompactSSTableMem::ObCompactSSTableMem() : table_id_(OB_INVALID_ID),
                                                 row_count_(0),
                                                 frozen_time_(0),
                                                 rows_(OB_COMPACTSSTABLE_BLOCK_SIZE,ObModIds::OB_COMPACTSSTABLE_WRITER),
                                                 inited_(false)
    {}

    ObCompactSSTableMem::~ObCompactSSTableMem()
    {
      clear();
    }
                                                 
    int ObCompactSSTableMem::init(const ObFrozenVersionRange& version_range,
                                  const int64_t frozen_time)
    {
      int ret = OB_SUCCESS;

      if (is_invalid_version_range(version_range) || (frozen_time < 0))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"invalid sstable argument");
      }
      else
      {
        frozen_time_   = frozen_time;
        version_range_ = version_range;
        inited_        = true;
      }
      return ret;
    }

    int ObCompactSSTableMem::append_row(uint64_t table_id,ObCompactRow& row,int64_t& approx_space_usage)
    {
      int ret = OB_SUCCESS;
      UNUSED(approx_space_usage);

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if ((OB_INVALID_ID == table_id))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"invalid table id");
      }

      if (OB_INVALID_ID == table_id_)
      {
        table_id_ = table_id;
      }
      
      if (table_id_ != table_id)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"only support one table");
      }

      if (OB_SUCCESS == ret)
      {
        const char* pos           = NULL;
        const char* last_row_buf  = NULL;
        int64_t     last_row_size = 0;
        if ((ret = get_last_row(last_row_buf,last_row_size)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"get last row failed");
        }

        if ((last_row_buf != NULL) && (last_row_size > 0))
        {
          ObCompactRow last_row;
          if ((ret = last_row.set_row(last_row_buf,last_row_size)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"set row failed,ret=%d",ret);
          }
          else if (ObCompactRow::compare(last_row,row,ObCompactRow::ROWKEY_COLUMN_NUM) >= 0)
          {
            TBSYS_LOG(WARN,"can not add current_row_ ,last row great than current row");
            hex_dump(last_row.get_key_buffer(),
                     static_cast<int32_t>(last_row.get_key_columns_len(ObCompactRow::ROWKEY_COLUMN_NUM)),
                     true,
                     TBSYS_LOG_LEVEL_WARN);
            hex_dump(row.get_key_buffer(),
                     static_cast<int32_t>(row.get_key_columns_len(ObCompactRow::ROWKEY_COLUMN_NUM)),
                     true,
                     TBSYS_LOG_LEVEL_WARN);
            ret = OB_ERROR;
          }          
        }
        
        if ((OB_SUCCESS == ret) && (ret = rows_.write(row.get_key_buffer(),row.get_row_size(),&pos)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"write row failed, ret = %d",ret);
        }
        //add index
        if ((OB_SUCCESS == ret) && (OB_SUCCESS == (ret = add_index(pos))))
        {
          row_count_ += 1;
        }
      }
      return ret;
    }

    void ObCompactSSTableMem::clear()
    {
      TBSYS_LOG(INFO,"clear compactsstable mem");
      rows_.clear();
      row_count_ = 0;
      frozen_time_ = 0;
      if (row_index_.row_index_ != NULL)
      {
        ob_free(row_index_.row_index_);
        row_index_.offset_ = 0;
        row_index_.max_index_entry_num_ = 0;
        row_index_.row_index_ = NULL;
      }
    }

    int64_t ObCompactSSTableMem::get_data_version() const
    {
      return ObVersion::get_version(version_range_.major_version_,
                                    version_range_.minor_version_end_,
                                    version_range_.is_final_minor_version_ != 0);
    }



    ObCompactSSTableMem::Compare::Compare(ObCompactSSTableMem& mem) : mem_(mem)
    {}

    bool ObCompactSSTableMem::Compare::operator()(const char* index,const common::ObRowkey& rowkey)
    {
      ObCompactRow row;
      ObRowkey left_rowkey;
      int  err = OB_SUCCESS;
      bool ret = false;
      if ((err = row.set_row(index,0)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"invalid row,index:%p",index);
      }
      else if ((err = row.get_row_key(left_rowkey)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"get rowkey of row %p failed,ret=%d",index,err);
      }
      else
      {
        ret = (left_rowkey.compare(rowkey) < 0);
      }

      return ret;
    }

    int ObCompactSSTableMem::locate_start_pos(const ObNewRange& range,const char**& start_pos)
    {
      int ret = OB_SUCCESS;
      if (range.start_key_.is_min_row())
      {
        start_pos = row_index_.start();
      }
      else if(row_index_.end() == (start_pos = lower_bound(row_index_.start(),row_index_.end(),range.start_key_)))
      {
        ret = OB_BEYOND_THE_RANGE;
      }
      else
      {
        ObCompactRow row;
        ObRowkey found_key;
        if (row_index_.end() == start_pos)
        {
          ret = OB_BEYOND_THE_RANGE;
        }
        else if ((ret = row.set_row(*start_pos,0)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"set row failed,row:%p,ret=%d",*start_pos,ret);
        }
        else if ((ret = row.get_row_key(found_key)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"invalid row,row:%p,ret=%d",*start_pos,ret);
        }
        else if (!range.border_flag_.inclusive_start() && (found_key == range.start_key_))
        {
          ++start_pos;
        }
      }
      return ret;
    }

    int ObCompactSSTableMem::locate_end_pos(const ObNewRange& range,const char**& end_pos)
    {
      int ret = OB_SUCCESS;
      if (range.end_key_.is_max_row())
      {
        end_pos = (row_index_.end() - 1);
      }
      else
      {
        if (row_index_.end() == (end_pos = lower_bound(row_index_.start(),row_index_.end(),range.end_key_)))
        {
          --end_pos;
        }
        else
        {
          ObCompactRow row;
          ObRowkey found_key;
          if ((ret = row.set_row(*end_pos,0)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"set row failed,row:%p,ret=%d",*end_pos,ret);
          }
          else if ((ret = row.get_row_key(found_key)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"invalid row,row:%p,ret=%d",*end_pos,ret);
          }
          else if (row_index_.start() == end_pos)
          {
            if (range.end_key_ < found_key ||
                (range.end_key_ == found_key && !range.border_flag_.inclusive_end()))
            {
              ret = OB_BEYOND_THE_RANGE;              
            }
          }
          else if (OB_SUCCESS == ret)
          {
            if (range.end_key_ < found_key)
            {
              --end_pos;
            }
            else if(range.end_key_ == found_key && !range.border_flag_.inclusive_end())
            {
              --end_pos;
            }
          }
        }
      }
      return ret;
    }


    const char** ObCompactSSTableMem::lower_bound(const char** start,const char** end,const ObRowkey& rowkey)
    {
      return std::lower_bound(start,end,rowkey,Compare(*this));
    }

    int ObCompactSSTableMem::find(const ObRowkey& rowkey,const char**& start_iterator)
    {
      int ret = OB_SUCCESS;
      const char** pos = lower_bound(row_index_.start(),row_index_.end(),rowkey);
      if (row_index_.end() == pos)
      {
        //not found
        ret = OB_ITER_END;
      }
      else
      {
        ObCompactRow row;
        ObRowkey found_key;
        if ((ret = row.set_row(*pos,0)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"set row failed,row:%p,ret=%d",*pos,ret);
        }
        else if ((row.get_row_key(found_key) != OB_SUCCESS) || found_key != rowkey)
        {
          pos = row_index_.end();
          ret = OB_ITER_END;
        }
        else
        {
          start_iterator = pos;
        }
      }
      return ret;
    }

    bool ObCompactSSTableMem::is_row_exist(const ObRowkey& rowkey)
    {
      bool ret = false;
      const char** pos = NULL;
      if (OB_SUCCESS == find(rowkey,pos))
      {
        ret = true;
      }
      return ret;
    }

    int ObCompactSSTableMem::add_index(const char* pos)
    {
      int ret = OB_SUCCESS;

      if (NULL == pos)
      {
        TBSYS_LOG(WARN,"pos is null,invalid argument");
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        int64_t size = 0;
        char* tmp    = NULL;
        if (row_index_.max_index_entry_num_ <= row_index_.offset_)
        {
          size = (row_index_.max_index_entry_num_ * INDEX_ENTRY_SIZE) + INDEX_BLOCK_SIZE;
          // the number of tablets in cs maybe very large
          // but a tablet,generally rather small,
          // so we do not use x2
        }

        if (size > 0)
        {
          if (NULL == (tmp = reinterpret_cast<char*>(ob_malloc(size,ObModIds::OB_COMPACTSSTABLE_WRITER))))
          {
            TBSYS_LOG(WARN,"malloc row index failed");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else if (row_index_.max_index_entry_num_ != 0)
          {
            memcpy(tmp,row_index_.row_index_,row_index_.offset_ * INDEX_ENTRY_SIZE);
          }
          row_index_.row_index_ = reinterpret_cast<const char**>(tmp);
          row_index_.max_index_entry_num_ = size / INDEX_ENTRY_SIZE;
        }
      }

      if (OB_SUCCESS == ret)
      {
        *(row_index_.row_index_ + row_index_.offset_) = pos;
        ++row_index_.offset_;
      }
      return ret;
    }

    int ObCompactSSTableMem::get_last_row(const char*& pos,int64_t& size) const
    {
      int ret = OB_SUCCESS;
      if (row_index_.offset_ < 0)
      {
        ret = OB_ERROR;
      }
      else if (0 == row_index_.offset_)
      {
        pos = NULL;
        size = 0;
      }
      else
      {
        pos = row_index_.row_index_[row_index_.offset_ - 1];
        size = rows_.get_current_pos() - pos;
      }
      return ret;
    }

    /**
     * impl of ObCompactSSTableMemIterator
     * 
     */

    ObCompactSSTableMemIterator::ObCompactSSTableMemIterator()
    {
      reset();
      nop_cell_.value_.set_ext(ObActionFlag::OP_NOP);
      nop_cell_.column_id_ = OB_INVALID_ID;
    }

    ObCompactSSTableMemIterator::~ObCompactSSTableMemIterator()
    {
      reset();
    }

    int ObCompactSSTableMemIterator::init(ObCompactSSTableMem* mem)
    {
      int ret = OB_SUCCESS;
      if (NULL == mem)
      {
        TBSYS_LOG(WARN,"mem is null");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        mem_ = mem;
        inited_ = true;
      }
      return ret;
    }

    void ObCompactSSTableMemIterator::reset()
    {
      if (inited_)
      {
        inited_ = false;        
        mem_ = NULL;
        start_iterator_ = NULL;
        end_iterator_   = NULL;
        row_cursor_     = NULL;
        iterator_status_= OB_NOT_INIT;
        is_reverse_scan_= false;
        is_row_changed_ = false;
        first_cell_got_ = false;
        fill_nop_       = false;
        add_delete_row_nop_  = false;
        has_delete_row_ = false;
      }
    }

    int ObCompactSSTableMemIterator::set_scan_param(const ObNewRange& range,const ColumnFilter* cf,const bool is_reverse_scan)
    {
      int ret         = OB_SUCCESS;
      start_iterator_ = NULL;
      end_iterator_   = NULL;
      row_cursor_     = NULL;

      if (!inited_)
      {
        TBSYS_LOG(WARN,"have not init");
        ret = OB_NOT_INIT;
      }
      else if ((ret = mem_->locate_start_pos(range,start_iterator_)) != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG,"locate start key out of range,ret=%d",ret);
      }
      else if ((ret = mem_->locate_end_pos(range,end_iterator_)) != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG,"locate end key out of range,ret=%d",ret);
      }
      else if (start_iterator_ > end_iterator_)
      {
        ret = OB_BEYOND_THE_RANGE;
      }
      else
      {
        column_filter_ = cf;
        
        if (is_reverse_scan)
        {
          row_cursor_ = end_iterator_;
        }
        else
        {
          row_cursor_ = start_iterator_;
        }

        if ((ret = load_row(*row_cursor_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"set row failed,row:%p,ret=%d",*row_cursor_,ret);
        }
      }

      if (OB_BEYOND_THE_RANGE == ret)
      {
        //this range have no data
        ret = OB_SUCCESS;
        iterator_status_ = OB_ITER_END;
      }
      else
      {
        iterator_status_ = ret;
      }

      return ret;
    }

    int ObCompactSSTableMemIterator::set_get_param(const common::ObRowkey& rowkey,const ColumnFilter* cf)
    {
      int ret = OB_SUCCESS;
      start_iterator_ = NULL;
      end_iterator_   = NULL;
      row_cursor_     = NULL;

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS == (ret = mem_->find(rowkey,start_iterator_)))
      {
        end_iterator_ = start_iterator_;
        row_cursor_ = start_iterator_;
        if ((ret = load_row(*row_cursor_)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"set row failed,row:%p,ret=%d",*row_cursor_,ret);
        }
      }
      iterator_status_ = ret;
      column_filter_ = cf;
      return ret;
    }

    int ObCompactSSTableMemIterator::next_cell()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != iterator_status_)
      {
        ret = iterator_status_;
      }
      else if (NULL == row_cursor_ 
               || NULL == start_iterator_
               || NULL == end_iterator_
               || row_cursor_ < start_iterator_
                                || row_cursor_ > end_iterator_)
      {
        TBSYS_LOG(ERROR, "not initialized, cursor=%p, start=%p, last=%p",
                  row_cursor_, start_iterator_,end_iterator_);
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = next_cell_();
      }
      return ret;
    }

    int ObCompactSSTableMemIterator::next_cell_()
    {
      int ret = OB_SUCCESS;
      ObCellInfo *cell = NULL;

      while(OB_SUCCESS == ret)
      {
        ret = current_row_.next_cell();
        if (OB_ITER_END == ret)
        {
          if (!first_cell_got_)
          {
            fill_nop_ = true;
            ret = OB_SUCCESS;
            break;
          }
          else if (add_delete_row_nop_)
          {
            fill_nop_ = true;
            add_delete_row_nop_ = false;
            ret = OB_SUCCESS;
            break;            
          }
          else if (OB_SUCCESS == (ret = next_row()))
          {
            ret = current_row_.next_cell();
          }
        }

        if ((OB_SUCCESS != ret) ||
            ((OB_SUCCESS == ret) && ((ret = get_cell(&cell)) != OB_SUCCESS)))
        {
          break;
        }
        else
        {
          if (has_delete_row_)
          {
            add_delete_row_nop_ = true;
          }
          
          if ((ObExtendType == cell->value_.get_type()) &&
              (ObActionFlag::OP_DEL_ROW == cell->value_.get_ext()))
          {
            has_delete_row_ = true;
            break;
          }
          else if (column_filter_ != NULL && !column_filter_->column_exist(cell->column_id_))
          {
            continue;
          }
          else
          {
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        first_cell_got_ = true;
      }
      return ret;
    }

    int ObCompactSSTableMemIterator::next_row()
    {
      int ret = OB_SUCCESS;
      if (is_reverse_scan_)
      {
        --row_cursor_;
      }
      else
      {
        ++row_cursor_;
      }

      if (row_cursor_ < start_iterator_ || row_cursor_ > end_iterator_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = load_row(*row_cursor_);
      }
      return ret;
    }

    int ObCompactSSTableMemIterator::load_row(const char* row)
    {
      int ret = 0;
      if ((ret = current_row_.set_row(row,0)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"set row failed,row:%p,ret=%d",row,ret);
      }
      else if ((ret = current_row_.get_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"invalid row,row:%p",row);
      }
      else
      {
        is_row_changed_ = true;
        first_cell_got_ = false;
        fill_nop_       = false;
        add_delete_row_nop_ = false;
        has_delete_row_ = false;
      }
      return ret;
    }
  } //end namespace compactsstable
} //end namespace oceanbase
