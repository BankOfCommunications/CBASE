/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_sstable_merger.cpp for merge multi-sstable.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include "common/utility.h"
#include "ob_blockcache.h"
#include "ob_block_index_cache.h"
#include "ob_sstable_merger.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace common;

    ObSSTableMerger::ObSSTableMerger()
    : block_cache_(NULL),
      block_index_cache_(NULL),
      cell_(NULL),
      sstable_schema_(NULL),
      trailer_(NULL),
      current_sstable_size_(0),
      row_num_(0),
      pre_column_group_row_num_(0)
    {
      path_[0] = '\0';
    }

    ObSSTableMerger::~ObSSTableMerger()
    {

    }

    void ObSSTableMerger::reset()
    {
      block_cache_ = NULL;
      block_index_cache_ = NULL;
      
      path_[0] = '\0';
      sstable_path_.assign_ptr(NULL, 0);
      cell_ = NULL;
      row_.clear();
      sstable_schema_ = NULL;
      trailer_ = NULL;

      current_sstable_size_ = 0;
      row_num_ = 0;
      pre_column_group_row_num_ = 0;

      scan_param_.reset();
      seq_scanner_.cleanup();
    }

    void ObSSTableMerger::cleanup()
    {
      reset();
    }

    int ObSSTableMerger::merge_sstables(
      const ObString& sstable_path,
      ObBlockCache& block_cache, 
      ObBlockIndexCache& block_index_cache,
      ObSSTableReader* readers[], 
      const int64_t reader_size, 
      const ObNewRange& new_range)
    {
      int ret = OB_SUCCESS;

      if (NULL == sstable_path.ptr() || sstable_path.length() <= 0)
      {
        TBSYS_LOG(WARN, "invalid sstable file path, path_ptr=%p, path_len=%d",
                  sstable_path.ptr(), sstable_path.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (new_range.empty())
      {
        TBSYS_LOG(WARN, "invalid empty range for sstable merge");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = check_readers_param(readers, reader_size)))
      {
        TBSYS_LOG(WARN, "failed to check reader param, reader_size=%ld, ret=%d", 
          reader_size, ret);
      }
      else
      {
        reset();
        block_cache_ = &block_cache;
        block_index_cache_ = &block_index_cache;
        strncpy(path_, sstable_path.ptr(), sstable_path.length() + 1);
        sstable_path_.assign_ptr(path_, sstable_path.length());
        trailer_ = &readers[0]->get_trailer();
        sstable_schema_ = readers[0]->get_schema();

        ret = merge_column_groups(readers, reader_size, new_range);
      }

      return ret;
    }

    int ObSSTableMerger::check_readers_param(
      ObSSTableReader* readers[], const int64_t reader_size)
    {
      int ret = OB_SUCCESS;

      if (reader_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, reader_size=%ld", reader_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else 
      {
        for (int64_t i = 0; i < reader_size; ++i)
        {
          if (NULL == readers[i])
          {
            TBSYS_LOG(WARN, "reader is NULL, index=%ld", i);
            ret = OB_INVALID_ARGUMENT;
            break;
          }
        }
      }

      return ret;
    }

    int ObSSTableMerger::merge_column_groups(
      ObSSTableReader* readers[], const int64_t reader_size, const ObNewRange& new_range)
    {
      int ret = OB_SUCCESS;
      uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
      int64_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

      if (NULL == sstable_schema_ || NULL == trailer_)
      {
        TBSYS_LOG(WARN, "invalid sstable_schema=%p or trailer=%p",
          sstable_schema_, trailer_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = sstable_schema_->get_table_column_groups_id(
        trailer_->get_first_table_id(), column_group_ids, column_group_num)))
      {
        TBSYS_LOG(WARN, "get column groups failed, ret=%d", ret);
      }
      else 
      {
        for (int64_t group_index = 0; 
             (group_index < column_group_num) && (OB_SUCCESS == ret); ++group_index)
        {
          if (OB_SUCCESS != (ret = merge_one_column_group(
              group_index, column_group_ids[group_index], readers, reader_size)))
          {
            TBSYS_LOG(WARN, "merge column group[%ld]=%lu, group num=%ld error.", 
                group_index, column_group_ids[group_index], column_group_num);
          }
          else if (OB_SUCCESS != (ret = check_row_count_in_column_group()))
          {
            TBSYS_LOG(WARN, "check row count column group[%ld]=%lu", 
                group_index, column_group_ids[group_index]);
          }

          reset_for_next_column_group();
        }

        if (OB_SUCCESS == ret && OB_SUCCESS != (ret = finish_sstable(new_range)))
        {
          TBSYS_LOG(WARN, "close sstable failed, ret=%d", ret);
        }

        if (OB_SUCCESS != ret)
        {
          int64_t off = 0;
          int64_t size = 0;
          writer_.close_sstable(off, size);
          if (strlen(path_) > 0)
          {
            unlink(path_); //delete 
            TBSYS_LOG(WARN, "delete %s", path_);
          }
        }
      }

      return ret;
    }

    int ObSSTableMerger::merge_one_column_group(
      const int64_t column_group_idx,
      const uint64_t column_group_id,
      ObSSTableReader* readers[], 
      const int64_t reader_size)
    {
      int ret = OB_SUCCESS;
      RowStatus row_status = ROW_START;
      bool is_row_changed = false;

      if (OB_SUCCESS != (ret = fill_scan_param(column_group_id)))
      {
        TBSYS_LOG(ERROR, "prepare scan param failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = seq_scanner_.set_scan_param(scan_param_,
              *block_cache_, *block_index_cache_)))
      {
        TBSYS_LOG(ERROR, "set request param for sequence sstable scanner "
                         "failed, ret=%d",ret);
        scan_param_.reset();
      }
      else if (0 == column_group_idx && OB_SUCCESS != (ret = create_new_sstable()))
      {
        TBSYS_LOG(ERROR, "create sstable failed.");
      }
      else
      {
        for (int64_t i = 0; i < reader_size && OB_SUCCESS == ret; ++i)
        {
          ret = seq_scanner_.add_sstable_reader(readers[i]);
        }
      }

      while(OB_SUCCESS == ret)
      {
        cell_ = NULL;
        if (OB_SUCCESS != (ret = seq_scanner_.next_cell()) ||
            OB_SUCCESS != (ret = seq_scanner_.get_cell(&cell_, &is_row_changed)))
        {
          //end or error
          if (OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            if (OB_SUCCESS == ret && ROW_GROWING == row_status 
                && OB_SUCCESS == (ret = save_current_row()))
            {
              ++row_num_;
              row_.clear();
              row_status = ROW_END;
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "get data error, ret=%d", ret);
          }

          // end of file
          break;
        }
        else if (cell_ != NULL)
        {
          /**
           * there are some data in scanner
           */
          if ((ROW_GROWING == row_status && is_row_changed))
          {
            // we got new row, write last row we build.
            if (OB_SUCCESS == ret 
                && OB_SUCCESS == (ret = save_current_row()))
            {
              ++row_num_;
              // start new row.
              row_status = ROW_START;
            }

            // before we build new row, must clear last row in use.
            row_.clear();
          }

          if (OB_SUCCESS == ret && ROW_START == row_status)
          { 
            // first cell in current row, set rowkey and table info.
            row_.set_rowkey(cell_->row_key_);
            row_.set_table_id(cell_->table_id_);
            row_.set_column_group_id(column_group_id);
            row_status = ROW_GROWING;
          }

          if (OB_SUCCESS == ret && ROW_GROWING == row_status)
          {
            if (OB_SUCCESS != (ret = row_.add_obj(cell_->value_)))
            {
              TBSYS_LOG(ERROR, "add_cell_to_row failed, ret=%d", ret);
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "get cell return success but cell is null");
          ret = OB_ERROR;
        }
      }

      TBSYS_LOG(INFO, "column group %ld merge completed, ret=%d, version_range=%s, "
          "total row count =%ld", 
          column_group_id, ret, range2str(scan_param_.get_version_range()), row_num_);

      return ret;
    }

    int ObSSTableMerger::fill_scan_param(const uint64_t column_group_id) 
    {
      int ret = OB_SUCCESS;
      ObString table_name_string;
      ObNewRange range;

      //(min, max) range
      range.table_id_ = trailer_->get_first_table_id();
      range.set_whole_range();

      scan_param_.set(range.table_id_, table_name_string, range);
      /**
       * in merge,we do not use cache,
       * and just read frozen mem table.
       */
      scan_param_.set_is_result_cached(false); 
      scan_param_.set_is_read_consistency(false);
      scan_param_.set_read_mode(ScanFlag::ASYNCREAD);

      //(0, max) version range
      ObVersionRange version_range;
      version_range.border_flag_.set_max_value();
      version_range.start_version_ =  0;

      scan_param_.set_version_range(version_range);

      int64_t size = 0;
      const ObSSTableSchemaColumnDef *col = sstable_schema_->get_group_schema(
          range.table_id_, column_group_id, size);

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(WARN, "cann't find this column group=%lu", column_group_id);
        ret = OB_ERROR;
      }
      else
      {
        for(int64_t i = 0; i < size; ++i)
        {
          if (OB_SUCCESS != (ret = scan_param_.add_column((col + i)->column_name_id_)))
          {
            TBSYS_LOG(WARN, "add column id [%u] to scan_param_ error[%d]",
              (col + i)->column_group_id_, ret);
            break;
          }
        }
      }

      return ret;
    }

    int ObSSTableMerger::create_new_sstable()
    {
      int ret = OB_SUCCESS;
      const char* compressor_name = trailer_->get_compressor_name();
      ObString compressor_string;
   
      TBSYS_LOG(INFO, "create new sstable, sstable_path=%s, compressor=%s, "
                      "version=%ld, block_size=%ld",
          path_, compressor_name, trailer_->get_table_version(), 
          trailer_->get_block_size());
      compressor_string.assign_ptr(const_cast<char*>(compressor_name), 
        static_cast<int32_t>(strlen(compressor_name)));

      if (OB_SUCCESS != (ret = writer_.create_sstable(*sstable_schema_,
          sstable_path_, compressor_string, trailer_->get_table_version(), 
          trailer_->get_row_value_store_style(), trailer_->get_block_size())))
      {
        TBSYS_LOG(WARN, "create sstable failed, ret=%d", ret);
      }

      return ret;
    }

    int ObSSTableMerger::finish_sstable(const ObNewRange& new_range)
    {
      int ret = OB_SUCCESS;
      int64_t trailer_offset = 0;
      current_sstable_size_ = 0;

      if (OB_SUCCESS != (ret = writer_.set_tablet_range(new_range)))
      {
        TBSYS_LOG(ERROR, "failed to set tablet range for sstable writer");
      }
      else if (OB_SUCCESS != (ret = writer_.close_sstable(trailer_offset, 
        current_sstable_size_)) || current_sstable_size_ < 0)
      {
        TBSYS_LOG(WARN, "close sstable failed, ret=%d", ret);
      }

      return ret;
    }

    int ObSSTableMerger::save_current_row()
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = writer_.append_row(row_, current_sstable_size_)))
      {
        ObRowkey tmp_rowkey;
        row_.get_rowkey(tmp_rowkey);
        TBSYS_LOG(WARN, "append row failed ret=%d, this row_, obj count=%ld, "
                        "table_id=%lu, group_id=%lu, rowkey=%s",
            ret, row_.get_obj_count(), row_.get_table_id(), row_.get_column_group_id(), 
            to_cstring(tmp_rowkey));
        for(int32_t i=0; i<row_.get_obj_count(); ++i)
        {
          row_.get_obj(i)->dump(TBSYS_LOG_LEVEL_ERROR);
        }
      }

      return ret;
    }

    int ObSSTableMerger::check_row_count_in_column_group()
    {
      int ret = OB_SUCCESS;

      if (0 != pre_column_group_row_num_)
      {
        if (row_num_ != pre_column_group_row_num_)
        {
          TBSYS_LOG(ERROR, "the row num between two column groups is difference, "
                           "row_num_=%ld, pre_column_group_row_num_=%ld",
              row_num_, pre_column_group_row_num_);
          ret = OB_ERROR;
        }
      }
      else
      {
        pre_column_group_row_num_ = row_num_;
      }

      return ret;
    }

    void ObSSTableMerger::reset_for_next_column_group()
    {
      row_.clear();
      scan_param_.reset();
      seq_scanner_.cleanup();
      row_num_ = 0;
    }
  } // end namespace sstable
} // end namespace oceanbase
