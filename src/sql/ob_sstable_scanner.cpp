/**
 * (C) 2010-2011 Taobao Inc.
 *  
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_scanner.h for what ... 
 *  
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *   qushan <qushan@taobao.com>
 *     modified at 2010/10/8
 *     use new ObSSTableBlockScanner.
 *
 */
#include "ob_sstable_scanner.h"
#include "ob_sstable_scan.h"
#include "ob_column_group_scanner.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_record_header.h"
#include "common/ob_schema_manager.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_blockcache.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace sql
  {
    ObSSTableScanner::ObSSTableScanner()
      : scan_context_(NULL), 
      internal_scanner_obj_ptr_(NULL),
      column_group_size_(0),
      end_of_data_(false)
    {
    }

    ObSSTableScanner::~ObSSTableScanner()
    {
      cleanup();
    }

    void ObSSTableScanner::cleanup()
    {
      destroy_internal_scanners();
    }

    //add whx, [bugfix: free memory]20161030:b
    void ObSSTableScanner::clear()
    {
      ObColumnGroupScanner* scanner = NULL;

      if (NULL != internal_scanner_obj_ptr_ &&
          0 < column_group_size_)
      {
        for (int64_t i = 0; i < column_group_size_; ++i)
        {
          scanner = reinterpret_cast<ObColumnGroupScanner*>(
              internal_scanner_obj_ptr_ + sizeof(ObColumnGroupScanner) * i);
          scanner->free_buffer();//LMZ, free thread local memory
          scanner->~ObColumnGroupScanner();
        }
        internal_scanner_obj_ptr_ = NULL;
        column_group_size_ = 0;
      }
      column_group_scanner_.free_buffer();//LMZ, free thread local memory
    }
	//add:e

    void ObSSTableScanner::destroy_internal_scanners()
    {
      ObColumnGroupScanner* scanner = NULL;

      if (NULL != internal_scanner_obj_ptr_ &&
          0 < column_group_size_)
      {
        for (int64_t i = 0; i < column_group_size_; ++i)
        {
          scanner = reinterpret_cast<ObColumnGroupScanner*>(
              internal_scanner_obj_ptr_ + sizeof(ObColumnGroupScanner) * i);
          scanner->~ObColumnGroupScanner();
        }
        internal_scanner_obj_ptr_ = NULL;
        column_group_size_ = 0;
      }
    }

    int ObSSTableScanner::initialize(const ScanContext* scan_context, 
        const sstable::ObSSTableScanParam& scan_param)
    {
      int iret = OB_SUCCESS;

      scan_context_ = scan_context;

      // reset members of object.
      scan_param_.reset();
      merger_.reset();
      column_group_size_ = 0;
      end_of_data_ = false;

      iret = trans_input_scan_range(scan_param);

      return iret;
    }

    int ObSSTableScanner::set_column_group_scanner(
        const uint64_t *group_array, const int64_t group_size,
        const ObRowkeyInfo* rowkey_info)
    {
      int ret = OB_SUCCESS;

      if (NULL == group_array || group_size < 0)
      {
        TBSYS_LOG(WARN, "invalid param, group_array=%p, group_size=%ld",
            group_array, group_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (1 == group_size)
      {
        column_group_size_ = group_size;
        ret = set_single_column_group_scanner(group_array[0], rowkey_info);
      }
      else
      {
        column_group_size_ = group_size;
        ret = set_mult_column_group_scanner(group_array, group_size, rowkey_info);
      }

      return ret;
    }


    int ObSSTableScanner::set_mult_column_group_scanner(
        const uint64_t *group_array, const int64_t group_size,
        const common::ObRowkeyInfo* rowkey_info)
    {
      int iret = OB_SUCCESS;
      ObColumnGroupScanner* column_group_scanner = NULL;
      uint64_t current_group_id = 0;
      int64_t group_seq = 0;

      if (NULL != internal_scanner_obj_ptr_)
      {
        iret = OB_NOT_FREE;
        TBSYS_LOG(WARN, "internal_scanner_obj_ptr_ must be freed before used:ret[%d]", iret);
      }
      else
      {
        //LMZ, alloc thread local memory
        internal_scanner_obj_ptr_ = GET_TSI_MULT(ModuleArena, TSI_SSTABLE_MODULE_ARENA_1)->alloc(
            sizeof(ObColumnGroupScanner) * group_size);
        if (NULL == internal_scanner_obj_ptr_)
        {
          TBSYS_LOG(ERROR, "alloc memory for column group scanner failed,"
              "group_size=%ld", group_size);
          iret = OB_ALLOCATE_MEMORY_FAILED;
          common::ModuleArena* internal_buffer_arena = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
          TBSYS_LOG(ERROR, "thread local page arena hold memory usage,"
              "total=%ld,used=%ld,pages=%ld", internal_buffer_arena->total(),
              internal_buffer_arena->used(), internal_buffer_arena->pages());
        }
        else
        {
          ObColumnGroupScanner::Group group;
          group.size_ = group_size;

          //ensure all the column_group_scanner object has been constructed
          //otherwise coredump when deconstructing
          for (group_seq = 0; group_seq < group_size; ++group_seq)
          {
            char* object_ptr = internal_scanner_obj_ptr_ + group_seq * sizeof(ObColumnGroupScanner);
            column_group_scanner = new (object_ptr) ObColumnGroupScanner();
          }

          for (group_seq = 0; group_seq < group_size && OB_SUCCESS == iret; ++group_seq)
          {
            char* object_ptr = internal_scanner_obj_ptr_ + group_seq * sizeof(ObColumnGroupScanner);
            current_group_id = group_array[group_seq];
            group.id_ = current_group_id;
            group.seq_ = group_seq;

            column_group_scanner = reinterpret_cast<ObColumnGroupScanner *>(object_ptr);
            if (OB_SUCCESS != (iret = 
                  column_group_scanner->initialize(scan_context_->block_index_cache_, scan_context_->block_cache_, rowkey_info)))
            {
              TBSYS_LOG(ERROR, "column group scanner initialize , iret=%d,"
                  "current_group_id=%ld, group_seq=%ld", iret, current_group_id, group_seq);
            }
            else if (OB_SUCCESS != (iret = 
                  column_group_scanner->set_scan_param(group, &scan_param_, scan_context_->sstable_reader_)) )
            {
              TBSYS_LOG(ERROR, "column group scanner set scan parameter error, iret=%d,"
                  "current_group_id=%ld, group_seq=%ld", iret, current_group_id, group_seq);
            }
            else if (OB_SUCCESS != (iret = merger_.add_row_iterator(column_group_scanner)) )
            {
              TBSYS_LOG(ERROR, "add iterator to ObMerger error, iret=%d,"
                  "current_group_id=%ld, group_seq=%ld", iret, current_group_id, group_seq);
            }
            else
            {
              TBSYS_LOG(DEBUG, "add %ldth group id=%ld to scan, ret = %d", 
                  group_seq, group_array[group_seq], iret);
            }
          }

        }
      }

      return iret;
    }

    int ObSSTableScanner::set_single_column_group_scanner(
        const uint64_t column_group_id, 
        const common::ObRowkeyInfo* rowkey_info)
    {
      int ret = OB_SUCCESS;
      ObColumnGroupScanner::Group group;
      group.id_ = column_group_id;
      group.seq_ = 0;
      group.size_ = 1;

      if (OB_SUCCESS != (ret = 
            column_group_scanner_.initialize(scan_context_->block_index_cache_, scan_context_->block_cache_, rowkey_info)))
      {
        TBSYS_LOG(ERROR, "single column group scanner initialize , ret=%d,"
            "column_group_id=%lu, group_seq=%d", ret, column_group_id, 0);
      }
      else if (OB_SUCCESS != (ret = column_group_scanner_.set_scan_param(group, &scan_param_, scan_context_->sstable_reader_)))
      {
        TBSYS_LOG(ERROR, "single column group scanner set scan parameter error, ret=%d,"
            "column_group_id=%lu, group_seq=%d", ret, column_group_id, 0);
      }

      return ret;
    }

    /**
     * 设置scan所用到的参数, 这时会初始化用到的变量
     * @param [in] context scan所用到的外部参数
     * @param [in] sstable_reader SSTableReader
     * @return 
     * 1. 成功:
     *    a. OB_SUCCESS 
     *    b. OB_BEYOND_THE_RANGE 超出查询范围
     * 2. 错误, 错误码如下:
     *    a. 查询blockindex失败
     */
    int ObSSTableScanner::set_scan_param(
        const ObSSTableScanParam& scan_param, 
        const ScanContext* scan_context)
    {
      int iret = OB_SUCCESS;
      uint64_t group_array[OB_MAX_COLUMN_GROUP_NUMBER];
      int64_t group_size = OB_MAX_COLUMN_GROUP_NUMBER;

      int64_t table_id = scan_param.get_range().table_id_;

      if (NULL == scan_context)
      {
        iret = OB_INVALID_ARGUMENT;
      }
      else if ( OB_SUCCESS != (iret = initialize(scan_context, scan_param)) )
      {
        TBSYS_LOG(ERROR, "initialize column_group_scanner error ret=%d", iret);
      }
      else if (is_empty_sstable())
      {
        end_of_data_ = true;
        if (OB_SUCCESS != (iret = build_row_desc_from_scan_param(scan_param)))
        {
          TBSYS_LOG(WARN, "build_row_desc_from_scan_param ret=%d", iret);
        }
      }
      else if ( OB_SUCCESS != (iret = trans_input_column_id(scan_param, 
              scan_context->sstable_reader_->get_schema(), group_array, group_size)) )
      {
        TBSYS_LOG(ERROR, "trans_input_column_id error, ret=%d", iret);
      }
      else if ( scan_context->sstable_reader_->get_schema()->is_binary_rowkey_format(table_id) 
          && OB_SUCCESS != (iret = get_global_schema_rowkey_info(table_id, rowkey_info_)))
      {
        TBSYS_LOG(ERROR, "old fashion table(%ld) binary rowkey format, "
            "MUST set rowkey schema, ret=%d", table_id, iret);
      }
      else if ( OB_SUCCESS != (iret = set_column_group_scanner(group_array, group_size, &rowkey_info_)) )
      {
        TBSYS_LOG(ERROR, "set_column_group_scanner error, ret=%d", iret);
      }
      return iret;
    }

    bool ObSSTableScanner::column_group_exists(const uint64_t* group_array, 
        const int64_t group_size, const uint64_t group_id) const
    {
      bool ret = false;
      for (int64_t i = 0; i < group_size; ++i)
      {
        if (group_array[i] == group_id)
        {
          ret = true;
          break;
        }
      }
      return ret;
    }

    int ObSSTableScanner::trans_input_whole_row(
        const sstable::ObSSTableScanParam& scan_param, 
        const ObSSTableSchema* schema, 
        uint64_t* group_array, int64_t& group_size)
    {
      int iret = OB_SUCCESS;
      uint64_t table_id = scan_param.get_table_id();

      if (NULL == schema || NULL == group_array || group_size <= 0)
      {
        TBSYS_LOG(ERROR, "invalid param, schema=%p, table_id=%ld, group_array=%p, group_size=%ld.", 
            schema, table_id, group_array, group_size);
        iret = OB_ERROR;
      }
      else
      {
        iret = schema->get_table_column_groups_id(table_id, group_array, group_size);
      }

      return iret;
    }

    bool ObSSTableScanner::is_columns_in_one_group(const ObSSTableScanParam &scan_param, 
        const ObSSTableSchema* schema, const uint64_t group_id)
    {
      bool ret = false;
      int64_t i = 0;
      int64_t index = -1;
      uint64_t table_id = scan_param.get_table_id();
      int64_t column_id_size = scan_param.get_column_id_size();
      const uint64_t* const column_ids = scan_param.get_column_id();

      for (i = 0; i < column_id_size; ++i)
      {
        index = schema->find_offset_column_group_schema(table_id, group_id, 
            column_ids[i]);
        if (index < 0)
        {
          break;
        }
      }
      if (i == column_id_size)
      {
        ret = true;
      }

      return ret;
    }

    int ObSSTableScanner::trans_input_column_id(
        const sstable::ObSSTableScanParam &scan_param,
        const ObSSTableSchema* schema,
        uint64_t *group_array, int64_t& group_size)
    {
      int iret = OB_SUCCESS;
      int64_t column_index = -1;
      int64_t column_id_size = scan_param.get_column_id_size();
      const uint64_t* const column_id_begin = scan_param.get_column_id();
      if (NULL == schema || NULL == column_id_begin)
      {
        TBSYS_LOG(ERROR, "invalid arguments, sstable schema "
            "object =%p, column_id =%p", schema, column_id_begin);
        iret = OB_ERROR;
      }
      else if (scan_param.is_full_row_columns())
      {
        iret = trans_input_whole_row(scan_param, schema, group_array, group_size);
      }
      else
      {
        int64_t current_group_size = 0;
        uint64_t column_id = OB_INVALID_ID;
        uint64_t column_group_id = OB_INVALID_ID;

        bool has_invalid_column = false;

        for (int64_t i = 0; i < column_id_size && OB_SUCCESS == iret; ++i)
        {
          column_id = column_id_begin[i];
          column_index = schema->find_offset_first_column_group_schema(
              scan_param.get_table_id(), column_id, column_group_id);
          if (column_index < 0 || OB_INVALID_ID == column_group_id 
              || ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID == column_group_id)
          {
            has_invalid_column = true;
          }
          else if (!column_group_exists(group_array, current_group_size, column_group_id))
          {
            if (current_group_size > group_size)
            {
              iret = OB_SIZE_OVERFLOW;
              TBSYS_LOG(ERROR, "input group size =%ld not "
                  "enough with current=%ld, i=%ld, group id=%ld",
                  group_size, current_group_size, i, column_group_id);
              break;
            }
            group_array[current_group_size++] = column_group_id;
          }
        }

        /**
         * if all columns are in one column group, just read one column 
         * group, it's better for daily merger column group by column 
         * group. 
         */
        if (current_group_size > 1)
        {
          for (int64_t i = 0; i < current_group_size; ++i)
          {
            column_group_id = group_array[i];
            if (is_columns_in_one_group(scan_param, schema, column_group_id))
            {
              group_array[0] = column_group_id;
              current_group_size = 1;
              break;
            }
          }
        }

        // all columns are invalid column, 
        if (0 == current_group_size && has_invalid_column && group_size > 0)
        {
          group_array[current_group_size++] = schema->get_table_first_column_group_id(scan_param.get_table_id());
        }

        group_size = current_group_size;
      }
      return iret;
    }

    int ObSSTableScanner::trans_input_scan_range(const ObSSTableScanParam &scan_param)
    {
      int iret = OB_SUCCESS;
      scan_param_ = scan_param; 

      if (!scan_param_.is_valid())
      {
        TBSYS_LOG(ERROR, "input scan parmeter invalid, cannot scan any data.");
        iret = OB_INVALID_ARGUMENT;
      }

      const ObNewRange &input_range = scan_param.get_range(); 
      ObRowkey start_key = input_range.start_key_;
      ObRowkey end_key   = input_range.end_key_;

      if (OB_SUCCESS == iret && input_range.start_key_.is_min_row())// only end_key valid
      {
        if ( (!input_range.end_key_.is_max_row())
            && (NULL == end_key.ptr() || end_key.length() <= 0))
        {
          TBSYS_LOG(ERROR, "invalid end key, rang=%s", to_cstring(input_range));
          iret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == iret && input_range.end_key_.is_max_row())// only start_key valid
      {
        if ( (!input_range.start_key_.is_min_row())
            && (NULL == start_key.ptr() || start_key.length() <= 0))
        {
          TBSYS_LOG(ERROR, "invalid start key, range=%s", to_cstring(input_range));
          iret = OB_INVALID_ARGUMENT;
        }
      }

      return iret;
    }

    int ObSSTableScanner::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (is_empty_sstable())
      {
        if (0 >= row_desc_.get_column_num())
        {
          TBSYS_LOG(WARN, "row_desc maybe not set:row_desc_[%ld]", row_desc_.get_column_num());
          ret = OB_NOT_INIT;
        }
        else
        {
          row_desc = &row_desc_;
        }
      }
      else if (1 == column_group_size_)
      {
        ret = column_group_scanner_.get_row_desc(row_desc);
      }
      else
      {
        ret = merger_.get_row_desc(row_desc);
      }
      return ret;
    }

    int ObSSTableScanner::build_row_desc_from_scan_param(const sstable::ObSSTableScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      int64_t column_id_size = scan_param.get_column_id_size();
      int64_t table_id = scan_param.get_table_id();
      const uint64_t* const column_id_begin = scan_param.get_column_id();
      row_desc_.reset();
      // if sstable is empty, we build row desc from scan param query columns;
      for (int64_t i = 0; i < column_id_size && OB_SUCCESS == ret; ++i)
      {
        if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id, column_id_begin[i])))
        {
          TBSYS_LOG(WARN, "add_column_desc table=%ld, column=%ld, ret=%d", 
              table_id, column_id_begin[i], ret);
        }
      }
      // in daily merge mode, with no project operator, so set rowkey column
      // in row_desc_
      if (OB_SUCCESS == ret && scan_param.is_daily_merge_scan())
      {
        row_desc_.set_rowkey_cell_count(scan_param.get_rowkey_column_count());
      }

      return ret;
    }

    inline bool ObSSTableScanner::is_empty_sstable() const
    {
      int64_t table_id = scan_param_.get_range().table_id_;
      bool  is_empty = (NULL == scan_context_->sstable_reader_ 
          || scan_context_->sstable_reader_->get_row_count() == 0
          || (!scan_context_->sstable_reader_->get_schema()->is_table_exist(table_id)));
      return is_empty;
    }


  }//end namespace sstable
}//end namespace oceanbase
