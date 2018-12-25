/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *   qushan <qushan@taobao.com>
 *
 */
#include "ob_column_group_scanner.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_record_header.h"
#include "common/ob_cur_time.h"
#include "common/ob_profile_log.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_scan_param.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace sql
  {
    ObColumnGroupScanner::ObColumnGroupScanner() 
      : block_index_cache_(NULL), block_cache_(NULL), rowkey_info_(NULL),
      iterate_status_(ITERATE_NOT_INITIALIZED), index_array_cursor_(INVALID_CURSOR), 
      scan_param_(NULL), sstable_reader_(NULL), 
      uncompressed_data_buffer_(NULL), uncompressed_data_bufsiz_(0), 
      block_internal_buffer_(NULL), block_internal_bufsiz_(0), 
      current_scan_column_indexes_(OB_MAX_COLUMN_NUMBER),
      scanner_(current_scan_column_indexes_)
    {
    }

    ObColumnGroupScanner::~ObColumnGroupScanner()
    {
    }

    inline int ObColumnGroupScanner::check_status() const
    {
      int iret = OB_SUCCESS;

      switch (iterate_status_)
      {
        case ITERATE_IN_PROGRESS:
        case ITERATE_LAST_BLOCK:
        case ITERATE_NEED_FORWARD:
          iret = OB_SUCCESS;
          break;
        case ITERATE_NOT_INITIALIZED:
        case ITERATE_NOT_START:
          iret = OB_NOT_INIT;
          break;
        case ITERATE_END:
          iret = OB_ITER_END;
          break;
        default:
          iret = OB_ERROR;
      }

      return iret;
    }

    int ObColumnGroupScanner::get_next_row(const ObRowkey* &row_key, const ObRow *&row_value)
    {
      int iret = check_status();

      if (OB_SUCCESS == iret)
      {
        iret = scanner_.get_next_row(row_key, row_value);
        do
        {
          // if reach end of current block, skip to next
          // fetch_next_block return OB_SUCCESS means still has block(s) 
          // or OB_ITER_END means reach the end of this scan(%scan_param.end_key).
          if (OB_BEYOND_THE_RANGE == iret 
              && (OB_SUCCESS == (iret = fetch_next_block())) 
              && is_forward_status())
          {
            // got block(s) ahead, continue iterate block data.
            // current block may contains valid keys(dense format), 
            // otherwise fetch_next_block returned OB_ITER_END ,
            // or OB_BEYOND_THE_RANGE when block contains no data 
            // (sparse format) in case continue iterate next block.
            iret = scanner_.get_next_row(row_key, row_value);
          } 
        } while (OB_BEYOND_THE_RANGE == iret);
      }

      iret = check_status();

      return iret;
    }

    int ObColumnGroupScanner::alloc_buffer(char* &buffer, const int64_t bufsiz)
    {
      int ret = OB_SUCCESS;
      buffer = NULL;

      common::ModuleArena * arena = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);

      if (NULL == arena || NULL == (buffer = arena->alloc(bufsiz)) )
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      return ret;
    }

    bool ObColumnGroupScanner::is_end_of_block() const
    {
      bool bret = true;

      if (is_valid_cursor())
      {
        if (scan_param_->is_reverse_scan())
        {
          bret = index_array_cursor_ < 0;
        }
        else
        {
          bret = index_array_cursor_ >= index_array_.block_count_;
        }
      }

      return bret;
    }

    int ObColumnGroupScanner::add_column_offset(const uint64_t table_id, 
        const uint64_t column_id, const int64_t offset)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id, column_id)))
      {
        TBSYS_LOG(WARN, "add_column_desc ret=%d, table_id=%ld, column_id=%ld, offset=%ld",
            ret, table_id, column_id, offset);
      }
      else if (OB_SUCCESS != (ret = current_scan_column_indexes_.add_column(offset)))
      {
        TBSYS_LOG(WARN, "add_column_offset ret=%d, table_id=%ld, column_id=%ld, offset=%ld",
            ret, table_id, column_id, offset);
      }
      return ret;
    }

    int ObColumnGroupScanner::build_all_column_index(
        const uint64_t table_id, const Group& group, const ObSSTableSchema& schema)
    {
      // whole row, query whole column group;
      int iret = OB_SUCCESS;
      int64_t column_size = 0;
      int64_t rowkey_count = 0;
      uint64_t column_group_id = OB_INVALID_ID;
      const ObSSTableSchemaColumnDef* def_array = NULL; 

      if (group.seq_ == 0)
      {
        // add rowkey columns in first column group;
        if (schema.is_binary_rowkey_format(table_id))
        {
          if (NULL == rowkey_info_)
          {
            TBSYS_LOG(ERROR, "binary rowkey sstable NOT set rowkey info, table=%ld", table_id);
            iret = OB_ERROR;
          }
          else
          {
            rowkey_count = rowkey_info_->get_size();
            for (int32_t i = 0; i < rowkey_count && OB_SUCCESS == iret; ++i)
            {
              // indexs[offset_in_row] = row_desc_offset;
              iret = add_column_offset(table_id, rowkey_info_->get_column(i)->column_id_, i);
            }
          }
        }
        else if ( NULL == ( def_array = schema.get_group_schema(table_id, 
                ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID, column_size)))
        {
          iret = OB_ERROR;
          TBSYS_LOG(INFO, "rowkey column group not exist:table:%ld", table_id);
        }
        else
        {
          rowkey_count = column_size;
          for (int32_t i = 0; i < column_size && OB_SUCCESS == iret; ++i)
          {
            iret = add_column_offset(table_id, def_array[i].column_name_id_, def_array[i].rowkey_seq_ - 1);
          }
        }
      }

      if (OB_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "add rowkey columns error, iret=%d", iret);
      }
      else if (NULL == (def_array = schema.get_group_schema(table_id, group.id_, column_size)))
      {
        iret = OB_ERROR;
        TBSYS_LOG(ERROR, "find column group def array error.");
      }
      else
      {
        // add every column id in this column group.
        for (int32_t i = 0; i < column_size && OB_SUCCESS == iret; ++i)
        {
          /**
           * if one column belongs to several column group, only the first 
           * column group will handle it. except there is only one column 
           * group. 
           */
          if (group.size_ > 1)
          {
            schema.find_offset_first_column_group_schema(
                table_id, def_array[i].column_name_id_, column_group_id);
          }
          else 
          {
            column_group_id = group.id_;
          }
          if (column_group_id == group.id_)
          {
            iret = add_column_offset(table_id, def_array[i].column_name_id_, i + rowkey_count);
          }
        }
      }
      return iret;
    }

    int ObColumnGroupScanner::build_input_column_index(
        const uint64_t table_id, const uint64_t *const column_id_begin, 
        const int64_t column_id_size, const Group& group, const ObSSTableSchema& schema)
    {
      int iret = OB_SUCCESS;
      int64_t index = 0;
      int64_t rowkey_column_count = 0;
      uint64_t current_column_id = OB_INVALID_ID;
      uint64_t column_group_id = OB_INVALID_ID;
      ObRowkeyColumn column;

      if (schema.is_binary_rowkey_format(table_id) && NULL != rowkey_info_)
      {
        rowkey_column_count = rowkey_info_->get_size();;
      }
      else if (OB_SUCCESS != (iret = schema.get_rowkey_column_count(table_id, rowkey_column_count)))
      {
        TBSYS_LOG(WARN, "get_rowkey_column_count ret=%d, table_id=%ld", iret, table_id);
      }

      // query columns in current group
      for (int32_t i = 0; i < column_id_size && OB_SUCCESS == iret; ++i)
      {
        current_column_id = column_id_begin[i];
        if (0 == current_column_id || OB_INVALID_ID == current_column_id)
        {
          TBSYS_LOG(ERROR, "input column id =%ld (i=%d) is invalid.", 
              current_column_id, i);
          iret = OB_INVALID_ARGUMENT;
        }
        else if ( schema.is_binary_rowkey_format(table_id) && NULL != rowkey_info_
            && OB_SUCCESS == rowkey_info_->get_index(current_column_id, index, column))
        {
          // is binary rowkey column?
          if (0 == group.seq_)
          {
            iret = add_column_offset(table_id, current_column_id, index);
          }
        }
        else if ( (index = schema.find_offset_column_group_schema(
                table_id, ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID, current_column_id)) >= 0 )
        {
          // is rowkey column?
          if (0 == group.seq_)
          {
            iret = add_column_offset(table_id, current_column_id, index);
          }
        }
        else if (!schema.is_column_exist(table_id, current_column_id))
        {
          // column id not exist in schema, set to NOT_EXIST_COLUMN
          // return NullType .
          // ATTENTION! set index to -1 presents nonexist.
          if (0 == group.seq_)
          {
            // no need add NotExist column in %current_scan_column_indexes_
            // default is not exist;
            iret = add_column_offset(table_id, current_column_id, -1);
          }
        }
        else
        {
          /**
           * if one column belongs to several column group, only the first 
           * column group will handle it. except there is only one column 
           * group. 
           */
          if (group.size_ > 1)
          {
            index = schema.find_offset_first_column_group_schema(
                table_id, current_column_id, column_group_id);
          }
          else 
          {
            index = schema.find_offset_column_group_schema(
                table_id, group.id_, current_column_id);
            column_group_id = group.id_;
          }
          if (index >= 0 && column_group_id == group.id_)
          {
            iret = add_column_offset(table_id, current_column_id, index + rowkey_column_count);
          }
        }

      }
      return iret;
    }

    int ObColumnGroupScanner::consummate_row_desc(const uint64_t table_id,
        const ObSSTableReader* const sstable_reader)
    {
      int ret = OB_SUCCESS;
      if (NULL != sstable_reader)
      {
        if (OB_SUCCESS != (ret = match_rowkey_desc(table_id, sstable_reader)))
        {
          TBSYS_LOG(ERROR, "match_rowkey_desc table=%ld, ret=%d", table_id, ret);
        }
        else if (sstable_reader->get_trailer().get_row_value_store_style() == OB_SSTABLE_STORE_SPARSE)
        {
          ret = row_desc_.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        }
      }

      return ret;
    }

    // check input columns if inclusive rowkey columns;
    int ObColumnGroupScanner::match_rowkey_desc(const uint64_t table_id, 
        const sstable::ObSSTableReader *sstable_reader)
    {
      int ret = OB_SUCCESS;
      int64_t rowkey_column_count = 0;
      uint64_t rowkey_column_id = 0;
      uint64_t tid = 0;
      uint64_t cid = 0;
      bool match_all_rowkey = false;
      const ObSSTableSchema* schema = sstable_reader->get_schema();
      if (NULL == schema)
      {
        TBSYS_LOG(ERROR, "schema of table =%ld not exist.", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (schema->is_binary_rowkey_format(table_id))
      {
        if (NULL == rowkey_info_)
        {
          TBSYS_LOG(ERROR, "binary rowkey sstable NOT set rowkey info, table=%ld", table_id);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          rowkey_column_count = rowkey_info_->get_size();
        }
      }
      else if (OB_SUCCESS != (ret = schema->get_rowkey_column_count(
              table_id, rowkey_column_count)))
      {
        TBSYS_LOG(ERROR, "cannot get rowkey column of table=%ld", table_id);
      }

      if (OB_SUCCESS == ret && rowkey_column_count > 0
        && row_desc_.get_column_num() > 0)
      {
        int64_t i = 0;
        for (; i < rowkey_column_count; ++i)
        {
          if (schema->is_binary_rowkey_format(table_id))
          {
            rowkey_column_id = rowkey_info_->get_column(i)->column_id_;
          }
          else
          {
            rowkey_column_id = schema->get_rowkey_column(table_id, i+1)->column_name_id_;
          }

          if (i >= row_desc_.get_column_num() 
              || OB_SUCCESS != row_desc_.get_tid_cid(i, tid, cid)
              ||  rowkey_column_id != cid)
          {
            match_all_rowkey = false;
            break;
          }
        }

        if (i >= rowkey_column_count) match_all_rowkey = true;
        if (match_all_rowkey)
        {
          row_desc_.set_rowkey_cell_count(rowkey_column_count);
        }
      }

      return ret;
    }

    int ObColumnGroupScanner::trans_input_column_id(
        const Group &group,
        const ObSSTableScanParam *scan_param, 
        const ObSSTableReader *sstable_reader)
    {
      int iret = OB_SUCCESS;

      const ObSSTableSchema* schema = sstable_reader->get_schema();
      int64_t column_id_size = scan_param->get_column_id_size();
      const uint64_t *const column_id_begin = scan_param->get_column_id();

      uint64_t table_id = scan_param->get_table_id();
      uint64_t sstable_file_id = sstable_reader->get_sstable_id().sstable_file_id_;

      row_desc_.reset();
      current_scan_column_indexes_.reset(); //reset scan column index first
      current_scan_column_indexes_.set_table_id(table_id);

      if (NULL == schema || NULL == column_id_begin)
      {
        iret = OB_ERROR;
        TBSYS_LOG(ERROR, "internal error, schema=%p is null "
            "or input scan param column size=%ld.", schema, column_id_size);
      }
      else if (scan_param->is_full_row_columns())
      {
        iret = build_all_column_index(table_id, group, *schema);
      }
      else
      {
        iret = build_input_column_index(table_id, column_id_begin, column_id_size, group, *schema);
      }

      if (OB_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "trans input param error, iret=%d,"
            "sstable id=%ld, table id=%ld, group id =%ld, group seq=%ld",
            iret, sstable_file_id, table_id, group.id_, group.seq_);
      }
      else
      {
        TBSYS_LOG(DEBUG, "trans input param succeed,sstable id=%ld, "
            "table id=%ld, group id =%ld, group seq=%ld, column count=%ld",
             sstable_file_id, table_id, group.id_, group.seq_, 
             current_scan_column_indexes_.get_column_count());
      }

      return iret;
    }

    void ObColumnGroupScanner::advance_to_next_block()
    {
      if (scan_param_->is_reverse_scan())
      {
        --index_array_cursor_;
      }
      else
      {
        ++index_array_cursor_;
      }
    }

    void ObColumnGroupScanner::reset_block_index_array()
    {
      index_array_cursor_ = INVALID_CURSOR; 

      const ObNewRange &range = scan_param_->get_range();
      index_array_.block_count_ = ObBlockPositionInfos::NUMBER_OF_BATCH_BLOCK_INFO; 

      if ( (!range.start_key_.is_min_row())
          && (!range.end_key_.is_max_row())
          && range.start_key_ == range.end_key_) // single row scan, just one block.
      {
        index_array_.block_count_ = 1; 
      }
    }

    int ObColumnGroupScanner::prepare_read_blocks()
    {
      int iret = OB_SUCCESS;
      int64_t sstable_file_id = sstable_reader_->get_sstable_id().sstable_file_id_;
      int64_t table_id = scan_param_->get_table_id(); 
      bool is_result_cached = scan_param_->get_is_result_cached();

      // reset cursor and state
      if (scan_param_->is_reverse_scan())
      {
        index_array_cursor_ = index_array_.block_count_ - 1;
      }
      else
      {
        index_array_cursor_ = 0;
      }

      iterate_status_ = ITERATE_IN_PROGRESS;

      if (!scan_param_->is_sync_read())
      {
        iret = block_cache_->advise( sstable_file_id, 
            index_array_, table_id, group_.id_, is_result_cached, 
            scan_param_->is_reverse_scan());
      }

      return iret;
    }


    int ObColumnGroupScanner::search_block_index(const bool first_time)
    {
      int iret = OB_SUCCESS;

      // load block from block index cache
      ObBlockIndexPositionInfo info;
      memset(&info, 0, sizeof(info));
      const ObSSTableTrailer& trailer = sstable_reader_->get_trailer();
      info.sstable_file_id_ = sstable_reader_->get_sstable_id().sstable_file_id_;
      info.offset_ = trailer.get_block_index_record_offset();
      info.size_   = trailer.get_block_index_record_size();

      int64_t end_offset = 0;
      SearchMode mode = OB_SEARCH_MODE_LESS_THAN;
      if (!first_time)
      {
        if (!is_end_of_block() && index_array_.block_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "cursor=%ld, block count=%ld error, cannot search next batch blocks.", 
              index_array_cursor_, index_array_.block_count_);
          iret = OB_ERROR;
        }
        else if (scan_param_->is_reverse_scan())
        {
          end_offset = index_array_.position_info_[0].offset_;
          mode = OB_SEARCH_MODE_LESS_THAN;
        }
        else
        {
          end_offset = index_array_.position_info_[index_array_.block_count_ - 1].offset_;
          mode = OB_SEARCH_MODE_GREATER_THAN;
        }
      }

      if (OB_SUCCESS == iret)
      {
        reset_block_index_array();

        if (first_time)
        {
          iret = block_index_cache_->get_block_position_info(
              info, scan_param_->get_table_id(), group_.id_, 
              scan_param_->get_range(), 
              scan_param_->is_reverse_scan(), index_array_); 
        }
        else
        {
          iret = block_index_cache_->next_offset(
              info, scan_param_->get_table_id(), group_.id_, 
              end_offset, mode, index_array_);
        }
      }

      if (OB_SUCCESS == iret)
      {
        iret = prepare_read_blocks();
      }
      else if (OB_BEYOND_THE_RANGE == iret)
      {
        TBSYS_LOG(DEBUG, "search block index out of range.");
        iterate_status_ = ITERATE_END;
        iret = OB_SUCCESS;
      }
      else if (OB_SUCCESS != iret)
      {
        iterate_status_ = ITERATE_IN_ERROR;
        TBSYS_LOG(ERROR, "search block index error, iret=%d, info=%ld,%ld,%ld,"
            " table id=%ld,group id=%ld, index_array.block_count=%ld",
            iret, info.sstable_file_id_, info.offset_, info.size_,  
            scan_param_->get_table_id(), group_.id_, index_array_.block_count_);
      }


      return iret;
    }

    int ObColumnGroupScanner::initialize(
        ObBlockIndexCache* block_index_cache, ObBlockCache* block_cache,
        const common::ObRowkeyInfo* rowkey_info)
    {
      int ret = OB_SUCCESS;
      uncompressed_data_bufsiz_ = UNCOMPRESSED_BLOCK_BUFSIZ;
      block_internal_bufsiz_ = BLOCK_INTERNAL_BUFSIZ;

      if (NULL == block_index_cache || NULL == block_cache)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid arguments, bic=%p, bc=%p.", 
            block_index_cache, block_cache);
      }
      else if (OB_SUCCESS != (ret = (alloc_buffer(//LMZ, alloc thread local memory
                uncompressed_data_buffer_, uncompressed_data_bufsiz_))) )
      {
        TBSYS_LOG(ERROR, "allocate uncompressed data buffer failed, sz=%ld",
            uncompressed_data_bufsiz_);
        common::ModuleArena* internal_buffer_arena = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
        TBSYS_LOG(ERROR, "thread local page arena hold memory usage,"
            "total=%ld,used=%ld,pages=%ld", internal_buffer_arena->total(),
            internal_buffer_arena->used(), internal_buffer_arena->pages());
      }
      else if (OB_SUCCESS != (ret = (alloc_buffer(//LMZ, alloc thread local memory
                block_internal_buffer_, block_internal_bufsiz_))) )
      {
        TBSYS_LOG(ERROR, "allocate block internal data buffer failed, sz=%ld",
            block_internal_bufsiz_);
        common::ModuleArena* internal_buffer_arena = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
        TBSYS_LOG(ERROR, "thread local page arena hold memory usage,"
            "total=%ld,used=%ld,pages=%ld", internal_buffer_arena->total(),
            internal_buffer_arena->used(), internal_buffer_arena->pages());
      }
      else
      {
        block_index_cache_ = block_index_cache;
        block_cache_ = block_cache;
        rowkey_info_ = rowkey_info;

        iterate_status_ = ITERATE_NOT_START;
        index_array_.block_count_ = 0;
      }

      return ret;
    }

    int ObColumnGroupScanner::set_scan_param(
        const Group & group, const ObSSTableScanParam *scan_param,
        const ObSSTableReader* const sstable_reader)
    {
      int iret = OB_SUCCESS;

      INIT_PROFILE_LOG_TIMER();

      if (NULL == scan_param || NULL == sstable_reader || !scan_param->is_valid())
      {
        iret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "set invalid scan param =%p or reader=%p",
            scan_param, sstable_reader);
      }
      else if (ITERATE_NOT_INITIALIZED == iterate_status_)
      {
        iret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (iret = 
            trans_input_column_id(group, scan_param, sstable_reader)))
      {
        TBSYS_LOG(ERROR, "trans_input_column_id error, iret=%d", iret);
      }
      else if (OB_SUCCESS != (iret = consummate_row_desc(scan_param->get_table_id(), sstable_reader)))
      {
        TBSYS_LOG(ERROR, "consummate_row_desc error, iret=%d", iret);
      }
      else
      {
        // save parameters
        scan_param_ = scan_param;
        sstable_reader_ = sstable_reader;
        group_ = group;

        if ( OB_SUCCESS != (iret = search_block_index(true)) )
        {
          TBSYS_LOG(ERROR, "search in block index error, iret:%d.", iret);
        }
        else if ( OB_SUCCESS != (iret = fetch_next_block()) )
        {
          TBSYS_LOG(ERROR, "error in first fetch_next_block, iret:%d", iret);
        }

        PROFILE_LOG_TIME(DEBUG, "cg scanner fetch first block table id=%ld,"
            "group id=%ld, sstable id=%ld, input range:%s, iterate_status_=%ld, "
            "scan param::is reverse=%d, result cache:%d, read mode:%d, search block:%ld, ret=%d", 
            scan_param_->get_table_id(), group_.id_, sstable_reader_->get_sstable_id().sstable_file_id_, 
            to_cstring(scan_param_->get_range()), iterate_status_, 
            scan_param_->is_reverse_scan(), scan_param_->get_is_result_cached(), 
            scan_param_->get_read_mode(), index_array_.block_count_, iret);


      }

      return iret;
    }


    int ObColumnGroupScanner::read_current_block_data( 
        const char* &block_data_ptr, int64_t &block_data_size)
    {

      int iret = OB_SUCCESS;
      ObBufferHandle handler;

      const ObBlockPositionInfo & pos = index_array_.position_info_[index_array_cursor_];
      block_data_ptr = NULL;
      block_data_size = pos.size_;

      const char* compressed_data_buffer = NULL;
      int64_t compressed_data_bufsiz = 0;

      int64_t sstable_file_id = sstable_reader_->get_sstable_id().sstable_file_id_;
      int64_t table_id = scan_param_->get_table_id();

      INIT_PROFILE_LOG_TIMER();

      if (OB_SUCCESS == iret)
      {
        if (scan_param_->is_sync_read())
        {
          if (scan_param_->get_is_result_cached())
          {
            iret = block_cache_->get_block_readahead(sstable_file_id,
                table_id, index_array_, index_array_cursor_, 
                scan_param_->is_reverse_scan(), handler);
          }
          else
          {
            iret = block_cache_->get_block_sync_io(sstable_file_id,
                pos.offset_, pos.size_, handler, table_id, true);
          }
        }
        else
        {
          iret = block_cache_->get_block_aio(sstable_file_id, 
              pos.offset_, pos.size_, handler, OB_AIO_TIMEOUT_US, table_id, group_.id_);
        }

        if (iret == pos.size_)
        {
          block_data_ptr = handler.get_buffer();
          iret = OB_SUCCESS;
        }
        else
        {
          iret = OB_IO_ERROR;
          TBSYS_LOG(ERROR, "read sstable block data in cache failed, iret=%d", iret);
        }
      }

      PROFILE_LOG_TIME(DEBUG, "read block data complete sstable=%ld, pos=%ld,%ld, is sync read=%d, ret=%d", 
          sstable_file_id, pos.offset_, pos.size_, scan_param_->is_sync_read(), iret);

      ObRecordHeader header;
      if (OB_SUCCESS == iret)
      {
        memset(&header, 0, sizeof(header));
        iret = ObRecordHeader::get_record_header(
            block_data_ptr, block_data_size, 
            header, compressed_data_buffer, compressed_data_bufsiz);
        if (OB_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "get record header error, iret=%d, block data size=%ld", 
              iret, compressed_data_bufsiz);
        }
        else
        {
          // size not enough; realloc new memory.
          if (header.data_length_ > uncompressed_data_bufsiz_)
          {
            TBSYS_LOG(WARN, "block data length=%d > fixed length=%ld, "
                "block:(fileid=%ld,table_id=%lu,offset=%ld,size=%ld)",
                header.data_length_, uncompressed_data_bufsiz_,
                sstable_file_id, table_id, pos.offset_, pos.size_
                );
            uncompressed_data_bufsiz_ = header.data_length_;
            iret = alloc_buffer(uncompressed_data_buffer_,  uncompressed_data_bufsiz_);
          }
        }
      }

      if (OB_SUCCESS == iret)
      {
        int64_t real_size = 0;
        if (header.is_compress())
        {
          ObCompressor* dec = const_cast<ObSSTableReader *>(sstable_reader_)->get_decompressor();
          if (NULL != dec)
          {
            iret = dec->decompress(compressed_data_buffer, compressed_data_bufsiz, 
                uncompressed_data_buffer_, header.data_length_, real_size);
            if (iret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "decompress failed, iret=%d, real_size=%ld", iret, real_size);
            }
            else
            {
              block_data_ptr = uncompressed_data_buffer_;
              block_data_size = real_size;
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "get_decompressor failed, maybe decompress library install incorrectly.");
            iret = OB_CS_COMPRESS_LIB_ERROR;
          }
        }
        else
        {
          // sstable block data is not compressed, copy block to uncompressed buffer.
          memcpy(uncompressed_data_buffer_, compressed_data_buffer, compressed_data_bufsiz);
          block_data_ptr = uncompressed_data_buffer_;
        }
      }


      if (OB_SUCCESS != iret && NULL != sstable_reader_ && NULL != scan_param_)
      {
        TBSYS_LOG(ERROR, "read_current_block_data error, input param:"
            "iret=%d, file id=%ld, table_id=%ld, pos=%ld,%ld", iret, 
            sstable_reader_->get_sstable_id().sstable_file_id_, 
            scan_param_->get_table_id(), pos.offset_, pos.size_);
      }

      PROFILE_LOG_TIME(DEBUG, "decompress block data real size=%ld, iret=%d", block_data_size, iret);

      return iret;
    }

    inline int ObColumnGroupScanner::fetch_next_block()
    {
      int iret = OB_SUCCESS;
      do
      {
        iret = load_current_block_and_advance();
      }
      while (OB_SUCCESS == iret && iterate_status_ == ITERATE_NEED_FORWARD) ;
      return iret;
    }

    int ObColumnGroupScanner::load_current_block_and_advance() 
    {
      int iret = OB_SUCCESS;
      if (OB_SUCCESS == iret && is_forward_status())
      {
        if (ITERATE_LAST_BLOCK == iterate_status_)
        {
          // last block is the end, no need to looking forward.
          iterate_status_ = ITERATE_END;
        }
        else if (is_end_of_block())
        {
          TBSYS_LOG(DEBUG, "current batch block scan over, "
              "begin fetch next batch blocks, group=%ld, cursor=%ld", 
              group_.id_, index_array_cursor_);
          // has more blocks ahead, go on.
          iret = search_block_index(false);
        }
      }

      if (OB_SUCCESS == iret && is_forward_status())
      {
        if (is_end_of_block())
        {
          // maybe search_block_index got nothing.
          iterate_status_ = ITERATE_END;
        }
        else
        {
          const char *block_data_ptr = NULL;
          int64_t block_data_size = 0;
          iret = read_current_block_data(block_data_ptr, block_data_size);

          if (OB_SUCCESS == iret && NULL != block_data_ptr && block_data_size > 0)
          {
            bool need_looking_forward = false;
            ObSSTableBlockReader::BlockData block_data(
                block_internal_buffer_, block_internal_bufsiz_,
                block_data_ptr, block_data_size);
            //TODO, set rowkey info
            int64_t rowkey_column_count = 0;
            sstable_reader_->get_schema()->get_rowkey_column_count(
                scan_param_->get_range().table_id_, rowkey_column_count);
            ObSSTableBlockReader::BlockDataDesc data_desc(
                rowkey_info_, rowkey_column_count,
                sstable_reader_->get_trailer().get_row_value_store_style());

            iret = scanner_.set_scan_param(row_desc_, *scan_param_, 
                 data_desc, block_data, need_looking_forward);

            if (OB_SUCCESS == iret)
            {
              // current block contains rowkey(s) we need, 
              // check current block is the end point?
              advance_to_next_block();
              iterate_status_ = need_looking_forward ? ITERATE_IN_PROGRESS : ITERATE_LAST_BLOCK;
            }
            else if (OB_BEYOND_THE_RANGE == iret)
            {
              TBSYS_LOG(DEBUG, "current cursor = %ld, out of range, need_looking_forward=%d", 
                  index_array_cursor_, need_looking_forward);
              // current block has no any rowkey we need, 
              // so check to continue search or not.
              if (!need_looking_forward)
              {
                iterate_status_ = ITERATE_END;
              }
              else
              {
                advance_to_next_block();
                // current block is not we wanted, has no data in query range
                // and either not the end of scan, set status to NEED_FORWARD 
                // tell fetch_next_block call this function again.
                // it happens only in reverse scan and query_range.end_key in 
                // (prev_block.end_key < query_range.end_key < current_block.start_key)
                iterate_status_ = ITERATE_NEED_FORWARD;
                TBSYS_LOG(DEBUG, "current block has no data, but maybe in next block."
                    "it could happen when reverse search and endkey fall into the hole.");
              }
              iret = OB_SUCCESS;
            }
            else
            {
              iterate_status_ = ITERATE_IN_ERROR;
              TBSYS_LOG(ERROR, "block scaner initialize error, iret=%d," 
                  "block_data(sz=%ld,style=%d)", iret, block_data_size, 
                  sstable_reader_->get_trailer().get_row_value_store_style());  
            }
          }
          else
          {
            iterate_status_ = ITERATE_IN_ERROR;
            TBSYS_LOG(ERROR, "get current block data error, iret=%d, cursor_=%ld, block count=%ld,"
                "is_reverse_scan=%d", iret, index_array_cursor_, index_array_.block_count_, 
                scan_param_->is_reverse_scan());
          }
        }
      }

      return iret;
    }

    int ObColumnGroupScanner::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      row_desc = &row_desc_;
      return ret;
    }

    //add whx, [bugfix: free memory]20161030:b
    void ObColumnGroupScanner::free_buffer()
    {
      common::ModuleArena * arena = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
      if (NULL != arena)
      {
        arena->free();
        TBSYS_LOG(DEBUG, "test::whx free arean!");
      }
    }
    //add e

  }//end namespace sstable
}//end namespace oceanbase
