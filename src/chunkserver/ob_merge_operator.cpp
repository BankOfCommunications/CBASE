/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This proram is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merge_operator.cpp for implementation of merge operator. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_merge_operator.h"
#include "common/ob_action_flag.h"
#include "common/ob_tsi_factory.h"
#include "common/utility.h"
#include "ob_read_param_modifier.h"
#include "ob_row_cell_vec.h"
#include "ob_get_param_cell_iterator.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    void ObMergeOperator::initialize()
    {
      ups_stream_ = NULL;
      request_finished_ = false;
      is_need_query_ups_ = true;
      is_need_join_ = true;
      is_unchanged_ = false;
      unmerge_if_unchanged_ = false;
      data_version_ = 0;
      scan_param_ = NULL;
      get_param_ = NULL;
      req_param_ = NULL;
      result_array_ = NULL;
      cur_get_param_.reset();
      got_cell_num_ = 0;
      cur_scan_param_.reset();
      cur_cs_result_.reset();
      merger_iterator_moved_ = false;
      merger_.reset();
      memset(&column_index_, 0, sizeof(column_index_));
      memset(&same_column_next_, -1, sizeof(same_column_next_));
      merger_iter_ = &merger_;
      cur_cell_initializer_.reset();
      is_static_truncated_ = false; /*add zhaoqiong [Truncate Table]:20160318*/
    }
    
    ObMergeOperator::ObMergeOperator(ObMergerRpcProxy &rpc_proxy)
      :cs_scan_buffer_(DEFAULT_RANGE_MEMBUF_SIZE), 
      ups_scan_buffer_(DEFAULT_RANGE_MEMBUF_SIZE),
      rpc_proxy_(&rpc_proxy)
    {
      initialize();
    }
    
    ObMergeOperator::~ObMergeOperator()  
    {
      clear();
    }
    
    bool ObMergeOperator::is_request_finished() const 
    {
      return request_finished_;
    }

    int64_t ObMergeOperator::get_data_version() const
    {
      return data_version_;
    }
    
    void ObMergeOperator::clear()
    {
      initialize();
    }

    int ObMergeOperator::start_merge(
        ObCellArray& result_array,
        const ObReadParam &read_param,
        ObCellStream &ups_stream, 
        const ObSchemaManagerV2 &schema_mgr,
        const int64_t max_memory_size,
        const bool unmerge_if_unchanged,
        const bool is_static_truncated) /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/
    {
      int err = OB_SUCCESS;
      is_static_truncated_ = is_static_truncated; /*add zhaoqiong [Truncate Table]:20160318*/
      /**
       * if dynamic cast fail, it returns NULL
       */
      scan_param_ = dynamic_cast<const ObScanParam*>(&read_param);
      get_param_ = dynamic_cast<const ObGetParam*>(&read_param);
      if (NULL == scan_param_ && NULL == get_param_)
      {
        TBSYS_LOG(WARN, "wrong get param or scan param, can't dynamic cast");
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        ups_stream_ = &ups_stream;
        schema_mgr_ = &schema_mgr;
        result_array_ = &result_array;
        max_memory_size_ = max_memory_size;
        unmerge_if_unchanged_ = unmerge_if_unchanged;
        
        if (NULL != scan_param_)
        {
          err = build_scan_column_index(*scan_param_, schema_mgr);
        }
        else if (NULL != get_param_)
        {
          //get operation just ignore max_memory_size
          max_memory_size_ = -1;
          req_param_ = get_param_;
          param_contain_duplicated_columns_ = true;
          is_need_join_ = true;
        }
      }

      if (OB_SUCCESS == err)
      {
        err = get_next_rpc_result();
        if (OB_ITER_END == err)
        {
          result_array_->reset();
          err = OB_SUCCESS;
        }
      }

      return err;
    }

    int ObMergeOperator::build_scan_column_index(
        const ObScanParam &scan_param, 
        const ObSchemaManagerV2 &schema_mgr)
    {
      int err = OB_SUCCESS;
      const ObColumnSchemaV2 *column_schema = NULL;
      int32_t column_idx = -1;
      int32_t column_size = static_cast<int32_t>(scan_param.get_column_id_size());
      const uint64_t* const column_ids = scan_param.get_column_id();

      param_contain_duplicated_columns_ = false;
      is_need_join_ = false;

      for (int32_t i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
      {
        batch_append_cells_in_[i] = &cur_cell_initializer_;
      }

      if (OB_SUCCESS == err)
      {
        cur_cell_initializer_.table_id_ = scan_param.get_table_id();
        for (int32_t cell_idx = 0; 
             cell_idx < column_size && OB_SUCCESS == err; cell_idx++)
        {
          column_schema = schema_mgr.get_column_schema(cur_cell_initializer_.table_id_,
                                                       column_ids[cell_idx], &column_idx);
          if (NULL == column_schema || column_idx < 0 
              || column_idx >= OB_MAX_COLUMN_NUMBER)
          {
            TBSYS_LOG(WARN, "unexpected error, fail to get column schema [table_id:%lu,"
                             "column_id:%lu, column_idx:%d,column_schema:%p]",
              cur_cell_initializer_.table_id_, column_ids[cell_idx], column_idx, column_schema);
            err = OB_ERR_UNEXPECTED;
          }
          else
          {
            if (column_index_[column_idx].size_ <= 0)
            {
              column_index_[column_idx].head_offset_ = static_cast<int16_t>(cell_idx);
              column_index_[column_idx].tail_offset_ = static_cast<int16_t>(cell_idx);
              column_index_[column_idx].size_ = 1;
            }
            else
            {
              same_column_next_[column_index_[column_idx].tail_offset_] = static_cast<int16_t>(cell_idx);
              column_index_[column_idx].tail_offset_ = static_cast<int16_t>(cell_idx);
              column_index_[column_idx].size_++;
            }
          }

          if (OB_SUCCESS == err)
          {
            if (!is_need_join_)
            {
              is_need_join_ = (NULL != column_schema->get_join_info());
            }
            if (column_index_[column_idx].size_ > 1)
            {
              param_contain_duplicated_columns_ = true;
            }
          }
        }
      }

      return err;
    }
        
    void ObMergeOperator::move_to_next_row(
        const ObGetParam & row_spec_arr,
        int64_t &cur_row_idx, 
        int64_t &cur_row_beg,
        int64_t &cur_row_end)
    {
      if (cur_row_idx < row_spec_arr.get_row_size())
      {
        const ObGetParam::ObRowIndex* row_index = row_spec_arr.get_row_index();
        cur_row_beg = row_index[cur_row_idx].offset_;
        cur_row_end = row_index[cur_row_idx].offset_ + row_index[cur_row_idx].size_;
        cur_row_idx++;
      }
    }

    int ObMergeOperator::apply_whole_row(const ObCellInfo &cell, const int64_t row_beg)
    {
      int err = OB_SUCCESS;
      int adjusted_non_exist_cell_num = 0;
      int64_t cell_idx = row_beg;
      ObInnerCellInfo *dst_cell = NULL;
      int64_t dest_idx = -1;
      const ObColumnSchemaV2 *column_schema = NULL;
      int32_t column_idx = -1;
    
      if (OB_INVALID_ID != cell.column_id_ && NULL != scan_param_)
      {
        column_schema = schema_mgr_->get_column_schema(cell.table_id_, cell.column_id_, 
                                                       &column_idx);
        if (NULL == column_schema || column_idx < 0 || column_idx >= OB_MAX_COLUMN_NUMBER)
        {
          TBSYS_LOG(ERROR, "unexpected error, fail to get column schema [table_id:%lu,"
                           "column_id:%lu, column_idx:%d,column_schema:%p]",
            cell.table_id_, cell.column_id_, column_idx, column_schema);
          err = OB_ERR_UNEXPECTED;
        }

        if (OB_SUCCESS == err)
        {
          dest_idx = column_index_[column_idx].head_offset_;
          for (int64_t i = 0; i < column_index_[column_idx].size_ && OB_SUCCESS == err; ++i)
          {
            if (dest_idx >= result_array_->get_cell_size() - row_beg || dest_idx < 0)
            {
              TBSYS_LOG(WARN,"dest idx err [dest_idx:%ld,column_id:%ld, "
                             "get_cell_size():%ld,row_beg:%ld]",
                dest_idx, cell.column_id_, result_array_->get_cell_size(),row_beg);
              err = OB_ERR_UNEXPECTED;
            }
            else
            {
              err = result_array_->get_cell(row_beg + dest_idx,dst_cell);
              if (OB_SUCCESS == err)
              {
                if (cell.column_id_ == dst_cell->column_id_)
                {
                  err = result_array_->apply(cell,dst_cell);
                  if (OB_SUCCESS != err)
                  {
                    TBSYS_LOG(WARN,"fail to apply mutation [err:%d]", err);
                  }
                }
                else
                {
                  TBSYS_LOG(WARN,"unexpected error, column id not coincident");
                  err = OB_ERR_UNEXPECTED;
                }
              }
              else
              {
                TBSYS_LOG(WARN,"fail to get dest cell from cell array [err:%d]", err);
              }
            }
            if (OB_SUCCESS == err)
            {
              dest_idx = same_column_next_[dest_idx];
            }
          }
        }
      }
      else
      {
        int64_t cell_size = result_array_->get_cell_size();
        for (; OB_SUCCESS == err && cell_idx < cell_size; ++cell_idx)
        {
          err = result_array_->get_cell(cell_idx,dst_cell);
          if (OB_SUCCESS == err)
          {
            if (NULL == scan_param_ && dst_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
            {
              dst_cell->value_.set_null();
              adjusted_non_exist_cell_num ++;
            }

            if (cell.column_id_ == dst_cell->column_id_)
            {
              err = result_array_->apply(cell,dst_cell);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN,"fail to apply mutation [err:%d]", err);
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN,"fail to get dest cell from cell array [err:%d]", err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        err = adjusted_non_exist_cell_num;
      }

      return err;
    }

    int ObMergeOperator::append_fake_composite_column()
    {
      int err = OB_SUCCESS;
      int64_t comp_column_size = 0;

      /**
       * for scan operation, maybe the scan param includes composite 
       * column infos, when we caculate the composite columns, we also
       * use the same resut cell array, so we add some fake cells to 
       * reverse some space for composite columns in result cell array
       * after merge one row. after we merge and join with this result
       * cell array, we caculate the actual value of composite columns. 
       */
      if (NULL != scan_param_)
      {
        comp_column_size = scan_param_->get_composite_columns_size();
        if (comp_column_size > 0 && comp_column_size <= OB_MAX_COLUMN_NUMBER)
        {
          cur_cell_initializer_.column_id_ = OB_INVALID_ID;
          cur_cell_initializer_.value_.set_null();
          err = result_array_->batch_append(batch_append_cells_in_, 
            batch_append_cells_out_, comp_column_size);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail to append fake composite column cell err=%d,"
                            "comp_column_size=%ld", err, comp_column_size);
          }
        }
        else if (comp_column_size > OB_MAX_COLUMN_NUMBER)
        {
          TBSYS_LOG(WARN, "composite columns size is bigger than max columns size, "
                          "comp_column_size=%ld, OB_MAX_COLUMN_NUMBER=%ld",
            comp_column_size, OB_MAX_COLUMN_NUMBER);
          err = OB_ERROR;
        }
      }

      return err;
    }
    
    int ObMergeOperator::merge_next_row(int64_t &cur_row_idx, int64_t &cur_row_beg, int64_t &cur_row_end)
    {
      int err = OB_SUCCESS;
      ObCellInfo *cur_cell = NULL;
      bool keep_deleted_row = (NULL != get_param_);
      bool first_cell_got = false;
      bool row_changed = false;
      bool keep_move = true;
      bool iterator_end = false;
      move_to_next_row(*req_param_, cur_row_idx, cur_row_beg, cur_row_end);
      int64_t src_idx = cur_row_beg;
      ObInnerCellInfo *cell_out = NULL;
      int64_t not_exist_cell_num  = 0;
      int64_t exist_cell_num_got = 0;
      int64_t ext_val = 0;
      int64_t prev_cell_size = 0;

      if ((OB_SUCCESS == err) && (cur_row_beg < req_param_->get_cell_size()))
      {
        cur_cell_initializer_.table_id_ = req_param_->operator [](cur_row_beg)->table_id_;
        prev_cell_size = result_array_->get_cell_size();
      }

      while (OB_SUCCESS == err)
      {
        // move iterator
        if (!merger_iterator_moved_ && keep_move)
        {
          err  = merger_iter_->next_cell();
          if (OB_SUCCESS != err && OB_ITER_END != err)
          {
            TBSYS_LOG(WARN,"fail to move to next cell in merger [err:%d]", err);
          }
          else if (OB_SUCCESS == err)
          {
            merger_iterator_moved_ = true;
          }
        }

        // get current cell
        if (OB_SUCCESS == err && keep_move)
        {
          err = merger_iter_->get_cell(&cur_cell, &row_changed);
          if (OB_SUCCESS == err)
          {
            if (row_changed && !first_cell_got)
            {
              cur_cell_initializer_.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
              cur_cell_initializer_.row_key_ = cur_cell->row_key_;
            }

            if (OB_SUCCESS == err)
            {
              if (row_changed && first_cell_got)
              {
                // this row process was completed
                break;
              }
              else
              {
                merger_iterator_moved_ = false;
                if (first_cell_got)
                {
                  cur_cell->row_key_ = cur_cell_initializer_.row_key_;
                }
              }
            }
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get cell from merger [err:%d]", err);
          }
          else
          {
            // do nothing
          }
        }
    
        // bugfix by xielun.szd 20110805
        if (OB_SUCCESS == err)
        {
          ext_val = cur_cell->value_.get_ext();
        }
    
        // process current cell
        if (OB_SUCCESS == err 
          && (((ext_val != ObActionFlag::OP_DEL_ROW)
          && (ext_val != ObActionFlag::OP_DEL_TABLE)
          && (ext_val != ObActionFlag::OP_ROW_DOES_NOT_EXIST))
          || keep_deleted_row))
        {
          if ((ext_val != ObActionFlag::OP_DEL_ROW)
            && (ext_val != ObActionFlag::OP_DEL_TABLE)
            && (ext_val != ObActionFlag::OP_ROW_DOES_NOT_EXIST)
            )
          {
            if (keep_move)
            {
              exist_cell_num_got ++;
            }
          }
          else
          {
            cur_cell->column_id_ = OB_INVALID_ID;
          }

          if (src_idx < cur_row_end) // initialize stage
          {
            if (!param_contain_duplicated_columns_ 
              && req_param_->operator [](src_idx)->column_id_ == cur_cell->column_id_)
            {
              // binary merge
              err = result_array_->append(*cur_cell,cell_out);
              if (OB_SUCCESS == err)
              {
                // when data only exist on ups, OB semantic
                cell_out->value_.reset_op_flag();
                keep_move = true;
              }
              else
              {
                TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
              }
            }
            else
            {
              // append one empty cell
              keep_move = false;
              cur_cell_initializer_.column_id_  = req_param_->operator [](src_idx)->column_id_;
              err = result_array_->append(cur_cell_initializer_,cell_out);
              if (OB_SUCCESS == err)
              {
                if (exist_cell_num_got > 0)
                {
                  cell_out->value_.set_null();
                }
                else
                {
                  not_exist_cell_num ++;
                }
              }
              else
              {
                TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
              }
            }

            if (OB_SUCCESS == err)
            {
              src_idx ++;
            }

            if (!first_cell_got && OB_SUCCESS == err)
            {
              first_cell_got = true;
              cur_cell_initializer_.row_key_ = cell_out->row_key_;
            }
          }
          else
          { // cells in current row are initialized
            if ((ext_val == ObActionFlag::OP_DEL_ROW)
              || (ext_val == ObActionFlag::OP_DEL_TABLE)
              || (ext_val == ObActionFlag::OP_ROW_DOES_NOT_EXIST))
            {
              // just jump
              keep_move = true;
            }
            else
            {
              err = apply_whole_row(*cur_cell, result_array_->get_cell_size() - (cur_row_end - cur_row_beg));
              if (err >= 0)
              {
                not_exist_cell_num -= err;
                keep_move = true;
                err = OB_SUCCESS;
              }
              else
              {
                TBSYS_LOG(WARN,"fail to apply whole row [err:%d]", err);
              }
            }
          }
        }
        else
        {
          // do nothing
          keep_move = true;
        }
      }// end while

      if (OB_SUCCESS == err || OB_ITER_END == err)
      {
        if (OB_ITER_END == err)
        {
          err = OB_SUCCESS;
          iterator_end = true;
        }

        if (first_cell_got 
          && (exist_cell_num_got > 0 || keep_deleted_row)
          && src_idx < cur_row_end)
        {
          for (;src_idx < cur_row_end && OB_SUCCESS == err; src_idx ++)
          {
            cur_cell_initializer_.column_id_  = req_param_->operator [](src_idx)->column_id_;
            err = result_array_->append(cur_cell_initializer_,cell_out);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to append initialize cell [err:%d]", err);
            }
            else
            {
              if (exist_cell_num_got > 0)
              {
                cell_out->value_.set_null();
              }
              else
              {
                not_exist_cell_num ++;
              }
            }
          }
        }
      }
    
      if (OB_SUCCESS == err || OB_ITER_END == err)
      {
        if (not_exist_cell_num > 0 && exist_cell_num_got > 0)
        {
          cur_cell_initializer_.value_.set_ext(ObActionFlag::OP_NOP);
          cur_cell_initializer_.column_id_ = OB_INVALID_ID;
          err = apply_whole_row(cur_cell_initializer_, result_array_->get_cell_size() - (cur_row_end - cur_row_beg));
          if (err >= 0)
          {
            not_exist_cell_num -= err;
            if (0 != not_exist_cell_num)
            {
              TBSYS_LOG(ERROR, "fatal error, algorithm error [not_exist_cell_num:%ld]", not_exist_cell_num);
              err = OB_ERR_UNEXPECTED;
            }
            else
            {
              err = OB_SUCCESS;
            }
          }
          else
          {
            TBSYS_LOG(WARN,"fail to apply whole row [err:%d]", err);
          }
        }
      }

      /**
       * add some new cell into result array, append some fake 
       * composite column if necessary 
       */
      if (OB_SUCCESS == err && !request_finished_
          && result_array_->get_cell_size() > prev_cell_size)
      {
        err = append_fake_composite_column();
      }

      if (OB_SUCCESS == err && iterator_end)
      {
        err = OB_ITER_END;
      }

      return err;
    }

    inline bool ObMergeOperator::is_del_op(const int64_t ext_val)
    {
      return ((ext_val == ObActionFlag::OP_DEL_ROW)
              || (ext_val == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
              || (ext_val == ObActionFlag::OP_DEL_TABLE));
    }

    int ObMergeOperator::scan_merge_row_fast_path()
    {
      int err                     = OB_SUCCESS;
      ObCellInfo *cur_cell        = NULL;
      ObInnerCellInfo *cell_out        = NULL;
      bool first_cell_got         = false;
      bool row_changed            = false;
      bool iterator_end           = false;
      int64_t src_idx             = 0;
      const uint64_t* column_ids  = scan_param_->get_column_id();
      int64_t row_width           = scan_param_->get_column_id_size();
      int64_t prev_cell_size      = result_array_->get_cell_size();

      while (OB_SUCCESS == err)
      {
        // move iterator
        if (!merger_iterator_moved_)
        {
          err = merger_iter_->next_cell();
          if (OB_SUCCESS != err && OB_ITER_END != err)
          {
            TBSYS_LOG(WARN,"fail to move to next cell in merger [err:%d]", err);
          }
          else if (OB_SUCCESS == err)
          {
            merger_iterator_moved_ = true;
          }
        }

        // get current cell
        if (OB_SUCCESS == err)
        {
          err = merger_iter_->get_cell(&cur_cell, &row_changed);
          if (OB_SUCCESS == err)
          {
            if (row_changed && !first_cell_got)
            {
              cur_cell_initializer_.value_.set_null();
              cur_cell_initializer_.row_key_ = cur_cell->row_key_;
            }
            if (row_changed && first_cell_got)
            {
                // this row process was completed
              break;
            }
            else
            {
              merger_iterator_moved_ = false;
              if (first_cell_got)
              {
                cur_cell->row_key_ = cur_cell_initializer_.row_key_;
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN,"fail to get cell from merger [err:%d]", err);
          }
        }
    
        // process current cell
        if (OB_SUCCESS == err && !is_del_op(cur_cell->value_.get_ext()))
        {
          if (src_idx < row_width) // initialize stage
          {
            /**
             * if there is only one column group, the column order returned 
             * by chunkserver is same as the scan param, even through there 
             * are some repeated columns, but the update server only returns 
             * the same column once, and the column order is same as schema, 
             * not same as scan param. 
             */
            if (!param_contain_duplicated_columns_ 
                && column_ids[src_idx] == cur_cell->column_id_)
            {
              // binary merge
              err = result_array_->append(*cur_cell, cell_out, 
                cur_cell_initializer_.row_key_, !first_cell_got);
              if (OB_SUCCESS == err)
              {
                // when data only exist on ups, OB semantic
                cell_out->value_.reset_op_flag();
                src_idx ++;
              }
              else
              {
                TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
              }
            }
            else
            {
              // append one empty cell
              int64_t batch_append_cnt = row_width - src_idx;
              err = result_array_->batch_append(batch_append_cells_in_, 
                batch_append_cells_out_, batch_append_cnt);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN,"fail to batch append initialize cell, "
                               "batch_append_cnt=%ld [err:%d]", 
                  batch_append_cnt, err);
              }
              else 
              {
                for (int64_t i = 0; i < batch_append_cnt; ++i, src_idx ++)
                {
                  batch_append_cells_out_[i]->column_id_ = column_ids[src_idx];
                }
                cell_out = batch_append_cells_out_[0];
                err = apply_whole_row(*cur_cell, result_array_->get_cell_size() - row_width);
                if (0 == err)
                {
                  err = OB_SUCCESS;
                }
                else
                {
                  TBSYS_LOG(WARN,"fail to apply whole row [err:%d]", err);
                }
              }
            }

            if (!first_cell_got && OB_SUCCESS == err)
            {
              first_cell_got = true;
              cur_cell_initializer_.row_key_ = cell_out->row_key_;
            }
          }
          else
          { // cells in current row are initialized
            err = apply_whole_row(*cur_cell, result_array_->get_cell_size() - row_width);
            if (0 == err)
            {
              err = OB_SUCCESS;
            }
            else
            {
              TBSYS_LOG(WARN,"fail to apply whole row [err:%d]", err);
            }
          }
        }
      }// end while

      if (OB_SUCCESS == err || OB_ITER_END == err)
      {
        if (OB_ITER_END == err)
        {
          err = OB_SUCCESS;
          iterator_end = true;
        }

        if (result_array_->get_cell_size() > prev_cell_size
            && src_idx < row_width)
        {
          /**
           * after merge all the row, maybe only fill part of the row, we 
           * need append the left columns with null cell. 
           *   for example: the row is not existent in chunk server, only 
           * part of the row is in update server  
           */
          int64_t batch_append_cnt = row_width - src_idx;
          err = result_array_->batch_append(batch_append_cells_in_, 
            batch_append_cells_out_, batch_append_cnt);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to batch append initialize cell, "
                           "batch_append_cnt=%ld [err:%d]", 
              batch_append_cnt, err);
          }
          else 
          {
            for (int64_t i = 0; i < batch_append_cnt; ++i, src_idx ++)
            {
              batch_append_cells_out_[i]->column_id_ = column_ids[src_idx];
            }
          }
        }

        /**
         * add some new cell into result array, append some fake 
         * composite column if necessary 
         */
        if (OB_SUCCESS == err && !request_finished_
            && result_array_->get_cell_size() > prev_cell_size
            && OB_SUCCESS != (err = append_fake_composite_column()))
        {
          TBSYS_LOG(WARN, "failed to append_fake_composite_column, err=%d", err);
        }

        if (OB_SUCCESS == err && iterator_end)
        {
          err = OB_ITER_END;
        }
      }

      return err;
    }
    
    int ObMergeOperator::merge()
    {
      int err             = OB_SUCCESS;
      int64_t cur_row_idx = 0;
      int64_t cur_row_beg = 0;
      int64_t cur_row_end = 0;
      int64_t got_row_count = 0;
      int64_t limit_count = 0;
      int64_t limit_offset = 0;
      int64_t max_scan_rows = 0;
      bool need_fetch_all_result = true;
      
      if (NULL != scan_param_)
      {
        scan_param_->get_limit_info(limit_offset, limit_count);
        limit_count += limit_count / 10;
        need_fetch_all_result = ((scan_param_->get_group_by_param().get_aggregate_row_width() > 0) 
            || (scan_param_->get_orderby_column_size() > 0));
        max_scan_rows = GET_SCAN_ROWS(scan_param_->get_scan_size());
      }

      while (OB_SUCCESS == err)
      {
        if (NULL != scan_param_)
        {
          err = scan_merge_row_fast_path();
          if (OB_SUCCESS == err)
          {
            got_row_count++;
            if (!need_fetch_all_result 
                && ((limit_count > 0) && (limit_count + limit_offset <= got_row_count)))
            {
              break;
            }
            else if ((max_scan_rows > 0) && (max_scan_rows <= got_row_count))
            {
              break;
            }
          }
        }
        else
        {
          err = merge_next_row(cur_row_idx, cur_row_beg, cur_row_end);
        }
        if (OB_SUCCESS == err && max_memory_size_ > 0 
            && result_array_->get_real_memory_used() > max_memory_size_)
        {
          break;
        }
      }

      if (OB_ITER_END == err)
      {
        /**
         * FIXME: currently we just assume that merge join agent only 
         * handle one tablet range or one obscanner got by cs_get(), 
         * after iterate all the data in iterator, we just think the 
         * merge join process completes. 
         */
        request_finished_ = true;
        err = OB_SUCCESS;
      }

      return err;
    }

    int ObMergeOperator::get_cell(ObInnerCellInfo** cell)
    {
      return result_array_->get_cell(cell);
    }

    int ObMergeOperator::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
    {
      return result_array_->get_cell(cell, is_row_changed);
    }
    
    int ObMergeOperator::next_cell()
    {
      return result_array_->next_cell();
    }
    
    int ObMergeOperator::get_next_rpc_result()
    {
      int err = OB_SUCCESS;

      if (NULL != get_param_)
      {
        err = get_next_get_rpc_result();
      }
      else if (NULL != scan_param_)
      {
        err = get_next_scan_rpc_result();
      }
      else
      {
        TBSYS_LOG(WARN, "cell stream was not initilized");
        err = OB_ITER_END;
      }

      if (OB_ITER_END == err)
      {
        request_finished_ = true;
      }

      if (OB_SUCCESS == err)
      {
        err = do_merge();
      }

      return err;
    }
    
    int ObMergeOperator::get_next_get_rpc_result()
    {
      int err = OB_SUCCESS;
      ObReadParam &cur_read_param = cur_get_param_;
      ObCellArrayHelper cur_get_cells(cur_get_param_);
      const ObReadParam &org_read_param = *get_param_;
      ObCellArrayHelper get_cells(*get_param_);
      err = get_next_param(org_read_param,get_cells,got_cell_num_,&cur_get_param_);
      ObIterator *cur_cs_cell_it[OB_MAX_ITERATOR];
      int64_t it_size = OB_MAX_ITERATOR;

      // get result from cs
      if (OB_SUCCESS == err)
      {
        err = rpc_proxy_->cs_get(cur_get_param_, cur_cs_result_, cur_cs_cell_it,it_size);
        TBSYS_LOG(DEBUG, "OP:cs_get [got_cell_num:%ld,cur_get_cell_num:%ld,data_version:%ld,res:%d,it_size:%ld]",
                  got_cell_num_, cur_get_param_.get_cell_size(),cur_cs_result_.get_data_version(), err,it_size);
        if (OB_SUCCESS == err)
        {
          int64_t now_got_cell_num = 0;
          bool fullfilled = false;
          err = cur_cs_result_.get_is_req_fullfilled(fullfilled,now_got_cell_num);
          now_got_cell_num += got_cell_num_;
        }
      }

      if (OB_SUCCESS == err)
      {
        err = get_ups_param(cur_get_param_, cur_cs_result_);
        if (OB_SUCCESS == err)
        {
          is_need_query_ups_ = true;
          err = ups_stream_->get(cur_read_param,cur_get_cells);
        }
        if (OB_ITER_END == err)
        {
          err = OB_SUCCESS;
          is_need_query_ups_ = false;
          ups_stream_->reset();
        }
      }

      if (OB_SUCCESS == err)
      {
        merger_.reset();
        if (NULL != scan_param_)
        {
          merger_.set_asc(scan_param_->get_scan_direction() == ScanFlag::FORWARD);
        }
        for(int64_t i=0; i < it_size; ++i)
        {
          err = merger_.add_iterator(cur_cs_cell_it[i]);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = merger_.add_iterator(ups_stream_);
      }

      /**
       * no need read ups or ups return empty scanner, don't use the 
       * ObMerger to merge the result, only iterate the cs result. 
       */
      if (OB_SUCCESS == err && !is_need_query_ups_ && NULL != cur_cs_cell_it && 1 == it_size)
      {
        merger_iter_ = cur_cs_cell_it[0];
      }
      else 
      {
        merger_iter_ = &merger_;
      }

      if (OB_SUCCESS == err)
      {
        bool is_fullfilled = false;
        int64_t fullfilled_cell_num  = 0;
        err = cur_cs_result_.get_is_req_fullfilled(is_fullfilled, fullfilled_cell_num);
        if (OB_SUCCESS == err)
        {
          got_cell_num_ += fullfilled_cell_num;
        }
        req_param_ = &cur_get_param_;
      }

      return err;
    }
    
    int ObMergeOperator::get_next_scan_rpc_result()
    {
      int err = OB_SUCCESS;
      const ObNewRange *org_scan_range = scan_param_->get_range();
      ObIterator *cur_cs_cell_it[OB_MAX_ITERATOR];
      int64_t it_size = OB_MAX_ITERATOR;
      bool ups_scan_iter_end = false;
	  
      if (NULL == org_scan_range)
      {
        TBSYS_LOG(WARN, "%s", "unexpected error, fail to get range from scan param");
        err = OB_INVALID_ARGUMENT;
      }
    
      if (OB_SUCCESS == err)
      {
        err = get_next_param(*scan_param_,cur_cs_result_, &cur_scan_param_, cs_scan_buffer_); 
      }
    
      if (OB_SUCCESS == err )
      {
        //mod zhaoqiong [Truncate Table]:20160318:b
        //err = rpc_proxy_->cs_scan(cur_scan_param_, cur_cs_result_, cur_cs_cell_it,it_size);
        //TBSYS_LOG(DEBUG, "OP:cs_scan [data_version:%ld,res:%d]", cur_cs_result_.get_data_version(), err);
        if (!is_static_truncated_)
        {
          err = rpc_proxy_->cs_scan(cur_scan_param_, cur_cs_result_, cur_cs_cell_it,it_size);
          TBSYS_LOG(DEBUG, "OP:cs_scan [data_version:%ld,res:%d]", cur_cs_result_.get_data_version(), err);

        }
        else
        {
          cur_cs_result_.reset();
          it_size = 0;
        }
        //mod:e
      }
    
      if (OB_SUCCESS == err)
      {
        //mod zhaoqiong [Truncate Table]:20160318:b
        //err = get_ups_param(cur_scan_param_,cur_cs_result_, ups_scan_buffer_);
        if (!is_static_truncated_)
        {
          err = get_ups_param(cur_scan_param_,cur_cs_result_, ups_scan_buffer_);
        }
        else
        {
          err = get_ups_param(cur_scan_param_,ups_scan_buffer_);
          //err = get_ups_param_truncate(cur_scan_param_, ups_scan_buffer_);
        }
        //mod:e
        if (OB_SUCCESS == err)
        {
          is_need_query_ups_ = true;
          cur_scan_param_.set_scan_size(scan_param_->get_scan_size() & 0xFFFFFFFF);
          err = ups_stream_->scan(cur_scan_param_);
          if (OB_ITER_END == err)
          {
            err = OB_SUCCESS;
            ups_stream_->reset();
            ups_scan_iter_end = true;
          }
        }
        else if (OB_ITER_END == err)
        {
          err = OB_SUCCESS;
          is_need_query_ups_ = false;
          ups_stream_->reset();
        }
      }
    
      if (OB_SUCCESS == err)
      {
        merger_.reset();
        if (NULL != scan_param_)
        {
          merger_.set_asc(scan_param_->get_scan_direction() == ScanFlag::FORWARD);
        }
        for(int32_t i=0; i<it_size;++i)
        {
          merger_.add_iterator(cur_cs_cell_it[i]);
        }
        merger_.add_iterator(ups_stream_);
      }

      /**
       * no need read ups or ups return empty scanner, don't use the 
       * ObMerger to merge the result, only iterate the cs result. 
       */
      if (OB_SUCCESS == err && (!is_need_query_ups_ || ups_scan_iter_end) 
          && ((NULL != cur_cs_cell_it) && (1 == it_size)) )
      {
        merger_iter_ = cur_cs_cell_it[0];
        if (unmerge_if_unchanged_ && NULL != scan_param_ && !is_need_join_)
        {
          is_unchanged_ = true;
        }
      }
      else 
      {
        merger_iter_ = &merger_;
      }

      return err;
    }
    
    int ObMergeOperator::do_merge()
    {
      int err = OB_SUCCESS;
    
      if (NULL == schema_mgr_)
      {
        TBSYS_LOG(WARN, "please set argument first");
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err && !is_unchanged_)
      {
        err = merge();
      }

      if (OB_SUCCESS == err)
      {
        if (is_need_query_ups_)
        {
          data_version_ = ups_stream_->get_data_version();
        }
        else
        {
          data_version_ = cur_cs_result_.get_data_version();
        }
      }

      return err;
    }

    const ObReadParam& ObMergeOperator::get_cur_read_param() const
    {
      if (NULL != scan_param_)
      {
        return cur_scan_param_;
      } 
      else
      {
        return cur_get_param_;
      }
    }

    bool ObMergeOperator::need_join() const
    {
      return (is_need_query_ups_ && is_need_join_);
    }

    bool ObMergeOperator::is_unchanged() const
    {
      return is_unchanged_;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
