/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merge_join_operator.cpp for implementation of merge and 
 * join operator. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_merge_join_operator.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;
    
    ObMergeJoinOperator::ObMergeJoinOperator(ObMergerRpcProxy &rpc_proxy) 
    : merge_operator_(rpc_proxy)
    {
      initialize();
    }

    ObMergeJoinOperator::~ObMergeJoinOperator()
    {
      clear();
    }

    void ObMergeJoinOperator::initialize()
    {
      is_need_join_ = true; 
      is_need_compose_ = false;
      merge_mem_size_ = -1;
      max_merge_mem_size_ = -1; 
      scan_param_  = NULL; 
      row_width_ = 0;
    }

    int ObMergeJoinOperator::start_merge_join(
        const ObReadParam &read_param, 
        ObCellStream &ups_stream, 
        ObCellStream &ups_join_stream, 
        const ObSchemaManagerV2 &schema_mgr, 
        const int64_t merge_mem_size,
        const int64_t max_merge_mem_size, 
        const bool unmerge_if_unchanged,
        const bool is_static_truncated ) /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/
    {
      int ret = OB_SUCCESS;

      /**
       * if dynamic cast fail, it returns NULL
       */
      scan_param_ = dynamic_cast<const ObScanParam*>(&read_param);
      merge_mem_size_ = merge_mem_size;
      max_merge_mem_size_ = max_merge_mem_size;
      if (NULL != scan_param_)
      {
        row_width_ = scan_param_->get_column_id_size()
             + scan_param_->get_composite_columns_size();
      }

      if (OB_SUCCESS == ret)
      {
        //mod zhaoqiong [Truncate Table]:20160318:b
//        ret = merge_operator_.start_merge(result_array_, read_param, ups_stream,
//                                          schema_mgr, merge_mem_size_,
//                                          unmerge_if_unchanged);
        ret = merge_operator_.start_merge(result_array_, read_param, ups_stream,
                                          schema_mgr, merge_mem_size_, 
                                          unmerge_if_unchanged, is_static_truncated);
        //mod:e
      }

      if (OB_SUCCESS == ret)
      {
        is_need_join_ = merge_operator_.need_join();
        if (is_need_join_)
        {
          ret = join_operator_.start_join(result_array_, 
                                          merge_operator_.get_cur_read_param(),
                                          ups_join_stream, schema_mgr);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to start join, ret=%d", ret);
          }
        }
      }

      if (OB_SUCCESS == ret && NULL != scan_param_)
      {
        int64_t comp_column_size = scan_param_->get_composite_columns_size();
        if (comp_column_size > 0)
        {
          is_need_compose_ = true;
          ret = compose_operator_.start_compose(
            result_array_, scan_param_->get_composite_columns(), row_width_);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to start compose, ret=%d", ret);
          }
        }

        if (result_array_.get_cell_size() % row_width_ != 0)
        {
          TBSYS_LOG(WARN, "the result array includes part row, "
                          "total_cell_size=%ld, row_width=%ld", 
                    result_array_.get_cell_size(), row_width_);
          ret = OB_ERROR;
        }
      }

      return ret;
    }
    
    int ObMergeJoinOperator::get_cell(ObInnerCellInfo** cell)
    {
      return result_array_.get_cell(cell);
    }
    
    int ObMergeJoinOperator::get_cell(ObInnerCellInfo** cell, bool *is_row_changed)
    {
      return result_array_.get_cell(cell, is_row_changed);
    }

    int ObMergeJoinOperator::do_merge_join()
    {
      int ret = OB_SUCCESS;

      result_array_.reset();
      ret = merge_operator_.do_merge();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to do merge, ret=%d", ret);
      }

      if (OB_SUCCESS == ret && is_need_join_)
      {
        ret = join_operator_.do_join();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to do join, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret && NULL != scan_param_ && is_need_compose_)
      {
        ret = compose_operator_.do_compose();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to do compose, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret && NULL != scan_param_)
      {
        if (result_array_.get_cell_size() % row_width_ != 0)
        {
          TBSYS_LOG(WARN, "the result array includes part row, "
                          "total_cell_size=%ld, row_width=%ld", 
                    result_array_.get_cell_size(), row_width_);
          ret = OB_ERROR;
        }
      }

      return ret;
    }
    
    int ObMergeJoinOperator::next_cell()
    {
      int ret = OB_SUCCESS;

      ret = result_array_.next_cell();
      if (OB_ITER_END == ret && !merge_operator_.is_request_finished())
      {
        ret = do_merge_join();
        if (OB_SUCCESS == ret)
        {
          ret = result_array_.next_cell();
        }
      }

      return ret;
    }

    void ObMergeJoinOperator::reset_result_array()
    {
      int64_t memory_size_water_mark = 
        std::max<int64_t>(merge_mem_size_, max_merge_mem_size_);
      if (memory_size_water_mark > 0 
          && result_array_.get_memory_size_used() > 2 * memory_size_water_mark)
      {
        result_array_.clear();
      }
      else
      {
        result_array_.reset();
      }
    }
    
    void ObMergeJoinOperator::clear()
    {
      reset_result_array();
      initialize();
      merge_operator_.clear();
      join_operator_.clear();
      compose_operator_.clear();
    }
    
    bool ObMergeJoinOperator::is_request_finished() const
    {
      return merge_operator_.is_request_finished();
    }

    int64_t ObMergeJoinOperator::get_data_version() const
    {
      return merge_operator_.get_data_version();
    }

    ObCellArray& ObMergeJoinOperator::get_cell_array()
    {
      return result_array_;
    }

    bool ObMergeJoinOperator::is_unchanged() const
    {
      return merge_operator_.is_unchanged();
    }
  } // end namespace chunkserver
} // end namespace oceanbase
