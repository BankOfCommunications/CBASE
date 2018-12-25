/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_query_agent.cpp for define query agent for get and scan 
 * operation.  
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_trace_log.h"
#include "ob_query_agent.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObQueryAgent::ObQueryAgent(ObMergerRpcProxy& proxy) 
    : merge_join_operator_(proxy)
    {
      inited_ = false;
      pfinal_result_ = NULL;
      timeout_time_ = 0;
    }
    
    ObQueryAgent::~ObQueryAgent()  
    {
      clear();
    }
    
    int ObQueryAgent::start_agent(
        const ObReadParam& read_param, 
        ObCellStream& ups_stream, 
        ObCellStream& ups_join_stream, 
        const ObSchemaManagerV2& schema_mgr, 
        const ObOperatorMemLimit& mem_limit,
        const int64_t timeout_time,
        const bool unmerge_if_unchanged, const bool is_static_truncated) /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/
    {
      int ret = OB_SUCCESS;
      clear();

      /**
       * if dynamic cast fail, it returns NULL
       */
      scan_param_ = dynamic_cast<const ObScanParam*>(&read_param);
      get_param_ = dynamic_cast<const ObGetParam*>(&read_param);
      if (NULL == scan_param_ && NULL == get_param_)
      {
        TBSYS_LOG(WARN, "wrong get param or scan param, can't dynamic cast");
        ret = OB_ERROR;
      }
      else
      {
        timeout_time_ = timeout_time;
        mem_limit_ = mem_limit;
        ups_stream.set_timeout_time(timeout_time);
        ups_join_stream.set_timeout_time(timeout_time);

        ret = build_query_stage();
        if (OB_SUCCESS == ret)
        {
          inited_ = true;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to build query stage, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        //mod zhaoqiong [Truncate Table]:20160318
//        ret = query_stage_machine(read_param, ups_stream, ups_join_stream,
//                                  schema_mgr, unmerge_if_unchanged);
        ret = query_stage_machine(read_param, ups_stream, ups_join_stream, 
          schema_mgr, unmerge_if_unchanged, is_static_truncated);
        //mod:e
        if (OB_ITER_END == ret)
        {
          //ignore OB_ITER_END, call next_cell() will return OB_ITER_END again
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }

   int ObQueryAgent::start_agent(
       ObCellArray& merge_join_result,
       const ObReadParam& read_param, 
       const int64_t max_memory_size, 
       const int64_t timeout_time)
   {
      int ret = OB_SUCCESS;
      clear();

      /**
       * if dynamic cast fail, it returns NULL
       */
      scan_param_ = dynamic_cast<const ObScanParam*>(&read_param);
      get_param_ = dynamic_cast<const ObGetParam*>(&read_param);
      if (NULL == scan_param_ && NULL == get_param_)
      {
        TBSYS_LOG(WARN, "wrong get param or scan param, can't dynamic cast");
        ret = OB_ERROR;
      }
      else
      {
        merge_join_result_ = &merge_join_result;
        merge_join_iter_ = &merge_join_result;
        pfinal_result_ = &merge_join_result;
        mem_limit_.merge_mem_size_ = max_memory_size;
        mem_limit_.max_merge_mem_size_ = max_memory_size;
        mem_limit_.groupby_mem_size_ = max_memory_size;
        mem_limit_.max_groupby_mem_size_ = max_memory_size;
        timeout_time_ = timeout_time;
        ret = build_query_stage();
        if (OB_SUCCESS == ret)
        {
          inited_ = true;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to build query stage, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = stage_two_machine();
        if (OB_ITER_END == ret)
        {
          //ignore OB_ITER_END, call next_cell() will return OB_ITER_END again
          ret = OB_SUCCESS;
        }
      }

      return ret;
   }
    
    bool ObQueryAgent::is_request_fullfilled()
    {
      return merge_join_operator_.is_request_finished();
    }

    int64_t ObQueryAgent::get_data_version() const
    {
      return merge_join_operator_.get_data_version();
    }

    int64_t ObQueryAgent::get_total_result_row_count() const
    {
      int64_t ret = -1;

      if (NULL != get_param_)
      {
        ret = get_param_->get_row_size();
      }
      else if (NULL != scan_param_)
      {
        if (need_groupby_ && NULL != cur_groupby_operator_ && row_width_ > 0)
        {
          ret = cur_groupby_operator_->get_cell_size() / row_width_;
        }
        else if (!need_groupby_ && NULL != merge_join_result_ && row_width_ > 0)
        {
          ret = merge_join_result_->get_cell_size() / row_width_;
        }
      }

      return ret;
    }
    
    void ObQueryAgent::clear()
    {
      inited_ = false;
      scan_param_ = NULL;
      get_param_ = NULL;

      merge_join_operator_.clear();
      groupby_operator_.clear();
      groupby_operator_topk_.clear();
      compose_operator_.clear();
      return_operator_.clear();

      cur_groupby_operator_ = &groupby_operator_;
      ahead_groupby_operator_ = &groupby_operator_topk_;

      merge_join_result_ = NULL;
      merge_join_iter_ = NULL;
      pfinal_result_ = NULL;

      mem_limit_.reset();
      row_width_ = 0;
      limit_count_ = 0;
      limit_offset_ = 0;
      sharding_min_row_cnt_ = 0;
      precision_ = 0.0;
      need_groupby_ = false;
      need_compute_topk_ = false;

      memset(&query_stage_, 0, sizeof(query_stage_) / sizeof(ObStageOperation));
      query_stage_size_ = 0;
      cur_stage_idx_ = 0;
    }

    int ObQueryAgent::build_query_stage()
    {
      int ret = OB_SUCCESS;

      /**
       * according to the get param and scan param, build the query 
       * plan, and store the operations in query stage array in order, 
       * then query stage machcine can do all the operation in the 
       * stage array one by one. 
       */

      if (NULL != get_param_)
      {
        if (NULL == merge_join_result_)
        {
          query_stage_[query_stage_size_++] = MERGE_JOIN_COMPOSE;
        }
        query_stage_[query_stage_size_++] = STAGE_END;
      }
      else if (NULL != scan_param_)
      {
        const ObGroupByParam& groupby_param = scan_param_->get_group_by_param();
        int64_t filter_count = scan_param_->get_filter_info().get_count();
        int64_t aggregate_row_width = groupby_param.get_aggregate_row_width();
        int64_t org_row_width = scan_param_->get_column_id_size()
                                + scan_param_->get_composite_columns_size();

        //init the final row width
        row_width_ = aggregate_row_width > 0 ? aggregate_row_width : org_row_width;

        int64_t groupby_comp_column_size = 
          groupby_param.get_composite_columns().get_array_index();
        int64_t groupby_filter_count = groupby_param.get_having_condition().get_count();
        int64_t orderby_count = scan_param_->get_orderby_column_size();
        ScanFlag::Direction scan_direction = scan_param_->get_scan_direction();

        //init limit offset, limit count, sharding_min_row_cnt and precision
        scan_param_->get_limit_info(limit_offset_, limit_count_);
        scan_param_->get_topk_precision(sharding_min_row_cnt_, precision_);

        int64_t return_info_size = scan_param_->get_return_info_size();
        int64_t groupby_return_info_size = 
          groupby_param.get_return_infos().get_array_index();
        int64_t return_size = groupby_return_info_size > 0 ? 
          groupby_return_info_size : return_info_size;

        if (groupby_filter_count > 0)
        {
          TBSYS_LOG(WARN, "the groupby param includes filter info, "
                          "chunk server can't handle, groupby_filter_count=%ld", 
                    groupby_filter_count);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (return_size != row_width_)
        {
          TBSYS_LOG(WARN, "invalid scan param, the return info size "
                          "is not equal to row column size, return_info_size=%ld, "
                          "row_width_=%ld", 
                    return_size, row_width_);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (limit_offset_ < 0 || limit_count_ < 0 || precision_ < 0
                 || sharding_min_row_cnt_ < 0)
        {
          TBSYS_LOG(WARN, "invalid scan param, limit_offset_=%ld, limit_count_=%ld, "
                          "precision_=%lf, sharding_min_row_cnt_=%ld",
            limit_offset_, limit_count_, precision_, sharding_min_row_cnt_);
          ret = OB_ERR_UNEXPECTED;
        }
        else if ((0 == orderby_count && (sharding_min_row_cnt_ > 0 || precision_ > 0))
                 || (orderby_count > 0 && 0 == limit_offset_ && limit_count_ > 0
                     && (sharding_min_row_cnt_ > 0 || precision_ > 0)))
        {
          TBSYS_LOG(WARN, "invalid scan param, orderby_count=%ld, "
                          "limit_count_=%ld, sharding_min_row_cnt_=%ld, "
                          "precision_=%lf", 
                    orderby_count, limit_count_, sharding_min_row_cnt_, precision_);
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          if (NULL == merge_join_result_)
          {
            //start with the MERGE_JOIN_COMPOSE stage
            query_stage_[query_stage_size_++] = MERGE_JOIN_COMPOSE;
          }

          //check if it need do group by
          if (filter_count > 0)
          {
            need_groupby_ = true;
            query_stage_[query_stage_size_++] = FILTER_AND_GROUP_BY;
          }
          else if (orderby_count > 0 || aggregate_row_width > 0 
                   || ScanFlag::BACKWARD == scan_direction)
          {
            need_groupby_ = true;
            query_stage_[query_stage_size_++] = GROUP_BY;
          }
  
          //check if it need do compose after groupby
          if (groupby_comp_column_size > 0)
          {
            query_stage_[query_stage_size_++] = COMPOSE;
          }
          
          //check if it need do orderby after groupby
          if (orderby_count > 0 && 0 == limit_count_
              && 0 == sharding_min_row_cnt_ && 0 == precision_)
          {
            query_stage_[query_stage_size_++] = ORDER_BY;
          }

          //check if it need do topk after groupby
          if (orderby_count > 0 && (limit_count_ > 0
              || (sharding_min_row_cnt_ > 0 && precision_ > 0)))
          {
            query_stage_[query_stage_size_++] = TOPK;
            need_compute_topk_ = true;
          }

          //check if it need do reuturn filter at the end
          if (groupby_return_info_size > 0 || return_info_size > 0
              || limit_offset_ > 0 || limit_count_ > 0)
          {
            query_stage_[query_stage_size_++] = RETUREN_COLUMN;
          }
  
          query_stage_[query_stage_size_++] = STAGE_END;
        }
      }

      return ret;
    }

    /**
     * query process: 
     *   1. stage one, store the result in merge cell array
     *     (1) merge data from chunk server and update server
     *     (2) join 
     *     (3) compose
     *   2. stage two, store the result in the group by cell array
     *     (1) copy result from stage one and filter
     *     (2) group by, caculate the aggregate column
     *     (3) caculate the composite columns after group by
     *     (4) filter the group by result
     *     (5) order by the group by result
     *   3. stage three, filter the final result
     *      (1) limit offset
     *      (2) limit count
     *      (3) column is return
     *  
     *  not all the stages and operations are done by get or scan.
     *  query stage machine only does the operation mentioned in get
     *  param or scan param.
     *  get only run stage one, and stage two and stage three only
     *  for scan if the scan param includes corresponding
     *  parameters.
     */
    int ObQueryAgent::query_stage_machine(
        const ObReadParam& read_param, 
        ObCellStream& ups_stream, 
        ObCellStream& ups_join_stream, 
        const ObSchemaManagerV2& schema_mgr,
        const bool unmerge_if_unchanged, const bool is_static_truncated) /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/
    {
      int ret = OB_SUCCESS;

      //stage one
      if (MERGE_JOIN_COMPOSE != query_stage_[cur_stage_idx_])
      {
        TBSYS_LOG(WARN, "first stage is not MERGE_JOIN_COMPOSE");
        ret = OB_ERROR;
      }
      else
      {
        //mod zhaoqiong [Truncate Table]:20160318:b
//        ret = merge_join_operator_.start_merge_join(
//              read_param, ups_stream, ups_join_stream, schema_mgr,
//              mem_limit_.merge_mem_size_, mem_limit_.max_merge_mem_size_,
//              unmerge_if_unchanged);
        ret = merge_join_operator_.start_merge_join(
              read_param, ups_stream, ups_join_stream, schema_mgr,
              mem_limit_.merge_mem_size_, mem_limit_.max_merge_mem_size_,
              unmerge_if_unchanged, is_static_truncated);
        //mod:e
        if (OB_SUCCESS == ret && OB_SUCCESS == (ret = timeout_check()))
        {
          cur_stage_idx_++;
          merge_join_result_ = &merge_join_operator_.get_cell_array();
          merge_join_iter_ = &merge_join_operator_;
          pfinal_result_ = &merge_join_operator_;
        }
        FILL_TRACE_LOG("start_merge_join result_cell_num=%ld, row_num=%ld, "
                       "size=%ld, is_fullfilled=%d, ret=%d",
          merge_join_operator_.get_cell_array().get_cell_size(),
          (NULL != get_param_) ? get_param_->get_row_size() :
          merge_join_operator_.get_cell_array().get_cell_size() 
          / (scan_param_->get_column_id_size() + scan_param_->get_composite_columns_size()),
          merge_join_operator_.get_cell_array().get_real_memory_used(), 
          merge_join_operator_.is_request_finished(), ret);
      }

      //do stage two if necessary
      if (OB_SUCCESS == ret && STAGE_END != query_stage_[cur_stage_idx_])
      {
        ret = stage_two_machine();
      }

      return ret;
    }

    int ObQueryAgent::stage_two_machine()
    {
      int ret = OB_SUCCESS;
      bool stop = false;

      while (!stop && OB_SUCCESS == ret)
      {
        if (cur_stage_idx_ >= query_stage_size_)
        {
          TBSYS_LOG(WARN, "unexpected error, cur_stage_idx_=%ld, query_stage_size_=%ld",
                    cur_stage_idx_, query_stage_size_);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        switch (query_stage_[cur_stage_idx_])
        {
        case FILTER_AND_GROUP_BY:
          if (OB_SUCCESS == ret)
          {
            ret = do_groupby(true);
            if (OB_SUCCESS == ret)
            {
              cur_stage_idx_++;
            }
          }
          break;

        case GROUP_BY:
          if (OB_SUCCESS == ret)
          {
            ret = do_groupby();
            if (OB_SUCCESS == ret)
            {
              cur_stage_idx_++;
            }
          }
          break;

        case COMPOSE:
          if (OB_SUCCESS == ret)
          {
            ret = do_compose();
            if (OB_SUCCESS == ret)
            {
              cur_stage_idx_++;
            }
          }
          break;

        case FILTER:
          TBSYS_LOG(WARN, "chunk server doesn't support having operation");
          ret = OB_ERROR;
          stop = true;
          break;

        case ORDER_BY:
          if (OB_SUCCESS == ret)
          {
            ret = do_orderby();
            if (OB_SUCCESS == ret)
            {
              cur_stage_idx_++;
            }
          }
          break;

        case TOPK:
          if (OB_SUCCESS == ret)
          {
            ret = do_topk();
            if (OB_SUCCESS == ret)
            {
              cur_stage_idx_++;
            }
          }
          break;

        case RETUREN_COLUMN:
          if (OB_SUCCESS == ret)
          {
            ret = do_return();
            if (OB_SUCCESS == ret)
            {
              cur_stage_idx_++;
            }
          }
          break;

        case STAGE_END:
          stop = true;
          break;

        case MERGE_JOIN_COMPOSE:
        case STAGE_NULL:
        default:
          TBSYS_LOG(WARN, "invalid stage operation, stage=%d", 
                    query_stage_[cur_stage_idx_]);
          ret = OB_ERROR;
          stop = true;
          break;
        }

        if (OB_SUCCESS == ret)
        {
          ret = timeout_check();
        }
      }

      return ret;
    }
    
    int ObQueryAgent::do_compose()
    {
      int ret = OB_SUCCESS;

      //compose after group by
      ret = compose_operator_.start_compose(
        *cur_groupby_operator_, 
        scan_param_->get_group_by_param().get_composite_columns(), 
        row_width_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to do compose, ret=%d", ret);
      }      

      return ret;
    }

    int ObQueryAgent::do_orderby()
    {
      int ret = OB_SUCCESS;
      int64_t const* orderby_idxs = NULL;
      uint8_t const* orders = NULL;
      int64_t orderby_count = 0;
      ObCellArray::OrderDesc orderby_desc[OB_MAX_COLUMN_NUMBER];

      scan_param_->get_orderby_column(orderby_idxs, orders, orderby_count);
      for (int64_t i = 0; i < orderby_count; ++i)
      {
        orderby_desc[i].cell_idx_ = static_cast<int32_t>(orderby_idxs[i]);
        orderby_desc[i].order_ = orders[i];
      }

      //order by after group by
      ret = cur_groupby_operator_->orderby(row_width_, orderby_desc, orderby_count);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to orderby result ret=%d", ret);
      }

      return ret;
    }

    int ObQueryAgent::do_topk(const bool last_round)
    {
      int ret = OB_SUCCESS;
      int64_t total_row_size = 0;
      int64_t sharding_row_cnt = 0;
      int64_t const* orderby_idxs = NULL;
      uint8_t const* orders = NULL;
      int64_t orderby_count = 0;
      double size_prop = 0.0;
      ObCellArray::OrderDesc orderby_desc[OB_MAX_COLUMN_NUMBER];

      scan_param_->get_orderby_column(orderby_idxs, orders, orderby_count);
      for (int64_t i = 0; i < orderby_count; ++i)
      {
        orderby_desc[i].cell_idx_ = static_cast<int32_t>(orderby_idxs[i]);
        orderby_desc[i].order_ = orders[i];
      }

      total_row_size = cur_groupby_operator_->get_cell_size() / row_width_;
      if (sharding_min_row_cnt_ > 0 && precision_ > 0)
      {
        sharding_row_cnt = std::max(sharding_min_row_cnt_, 
          static_cast<int64_t>(static_cast<double>(total_row_size) * precision_));
      }
      else if (limit_count_ > 0)
      {
        sharding_row_cnt = limit_offset_ + limit_count_;
        if (sharding_row_cnt < 0)
        {
          sharding_row_cnt = INT64_MAX;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "unexpected error happens, needn't compute topk,"
                        "but call compute topk function, limit_count=%ld, "
                        "precision=%lf, sharding_min_row_cnt=%ld",
          limit_count_, precision_, sharding_min_row_cnt_);
        ret = OB_ERR_UNEXPECTED;
      }

      if (OB_SUCCESS == ret)
      {
        if (!last_round)
        {
          if (mem_limit_.groupby_mem_size_ > 0)
          {
            size_prop = static_cast<double>(cur_groupby_operator_->get_real_memory_used())
              / static_cast<double>(mem_limit_.groupby_mem_size_);
            /**
             * FIXME: if we need do topk more than once, we limit the
             * intermediate returned row count of topk, the intermediate row
             * count is about 25% total row count
             */
            if (size_prop < MAX_SIZE_RATIO_AFTER_TOPK)
            {
              sharding_row_cnt = std::min(sharding_row_cnt, total_row_size);
            }
            else
            {
              sharding_row_cnt = std::min(sharding_row_cnt,
                static_cast<int64_t>(static_cast<double>(total_row_size) / size_prop * MAX_SIZE_RATIO_AFTER_TOPK));
            }
          }
          else
          {
            TBSYS_LOG(WARN, "do more than one round topk, invalid param, "
                            "groupby_mem_size_=%ld, last_round=%d",
              mem_limit_.groupby_mem_size_, last_round);
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret && sharding_row_cnt >= total_row_size)
        {
          //use orderby to compute the result
          sharding_row_cnt = 0;
        }
      }

      //topk after group by
      if (OB_SUCCESS == ret)
      {
        ret = cur_groupby_operator_->topk(row_width_, orderby_desc, 
          orderby_count, sharding_row_cnt);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to topk result ret=%d", ret);
        }
      }

      return ret;
    }

    int ObQueryAgent::do_return()
    {
      int ret = OB_SUCCESS;
      int64_t aggregate_row_width = 
        scan_param_->get_group_by_param().get_aggregate_row_width();
      const ObArrayHelpers<bool>& return_columns_map = 
        (aggregate_row_width > 0) ? scan_param_->get_group_by_param().get_return_infos() 
        : scan_param_->get_return_infos();

      //pfinal_result_ may pointer to merge_join_operator_ or groupby_operator_
      ret = return_operator_.start_return(*pfinal_result_, return_columns_map, 
                                          row_width_, limit_offset_, limit_count_);
      if (OB_SUCCESS == ret || OB_ITER_END == ret)
      {
        pfinal_result_ = &return_operator_;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to start return operator, row_width=%ld, "
                        "limit_ofset=%ld, limit_count=%ld, ret=%d", 
                 row_width_, limit_offset_, limit_count_, ret);
      }

      return ret;
    }
    
    int ObQueryAgent::do_groupby(bool need_filter)
    {
      int ret = OB_SUCCESS;
      int64_t aggregate_row_width = 
        scan_param_->get_group_by_param().get_aggregate_row_width();

      ret = build_groupby_result(need_filter);
      if (OB_SUCCESS == ret)
      {
        bool all_record_is_one_group = ((0 < aggregate_row_width)
          && (0 == scan_param_->get_group_by_param().get_groupby_columns().get_array_index()));
        if (all_record_is_one_group && (0 == cur_groupby_operator_->get_cell_size()))
        {
          /**
           * if there is no result, and aggregate functions act on all 
           * rows, the row key and table is fake, just for returned 
           * obscanner. i.e. all rows belong to one group. 
           */
          //ObObj fake_obj;
          //fake_obj.set_int(0);
          //ObRowkey fake_rowkey(&fake_obj, 1);
          ObRowkey fake_rowkey;
          ret = cur_groupby_operator_->init_all_in_one_group_row(
            fake_rowkey, scan_param_->get_table_id());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to init all in one group row ret=%d", ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        pfinal_result_ = cur_groupby_operator_;
      }

      return ret;
    }

    int ObQueryAgent::filter_one_row(
        const ObCellArray& cells, const int64_t row_beg,
        const int64_t row_end, const ObSimpleFilter& filter,
        bool& result)
    {
      int ret = OB_SUCCESS;

      ret = filter.check(cells, row_beg, row_end, result);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "check row fail ret=%d", ret);
      }
      else
      {
        if (result
          && scan_param_->get_group_by_param().get_aggregate_row_width() == 0
          && scan_param_->get_orderby_column_size() == 0
          && limit_offset_ > 0)
        {
          // optimization for request has only filter and limit info
          result = false;
          limit_offset_--;
        }
      }

      return ret;
    }

    // deprecated function, we don't use multi-groupby to optimize topk
    int ObQueryAgent::switch_groupby_operator()
    {
      int ret = OB_SUCCESS;

      if (!need_compute_topk_)
      {
        TBSYS_LOG(WARN, "unexpected error happens, needn't compute topk,"
                        "but call compute topk function");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = do_topk(false);
        if (OB_SUCCESS == ret)
        {
          ahead_groupby_operator_->clear();
          ret = ahead_groupby_operator_->init(scan_param_->get_group_by_param(), 
                mem_limit_.groupby_mem_size_, mem_limit_.max_groupby_mem_size_);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to init ahead groupby operator ret=%d", ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = ahead_groupby_operator_->copy_topk_row(*cur_groupby_operator_, row_width_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to copy topk row from groupby operator ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        std::swap(cur_groupby_operator_, ahead_groupby_operator_);
      }

      return ret;
    }
    
    int ObQueryAgent::build_groupby_result(const bool need_filter)
    {
      int ret = OB_SUCCESS;
      bool pass_filter = true;
      bool size_over_flow = false;
      int64_t got_row_count = 0;
      int64_t handled_row_count = 0;
      int64_t merge_join_round = 0;
      int64_t max_scan_rows = GET_SCAN_ROWS(scan_param_->get_scan_size());
      int64_t aggregate_row_width = 
        scan_param_->get_group_by_param().get_aggregate_row_width();
      int64_t org_row_width = scan_param_->get_column_id_size()
                              + scan_param_->get_composite_columns_size();
      int64_t orderby_count = scan_param_->get_orderby_column_size();
      bool need_aggregate = 
        (scan_param_->get_group_by_param().get_aggregate_columns().get_array_index() > 0);

      if (NULL == merge_join_result_ || NULL == merge_join_iter_)
      {
        TBSYS_LOG(WARN, "invalid merge join result, merge_join_result_=%p, "
                        "merge_join_iter_=%p", 
                  merge_join_result_, merge_join_iter_);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        ret = cur_groupby_operator_->init(scan_param_->get_group_by_param(), 
              mem_limit_.groupby_mem_size_, mem_limit_.max_groupby_mem_size_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init groupby operator ret=%d", ret);
        }
      }

      bool need_fetch_all_result = ((aggregate_row_width > 0) || (orderby_count > 0));
      bool need_limit_memory_usage = !(need_compute_topk_ || orderby_count > 0);
      while (OB_SUCCESS == ret)
      {
        /**
         * at this place, iterator access and direct access both exist, 
         * must be careful, it is a bad experience 
         */
        for (int64_t i = merge_join_result_->get_consumed_cell_num(); 
             i < merge_join_result_->get_cell_size() && OB_SUCCESS == ret; 
             i += org_row_width)
        {
          if (need_filter)
          {
            ret = filter_one_row(*merge_join_result_, i, i + org_row_width - 1,
                                 scan_param_->get_filter_info(), pass_filter);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "filter one row failed ret=%d", ret);
            }
          }

          if (OB_SUCCESS == ret && pass_filter)
          {
            ret = cur_groupby_operator_->add_row(*merge_join_result_, i, i + org_row_width - 1);
            if (OB_SUCCESS == ret)
            {
              got_row_count ++;
              if (!need_fetch_all_result)
              {
                if (((limit_count_ > 0) && (limit_count_ + limit_offset_ <= got_row_count))
                    || ((mem_limit_.groupby_mem_size_ > 0) 
                      && (cur_groupby_operator_->get_real_memory_used() > mem_limit_.groupby_mem_size_)))
                {
                  if (!((limit_count_ > 0) && (limit_count_ + limit_offset_ <= got_row_count)))
                  {
                    TBSYS_LOG(WARN, "groupby result take too much memory "
                        "[used:%ld, total:%ld, max_avail:%ld]",
                        cur_groupby_operator_->get_real_memory_used(), 
                        cur_groupby_operator_->get_memory_size_used(), 
                        mem_limit_.groupby_mem_size_);
                  }
                  size_over_flow = true;
                  break;
                }
              }
              else
              {
                if (need_limit_memory_usage && mem_limit_.groupby_mem_size_ > 0
                  && cur_groupby_operator_->get_real_memory_used() > mem_limit_.groupby_mem_size_)
                {
                  TBSYS_LOG(WARN, "groupby result take too much memory "
                      "[used:%ld, total:%ld, max_avail:%ld]",
                      cur_groupby_operator_->get_real_memory_used(), 
                      cur_groupby_operator_->get_memory_size_used(), 
                      mem_limit_.groupby_mem_size_);
                  ret = OB_MEM_OVERFLOW;
                }
              }
            }
            else
            {
              TBSYS_LOG(WARN, "failed to add row to group by result ret=%d", ret);
            }
          }

          if (OB_SUCCESS == ret && !need_aggregate &&  max_scan_rows > 0 
              && ++handled_row_count >= max_scan_rows)
          {
            TBSYS_LOG(WARN, "user limits scan rows from one tablet, only return "
                            "approximate result, max_scan_rows_per_tablet=%ld, "
                            "got_row_count=%ld, scan_size=%ld, merge_join_round=%ld, "
                            "merge_join_rows=%ld, merge_cellarray_size=%ld, "
                            "merge_cells_per_row=%ld, groupby_rows=%ld, "
                            "groupby_cellarray_size=%ld, groupby_cells_per_row=%ld",
              max_scan_rows, handled_row_count, 
              GET_SCAN_SIZE(scan_param_->get_scan_size()), merge_join_round + 1, 
              merge_join_result_->get_cell_size() / org_row_width,
              merge_join_result_->get_real_memory_used(), org_row_width,
              cur_groupby_operator_->get_cell_size() / row_width_,
              cur_groupby_operator_->get_real_memory_used(), row_width_);
            size_over_flow = true;
            break;
          }
        }// end for
        merge_join_round++;

        if (OB_SUCCESS == ret && size_over_flow)
        {
          break;
        }
        else if (OB_SUCCESS == ret && OB_SUCCESS != (ret = timeout_check()))
        {
          break;
        }
        else if (OB_SUCCESS == ret)
        {
          merge_join_result_->consume_all_cell();
          ret = merge_join_iter_->next_cell();
          if (OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to get next cell from merge join "
                            "operator ret=%d", ret);
          }
          else if (OB_SUCCESS == ret && OB_SUCCESS != (ret = timeout_check()))
          {
            break;
          }
          else
          {
            if (merge_join_result_->get_cell_size() % org_row_width != 0)
            {
              TBSYS_LOG(WARN, "merge_join_operator result error "
                              "[merge_join_operator_.get_cell_size():%ld, "
                              "org_row_width:%ld]",
                merge_join_result_->get_cell_size(), org_row_width);
              ret = OB_ERR_UNEXPECTED;
            }
            else
            {
              ret = merge_join_result_->unget_cell();
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to unget one cell ret=%d", ret);
              }
            }
          }
        }
      }// end while

      FILL_TRACE_LOG("merge_join_round=%ld, size=%ld, row_num=%ld, "
                     "cells_per_row=%ld, ret=%d", 
        merge_join_round, cur_groupby_operator_->get_real_memory_used(),
        cur_groupby_operator_->get_cell_size() / row_width_, row_width_, ret);

      return ret;
    }

    int ObQueryAgent::timeout_check()
    {
      int ret = OB_SUCCESS;

      if (timeout_time_ > 0 && tbsys::CTimeUtil::getTime() >= timeout_time_)
      {
        ret = OB_RESPONSE_TIME_OUT;
      }

      return ret;
    }
    
    int ObQueryAgent::next_cell()
    {
      int ret = OB_SUCCESS;

      if (!inited_ || NULL == pfinal_result_)
      {
        TBSYS_LOG(WARN, "run start_agent() first, inited_=%d, pfinal_result_=%p", 
                  inited_, pfinal_result_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = pfinal_result_->next_cell();
      }

      return ret;
    }
    
    int ObQueryAgent::get_cell(ObInnerCellInfo** cell)
    {
      return get_cell(cell, NULL);
    }
    
    int ObQueryAgent::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
    {
      int ret = OB_SUCCESS;
      *cell = NULL;

      if (!inited_ || NULL == pfinal_result_)
      {
        TBSYS_LOG(WARN, "run start_agent() first, inited_=%d, pfinal_result_=%p", 
                  inited_, pfinal_result_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = pfinal_result_->get_cell(cell, is_row_changed);
      }

      return ret;
    }

    bool ObQueryAgent::is_unchanged() const
    {
      return merge_join_operator_.is_unchanged();
    }

    bool ObQueryAgent::need_groupby() const
    {
      return need_groupby_;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
