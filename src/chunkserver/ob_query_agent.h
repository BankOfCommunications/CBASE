/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_query_agent.h for define query agent for get and scan 
 * operation. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_QUERY_AGENT_H_ 
#define OCEANBASE_CHUNKSERVER_QUERY_AGENT_H_

#include "common/ob_schema.h"
#include "common/ob_groupby_operator.h"
#include "common/ob_return_operator.h"
#include "ob_cell_stream.h"
#include "ob_rpc_proxy.h"
#include "ob_merge_join_operator.h"

namespace oceanbase
{
  namespace chunkserver
  {
    enum ObStageOperation
    {
      STAGE_NULL = 0,
      MERGE_JOIN_COMPOSE,
      FILTER_AND_GROUP_BY,
      GROUP_BY,
      COMPOSE,
      FILTER,
      ORDER_BY,
      TOPK,
      RETUREN_COLUMN,
      STAGE_END
    };

    struct ObOperatorMemLimit
    {
      int64_t merge_mem_size_;
      int64_t max_merge_mem_size_;
      int64_t groupby_mem_size_;
      int64_t max_groupby_mem_size_;

      ObOperatorMemLimit()
      {
        reset();
      }

      void reset()
      {
        merge_mem_size_ = -1;
        max_merge_mem_size_ = -1;
        groupby_mem_size_ = -1;
        max_groupby_mem_size_ = -1;
      }
    };

    /**
     * query agent, do the actual get and scan work, it outputs the
     * final query result.
     * 
     * WARNING: this class is not thread safe, it's better to create
     * one query agent for each thread and reuse the query agent.
     */
    class ObQueryAgent : public common::ObInnerIterator
    {
    public:
      explicit ObQueryAgent(ObMergerRpcProxy& proxy);
      ~ObQueryAgent();

    public:
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);
      virtual int next_cell();

    public:
     /**
      * init and start query agent, it does the actual query work and 
      * store the final query result in an internal cell array. then 
      * user can call the iterator interface to get the query data. 
      * 
      * @param read_param get param or scan param, this function will 
      *                   use dynamic_cast<> to know which it is.
      * @param ups_stream ups stream provide data from update server
      * @param ups_join_stream ups stream provide data from update 
      *                        server for join
      * @param schema_mgr schema manager
      * @param mem_limit the max memory size that the result 
      *                  array can use, only for scan
      *                  operation, it's no function for get
      *                  operation
      * @param timeout_time when is time out. 
      * @param unmerge_if_unchanged whether do merge if there is not 
       *                             dynamic data in update server 
      * 
      * @return int if success, return OB_SUCCESS, else return 
      *         OB_ERROR, OB_RESPONSE_TIME_OUT
      */
     int start_agent(const common::ObReadParam& read_param, 
                     ObCellStream& ups_stream, 
                     ObCellStream& ups_join_stream, 
                     const common::ObSchemaManagerV2& schema_mgr,
                     const ObOperatorMemLimit& mem_limit,
                     const int64_t timeout_time = 0,
                     const bool unmerge_if_unchanged = false,
                     const bool is_static_truncated = false); /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/

     /**
      * WARNING: this function is only used for test, please don't 
      * call it 
      * 
      * @param merge_join_result input merge join result, query agent 
      *                          doesn't do merge join any more
      * @param read_param get param or scan param
      * @param max_memory_size max memory size can use 
      * @param timeout_time when is time out. 
      * 
      * @return int if success, return OB_SUCCESS, else return 
      *         OB_ERROR, OB_RESPONSE_TIME_OUT
      */
     int start_agent(common::ObCellArray& merge_join_result,
                     const common::ObReadParam& read_param,
                     const int64_t max_memory_size = -1,
                     const int64_t timeout_time = 0);

     // clear all internal status for next new query operation
      void clear();

      // check if query has been finised yet
      bool is_request_fullfilled();

      // get data version returned
      int64_t get_data_version() const;

      // get total result row count
      int64_t get_total_result_row_count() const;

      // check if there is dynamic data in ups for the scan range.
      // this interface is only for scan
      bool is_unchanged() const;

      // check if the scan operation need groupby
      bool need_groupby() const;

    private:
      int build_query_stage();
      int query_stage_machine(const common::ObReadParam& read_param, 
                              ObCellStream& ups_stream, 
                              ObCellStream& ups_join_stream, 
                              const common::ObSchemaManagerV2& schema_mgr,
                              const bool unmerge_if_unchanged,
                              const bool is_static_truncated); /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/
      int stage_two_machine();
      int do_groupby(bool need_filter = false);
      int filter_one_row(const common::ObCellArray& cells, const int64_t row_beg,
                         const int64_t row_end, const common::ObSimpleFilter& filter,
                         bool& result);
      int switch_groupby_operator();
      int build_groupby_result(const bool need_filter = false);
      int do_compose();
      int do_orderby();
      int do_topk(const bool last_round = true);
      int do_return();
      int timeout_check();

    private:
      static const int64_t MAX_STAGE_OPERATION_COUNT = 16;
      static const double MAX_SIZE_RATIO_AFTER_TOPK = 0.25;

      //query param
      bool inited_;
      int64_t timeout_time_;
      const common::ObScanParam* scan_param_;
      const common::ObGetParam* get_param_;

      //query operators and query final result
      ObMergeJoinOperator       merge_join_operator_;
      common::ObGroupByOperator groupby_operator_;
      common::ObGroupByOperator groupby_operator_topk_;
      common::ObComposeOperator compose_operator_;
      common::ObReturnOperator  return_operator_;

      common::ObGroupByOperator* cur_groupby_operator_;
      common::ObGroupByOperator* ahead_groupby_operator_;

      //intermediate result and final result
      common::ObCellArray* merge_join_result_;
      common::ObInnerIterator* merge_join_iter_;
      common::ObInnerIterator* pfinal_result_;

      //limitation of query result, only for scan
      ObOperatorMemLimit  mem_limit_;
      int64_t             row_width_;
      int64_t             limit_offset_;
      int64_t             limit_count_;
      int64_t             sharding_min_row_cnt_; 
      double              precision_;
      bool                need_groupby_;
      bool                need_compute_topk_;

      //query stage array
      ObStageOperation    query_stage_[MAX_STAGE_OPERATION_COUNT];
      int64_t             query_stage_size_;
      int64_t             cur_stage_idx_;
    };
  }
}

#endif // OCEANBASE_CHUNKSERVER_QUERY_AGENT_H_
