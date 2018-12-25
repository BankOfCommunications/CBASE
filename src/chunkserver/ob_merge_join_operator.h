/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merge_join_operator.h for implementation of merge and join
 * operator. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_MERGE_JOIN_OPERATOR_H_ 
#define OCEANBASE_CHUNKSERVER_MERGE_JOIN_OPERATOR_H_

#include "ob_cell_stream.h"
#include "ob_rpc_proxy.h"
#include "common/ob_schema.h"
#include "common/ob_cell_array.h"
#include "common/ob_compose_operator.h"
#include "ob_merge_operator.h"
#include "ob_join_operator.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * this class encapsulates merge operator and join operator as a
     * stream interface, we usually do merge operation after join
     * operation, this operator make it easier, even it needn't do
     * join operation, this operator can handle this case. this
     * merge join operator can do several merge round and join
     * round, and it's transparent to user.
     */
    class ObMergeJoinOperator : public common::ObInnerIterator
    {
    public:
      ObMergeJoinOperator(ObMergerRpcProxy &rpc_proxy);
      ~ObMergeJoinOperator();

    public:
      /**
       * init and start merge join, after call this functioin, user 
       * can use the iterator interface to iterate all the result 
       * cells. 
       * 
       * @param read_param get param or scan param, this function will 
       *                   use dynamic_cast<> to know which it is.
       * @param ups_stream ups stream provide data from update server
       * @param ups_join_stream ups stream provide join row data from 
       *                        update server
       * @param schema_mgr schema manager
       * @param max_memory_size max memory size used by result cell 
       *                        array
       * @param unmerge_if_unchanged whether do merge if there is not 
       *                             dynamic data in update server 
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int start_merge_join(const common::ObReadParam &read_param,
                           ObCellStream &ups_stream, 
                           ObCellStream &ups_join_stream, 
                           const common::ObSchemaManagerV2 &schema_mgr,
                           const int64_t merge_mem_size,
                           const int64_t max_merge_mem_size,
                           const bool unmerge_if_unchanged = false,
                           const bool is_static_truncated = false); /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/

      // clear all internal status for next new merge and join operation
      void clear(); 

      // check if result has been finised yet
      bool is_request_finished() const;

      // get data version returned
      int64_t get_data_version() const;

      // iterator vitual functions
      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);

      common::ObCellArray& get_cell_array();

      // check if there is dynamic data in ups for the scan range.
      // this interface is only for scan
      bool is_unchanged() const;

    private:
      void initialize();
      // reset result cell array
      void reset_result_array();
      int do_merge_join();

    private:
      bool is_need_join_;
      bool is_need_compose_;
      int64_t merge_mem_size_;
      int64_t max_merge_mem_size_;
      int64_t row_width_;

      const common::ObScanParam* scan_param_;
      common::ObCellArray result_array_;
      ObMergeOperator merge_operator_;
      ObJoinOperator join_operator_;
      common::ObComposeOperator compose_operator_;
    };
  }
}

#endif // OCEANBASE_CHUNKSERVER_MERGE_JOIN_OPERATOR_H_
