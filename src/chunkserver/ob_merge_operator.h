/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merge_operator.h for implementation of merge operator. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_MERGE_OPERATOR_H_ 
#define OCEANBASE_CHUNKSERVER_MERGE_OPERATOR_H_

#include "ob_cell_stream.h"
#include "ob_rpc_proxy.h"
#include "common/ob_schema.h"
#include "common/ob_merger.h"
#include "common/ob_scanner.h"
#include "common/ob_cell_array.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * merge operator, it does the actual merge work, it uses
     * obmerger to merge the get or scan stream from chunk server
     * and update server, it merges until the cell array container
     * is full. and it stores the status of last round, so user must
     * use this operator to merge stream round by round until to
     * completion.
     */
    class ObMergeOperator : public common::ObInnerIterator
    {
    public:
      ObMergeOperator(ObMergerRpcProxy &rpc_proxy);
      ~ObMergeOperator();

    public:
      /**
       * init and start merge operator, this function do the first 
       * round merge work and store the merge result in result array, 
       * then user can the iterator interface to get the cells in 
       * result array. because the size of result array is limited, 
       * maybe it's not large enough to store all the result, user 
       * must call is_request_finished() function to know if it 
       * finishes merging. 
       * 
       * @param result_array cell array to store the merge result
       * @param read_param get param or scan param, this function will 
       *                   use dynamic_cast<> to know which it is.
       * @param ups_stream ups stream provide data from update server
       * @param schema_mgr schema manager
       * @param max_memory_size the max memory size that the result 
       *                        array can use, only for scan
       *                        operation, it's no function for get
       *                        operation
       * @param unmerge_if_unchanged whether do merge if there is not 
       *                             dynamic data in update server
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int start_merge(common::ObCellArray &result_array, 
                      const common::ObReadParam &read_param,
                      ObCellStream &ups_stream, 
                      const common::ObSchemaManagerV2 &schema_mgr,
                      const int64_t max_memory_size,
                      const bool unmerge_if_unchanged = false,
                      const bool is_static_truncated_ = false); /*add zhaoqiong [Truncate Table]:20160318 param:is_static_truncated*/

      /**
       * after call start_merge() function, and read all the cells in 
       * result array, and there is data to merge, call this function 
       * to continue merge. 
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR, OB_INVALID_ARGUMENT
       */
      int do_merge(); 

      // clear all internal status for next new merge operation
      void clear(); 

      // check if result has been finised yet
      bool is_request_finished() const;

      // get data version returned
      int64_t get_data_version() const;

      // iterator vitual functions
      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);

      /**
       * for each merge round, mrege operator will build new param to 
       * query data form chunk server or update server, the join 
       * operator need some part of the scan param or get param.
       * 
       * @return const common::ObReadParam& 
       */
      const common::ObReadParam& get_cur_read_param() const;

      // check if it need join operation after merge
      bool need_join() const;
      
      // check if there is dynamic data in ups for the scan range.
      // this interface is only for scan
      bool is_unchanged() const;
      
    private:
      void initialize();
      int build_scan_column_index(const common::ObScanParam &scan_param,
                                const common::ObSchemaManagerV2 &schema_mgr);
      int merge();  
      int merge_next_row(int64_t &cur_row_idx, int64_t &cur_row_beg, int64_t &cur_row_end);
      int scan_merge_row_fast_path();
      bool is_del_op(const int64_t ext_val);
      int apply_whole_row(const common::ObCellInfo &cell, const int64_t row_beg);
      int get_next_rpc_result();
      int get_next_get_rpc_result();
      int get_next_scan_rpc_result();
      void move_to_next_row(const common::ObGetParam & row_spec_arr,
          int64_t &cur_row_idx, int64_t &cur_row_beg, int64_t &cur_row_end);
      int append_fake_composite_column();

    private:
      struct ObColumnIndex
      {
        int16_t size_;        //how many column offset in current column index
        int16_t head_offset_; //the first same column offset in scan param
        int16_t tail_offset_; //the last same column offset in scan param
        int16_t reserved_;
      };

    private:
      static const int64_t DEFAULT_RANGE_MEMBUF_SIZE = 64 * 1024;  //64K

      const common::ObSchemaManagerV2 *schema_mgr_;
      ObCellStream  *ups_stream_;
      int64_t       max_memory_size_;
      bool          request_finished_;
      bool          is_need_query_ups_;
      bool          is_need_join_;
      bool          is_unchanged_;
      bool          unmerge_if_unchanged_;
      int64_t       data_version_;
      const common::ObScanParam *scan_param_;
      const common::ObGetParam  *get_param_;
      // when get, req_param_ == get_param_; when scan req_param_ == & fake_get_param_
      const common::ObGetParam *req_param_;
      common::ObCellArray* result_array_;

      // properties for get request
      common::ObMerger           merger_;
      bool                       merger_iterator_moved_;
      common::ObGetParam         cur_get_param_;
      int64_t                    got_cell_num_;

      // properties for scan request
      common::ObScanParam        cur_scan_param_;
      common::ObMemBuf           cs_scan_buffer_; 
      common::ObMemBuf           ups_scan_buffer_; 
      common::ObScanner          cur_cs_result_;
      ObMergerRpcProxy*          rpc_proxy_;
      bool                       param_contain_duplicated_columns_;
      ObColumnIndex              column_index_[common::OB_MAX_COLUMN_NUMBER];
      int16_t                    same_column_next_[common::OB_MAX_COLUMN_NUMBER];
      common::ObIterator         *merger_iter_;
      common::ObCellInfo         cur_cell_initializer_;
      common::ObCellInfo         *batch_append_cells_in_[common::OB_MAX_COLUMN_NUMBER];
      common::ObInnerCellInfo    *batch_append_cells_out_[common::OB_MAX_COLUMN_NUMBER];

      bool is_static_truncated_; /*add zhaoqiong [Truncate Table]:20160318*/
    };
  }
}

#endif // OCEANBASE_CHUNKSERVER_MERGE_OPERATOR_H_
