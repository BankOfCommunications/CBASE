/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_scan_event.h,v 0.1 2011/09/28 14:28:10 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */


#ifndef OB_MERGER_SCAN_REQUEST_H_
#define OB_MERGER_SCAN_REQUEST_H_

#include "ob_ms_request.h"
#include "ob_ms_sub_scan_request.h"
#include "ob_merger_operator.h"
#include "ob_ms_scan_param.h"
#include "common/location/ob_tablet_location_range_iterator.h"

namespace oceanbase
{
  namespace common
  {
    class ObTabletLocationCacheProxy;
  }
  namespace mergeserver
  {
    class ObMergerRpcEvent;
    class ObMergerAsyncRpcStub;

    class ObMergerScanRequest:public ObMergerRequest
    {
    public:
      ObMergerScanRequest(common::ObTabletLocationCacheProxy * cache_proxy, ObMergerAsyncRpcStub * async_rpc);
      virtual ~ObMergerScanRequest();

      void reset();

      /// called by working thread when receive a request from client
      /// these two functions will trigger rpc event which will non-blocking rpc access cs
      int set_request_param(ObMergerScanParam &scan_param, const int64_t max_memory_size,
        const int64_t timeout_us, const int64_t max_parellel_count = DEFAULT_MAX_PARELLEL_COUNT);
      int do_request(const int64_t max_parellel_count, ObTabletLocationRangeIterator &iter,
        const int64_t timeout_us, const int64_t limit_offset = 0);

      int get_session_next(const int64_t sub_req_idx, const ObMergerRpcEvent &prev_rpc_event,
        common::ObNewRange &query_range, const int64_t timeout_us,  const int64_t limit_offset = 0);

      inline const int32_t get_total_sub_request_count() const;
      inline const int32_t get_finished_sub_request_count() const;

      /// callback by working thread when one of the rpc event finish
      virtual int process_result(const int64_t timeout_us, ObMergerRpcEvent *rpc_event, bool& finish);
      int retry(const int64_t sub_req_idx, ObMergerRpcEvent *rpc_event, int64_t timeout_us);
      /// cell stream interface
      virtual int get_cell(ObInnerCellInfo** cell);
      virtual int get_cell(ObInnerCellInfo** cell, bool* is_row_changed);
      virtual int next_cell();

      int64_t get_mem_size_used()const
      {
        return(merger_operator_.get_mem_size_used() + cs_result_mem_size_used_);
      }
      int64_t get_whole_result_row_count()const
      {
        return merger_operator_.get_whole_result_row_count();
      }
      int fill_result(common::ObScanner & scanner, common::ObScanParam &org_param,
        bool &got_all_result);

    private:
      int prepare_get_cell();
      int alloc_sub_scan_request(ObMergerSubScanRequest *& sub_req);
      int find_sub_scan_request(ObMergerRpcEvent * agent_event, bool &belong_to_this, bool &is_first,
        int64_t &idx);
      int check_if_need_more_req(const int64_t sub_req_idx,   const int64_t timeout_us, ObMergerRpcEvent &prev_rpc_event);
      int send_rpc_event(ObMergerSubScanRequest * sub_req, const int64_t timeout_us, uint64_t * triggered_rpc_event_id = NULL);
      bool check_if_location_cache_valid_(const common::ObScanner & scanner, const common::ObScanParam & scan_param);

    private:
      void end_sessions_();
      static const int64_t MAX_ROW_COLUMN_COUNT = common::OB_MAX_COLUMN_NUMBER * 4;
      int32_t               total_sub_request_count_;
      int32_t               finished_sub_request_count_;
      ObMergerSubScanRequest    sub_requests_[MAX_SUBREQUEST_NUM];
      ObMergerOperator  merger_operator_;
      ObScanParam * scan_param_;
      int64_t       cs_result_mem_size_used_;
      int64_t       max_parellel_count_;
      ObTabletLocationRangeIterator org_req_range_iter_;
      int64_t sharding_limit_count_;
      int64_t cur_row_cell_cnt_;
      common::ObCellInfo row_cells_[MAX_ROW_COLUMN_COUNT];
    };

    inline const int32_t ObMergerScanRequest::get_total_sub_request_count() const
    {
      return total_sub_request_count_;
    }

    inline const int32_t ObMergerScanRequest::get_finished_sub_request_count() const
    {
      return finished_sub_request_count_;
    }
  }
}

#endif // OB_MERGER_SCAN_REQUEST_H_

