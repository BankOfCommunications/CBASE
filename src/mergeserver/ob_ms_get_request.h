/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_get_request.h for
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MERGER_GET_REQUEST_H_
#define MERGESERVER_OB_MERGER_GET_REQUEST_H_

#include "ob_ms_request.h"
#include "ob_ms_sub_get_request.h"
#include "ob_ms_scan_param.h"
#include "common/location/ob_tablet_location_range_iterator.h"
#include "common/ob_get_param.h"
#include "common/ob_string_buf.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerGetRequest:public ObMergerRequest
    {
    public:
      ObMergerGetRequest(common::ObTabletLocationCacheProxy * cache_proxy, ObMergerAsyncRpcStub * async_rpc);
      ~ObMergerGetRequest()
      {
      }

      void reset();

      /// called by working thread when receive a request from client
      /// these two functions will trigger rpc event which will non-blocking rpc access cs
      static const int32_t DEFAULT_RESERVE_PARAM_COUNT = 3;
      static const int64_t DEFAULT_MAX_GET_ROWS_PER_SUBREQ = 20;
      int set_request_param(ObGetParam &get_param, const int64_t timeout_us,
        const int64_t max_parellel_count = DEFAULT_MAX_PARELLEL_COUNT,
        const int64_t max_get_rows_per_subreq = DEFAULT_MAX_GET_ROWS_PER_SUBREQ,
        const int64_t reserve_get_param_count = DEFAULT_RESERVE_PARAM_COUNT);

      /// callback by working thread when one of the rpc event finish
      virtual int process_result(const int64_t timeout_us, ObMergerRpcEvent *rpc_event, bool& finish);

      /// cell stream interface
      virtual int get_cell(ObInnerCellInfo** cell);
      virtual int get_cell(ObInnerCellInfo** cell, bool* is_row_changed);
      virtual int next_cell();

      int reset_iterator();

      int fill_result(common::ObScanner & scanner, common::ObGetParam &org_param,
        bool &got_all_result);
      void set_max_req_process_cs_timeout_percent(double max_req_process_cs_timeout_percent);
      double get_max_req_process_cs_timeout_percent();

    public:
      /*
      void TEST_get_sub_requests(const ObMergerSubGetRequest *& reqs, int64_t & req_count)const
      {
        reqs = sub_requests_;
        req_count = total_sub_request_count_;
      }
      int64_t TEST_get_triggered_sub_req_count()const
      {
        return triggered_sub_request_count_;
      }
      int64_t TEST_get_finished_sub_req_count()const
      {
        return finished_sub_request_count_;
      }
      */
    private:
      int distribute_request();
      int send_request(const int64_t sub_req_idx, const int64_t timeout_us);
      int check_if_need_more_req(const int64_t current_sub_req_idx, const int64_t timeout_us,
        ObMergerRpcEvent *prev_rpc_event = NULL);
      int get_session_next(const int64_t sub_req_idx, ObMergerRpcEvent & prev_rpc, const int64_t timeout_us);

    private:
      static const int32_t MAX_RETRY_TIMES = 3;
      static const int64_t HASH_BUCKET_NUM = 4096;
      static const int64_t MAX_ROW_COLUMN_COUNT = common::OB_MAX_COLUMN_NUMBER * 4;
      bool sealed_;
      ObMergerSubGetRequest sub_requests_[MAX_SUBREQUEST_NUM];
      ObGetMerger merger_operator_;
      int16_t same_cs_get_row_count_[MAX_SUBREQUEST_NUM];
      int32_t total_sub_request_count_;
      int32_t finished_sub_request_count_;
      int32_t triggered_sub_request_count_;
      int64_t max_parellel_count_;
      int64_t max_get_rows_per_subreq_;
      int64_t cur_row_cell_cnt_;
      bool req_dist_map_inited_;
      common::hash::ObHashMap<int64_t,int64_t,common::hash::NoPthreadDefendMode> req_dist_map_;
      common::ObInnerCellInfo last_not_exist_cell_;
      int64_t poped_cell_count_;
      int64_t reserve_get_param_count_;
      common::ObGetParam  *get_param_;
      common::PageArena<int64_t,common::ModulePageAllocator> page_arena_;
      common::ObCellInfo row_cells_[MAX_ROW_COLUMN_COUNT];
    };
  }
}

#endif // MERGESERVER_OB_MERGER_GET_REQUEST_H_
