/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_sql_get_event.h for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   xiaochu.yh <xiaochu.yh@alipay.com>
 *
 */
#ifndef _MERGESERVER_OB_MS_SQL_GET_REQUEST_H_
#define _MERGESERVER_OB_MS_SQL_GET_REQUEST_H_

#include "ob_ms_sql_request.h"
#include "ob_ms_sql_sub_get_request.h"
#include "common/location/ob_tablet_location_range_iterator.h"
#include "sql/ob_sql_get_param.h"
#include "common/ob_string_buf.h"
#include "common/page_arena.h"
#include "common/hash/ob_placement_hashmap.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsSqlGetRequest:public ObMsSqlRequest
    {
    public:
      ObMsSqlGetRequest();
      ~ObMsSqlGetRequest()
      {
      }

      void reset();

      /// called by working thread when receive a request from client
      /// these two functions will trigger rpc event which will non-blocking rpc access cs
      static const int32_t DEFAULT_RESERVE_PARAM_COUNT = 3;
      static const int64_t DEFAULT_MAX_GET_ROWS_PER_SUBREQ = 20;
      int set_request_param(ObSqlGetParam &get_param, const int64_t timeout_us,
        const int64_t max_parellel_count = DEFAULT_MAX_PARELLEL_COUNT,
        const int64_t max_get_rows_per_subreq = DEFAULT_MAX_GET_ROWS_PER_SUBREQ,
        const int64_t reserve_get_param_count = DEFAULT_RESERVE_PARAM_COUNT);
      inline int set_row_desc(const common::ObRowDesc &desc);
      // issue request to Chunkservers
      int open(void);

      /// callback by working thread when one of the rpc event finish
      virtual int process_result(const int64_t timeout_us, ObMsSqlRpcEvent *rpc_event, bool& finish);
      /// row stream interface
      int get_next_row(oceanbase::common::ObRow &row);

      void set_max_req_process_cs_timeout_percent(double max_req_process_cs_timeout_percent);
      double get_max_req_process_cs_timeout_percent();
      inline int set_skip_empty_row(const bool is_skip_empty_row);
    public:
      /*
      void TEST_get_sub_requests(const ObMsSqlSubGetRequest *& reqs, int64_t & req_count)const
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
      int send_request(const int32_t sub_req_idx, const int64_t timeout_us);
      int check_if_need_more_req(const int32_t current_sub_req_idx, const int64_t timeout_us,
        ObMsSqlRpcEvent *prev_rpc_event = NULL);
      int get_session_next(const int32_t sub_req_idx, ObMsSqlRpcEvent & prev_rpc, const int64_t timeout_us);
      int alloc_sub_request(ObMsSqlSubGetRequest *&sub_req);
    private:
      static const int32_t MAX_RETRY_TIMES = 3;
      static const int64_t HASH_BUCKET_NUM = 4099;
      static const int64_t MAX_ROW_COLUMN_COUNT = common::OB_MAX_COLUMN_NUMBER * 4;
      bool sealed_;
      ObSqlGetMerger merger_operator_;
      int16_t same_cs_get_row_count_[MAX_SUB_GET_REQUEST_NUM];
      int32_t total_sub_request_count_;
      int32_t finished_sub_request_count_;
      int32_t triggered_sub_request_count_;
      int64_t max_parellel_count_;
      int64_t max_get_rows_per_subreq_;
      int64_t reserve_get_param_count_;
      int64_t timeout_us_;
      common::hash::ObPlacementHashMap<int64_t, int64_t, HASH_BUCKET_NUM> req_dist_map_;
      sql::ObSqlGetParam  *get_param_;
      sql::ObSqlGetParam cur_sub_get_param_;
      common::PageArena<int64_t,common::ModulePageAllocator> page_arena_;
      const common::ObRowDesc *row_desc_;
      SubRequestVector sub_requests_;
    };

    inline int ObMsSqlGetRequest::set_row_desc(const common::ObRowDesc &desc)
    {
      row_desc_ = &desc;
      return OB_SUCCESS;
    }
  }
}


#endif /* _MERGESERVER_OB_MS_SQL_GET_REQUEST_H_ */
