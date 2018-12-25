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

#include "common/ob_trace_log.h"
#include "ob_ms_scan_request.h"
#include "ob_ms_async_rpc.h"
#include "ob_read_param_modifier.h"

namespace oceanbase
{
  namespace mergeserver
  {
    ObMergerScanRequest::ObMergerScanRequest(common::ObTabletLocationCacheProxy * cache_proxy, ObMergerAsyncRpcStub * async_rpc)
    :ObMergerRequest(cache_proxy, async_rpc)
    {
      reset();
    }

    ObMergerScanRequest::~ObMergerScanRequest()
    {

    }

    int ObMergerScanRequest::alloc_sub_scan_request(ObMergerSubScanRequest *& sub_req)
    {
      int ret = OB_SUCCESS;
      if ((total_sub_request_count_ >= 0)
        && (total_sub_request_count_ < MAX_SUBREQUEST_NUM))
      {
        sub_req = &sub_requests_[total_sub_request_count_];
        total_sub_request_count_++;
      }
      else
      {
        TBSYS_LOG(WARN, "allocate sub scan request failed. [total_sub_request_count_=%d][finished_sub_request_count_=%d]"
          "[MAX_SUBREQUEST_NUM=%d]", total_sub_request_count_, finished_sub_request_count_,
          MAX_SUBREQUEST_NUM);
        ret = OB_ERROR;
      }
      return ret;
    }


    int ObMergerScanRequest::set_request_param(ObMergerScanParam &scan_param, const int64_t max_memory_size,
      const int64_t timeout_us, const int64_t max_parellel_count)
    {
      int err = OB_SUCCESS;
      int64_t sharding_limit_offset = 0;

      scan_param_ = scan_param.get_cs_param();
      scan_param_->get_limit_info(sharding_limit_offset, sharding_limit_count_);
      if ((OB_SUCCESS == err) && (sharding_limit_offset > 0))
      {
        TBSYS_LOG(ERROR, "unexpected error, synmatic error [sharding_limit_offset:%ld]", sharding_limit_offset);
      }

      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = merger_operator_.set_param(max_memory_size,
        *scan_param.get_ms_param()))))
      {
        TBSYS_LOG(WARN, "fail to set merger_operator's param. [err=%d]", err);
      }

      if (OB_SUCCESS == err)
      {
        if (max_parellel_count > 0)
        {
          max_parellel_count_ = max_parellel_count;
        }
        else
        {
          max_parellel_count_ = DEFAULT_MAX_PARELLEL_COUNT;
        }
      }
      if ((OB_SUCCESS == err) &&(OB_SUCCESS != (err = org_req_range_iter_.initialize(get_cache_proxy(),
        scan_param_->get_range(), scan_param_->get_scan_direction() ,&get_buffer_pool()))))
      {
        TBSYS_LOG(WARN,"fail to init table range iterator [err:%d]", err);
      }

      if (OB_SUCCESS == err)
      {
        err = do_request(max_parellel_count_,org_req_range_iter_, timeout_us);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fail to scan cs. [err=%d]", err);
        }
      }
      return err;
    }


    int ObMergerScanRequest::send_rpc_event(ObMergerSubScanRequest * sub_req, const int64_t timeout_us,
      uint64_t * triggered_rpc_event_id)
    {
      int err = OB_SUCCESS;
      const ObMergerAsyncRpcStub * async_rpc_stub = NULL;
      ObMergerRpcEvent *rpc_event = NULL;
      ObChunkServerItem selected_server;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = create(&rpc_event))))
      {
        TBSYS_LOG(WARN, "fail to create rpc event. [err=%d]", err);
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sub_req->add_event(
        rpc_event, this, selected_server))))
      {
        TBSYS_LOG(WARN, "fail to init rpc event. [err=%d]", err);
      }
      if ((OB_SUCCESS == err) && (NULL == (async_rpc_stub = get_rpc())))
      {
        TBSYS_LOG(WARN, "fail to get rpc");
        err = OB_ERROR;
      }

      // 4. send the request to selected chunkserver
      if ((OB_SUCCESS == err) &&
        (OB_SUCCESS != (err = async_rpc_stub->scan(static_cast<int64_t>
        (static_cast<double>(timeout_us) * get_timeout_percent()),
        selected_server.addr_,
      /* @note range and offset set in get_scan_param() function */
        *(sub_req->get_scan_param()),
        *rpc_event))))
      {
        TBSYS_LOG(WARN, "fail to send scan request. [err=%d]", err);
        /// @Exception  scan failed, no failure packet would return to the framework
        ///             so, we need to release rpc_event manually here
        int tmp_err = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_err = ObMergerRequest::destroy(rpc_event)))
        {
          TBSYS_LOG(WARN, "fail to destroy rpc_event. [err=%d]", tmp_err);
        }
      }
      if ((OB_SUCCESS == err) && (NULL != triggered_rpc_event_id))
      {
        *triggered_rpc_event_id = rpc_event->get_event_id();
      }
      return err;
    }

    int ObMergerScanRequest::get_session_next(const int64_t sub_req_idx, const ObMergerRpcEvent &prev_rpc_event,
        ObNewRange &query_range, const int64_t timeout_us,  const int64_t limit_offset)
    {
      int err = OB_SUCCESS;
      const ObMergerAsyncRpcStub * async_rpc_stub = NULL;
      ObMergerRpcEvent *new_rpc_event = NULL;
      ObMergerSubScanRequest *sub_req = NULL;
      ObChunkServerItem only_replica;
      only_replica.addr_ = prev_rpc_event.get_server();
      int64_t limit_count = sharding_limit_count_;
      if ((limit_offset > 0) && (limit_count <= 0))
      {
        limit_count = common::ObMergerSchemaManager::MAX_INT64_VALUE;
      }
      if (OB_SUCCESS == err)
      {
        err = alloc_sub_scan_request(sub_req);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to allocate sub scan request, err = %d", err);
        }
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (err = sub_req->init(scan_param_, query_range, limit_offset,
        limit_count, &only_replica, 1, false, &get_buffer_pool()))))
      {
        TBSYS_LOG(WARN, "fail to init SubScanRequest. [err=%d]", err);
      }

      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = create(&new_rpc_event))))
      {
        TBSYS_LOG(WARN, "fail to create rpc event. [err=%d]", err);
      }
      else if (OB_SUCCESS == err)
      {
        new_rpc_event->set_session_id(prev_rpc_event.get_session_id());
      }

      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sub_req->add_event(
        new_rpc_event, this, only_replica))))
      {
        TBSYS_LOG(WARN, "fail to init rpc event. [err=%d]", err);
      }
      if ((OB_SUCCESS == err) && (NULL == (async_rpc_stub = get_rpc())))
      {
        TBSYS_LOG(WARN, "fail to get rpc");
        err = OB_ERROR;
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = async_rpc_stub->get_session_next(timeout_us,
        only_replica.addr_, prev_rpc_event.get_session_id(), prev_rpc_event.get_req_type(), *new_rpc_event))))
      {
        TBSYS_LOG(WARN,"fail to get session next [err:%d,session_id:%ld,req_id:%lu]", err,
          prev_rpc_event.get_session_id(),get_request_id());
      }
      if (OB_SUCCESS == err)
      {
        int64_t replica_count = ObTabletLocationList::MAX_REPLICA_COUNT;
        ObChunkServerItem     req_cs_replicas[replica_count];
        sub_requests_[sub_req_idx].get_cs_replicas(req_cs_replicas, replica_count);
        for (int64_t i = 0; i < replica_count; i++)
        {
          if (only_replica.addr_ == req_cs_replicas[i].addr_)
          {
            req_cs_replicas[i] = req_cs_replicas[replica_count - 1];
            replica_count --;
            break;
          }
        }
        if (replica_count > 0)
        {
          sub_req->reset_cs_replicas(static_cast<int32_t>(replica_count), req_cs_replicas);
        }
      }
      TBSYS_LOG(INFO, "[session next] timeout_us:%ld, limit_offset:%ld, limit_count:%ld, "
        "total_rpc_event_count:%d, session_id:%ld, reqest_id:%lu,"
        "prev_event_id:%lu,cur_event_id:%lu]",  timeout_us, limit_offset, limit_count,
        total_sub_request_count_, prev_rpc_event.get_session_id(), get_request_id(),
        prev_rpc_event.get_event_id(), (new_rpc_event ? (new_rpc_event->get_event_id()):0));
      return err;
    }

    int ObMergerScanRequest::do_request(const int64_t max_parellel_count,
      ObTabletLocationRangeIterator &range_iter,  const int64_t timeout_us,
      const int64_t limit_offset)
    {
      int err = OB_SUCCESS;
      ObNewRange query_range;

      ObMergerSubScanRequest *sub_req = NULL;
      int64_t limit_count = sharding_limit_count_;
      if ((limit_offset > 0) && (limit_count <= 0))
      {
        limit_count = common::ObMergerSchemaManager::MAX_INT64_VALUE;
      }

      int32_t replica_count = ObTabletLocationList::MAX_REPLICA_COUNT;
      ObChunkServerItem replicas[ObTabletLocationList::MAX_REPLICA_COUNT];
      int32_t triggered_rpc_event_count = 0;

      int64_t cur_limit_offset = limit_offset;
      int64_t cur_limit_count = limit_count;
      while (OB_SUCCESS == err)
      {
        if (triggered_rpc_event_count > 0)
        {
          cur_limit_offset = 0;
        }
        /// 1. split requested ObNewRange into small ranges
        err = range_iter.next(reinterpret_cast<ObChunkServerItem*>(replicas), replica_count, query_range);
        if (OB_ITER_END == err)
        {
          err = OB_SUCCESS;
          break;
        }
        else if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to get tablet locations.");
        }
        /// 2. allocate a sub scan request
        if (OB_SUCCESS == err)
        {
          err = alloc_sub_scan_request(sub_req);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail to allocate sub scan request");
          }
        }
        /// 3. init the sub scan request
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sub_req->init(scan_param_, query_range, cur_limit_offset,
          cur_limit_count, reinterpret_cast<ObChunkServerItem*>(replicas), replica_count, false, &get_buffer_pool()))))
        {
          TBSYS_LOG(WARN, "fail to init SubScanRequest. [err=%d]", err);
        }
        else if (OB_SUCCESS == err)
        {
          TBSYS_LOG(DEBUG, "sub query range info: %s", to_cstring(query_range));
        }
        /// 4. send request to cs
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = send_rpc_event(sub_req, timeout_us))))
        {
          TBSYS_LOG(WARN,"fail to send rpc event of ObMergerSubScanRequest [err:%d]", err);
        }

        if (OB_SUCCESS == err)
        {
          triggered_rpc_event_count ++;
          TBSYS_LOG(DEBUG, "success in sending a async scan request to selected server");
        }
        if ((max_parellel_count > 0)
          && (total_sub_request_count_ - finished_sub_request_count_ >= max_parellel_count))
        {
          break;
        }
      }
      FILL_TRACE_LOG("[parellel_count:%ld, total_count:%ld]", triggered_rpc_event_count, total_sub_request_count_);
      return err;
    }

    int ObMergerScanRequest::find_sub_scan_request(ObMergerRpcEvent * rpc_event,
      bool &belong_to_this, bool &is_first,  int64_t &idx)
    {
      int err = OB_SUCCESS;
      belong_to_this = false;
      is_first = false;
      for (idx = 0; idx < total_sub_request_count_; idx++)
      {
        if ((OB_SUCCESS == err) &&
          (OB_SUCCESS != (err = sub_requests_[idx].agent_event_finish(rpc_event, belong_to_this, is_first))))
        {
          TBSYS_LOG(WARN, "fail to determine if agent event finished. [err=%d]", err);
          break;
        }

        /// event not belong to the i-th subrequest, go on seraching
        if ((OB_SUCCESS == err) && (false == belong_to_this))
        {
          continue;
        }

        if ((OB_SUCCESS == err) && (false == is_first))
        {
          TBSYS_LOG(INFO, "not the first result of sub_scan[%ld]. ignore.", idx);
          break;
        }

        /// Must be: (OB_SUCCESS == err, true == is_first, true == belong_to_this)
        break;
      }

      if (!belong_to_this)
      {
        idx = -1;
      }
      return err;
    }


    int ObMergerScanRequest::check_if_need_more_req(const int64_t sub_req_idx,  const int64_t timeout_us, ObMergerRpcEvent &prev_rpc_event)
    {
      int err = OB_SUCCESS;
      ObScanner *scanner = NULL;
      ObScanParam *sub_scan_param = NULL;
      ObNewRange next_scan_range;
      int64_t next_limit_offset = 0;
      if ((OB_SUCCESS == err) && (NULL == (scanner = sub_requests_[sub_req_idx].get_scanner())))
      {
        TBSYS_LOG(WARN, "scanner is NULL. no result.");
        err = OB_ERROR;
      }

      if ((OB_SUCCESS == err) && (NULL == (sub_scan_param = sub_requests_[sub_req_idx].get_scan_param())))
      {
        TBSYS_LOG(WARN, "sub scan_param is NULL. no result.");
        err = OB_ERROR;
      }
      bool is_req_fullfilled = false;
      int64_t fullfilled_count = 0;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scanner->get_is_req_fullfilled(is_req_fullfilled,fullfilled_count))))
      {
        TBSYS_LOG(WARN,"fail to get fullfilled info from scanner [err:%d]", err);
      }

      /// (a)
      /// check if sub-request fullfilled
      /// if not , create more sub request, old scan range is updated too
      if (OB_SUCCESS == err)
      {
        if (OB_ITER_END == (err = get_next_range(*sub_scan_param, *scanner,
          sub_requests_[sub_req_idx].get_limit_offset(), next_scan_range, next_limit_offset, get_buffer_pool())))
        {
        }
        else if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to get next range, err=%d", err);
        }
        else
        {
          if ((OB_SUCCESS != (err= scan_param_->set_range(next_scan_range))))
          {
            TBSYS_LOG(WARN, "fail to set next scan range. [err:%d]", err);
          }
          else if (is_req_fullfilled)
          {
            // input scan range not fully scanned, that means input scan range splited on
            // chunkserver before issue scan request. send new sub request for new split range.
            ObTabletLocationRangeIterator range_iter;
            if (OB_SUCCESS != (err = range_iter.initialize(get_cache_proxy(),scan_param_->get_range(),
                    scan_param_->get_scan_direction(), &get_buffer_pool())))
            {
              TBSYS_LOG(WARN,"fail to initialize range iterator [err:%d]", err);
            }
            else if (OB_SUCCESS != (err = do_request(-1, range_iter,  timeout_us, next_limit_offset)))
            {
              TBSYS_LOG(WARN, "fail to issue new scan request [err:%d]", err);
            }
          }
          else if (prev_rpc_event.is_session_end())
          {
            // prev request not fullfilled(data too big to transfer); but session end.
            TBSYS_LOG(INFO, "prev_rpc_event return OB_SESSION_END, end session, no need more request.");
            err = OB_ITER_END;
          }
          else if (OB_SUCCESS != (err = get_session_next(
                  sub_req_idx, prev_rpc_event, next_scan_range,
                  timeout_us,  next_limit_offset)))
          {
            // prev request not fullfilled(data too big to transfer); use stream request.
            TBSYS_LOG(WARN, "fail to get session next [err:%d]", err);
          }
        }

        if (OB_ITER_END == err)
        {
          err = OB_SUCCESS; // must reset err code
        }
      }
      return err;
    }


    bool ObMergerScanRequest::check_if_location_cache_valid_(const ObScanner & scanner, const ObScanParam & scan_param)
    {
      int err = OB_SUCCESS;
      bool res = true;
      ObNewRange tablet_range;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scanner.get_range(tablet_range))))
      {
        TBSYS_LOG(WARN,"fail to get tablet range from scanner [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (ScanFlag::FORWARD == scan_param.get_scan_direction())
        && !(scan_param.get_range()->start_key_.is_min_row())
        && !(tablet_range.end_key_.is_max_row()))
      {
        if (scan_param.get_range()->border_flag_.inclusive_start() && (scan_param.get_range()->start_key_ > tablet_range.end_key_))
        {
          res = false;
        }
        else if (!scan_param.get_range()->border_flag_.inclusive_start()
          && (scan_param.get_range()->start_key_ >= tablet_range.end_key_))
        {
          res = false;
        }
      }

      if ((OB_SUCCESS == err)
        && res
        && (ScanFlag::BACKWARD == scan_param.get_scan_direction())
        && !(scan_param.get_range()->end_key_.is_max_row())
        && !(tablet_range.start_key_.is_min_row())
        && (scan_param.get_range()->end_key_ <= tablet_range.start_key_))
      {
        res = false;
      }
      if ((OB_SUCCESS == err)
        && res
        && (ScanFlag::FORWARD == scan_param.get_scan_direction())
        && (!tablet_range.start_key_.is_min_row()))
      {
        if (scan_param.get_range()->start_key_.is_min_row())
        {
          res = false;
        }
        else if (scan_param.get_range()->start_key_ < tablet_range.start_key_)
        {
          res = false;
        }
        else if ((scan_param.get_range()->start_key_ == tablet_range.start_key_)
          && (scan_param.get_range()->border_flag_.inclusive_start()))
        {
          res = false;
        }
      }
      if ((OB_SUCCESS == err)
        && res
        && (ScanFlag::BACKWARD == scan_param.get_scan_direction())
        && (!tablet_range.end_key_.is_max_row()))
      {
        if (scan_param.get_range()->end_key_.is_max_row())
        {
          res = false;
        }
        else if (scan_param.get_range()->end_key_ > tablet_range.end_key_)
        {
          res = false;
        }
      }

      if (OB_SUCCESS != err)
      {
        res = false;
      }
      return res;
    }

    /* @return
     *   OB_SUCCESS: normally successful
     *   OB_ITER_END: fail to retry. no more replica to retry
     */
    int ObMergerScanRequest::process_result(const int64_t timeout_us, ObMergerRpcEvent *rpc_event, bool & finish)
    {
      int64_t sub_req_idx = 0;
      int err = OB_SUCCESS;
      bool belong_to_this = false;
      bool is_first = false;
      ObNewRange next_scan_range;
      ObScanner *scanner = NULL;
      ObChunkServerItem selected_server;
      char ip_addr[ObMergerAsyncRpcStub::MAX_SERVER_LEN];
      bool can_free_res  = false;
      if (NULL != rpc_event)
      {
        rpc_event->get_server().to_string(ip_addr,sizeof(ip_addr));
        TBSYS_LOG(DEBUG, "rpc_event finised [us_used:%ld,timeout_us:%ld,request_event_id:%lu,"
          "rpc_event_id:%lu,rpc_event_client_id:%lu, server:%s,session_id:%lu, err:%d]",
          rpc_event->get_time_used(),rpc_event->get_timeout_us(), get_request_id(),
          rpc_event->get_event_id(), rpc_event->get_client_id(), ip_addr, rpc_event->get_session_id(),
          rpc_event->get_result_code());
      }

      /// param checking before start
      if (true == finish)
      {
        TBSYS_LOG(WARN, "request finished already. should not call process_result() anymore!");
        err = OB_ERROR;
      }
      else if (NULL == rpc_event || NULL == scan_param_)
      {
        TBSYS_LOG(WARN, "NULL pointer error. [rpc_event=%p][scan_param=%p]", rpc_event, scan_param_);
        err = OB_ERROR;
      }
      else
      {
        /// just to calculate how much time will be used by ms
        FILL_TRACE_LOG("");
      }

      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = find_sub_scan_request(rpc_event,
        belong_to_this, is_first,  sub_req_idx))))
      {
        TBSYS_LOG(WARN,"fail to find SubRequest for rpc event [rpc_event:%p,rpc_event_id:%ld,"
          "rpc_event->client_id:%lu,this->event_id:%lu]",  rpc_event, rpc_event->get_event_id(),
          rpc_event->get_client_id(), this->get_request_id());
      }

      if ((OB_SUCCESS == err) && (OB_SUCCESS == rpc_event->get_result_code()))
      {
        if ((OB_SUCCESS == err) && (false == belong_to_this))
        {
          TBSYS_LOG(WARN, "Unexpected. rpc event not found! The framework should deal with this case. not here");
          err = OB_ERR_UNEXPECTED;
        }

        if ((OB_SUCCESS == err)
          && (!check_if_location_cache_valid_(rpc_event->get_result(),*sub_requests_[sub_req_idx].get_scan_param())))
        {
          rpc_event->set_result_code(OB_DATA_NOT_SERVE);
          TBSYS_LOG(WARN,"location cache invalid");
        }
        if ((OB_SUCCESS == err) && (true == belong_to_this) && (true == is_first)
            && (OB_SUCCESS == rpc_event->get_result_code()))
        {
          FILL_TRACE_LOG("got first finished rpc event of [sub_request:%ld,us_used:%ld,"
              "request_event_id:%lu,rpc_event_id:%lu,session_id:%ld,server:%s,#whole_row:%ld,#rows:%ld,#cells:%ld]",
              sub_req_idx, rpc_event->get_time_used(), get_request_id(), rpc_event->get_event_id(),
              rpc_event->get_session_id(), ip_addr, rpc_event->get_result().get_whole_result_row_num(),
              rpc_event->get_result().get_row_num(), rpc_event->get_result().get_cell_num());
          cs_result_mem_size_used_ += rpc_event->get_result().get_size();
          if ((OB_SUCCESS == err) && (NULL == (scanner = sub_requests_[sub_req_idx].get_scanner())))
          {
            TBSYS_LOG(WARN, "scanner is NULL. no result.");
            err = OB_ERROR;
          }

          if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = check_if_need_more_req(sub_req_idx, timeout_us,
            *rpc_event))))
          {
            TBSYS_LOG(WARN,"fail to check if need any more request for subreq [err:%d,sub_req_idx:%ld]",
              err, sub_req_idx);
          }

          /// (d) add result to merger operator
          if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = merger_operator_.add_sharding_result(*scanner,
            sub_requests_[sub_req_idx].get_query_range(), sub_requests_[sub_req_idx].get_limit_offset(),
            finish, can_free_res))))
          {
            TBSYS_LOG(WARN, "fail to add sharding result. [err=%d]", err);
          }

          if (OB_SUCCESS == err)
          {
            finished_sub_request_count_++;
            bool is_fullfill = false;
            ObRowkey last_rowkey;
            rpc_event->get_result().get_last_row_key(last_rowkey);
            ObNewRange scanner_range;
            rpc_event->get_result().get_range(scanner_range);
            int64_t fullfill_num = 0;
            rpc_event->get_result().get_is_req_fullfilled(is_fullfill, fullfill_num);
            FILL_TRACE_LOG("finish add cs result[#cells:%ld,#rows:%ld,whole_rows:%ld,"
                "finished_sub_request_count_:%d,server:%s,fullfill:%d,fullfill_num:%ld,"
                "last_rowkey:%s, range:%s, scan_param:%s]",
                rpc_event->get_result().get_cell_num(),rpc_event->get_result().get_row_num(),
                rpc_event->get_result().get_whole_result_row_num(),  finished_sub_request_count_,
                ip_addr, is_fullfill, fullfill_num, to_cstring(last_rowkey), to_cstring(scanner_range),
                to_cstring(*(sub_requests_[sub_req_idx].get_scan_param()->get_range())));
          }
        }
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != rpc_event->get_result_code()) && belong_to_this)
      {
        rpc_event->get_server().to_string(ip_addr, sizeof(ip_addr));
        TBSYS_LOG(WARN, "rpc event return code not success. [err=%d,event_id:%lu, server=%s]", rpc_event->get_result_code(),
          rpc_event->get_event_id(), ip_addr);
        if (OB_SUCCESS != update_location_cache(rpc_event->get_server(),rpc_event->get_result_code(),
            *sub_requests_[sub_req_idx].get_scan_param()))
        {
          TBSYS_LOG(WARN,"fail to update location cache");
        }

        if ((OB_SUCCESS == err) && is_first)
        {
          if (OB_SUCCESS != (err = retry(sub_req_idx, rpc_event, timeout_us)))
          {
            TBSYS_LOG(WARN, "this retry not success");
          }
          if (OB_ITER_END == err)
          {
            TBSYS_LOG(WARN, "tried all replica and not successful.");
            err = rpc_event->get_result_code();
          }
        }
      }

      /// check org request finshed yet
      if ((OB_SUCCESS == err) && (!finish) && (!org_req_range_iter_.end()))
      {
        err = do_request(max_parellel_count_,org_req_range_iter_, timeout_us);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"fail to trigger left requests [err:%d]", err);
        }
      }

      /// if all request finish, can prepare to get cells
      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(DEBUG, "finished_sub_request_count_ =%d, total_sub_request_count_=%d",
          finished_sub_request_count_ ,
          total_sub_request_count_);
        if ((finished_sub_request_count_ == total_sub_request_count_) || (finish))
        {
          finish = true;
          if (OB_SUCCESS != (err = prepare_get_cell()))
          {
            TBSYS_LOG(WARN, "fail to prepare getting cell. [err=%d]", err);
          }
        }
        else if (finished_sub_request_count_ > total_sub_request_count_)
        {
          TBSYS_LOG(ERROR, "unexpected sub request finish count. [finished=%d][total=%d]",
            finished_sub_request_count_, total_sub_request_count_);
          err = OB_ERR_UNEXPECTED;
        }
      }

      if (can_free_res && (NULL != scanner))
      {
        scanner->clear();
      }
      if ((OB_SUCCESS != err) || (finish))
      {
        end_sessions_();
      }
      return err;
    }

    void ObMergerScanRequest::end_sessions_()
    {
      for (int64_t i = 0; (i < total_sub_request_count_); i++)
      {
        if (sub_requests_[i].get_session_id() > ObCommonRpcEvent::INVALID_SESSION_ID)
        {
          terminate_remote_session(sub_requests_[i].get_session_server(), sub_requests_[i].get_session_id());
          TBSYS_LOG(INFO,"end unfinished session [scan_event:%lu,idx:%ld,session_id:%lu]", get_request_id(),
            i, sub_requests_[i].get_session_id());
        }
      }
    }

    int ObMergerScanRequest::retry(const int64_t sub_req_idx, ObMergerRpcEvent *rpc_event, int64_t timeout_us)
    {
      int err = OB_SUCCESS;

      if (NULL == rpc_event)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "null pointer error. [rpc_event=%p][err=%d]", rpc_event, err);
      }

      /// update cs replicas if neccery
      if ((OB_SUCCESS == err) && (OB_DATA_NOT_SERVE == rpc_event->get_result_code()))
      {
        ObTabletLocationRangeIterator range_iter;
        ObNewRange query_range;
        int32_t replica_count = ObTabletLocationList::MAX_REPLICA_COUNT;
        ObChunkServerItem replicas[ObTabletLocationList::MAX_REPLICA_COUNT];
        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = range_iter.initialize(get_cache_proxy(),
                sub_requests_[sub_req_idx].get_scan_param()->get_range(),
                sub_requests_[sub_req_idx].get_scan_param()->get_scan_direction(),
                &get_buffer_pool()))))
        {
          TBSYS_LOG(WARN,"fail to initialize range iterator [err:%d]", err);
        }

        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = range_iter.next(replicas,replica_count,query_range)))
          && (OB_ITER_END != err))
        {
          TBSYS_LOG(WARN,"fail to find replicas [err:%d]", err);
        }

        if (OB_ITER_END == err)
        {
          TBSYS_LOG(WARN,"fail to find replicas, while retry [err:%d]", err);
          err = OB_ERR_UNEXPECTED;
        }

        if ((OB_SUCCESS == err) && (replica_count <= 0))
        {
          TBSYS_LOG(WARN,"fail to find replicas, while retry [err:%d,replica_count:%d]", err, replica_count);
          err = OB_DATA_NOT_SERVE;
        }

        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = sub_requests_[sub_req_idx].reset_cs_replicas(replica_count, replicas))))
        {
          TBSYS_LOG(WARN,"fail to set sub request's cs replicas [err:%d]", err);;
        }
      }

      if (OB_SUCCESS == err)
      {
        /// @note: only if tried_replica_count < total_replica_count we would retry
        if (sub_requests_[sub_req_idx].tried_replica_count() >= sub_requests_[sub_req_idx].total_replica_count())
        {

          TBSYS_LOG(WARN, "exceeds retry times. [tried_replica_count=%d][total_replica_count=%d]",
            sub_requests_[sub_req_idx].tried_replica_count(), sub_requests_[sub_req_idx].total_replica_count());
          err = rpc_event->get_result_code();
        }

        uint64_t retry_event_id = 0;
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = send_rpc_event(sub_requests_ + sub_req_idx,
              timeout_us, &retry_event_id))))
        {
          TBSYS_LOG(WARN,"fail to resend rpc event of ObMergerSubScanRequest [idx:%ld,err:%d]", sub_req_idx, err);
        }
        else if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "retry [prev_rpc:%lu,retry_rpc:%lu,request:%lu]", rpc_event->get_event_id(), retry_event_id,
            get_request_id());
        }
      }
      return err;
    }


    void ObMergerScanRequest::reset()
    {
      /// reset all finished rpc event request
      /// leave the rest to the framework
      for (int32_t i = 0; i < total_sub_request_count_; i++)
      {
        sub_requests_[i].reset();
      }
      total_sub_request_count_ = 0;
      finished_sub_request_count_ = 0;
      merger_operator_.reset();
      ObMergerRequest::reset();
      scan_param_ = NULL;
      cs_result_mem_size_used_ = 0;
      sharding_limit_count_ = 0;
      cur_row_cell_cnt_ = 0;
    }

    int ObMergerScanRequest::prepare_get_cell()
    {
      int err = OB_SUCCESS;
      err = merger_operator_.seal();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to seal merger operator. [err=%d]", err);
      }
      else
      {
        FILL_TRACE_LOG("finish seal local result");
      }
      return err;
    }

    int ObMergerScanRequest::next_cell()
    {
      int err = OB_ITER_END;
      err = merger_operator_.next_cell();
      return err;
    }

    int ObMergerScanRequest::get_cell(ObInnerCellInfo** cell)
    {
      return merger_operator_.get_cell(cell, NULL);
    }

    int ObMergerScanRequest::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
    {
      int err = OB_ERROR;
      err = merger_operator_.get_cell(cell, is_row_changed);
      return err;
    }

    int ObMergerScanRequest::fill_result(common::ObScanner & scanner, common::ObScanParam &org_scan_param,
      bool &got_all_result)
    {
      int err = OB_SUCCESS;
      ObInnerCellInfo *cur_cell = NULL;
      bool size_over_flow = false;
      bool is_row_changed = false;
      int64_t result_row_width = (org_scan_param.get_group_by_param().get_returned_column_num() > 0)
        ? org_scan_param.get_group_by_param().get_returned_column_num()
        : org_scan_param.get_returned_column_num();
      int64_t got_cell_count = 0;
      int64_t cell_remainder_num = 0;
      ObString table_name = org_scan_param.get_table_name();

      if (result_row_width <= 0)
      {
        TBSYS_LOG(WARN, "invalid result_row_width: %ld", result_row_width);
        err = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; (i < cur_row_cell_cnt_) && (OB_SUCCESS == err); i ++, got_cell_count ++)
      {
        if ((OB_SUCCESS != (err = scanner.add_cell(row_cells_[i], false,
                  (got_cell_count % result_row_width == 0) ? true : false))))
        {
          TBSYS_LOG(WARN,"fail to add cell to scanner [err:%d]", err);
        }
      }

      while ((OB_SUCCESS == err) && (!size_over_flow))
      {
        cell_remainder_num = (result_row_width == 0 ? 0 : got_cell_count % result_row_width);
        if ((OB_SUCCESS != (err = next_cell())) && (OB_ITER_END != err))
        {
          TBSYS_LOG(WARN,"fail to get next cell from ObMergerScanRequest [err:%d, got_cell_count:%ld]", err, got_cell_count);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = get_cell(&cur_cell, &is_row_changed))))
        {
          TBSYS_LOG(WARN,"fail to get cell from ObMergerScanRequest [err:%d, got_cell_count:%ld]", err, got_cell_count);
        }
        if ((OB_SUCCESS == err) && (is_row_changed))
        {
          if (cell_remainder_num != 0)
          {
            TBSYS_LOG(WARN,"unexpected error, row changed unexpected[got_cell_count:%ld,result_row_width:%ld]",
              got_cell_count+1, result_row_width);
            err = OB_ERR_UNEXPECTED;
          }
          else
          {
            cur_row_cell_cnt_ = 0;
          }
        }
        if ((OB_SUCCESS == err) && (cur_row_cell_cnt_ >= MAX_ROW_COLUMN_COUNT))
        {
          TBSYS_LOG(WARN,"row cells count is too big, [cur_row_cell_cnt_:%ld,max_row_cell_cnt:%ld]",
            cur_row_cell_cnt_, MAX_ROW_COLUMN_COUNT);
          err = OB_ARRAY_OUT_OF_RANGE;
        }
        if (OB_SUCCESS == err)
        {
          // cur_cell->table_name_ = table_name;
          row_cells_[cur_row_cell_cnt_].table_name_ = table_name;
          row_cells_[cur_row_cell_cnt_].table_id_ = OB_INVALID_ID;
          row_cells_[cur_row_cell_cnt_].column_id_ = OB_INVALID_ID;
          row_cells_[cur_row_cell_cnt_].row_key_ = cur_cell->row_key_;
          row_cells_[cur_row_cell_cnt_].value_ = cur_cell->value_;
        }
        if ((OB_SUCCESS == err)
          && (org_scan_param.get_group_by_param().get_aggregate_row_width() > 0)
          && (OB_SUCCESS != (err = org_scan_param.get_group_by_param().get_aggregate_column_name(cell_remainder_num,
          row_cells_[cur_row_cell_cnt_].column_name_))))
        {
          TBSYS_LOG(WARN,"fail to get aggregate column_name [cell_idx:%ld,err:%d]", cell_remainder_num, err);
        }
        if ((OB_SUCCESS == err)
          && (org_scan_param.get_group_by_param().get_aggregate_row_width() <= 0)
          && (OB_SUCCESS != (err = org_scan_param.get_select_column_name(cell_remainder_num,
          row_cells_[cur_row_cell_cnt_].column_name_))))
        {
          TBSYS_LOG(WARN,"fail to get select column_name [cell_idx:%ld,err:%d]", cell_remainder_num, err);
        }
        if (OB_SUCCESS == err)
        {
          ++cur_row_cell_cnt_;
        }

        if (OB_SUCCESS == err)
        {
          if ((OB_SUCCESS != (err = scanner.add_cell(row_cells_[cur_row_cell_cnt_ - 1], false,
                    (cell_remainder_num == 0) ? true : false))) && (OB_SIZE_OVERFLOW != err))
          {
            TBSYS_LOG(WARN,"fail to add cell to result [err:%d]", err);
          }
          else if (OB_SIZE_OVERFLOW == err)
          {
            if (OB_SUCCESS != (err = scanner.rollback()))
            {
              TBSYS_LOG(WARN,"fail to rollback ObScanner [err:%d]", err);
            }
            else
            {
              size_over_flow = true;
              got_cell_count -= cur_row_cell_cnt_ - 1;
            }
          }
          else if (OB_SUCCESS == err)
          {
            got_cell_count ++;
          }
        }
      }
      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      if (OB_SUCCESS == err)
      {
        if (0 == result_row_width)
        {
          TBSYS_LOG(WARN,"ridiculous query which returned zero column");
        }
        else if (scanner.get_cell_num() % result_row_width != 0)
        {
          TBSYS_LOG(WARN,"unexpected err [got_cell_count:%ld,result_row_width:%ld]", scanner.get_cell_num(), result_row_width);
          err = OB_ERR_UNEXPECTED;
        }
      }

      if (OB_SUCCESS == err)
      {
        scanner.set_is_req_fullfilled(!size_over_flow,
            result_row_width == 0 ? 0 : scanner.get_cell_num()/result_row_width);
        scanner.set_whole_result_row_num(get_whole_result_row_count());
        got_all_result = !size_over_flow;
      }
      return err;
    }
    /// namespace
  }
}

