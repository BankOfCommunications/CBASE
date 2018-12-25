/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_get_request.cpp for
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *
 */

#include "ob_ms_request.h"
#include "ob_ms_get_request.h"
#include "ob_ms_async_rpc.h"
#include "ob_chunk_server_task_dispatcher.h"
#include "common/ob_trace_log.h"
#include "common/utility.h"
#include "common/ob_action_flag.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerGetRequest::ObMergerGetRequest(common::ObTabletLocationCacheProxy * cache_proxy,
  ObMergerAsyncRpcStub * async_rpc): ObMergerRequest(cache_proxy, async_rpc),
  page_arena_(PageArena<int64_t, ModulePageAllocator>::DEFAULT_PAGE_SIZE,
  ModulePageAllocator(ObModIds::OB_MS_GET_EVENT))
{
  req_dist_map_inited_ = false;
  reserve_get_param_count_ = 0;
  total_sub_request_count_ = 0;
  memset(same_cs_get_row_count_, 0, sizeof(same_cs_get_row_count_));
  reset();
}

void ObMergerGetRequest::reset()
{
  for (int64_t i = 0; i < total_sub_request_count_; i ++)
  {
    if (i >= reserve_get_param_count_)
    {
      sub_requests_[i].clear_get_param();
    }
    sub_requests_[i].reset();
    same_cs_get_row_count_[i] = 0;
  }
  total_sub_request_count_ = 0;
  finished_sub_request_count_ = 0;
  page_arena_.free();
  get_param_ = NULL;
  max_parellel_count_ = 0;
  max_get_rows_per_subreq_ = 0;
  cur_row_cell_cnt_ = 0;
  triggered_sub_request_count_ = 0;
  req_dist_map_.clear();
  ObMergerRequest::reset();
  sealed_ = false;
  last_not_exist_cell_.table_id_ = OB_INVALID_ID;
  poped_cell_count_ = 0;
}

int ObMergerGetRequest::distribute_request()
{
  int err = OB_SUCCESS;
  ObTabletLocationList loc_list;
  loc_list.set_buffer(ObMergerRequest::get_buffer_pool());
  int64_t cell_size = get_param_->get_cell_size();
  int64_t row_count = get_param_->get_row_size();
  int64_t row_start = 0;
  int64_t row_end   = 0;
  const ObGetParam::ObRowIndex* row_index = get_param_->get_row_index();

  /**
   * if we just send one sub get request to one chunkserver, maybe
   * the sub get request includes tens of rows, the chunkserver
   * will take much time to operate a big get request, so we limit
   * the row count of get request, it's good for the average
   * response time of get requst. if max_get_rows_per_subreq_ ==
   * 0, we only choose chunkserver randomly. if
   * max_get_rows_per_subreq_ > 0, we choose chunkserver randomly
   * and limit the row count of sub get request at the same time.
   */
  for (int64_t row_idx = 0; (row_idx < row_count) && (OB_SUCCESS == err); ++row_idx)
  {
    row_start = row_index[row_idx].offset_;
    row_end   = row_index[row_idx].offset_ + row_index[row_idx].size_;
    if (row_start >= cell_size || row_end > cell_size)
    {
      TBSYS_LOG(WARN, "wrong row range, cell_size=%ld, start=%ld, end=%ld",
                cell_size, row_end, row_end);
      err = OB_ERROR;
      break;
    }
    if (OB_SUCCESS != (err = get_cache_proxy()->get_tablet_location(
      get_param_->operator [](row_start)->table_id_,
      get_param_->operator [](row_start)->row_key_, loc_list)))
    {
      TBSYS_LOG(WARN,"fail to get tablet location [err:%d,table_id:%lu,rowkey:%s]",
        err, get_param_->operator [](row_start)->table_id_,
        to_cstring(get_param_->operator [](row_start)->row_key_));
      break;
    }

    if ((OB_SUCCESS == err) && (0 >= loc_list.size()))
    {
      TBSYS_LOG(WARN,"fail to get tablet location [table_id:%lu,rowkey:%s]",
        get_param_->operator [](row_start)->table_id_,
        to_cstring(get_param_->operator [](row_start)->row_key_));
      err = OB_DATA_NOT_SERVE;
      break;
    }
    // for each row, choose chunkserver randomly
    int32_t svr_idx = ObChunkServerTaskDispatcher::get_instance()->select_cs(loc_list);
    int64_t sub_req_idx = -1;
    int64_t same_cs_subreq_idx = -1;
    int64_t ip = loc_list[svr_idx].server_.chunkserver_.get_ipv4();
    if (req_dist_map_.get(ip, sub_req_idx) == oceanbase::common::hash::HASH_EXIST
        && sub_req_idx >= 0)
    {
      same_cs_subreq_idx = sub_req_idx;
    }
    else
    {
      same_cs_subreq_idx = total_sub_request_count_;
    }

    bool is_subreq_full = (max_get_rows_per_subreq_ > 0 && svr_idx < loc_list.size()
                           && sub_req_idx >= 0 && same_cs_get_row_count_[sub_req_idx] > 0
                           && (0 == same_cs_get_row_count_[sub_req_idx] % max_get_rows_per_subreq_));
    if ((OB_SUCCESS == err) && (sub_req_idx < 0 || is_subreq_full))
    {
      if (total_sub_request_count_ < MAX_SUBREQUEST_NUM)
      {
        int hash_err = 0;
        sub_req_idx = total_sub_request_count_;
        total_sub_request_count_ ++;
        sub_requests_[sub_req_idx].reset();
        sub_requests_[sub_req_idx].init(page_arena_);
        if (!is_subreq_full && oceanbase::common::hash::HASH_INSERT_SUCC !=
          (hash_err = req_dist_map_.set(loc_list[svr_idx].server_.chunkserver_.get_ipv4(),sub_req_idx)))
        {
          TBSYS_LOG(WARN,"fail to insert hash map [hash_err:%d]", hash_err);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err)
        {
          sub_requests_[sub_req_idx].set_last_svr_ipv4(loc_list[svr_idx].server_.chunkserver_.get_ipv4());
        }
        // start a new sub request for this server
        if (true == is_subreq_full)
        {
          hash_err = req_dist_map_.set(ip, sub_req_idx, 1);
          if (hash_err != hash::HASH_OVERWRITE_SUCC)
          {
            TBSYS_LOG(WARN, "update the full cs failed:%ld, idx:%ld, ret:%d", ip, sub_req_idx, hash_err);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN,"sub request out of range [total_sub_request_count_:%d,"
            "MAX_SUBREQUEST_NUM:%d,max_get_rows_per_subreq_:%ld]",
            total_sub_request_count_, MAX_SUBREQUEST_NUM, max_get_rows_per_subreq_);
        err = OB_ERROR_OUT_OF_RANGE;
      }

      if (OB_SUCCESS == err)
      {
        sub_requests_[sub_req_idx].set_param(*get_param_);
      }
    }

    if (OB_SUCCESS == err && max_get_rows_per_subreq_ > 0)
    {
      same_cs_get_row_count_[same_cs_subreq_idx]++;
    }
    for (int64_t i = row_start; i < row_end && OB_SUCCESS == err; ++i)
    {
      err = sub_requests_[sub_req_idx].add_cell(i);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to add cell to sub request [err:%d]", err);
      }
    }
  }
  ObMergerRequest::get_buffer_pool().reset();
  return err;
}

int ObMergerGetRequest::send_request(const int64_t sub_req_idx, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  ObGetParam *cur_get_param = NULL;
  if ((OB_SUCCESS == err)
    && (OB_SUCCESS != (err = sub_requests_[sub_req_idx].get_cur_param(cur_get_param)))
    && (OB_ITER_END != err))
  {
    TBSYS_LOG(WARN,"fail to get next param of sub request [request_id:%lu,sub_req_idx:%ld,err:%d]",
      get_request_id(), sub_req_idx, err);
  }
  ObTabletLocationList loc_list;
  loc_list.set_buffer(ObMergerRequest::get_buffer_pool());
  int64_t cell_idx = 0;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = get_cache_proxy()->get_tablet_location(
    (*cur_get_param)[cell_idx]->table_id_, (*cur_get_param)[cell_idx]->row_key_, loc_list))))
  {
    TBSYS_LOG(WARN,"fail to get tablet location [err:%d,table_id:%lu,rowkey:%s]", err,
      (*cur_get_param)[cell_idx]->table_id_,
      to_cstring((*cur_get_param)[cell_idx]->row_key_));
  }
  if ((OB_SUCCESS == err) && (0 >= loc_list.size()))
  {
    TBSYS_LOG(WARN,"fail to get tablet location [table_id:%lu,rowkey:%s]",
      (*cur_get_param)[cell_idx]->table_id_,
      to_cstring((*cur_get_param)[cell_idx]->row_key_));
    err = OB_DATA_NOT_SERVE;
  }
  int64_t svr_idx = 0;
  for (;(svr_idx < loc_list.size()) && (OB_SUCCESS == err); svr_idx ++)
  {
    if (loc_list[svr_idx].server_.chunkserver_.get_ipv4() == sub_requests_[sub_req_idx].get_last_svr_ipv4())
    {
      if (sub_requests_[sub_req_idx].get_last_svr_ipv4() == sub_requests_[sub_req_idx].get_fail_svr_ipv4())
      {
        svr_idx = (svr_idx + 1)%loc_list.size();
        TBSYS_LOG(INFO, "sub req retry [request_id:%lu,sub_req_idx:%ld,tried_times:%ld,retry_svr:%d]", get_request_id(),
          sub_req_idx, sub_requests_[sub_req_idx].get_retry_times(), loc_list[svr_idx].server_.chunkserver_.get_ipv4());
      }
      break;
    }
  }
  if ((OB_SUCCESS == err) && (svr_idx >= loc_list.size()))
  {
    svr_idx = random()%loc_list.size();
  }
  ObMergerRpcEvent *rpc_event = NULL;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = ObMergerRequest::create(&rpc_event))))
  {
    TBSYS_LOG(WARN,"fail to create ObMergerRpcEvent [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    rpc_event->set_server(loc_list[svr_idx].server_.chunkserver_);
    if (OB_SUCCESS != (err = get_rpc()->get(static_cast<int64_t>(static_cast<double>(timeout_us) * get_timeout_percent()),
            loc_list[svr_idx].server_.chunkserver_,(*cur_get_param),*rpc_event)))
    {
      TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
    }
    else
    {
      sub_requests_[sub_req_idx].set_last_rpc_event(*rpc_event);
      sub_requests_[sub_req_idx].set_last_svr_ipv4(loc_list[svr_idx].server_.chunkserver_.get_ipv4());
    }
  }
  ObMergerRequest::get_buffer_pool().reset();
  return err;
}


int ObMergerGetRequest::get_session_next(const int64_t sub_req_idx,
  ObMergerRpcEvent & prev_rpc, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  ObMergerRpcEvent *rpc_event = NULL;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = ObMergerRequest::create(&rpc_event))))
  {
    TBSYS_LOG(WARN,"fail to create ObMergerRpcEvent [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = get_rpc()->get_session_next(timeout_us,prev_rpc.get_server(),
            prev_rpc.get_session_id(), prev_rpc.get_req_type(), *rpc_event)))
    {
      TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
    }
    else
    {
      sub_requests_[sub_req_idx].set_last_rpc_event(*rpc_event, prev_rpc.get_session_id());
      sub_requests_[sub_req_idx].set_last_svr_ipv4(prev_rpc.get_server().get_ipv4());
      TBSYS_LOG(INFO, "session next [request_id:%lu,prev_rpc:%lu,cur_rpc:%lu]", get_request_id(),
        prev_rpc.get_event_id(), rpc_event->get_event_id());
    }
  }
  return err;
}


int ObMergerGetRequest::check_if_need_more_req(const int64_t current_sub_req_idx,
  const int64_t timeout_us, ObMergerRpcEvent *prev_rpc_event)
{
  int err = OB_SUCCESS;
  // not finished
  if ((current_sub_req_idx >= 0) && (!sub_requests_[current_sub_req_idx].finished()))
  {
    bool fullfilled = true;
    int64_t fullfilled_item_count = 0;
    if ((OB_SUCCESS == err)
      && (NULL != prev_rpc_event)
      && (OB_SUCCESS != (err = prev_rpc_event->get_result().get_is_req_fullfilled(fullfilled,fullfilled_item_count))))
    {
      TBSYS_LOG(WARN,"fail to get fullfill info from resut [request_id:%lu,rpc_event_id:%lu,err:%d]",
        get_request_id(), prev_rpc_event->get_event_id(), err);
    }
    if ((OB_SUCCESS == err) && (NULL != prev_rpc_event) && (prev_rpc_event->get_session_id() > 0) && !fullfilled)
    {
      // go stream interface
      if (OB_SUCCESS != (err = get_session_next(current_sub_req_idx, *prev_rpc_event,timeout_us)))
      {
        TBSYS_LOG(WARN,"fail to get_session_next [err:%d]", err);
      }
    }
    else if (OB_SUCCESS == err)
    {
      // if go here, retry
      if ((OB_SUCCESS != (err = send_request(current_sub_req_idx,timeout_us))) && (OB_ITER_END != err))
      {
        TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
      }
      else if (OB_ITER_END == err)
      {
        finished_sub_request_count_ ++;
        err  = OB_SUCCESS;
      }
    }
  }

  if ((current_sub_req_idx >= 0) && (sub_requests_[current_sub_req_idx].finished()))
  {
    finished_sub_request_count_ ++;
  }

  while ((OB_SUCCESS == err) && (triggered_sub_request_count_ < total_sub_request_count_)
    && (triggered_sub_request_count_ - finished_sub_request_count_ < max_parellel_count_))
  {
    if ((OB_SUCCESS != (err = send_request(triggered_sub_request_count_,timeout_us))) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
    }
    else if (OB_ITER_END == err)
    {
      finished_sub_request_count_ ++;
      err  = OB_SUCCESS;
    }
    else if (OB_SUCCESS == err)
    {
      triggered_sub_request_count_ ++;
    }
  }
  return err;
}

int ObMergerGetRequest::set_request_param(ObGetParam &get_param, const int64_t timeout_us,
  const int64_t max_parellel_count, const int64_t max_get_rows_per_subreq, const int64_t reserve_get_param_count)
{
  int err = OB_SUCCESS;
  if (max_parellel_count > 0)
  {
    max_parellel_count_ = max_parellel_count;
  }
  else
  {
    max_parellel_count_ = DEFAULT_MAX_PARELLEL_COUNT;
  }
  //max_parellel_count_ = (max_parellel_count > 0) ? max_parellel_count : DEFAULT_MAX_PARELLEL_COUNT;
  //max_get_rows_per_subreq_ = max_get_rows_per_subreq > 0 ? max_get_rows_per_subreq : DEFAULT_MAX_GET_ROWS_PER_SUBREQ;
  //reserve_get_param_count_ = reserve_get_param_count >= 0 ? reserve_get_param_count : DEFAULT_RESERVE_PARAM_COUNT;
  if (max_get_rows_per_subreq > 0)
  {
    max_get_rows_per_subreq_ = max_get_rows_per_subreq;
  }
  else
  {
    max_get_rows_per_subreq_ = DEFAULT_MAX_GET_ROWS_PER_SUBREQ;
  }
  if (reserve_get_param_count >= 0)
  {
    reserve_get_param_count_ = reserve_get_param_count;
  }
  else
  {
    reserve_get_param_count_ = DEFAULT_RESERVE_PARAM_COUNT;
  }
  get_param_ = &get_param;
  if (!req_dist_map_inited_ && (OB_SUCCESS == err) && (0 != req_dist_map_.create(HASH_BUCKET_NUM)))
  {
    TBSYS_LOG(WARN,"fail to create hash map for request distribution");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCCESS == err)
  {
    req_dist_map_inited_ = true;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = distribute_request())))
  {
    TBSYS_LOG(WARN,"fail to distribute request [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = check_if_need_more_req(-1,timeout_us))))
  {
    TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
  }
  return err;
}


int ObMergerGetRequest::process_result(const int64_t timeout_us, ObMergerRpcEvent *rpc_event, bool& finish)
{
  int err = OB_SUCCESS;
  finish = false;
  if (NULL == rpc_event)
  {
    TBSYS_LOG(WARN,"invalid argument [rpc_event:%p]", rpc_event);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    char ip_addr[ObMergerAsyncRpcStub::MAX_SERVER_LEN];
    rpc_event->get_server().to_string(ip_addr,sizeof(ip_addr));
    if (OB_SUCCESS == rpc_event->get_result_code())
    {
      TBSYS_LOG(DEBUG, "rpc event finish [request_id:%lu,rpc_event->client_id:%lu,rpc_event_id:%lu,#cell:%ld,"
        "timeused:%ld,timeout:%ld,server:%s,session_id:%ld,err:%d]",
        get_request_id(), rpc_event->get_client_id(), rpc_event->get_event_id(),
        rpc_event->get_result().get_cell_num(),  rpc_event->get_time_used(), rpc_event->get_timeout_us(),
        ip_addr, rpc_event->get_session_id(), rpc_event->get_result_code());
    }
    else
    {
      TBSYS_LOG(WARN, "rpc event finish [request_id:%lu,rpc_event->client_id:%lu,rpc_event_id:%lu,"
        "timeused:%ld,timeout:%ld,server:%s,session_id:%ld,err:%d]",
        get_request_id(), rpc_event->get_client_id(), rpc_event->get_event_id(),
        rpc_event->get_time_used(), rpc_event->get_timeout_us(),
        ip_addr, rpc_event->get_session_id(), rpc_event->get_result_code());
    }
    FILL_TRACE_LOG("finish rpc:server:%s,event:%lu,err:%d", ip_addr, rpc_event->get_event_id(), rpc_event->get_result_code());
  }
  int64_t sub_req_idx = 0;
  for (;(sub_req_idx < triggered_sub_request_count_) && (OB_SUCCESS == err); sub_req_idx ++ )
  {
    if (sub_requests_[sub_req_idx].get_last_rpc_event() == rpc_event->get_event_id())
    {
      break;
    }
  }
  if (sub_req_idx >= triggered_sub_request_count_)
  {
    TBSYS_LOG(ERROR, "rpc event not belong to current request [request_id:%lu,rpc_event_id:%lu]", get_request_id(),
      rpc_event->get_event_id());
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err)
    && (NULL != sub_requests_[sub_req_idx].get_first_unfetched_cell())
    && (OB_SUCCESS != update_location_cache(rpc_event->get_server(), rpc_event->get_result_code(), *sub_requests_[sub_req_idx].get_first_unfetched_cell())))
  {
    TBSYS_LOG(WARN,"fail to update location cache");
  }
  if ((OB_SUCCESS == err)
    && (OB_SUCCESS == rpc_event->get_result_code())
    && (OB_SUCCESS != (err = sub_requests_[sub_req_idx].add_result(rpc_event->get_result()))))
  {
    TBSYS_LOG(WARN,"fail to add result to sub request [request_id:%lu,rpc_event_id:%lu,err:%d]", get_request_id(),
      rpc_event->get_event_id(), err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != rpc_event->get_result_code()))
  {
    int64_t tried_times = 0;
    if ((tried_times =  sub_requests_[sub_req_idx].retry()) > MAX_RETRY_TIMES)
    {
      TBSYS_LOG(WARN,"sub request retry too many times [request_id:%lu,tried_times:%ld,MAX_RETRY_TIMES:%d]",
        get_request_id(), tried_times, MAX_RETRY_TIMES);
      err = rpc_event->get_result_code();
    }
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = check_if_need_more_req(sub_req_idx,timeout_us, rpc_event))))
  {
    TBSYS_LOG(WARN,"fail to send request [request_id:%lu,err:%d]", get_request_id(), err);
  }
  if ((OB_SUCCESS == err) && (finished_sub_request_count_ > total_sub_request_count_))
  {
    TBSYS_LOG(ERROR, "unexpected error [finished_sub_request_count_:%d,total_sub_request_count_:%d,request_id:%lu]",
      finished_sub_request_count_, total_sub_request_count_, get_request_id());
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err) && (finished_sub_request_count_ == total_sub_request_count_))
  {
    finish = true;
  }
  if ((OB_SUCCESS == err) && (finish))
  {
    if (OB_SUCCESS != (err = merger_operator_.init(sub_requests_,total_sub_request_count_, *get_param_)))
    {
      TBSYS_LOG(WARN,"fail to seal result [err:%d]", err);
    }
    else
    {
      sealed_ = true;
    }
  }
  return err;
}


int ObMergerGetRequest::reset_iterator()
{
  int err = OB_SUCCESS;
  if (!sealed_)
  {
    TBSYS_LOG(WARN,"get request haven't finished yet");
    err = OB_INVALID_ARGUMENT;
  }
  for (int64_t sub_req_idx = 0; (sub_req_idx < total_sub_request_count_) && (OB_SUCCESS == err); sub_req_idx ++)
  {
    if (OB_SUCCESS != (err =  sub_requests_[sub_req_idx].reset_iterator()))
    {
      TBSYS_LOG(WARN,"fail to reset iterator of sub req [err:%d]", err);
    }
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = merger_operator_.init(sub_requests_,total_sub_request_count_, *get_param_))))
  {
    TBSYS_LOG(WARN,"fail to seal result [err:%d]", err);
  }
  return err;
}

int ObMergerGetRequest::next_cell()
{
  int err = OB_SUCCESS;
  if (sealed_)
  {
    if ((OB_SUCCESS !=(err = merger_operator_.next_cell())) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to call next_cell [err:%d]", err);
    }
  }
  else
  {
    TBSYS_LOG(WARN,"get request not finished yet");
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}

int ObMergerGetRequest::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
{
  int err = OB_SUCCESS;
  if (sealed_)
  {
    if (OB_SUCCESS !=(err = merger_operator_.get_cell(cell,is_row_changed)))
    {
      TBSYS_LOG(WARN,"fail to call next_cell [err:%d]", err);
    }
  }
  else
  {
    TBSYS_LOG(WARN,"get request not finished yet");
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}

int ObMergerGetRequest::get_cell(ObInnerCellInfo** cell)
{
  int err = OB_SUCCESS;
  if (sealed_)
  {
    if (OB_SUCCESS !=(err = merger_operator_.get_cell(cell)))
    {
      TBSYS_LOG(WARN,"fail to call next_cell [err:%d]", err);
    }
  }
  else
  {
    TBSYS_LOG(WARN,"get request not finished yet");
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}

int ObMergerGetRequest::fill_result(ObScanner & scanner, ObGetParam &org_param,
  bool &got_all_result)
{
  int err = OB_SUCCESS;
  ObInnerCellInfo *cur_cell = NULL;
  bool size_over_flow = false;
  bool row_changed = false;
  int64_t got_cell_count = 0;
  if (!sealed_)
  {
    TBSYS_LOG(WARN,"request not finished yet");
    err  = OB_INVALID_ARGUMENT;
  }

  for (int64_t i = 0; (i < cur_row_cell_cnt_) && (OB_SUCCESS == err); i ++, poped_cell_count_ ++)
  {
    if ((OB_SUCCESS != (err = scanner.add_cell(row_cells_[i]))))
    {
      TBSYS_LOG(WARN,"fail to add cell to scanner [err:%d]", err);
    }
  }
  while ((OB_SUCCESS == err) && (!size_over_flow))
  {
    if ((OB_SUCCESS != (err = merger_operator_.next_cell())) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to call next_cell [err:%d]", err);
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = merger_operator_.get_cell(&cur_cell, &row_changed))))
    {
      TBSYS_LOG(WARN,"fail to get cell from ObGetMerger [err:%d]", err);
    }
    if (OB_SUCCESS == err)
    {
      got_cell_count ++;
    }
    if ((OB_SUCCESS == err) && row_changed)
    {
      cur_row_cell_cnt_ = 0;
      last_not_exist_cell_.table_id_ = OB_INVALID_ID;
    }
    if ((OB_SUCCESS == err)
      && ((cur_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
      || (cur_cell->value_.get_ext() == ObActionFlag::OP_DEL_TABLE)))
    {
      if ((OB_INVALID_ID != last_not_exist_cell_.table_id_) && (last_not_exist_cell_.table_id_ == cur_cell->table_id_)
        && (last_not_exist_cell_.row_key_ == cur_cell->row_key_))
      {
        poped_cell_count_ ++;
        continue ;
      }
      else
      {
        last_not_exist_cell_ = *cur_cell;
      }
    }

    if ((OB_SUCCESS == err) && (cur_row_cell_cnt_ >= MAX_ROW_COLUMN_COUNT))
    {
      TBSYS_LOG(WARN,"row cell count is too large [cur_row_cell_cnt_:%ld, MAX_ROW_COLUMN_COUNT:%ld]",
        cur_row_cell_cnt_, MAX_ROW_COLUMN_COUNT);
      //err = OB_ARRAY_OUT_OF_RANGE;
      cur_row_cell_cnt_ = 0;
    }
    if (OB_SUCCESS == err)
    {
      row_cells_[cur_row_cell_cnt_].table_id_ = OB_INVALID_ID;
      row_cells_[cur_row_cell_cnt_].table_name_ = org_param[poped_cell_count_]->table_name_;
      row_cells_[cur_row_cell_cnt_].column_name_ = org_param[poped_cell_count_]->column_name_;
      row_cells_[cur_row_cell_cnt_].column_id_ = OB_INVALID_ID;
      row_cells_[cur_row_cell_cnt_].row_key_ = cur_cell->row_key_;
      row_cells_[cur_row_cell_cnt_].value_ = cur_cell->value_;
      ++cur_row_cell_cnt_;
    }
    if (OB_SUCCESS == err)
    {
      if ((OB_SUCCESS != (err = scanner.add_cell(row_cells_[cur_row_cell_cnt_ - 1]))) && (OB_SIZE_OVERFLOW != err))
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
          poped_cell_count_ -= cur_row_cell_cnt_ - 1;
          got_cell_count -= cur_row_cell_cnt_;
        }
      }
      else if (OB_SUCCESS == err)
      {
        poped_cell_count_ ++;
      }
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  if (OB_SUCCESS == err)
  {
    got_all_result = !size_over_flow;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scanner.set_is_req_fullfilled(got_all_result,got_cell_count))))
  {
    TBSYS_LOG(WARN,"fail to set fullfill infomation [err:%d]", err);
  }
  return err;
}
