/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_sql_get_request.cpp for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   xiaochu <xiaochu.yh@alipay.com>
 */

#include "ob_ms_sql_request.h"
#include "ob_ms_sql_get_request.h"
#include "ob_chunk_server_task_dispatcher.h"
#include "ob_ms_async_rpc.h"
#include "common/ob_trace_log.h"
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include "common/ob_common_stat.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMsSqlGetRequest::ObMsSqlGetRequest():
  page_arena_(PageArena<int64_t, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_MS_GET_EVENT))
{
  reserve_get_param_count_ = 0;
  total_sub_request_count_ = 0;
  memset(same_cs_get_row_count_, 0, sizeof(same_cs_get_row_count_));
  reset();
}

void ObMsSqlGetRequest::reset()
{
  // TODO: need review clear & get difference
  for (int32_t i = 0; i < total_sub_request_count_; i ++)
  {
    sub_requests_[i]->reset();
    same_cs_get_row_count_[i] = 0;
  }
  total_sub_request_count_ = 0;
  finished_sub_request_count_ = 0;
  page_arena_.free();
  get_param_ = NULL;
  max_parellel_count_ = 0;
  max_get_rows_per_subreq_ = 0;
  triggered_sub_request_count_ = 0;
  req_dist_map_.clear();
  sub_requests_.reset();
  ObMsSqlRequest::reset();
  sealed_ = false;
}

int ObMsSqlGetRequest::set_request_param(ObSqlGetParam &get_param, const int64_t timeout_us,
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
  // get request的get param
  get_param_ = &get_param;
  timeout_us_ = timeout_us;
  return err;
}


int ObMsSqlGetRequest::open()
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = distribute_request())))
  {
    TBSYS_LOG(WARN,"fail to distribute request [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = check_if_need_more_req(-1,timeout_us_))))
  {
    TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = wait(timeout_us_))))
  {
    TBSYS_LOG(WARN, "fail to wait result. err=%d", err);
  }
  return err;
}


int ObMsSqlGetRequest::distribute_request()
{
  int err = OB_SUCCESS;
  ObMsSqlSubGetRequest *sub_request = NULL;
  ObTabletLocationList loc_list;
  loc_list.set_buffer(ObMsSqlRequest::get_buffer_pool());
  int64_t row_count = get_param_->get_row_size();

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
    if (OB_SUCCESS != (err = get_cache_proxy()->get_tablet_location(
      get_param_->get_table_id(), *get_param_->operator [](row_idx), loc_list)))
    {
      TBSYS_LOG(WARN,"fail to get tablet location [err:%d,table_id:%lu,rowkey:%s]",
        err, get_param_->get_table_id(), to_cstring(*get_param_->operator [](row_idx)));
      break;
    }

    if ((OB_SUCCESS == err) && (0 >= loc_list.size()))
    {
      TBSYS_LOG(WARN,"fail to get tablet location [table_id:%lu,rowkey:%s]",
        get_param_->get_table_id(), to_cstring(*get_param_->operator [](row_idx)));
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
    // is_subreq_full说明这个子请求满了
    // sub_req_idx < 0 说明这个chunkserver原来没有被分配过子请求
    if ((OB_SUCCESS == err) && (sub_req_idx < 0 || is_subreq_full))
    {
      // 分配一个新的请求
      if (total_sub_request_count_ < MAX_SUB_GET_REQUEST_NUM)
      {
        int hash_err = 0;
        sub_req_idx = total_sub_request_count_;
        // 在page_arena_上分配
        if (OB_SUCCESS != (err = alloc_sub_request(sub_request)))
        {
          TBSYS_LOG(WARN, "fail to alloc sub request. err=%d", err);
        }
        else if (OB_SUCCESS != (err = sub_requests_.push_back(sub_request)))
        {
          TBSYS_LOG(WARN, "fail to add subreq to vector. err=%d", err);
        }
        else
        {
          total_sub_request_count_ ++;
          sub_request->reset();
          sub_request->init(page_arena_);
          if (NULL != row_desc_)
          {
            sub_request->set_row_desc(*row_desc_);
          }
          else
          {
            TBSYS_LOG(WARN, "row desc not set.");
            err = OB_NOT_INIT;
          }
        }
        //说明这个cs之前没有被分配过子请求
        if (!is_subreq_full && oceanbase::common::hash::HASH_INSERT_SUCC !=
          (hash_err = req_dist_map_.set(loc_list[svr_idx].server_.chunkserver_.get_ipv4(),sub_req_idx)))
        {
          TBSYS_LOG(WARN,"fail to insert hash map [hash_err:%d]", hash_err);
          err = OB_ERROR;
        }
        if (OB_SUCCESS == err)
        {
          sub_request->set_last_svr_ipv4(loc_list[svr_idx].server_.chunkserver_.get_ipv4());
          sub_request->set_first_cs_addr(loc_list[svr_idx].server_.chunkserver_);
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
            "MAX_SUB_GET_REQUEST_NUM:%d,max_get_rows_per_subreq_:%ld]",
            total_sub_request_count_, MAX_SUB_GET_REQUEST_NUM, max_get_rows_per_subreq_);
        err = OB_ERROR_OUT_OF_RANGE;
      }

      if (OB_SUCCESS == err)
      {
        //将整个大请求的get param设置到子请求中
        sub_request->set_param(*get_param_);
      }
    }
    else if (OB_SUCCESS == err && sub_req_idx < sub_requests_.size()) // exsit already
    {
      if (NULL == (sub_request = sub_requests_[static_cast<int32_t>(sub_req_idx)]))
      {
        TBSYS_LOG(WARN, "sub request not found. sub_req_idx=%ld", sub_req_idx);
        err = OB_ERR_UNEXPECTED;
      }
    }
    else
    {
      err = OB_ERR_UNEXPECTED; // should not be here
    }

    if (OB_SUCCESS == err && max_get_rows_per_subreq_ > 0)
    {
      //该cs上的第same_cs_subreq_idx个子请求的row 加1
      same_cs_get_row_count_[same_cs_subreq_idx]++;
    }

    // 将这行加入到这个子请求中
    if (OB_SUCCESS == err && (OB_SUCCESS != (err = sub_request->add_row(row_idx))))
    {
      TBSYS_LOG(WARN,"fail to add row to sub request [err:%d]", err);
    }
    TBSYS_LOG(DEBUG, "distribute request: row[%ld] distribute to sub req[%ld]", row_idx, sub_req_idx);
  } /* end for */
  ObMsSqlRequest::get_buffer_pool().reset();
  return err;
}


int ObMsSqlGetRequest::alloc_sub_request(ObMsSqlSubGetRequest *&sub_req)
{
  int err = OB_SUCCESS;
  void *tmp_buf = page_arena_.alloc(sizeof(ObMsSqlSubGetRequest));;
  // sub_req = &inner_sub_requests_[total_sub_request_count_];
  if (NULL == (sub_req = new(tmp_buf)ObMsSqlSubGetRequest()))
  {
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    TBSYS_LOG(DEBUG, "alloc sub request, sub_req=%p, total_sub_request_count_=%d",sub_req, total_sub_request_count_);
  }
  return err;
}

int ObMsSqlGetRequest::send_request(const int32_t sub_req_idx, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  //ObSqlGetParam *cur_get_param = NULL;
  OB_ASSERT(sub_req_idx < sub_requests_.size());
  ObMsSqlSubGetRequest &sub_request = *sub_requests_[sub_req_idx];
  ObServer cs_addr;
  //从大请求的get param中抽取出子请求的get param
  if ((OB_SUCCESS == err)
    && (OB_SUCCESS != (err = sub_request.get_cur_param(cur_sub_get_param_)))
    && (OB_ITER_END != err))
  {
    TBSYS_LOG(WARN,"fail to get next param of sub request [request_id:%lu,sub_req_idx:%d,err:%d]",
      get_request_id(), sub_req_idx, err);
  }
  if (sub_request.get_retry_times() == 0)
  {
    cs_addr = sub_request.get_first_cs_addr();
  }
  else
  {
    ObTabletLocationList loc_list;
    loc_list.set_buffer(ObMsSqlRequest::get_buffer_pool());
    int64_t row_idx = 0;
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = get_cache_proxy()->get_tablet_location(
      cur_sub_get_param_.get_table_id(), *(cur_sub_get_param_[row_idx]), loc_list))))
    {
      TBSYS_LOG(WARN,"fail to get tablet location [err:%d,table_id:%lu,rowkey:%s]", err,
        cur_sub_get_param_.get_table_id(), to_cstring(*(cur_sub_get_param_[row_idx])));
    }
    if ((OB_SUCCESS == err) && (0 >= loc_list.size()))
    {
      TBSYS_LOG(WARN,"fail to get tablet location [table_id:%lu,rowkey:%s]",
        cur_sub_get_param_.get_table_id(), to_cstring(*(cur_sub_get_param_[row_idx])));
      err = OB_DATA_NOT_SERVE;
    }
    int64_t svr_idx = 0;
    for (svr_idx = 0;(svr_idx < loc_list.size()) && (OB_SUCCESS == err); svr_idx ++)
    {
      if (loc_list[svr_idx].server_.chunkserver_.get_ipv4() == sub_request.get_last_svr_ipv4())
      {
        if (sub_request.get_last_svr_ipv4() == sub_request.get_fail_svr_ipv4())
        {
          svr_idx = (svr_idx + 1)%loc_list.size();
          TBSYS_LOG(INFO, "sub req retry [request_id:%lu,sub_req_idx:%d,tried_times:%ld,retry_svr:%s]", get_request_id(),
            sub_req_idx, sub_request.get_retry_times(), to_cstring(loc_list[svr_idx].server_.chunkserver_));
        }
        break;
      }
    }
    if ((OB_SUCCESS == err) && (svr_idx >= loc_list.size()))
    {
      svr_idx = random()%loc_list.size();
      TBSYS_LOG(WARN, "all servers get dirty. pick one randomly: loc_size[%ld], retry_svr:[%s]", 
          loc_list.size(), to_cstring(loc_list[svr_idx].server_.chunkserver_));
    }
    cs_addr = loc_list[svr_idx].server_.chunkserver_;
  }
  ObMsSqlRpcEvent *rpc_event = NULL;
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = ObMsSqlRequest::create(&rpc_event)))
    {
      TBSYS_LOG(WARN,"fail to create ObMsSqlRpcEvent [err:%d]", err);
    }
    else
    {
      rpc_event->set_server(cs_addr);
      FILL_TRACE_LOG("get_cs=%s", to_cstring(cs_addr));
      if (OB_SUCCESS != (err = get_rpc()->get(static_cast<int64_t>(timeout_us * get_timeout_percent() / 100),
              cs_addr,cur_sub_get_param_,*rpc_event)))
      {
        TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
        int e = ObMsSqlRequest::destroy(rpc_event);
        if (OB_SUCCESS != e)
        {
          TBSYS_LOG(WARN, "fail to destroy rpc_event. [e=%d]", e);
        }
        else
        {
          TBSYS_LOG(INFO, "rpc_event destroyed.");
        }
      }
      else
      {
        sub_request.set_last_rpc_event(*rpc_event);
        sub_request.set_last_svr_ipv4(cs_addr.get_ipv4());
      }
    }
  }
  ObMsSqlRequest::get_buffer_pool().reset();
  return err;
}


int ObMsSqlGetRequest::get_session_next(const int32_t sub_req_idx,
  ObMsSqlRpcEvent & prev_rpc, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  ObMsSqlRpcEvent *rpc_event = NULL;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = ObMsSqlRequest::create(&rpc_event))))
  {
    TBSYS_LOG(WARN,"fail to create ObMsSqlRpcEvent [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {

    FILL_TRACE_LOG("snext_cs=%s,sid=%ld", to_cstring(prev_rpc.get_server()), prev_rpc.get_session_id());
    if (OB_SUCCESS != (err = get_rpc()->get_session_next(timeout_us,prev_rpc.get_server(),
            prev_rpc.get_session_id(), prev_rpc.get_req_type(), *rpc_event)))
    {
      TBSYS_LOG(WARN,"fail to send request to server [request_id:%lu,err:%d]", get_request_id(),err);
    }
    else
    {
      sub_requests_[sub_req_idx]->set_last_rpc_event(*rpc_event, prev_rpc.get_session_id());
      sub_requests_[sub_req_idx]->set_last_svr_ipv4(prev_rpc.get_server().get_ipv4());
      TBSYS_LOG(DEBUG, "session next [request_id:%lu,prev_rpc:%lu,cur_rpc:%lu]", get_request_id(),
        prev_rpc.get_event_id(), rpc_event->get_event_id());
    }
  }
  return err;
}


int ObMsSqlGetRequest::check_if_need_more_req(const int32_t current_sub_req_idx,
  const int64_t timeout_us, ObMsSqlRpcEvent *prev_rpc_event)
{
  int err = OB_SUCCESS;
  // not finished
  if ((current_sub_req_idx >= 0) && (!sub_requests_[current_sub_req_idx]->finished()))
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
    else if ((OB_SUCCESS == err)  && fullfilled && fullfilled_item_count <= 0)
    {
      TBSYS_LOG(ERROR, "unexpect filfilled item count %ld", fullfilled_item_count);
      err = OB_ERR_UNEXPECTED;
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

  if ((current_sub_req_idx >= 0) && (sub_requests_[current_sub_req_idx]->finished()))
  {
    finished_sub_request_count_ ++;
  }

  //将子请求发出去
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

int ObMsSqlGetRequest::process_result(const int64_t timeout_us, ObMsSqlRpcEvent *rpc_event, bool& finish)
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
    int64_t get_event_time_cost = tbsys::CTimeUtil::getTime() - rpc_event->get_timestamp();
    OB_STAT_INC(MERGESERVER, SQL_GET_EVENT_TIME,  get_event_time_cost);
    OB_STAT_INC(MERGESERVER, SQL_GET_EVENT_COUNT);
    if (OB_SUCCESS == rpc_event->get_result_code())
    {
      TBSYS_LOG(DEBUG, "rpc event finish [request_id:%lu,rpc_event->client_id:%lu,rpc_event_id:%lu,#row:%ld,"
          "timeused:%ld,timeout:%ld,server:%s,session_id:%ld,err:%d]",
          get_request_id(), rpc_event->get_client_id(), rpc_event->get_event_id(),
          rpc_event->get_result().get_row_num(),  rpc_event->get_time_used(), rpc_event->get_timeout_us(),
          to_cstring(rpc_event->get_server()), rpc_event->get_session_id(), rpc_event->get_result_code());
    }
    else
    {
      TBSYS_LOG(WARN, "rpc event finish [request_id:%lu,rpc_event->client_id:%lu,rpc_event_id:%lu,"
          "timeused:%ld,timeout:%ld,server:%s,session_id:%ld,err:%d]",
          get_request_id(), rpc_event->get_client_id(), rpc_event->get_event_id(),
          rpc_event->get_time_used(), rpc_event->get_timeout_us(),
          to_cstring(rpc_event->get_server()), rpc_event->get_session_id(), rpc_event->get_result_code());
    }
    FILL_TRACE_LOG("finish rpc:server:%s,event:%lu,err:%d", to_cstring(rpc_event->get_server()), rpc_event->get_event_id(), rpc_event->get_result_code());
  }
  int32_t sub_req_idx = 0;
  for (;(sub_req_idx < triggered_sub_request_count_) && (OB_SUCCESS == err); sub_req_idx ++ )
  {
    if (sub_requests_[sub_req_idx]->get_last_rpc_event() == rpc_event->get_event_id())
    {
      break;
    }
  }
  if (sub_req_idx >= triggered_sub_request_count_)
  {
    TBSYS_LOG(ERROR, "rpc event not belong to current request [event server:%s,request_id:%lu,rpc_event_id:%lu]", 
        to_cstring(rpc_event->get_server()),
        get_request_id(),
        rpc_event->get_event_id());
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err)
    && (NULL != sub_requests_[sub_req_idx]->get_first_unfetched_row())
    && (OB_SUCCESS != update_location_cache(rpc_event->get_server(), rpc_event->get_result_code(),
        get_param_->get_table_id(),
        *sub_requests_[sub_req_idx]->get_first_unfetched_row())))
  {
    TBSYS_LOG(WARN,"fail to update location cache for server %s", to_cstring(rpc_event->get_server()));
  }
  if ((OB_SUCCESS == err)
    && (OB_SUCCESS == rpc_event->get_result_code())
    && (OB_SUCCESS != (err = sub_requests_[sub_req_idx]->add_result(rpc_event->get_result()))))
  {
    TBSYS_LOG(WARN,"fail to add result to sub request [event server:%s, request_id:%lu,rpc_event_id:%lu,err:%d]", 
        to_cstring(rpc_event->get_server()),
        get_request_id(),
        rpc_event->get_event_id(), 
        err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != rpc_event->get_result_code()))
  {
    int64_t tried_times = 0;
    if ((tried_times =  sub_requests_[sub_req_idx]->retry()) > MAX_RETRY_TIMES)
    {
      TBSYS_LOG(WARN,"sub request retry too many times [last server:%s, request_id:%lu,tried_times:%ld,MAX_RETRY_TIMES:%d,result_code=%d]",
          to_cstring(rpc_event->get_server()),
          get_request_id(),
          tried_times,
          MAX_RETRY_TIMES,
          rpc_event->get_result_code());
      err = rpc_event->get_result_code();
    }
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = check_if_need_more_req(sub_req_idx,timeout_us, rpc_event))))
  {
    TBSYS_LOG(WARN,"fail to send request [server:%s,request_id:%lu,err:%d]", to_cstring(rpc_event->get_server()), get_request_id(), err);
  }
  if ((OB_SUCCESS == err) && (finished_sub_request_count_ > total_sub_request_count_))
  {
    TBSYS_LOG(ERROR, "unexpected error [server:%s,finished_sub_request_count_:%d,total_sub_request_count_:%d,request_id:%lu]",
        to_cstring(rpc_event->get_server()),
        finished_sub_request_count_, 
        total_sub_request_count_,
        get_request_id());
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err) && (finished_sub_request_count_ == total_sub_request_count_))
  {
    finish = true;
  }
  if ((OB_SUCCESS == err) && (finish))
  {
    if (OB_SUCCESS != (err = merger_operator_.init(&sub_requests_,total_sub_request_count_, *get_param_)))
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

int ObMsSqlGetRequest::get_next_row(ObRow &row)
{
  int err = OB_SUCCESS;
  if (sealed_)
  {
    err = merger_operator_.get_next_row(row);
  }
  else
  {
    TBSYS_LOG(WARN,"get request not finished yet");
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}
