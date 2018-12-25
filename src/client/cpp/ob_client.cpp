/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_client.cpp : C++ client of Oceanbase
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */

#include "ob_client.h"
#include "libobapi.h"

#include <common/ob_atomic.h>
#include <common/utility.h>
#include <common/ob_ups_info.h>

using namespace oceanbase::common;
using namespace oceanbase::client;

extern OB_ERR_PMSG OB_ERR_DEFAULT_MSG[];

OB_ERR_CODE err_code_map(int err)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  switch (err)
  {
    case OB_SUCCESS:
      err_code = OB_ERR_SUCCESS;
      break;
    case OB_NOT_SUPPORTED:
      err_code = OB_ERR_NOT_SUPPORTED;
      break;
    case OB_RESPONSE_TIME_OUT:
      err_code = OB_ERR_RESPONSE_TIMEOUT;
      break;
    case OB_SCHEMA_ERROR:
      err_code = OB_ERR_SCHEMA;
      break;
    case OB_NOT_MASTER:
      err_code = OB_ERR_NOT_MASTER;
      break;
//    case OB_USER_NOT_EXIST:
//      err_code = OB_ERR_USER_NOT_EXIST;
//      break;
//    case OB_PASSWORD_WRONG:
//      err_code = OB_ERR_WRONG_PASSWORD;
//      break;
    default:
      err_code = OB_ERR_ERROR;
  }
  return err_code;
}

const char* ob_server_to_string(const ObServer& server)
{
  const int SIZE = 32;
  static char buf[SIZE];
  if (!server.to_string(buf, SIZE))
  {
    buf[0] = '\0';
    snprintf(buf, SIZE, "InvalidServerAddr");
  }
  return buf;
}

ServerCache::ServerCache()
    : rs_(), last_update_time_(0), client_(NULL),
      refresh_interval_(OB_DEFAULT_REFRESH_INTERVAL)
{
}

ServerCache::~ServerCache()
{
  rs_ = ObServer();
  initialized_ = false;
  last_update_time_ = 0;
}

int ServerCache::init(const ObServer &rs, ObClientManager *client,
    int32_t refresh_interval)
{
  int ret = OB_SUCCESS;
  if (0 == rs.get_ipv4() || 0 == rs.get_port() || NULL == client) 
  {
    TBSYS_LOG(ERROR, "init error, arguments are invalid, rs=%s client=NULL",
            ob_server_to_string(rs));
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    rs_ = rs;
    client_ = client;
    refresh_interval_ = refresh_interval;
    initialized_ = true;
    TBSYS_LOG(INFO, "ServerCache initialized succ, rs=%s client=%p",
            ob_server_to_string(rs_), client_);
  }

  if (OB_SUCCESS == ret)
  {
    ret = manual_update();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update for the first time failed");
    }
  }

  return ret;
}

void ServerCache::runTimerTask()
{
  periodic_update();
}

MsList::MsList()
    : ms_list_(), ms_iter_(0), buff_(), rwlock_()
{
}

MsList::~MsList()
{
  clear();
}

void MsList::clear()
{
  ms_list_.clear();
  atomic_exchange(&ms_iter_, 0);
}

const ObServer MsList::get_one()
{
  ObServer ret;
  rwlock_.rdlock();
  if (ms_list_.size() > 0)
  {
    uint64_t i = atomic_inc(&ms_iter_);
    ret = ms_list_[i % ms_list_.size()];
  }
  rwlock_.unlock();
  return ret;
}

std::vector<ObServer> MsList::get_whole_list()
{
  rwlock_.rdlock();
  std::vector<ObServer> res = ms_list_;
  rwlock_.unlock();
  return res;
}

int MsList::periodic_update()
{
  const bool manually_update = false;
  return update_(manually_update);
}

int MsList::manual_update()
{
  const bool manually_update = true;
  return update_(manually_update);
}

int MsList::update_(bool manual)
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int64_t timeout = 1000000;
  bool has_updated = false;
  int32_t ms_num = 0;
  std::vector<common::ObServer> new_ms;

  update_mutex_.lock();

  if (!manual || tbsys::CTimeUtil::getTime() - last_update_time_ > refresh_interval_)
  {
    ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
    ObResultCode res;

    //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
    ///get all ms in all clusters
    int32_t cluster_id = 0; // all cluster ms
    int32_t master_cluster_id = 0; //no used
    //add:e
    if (!initialized_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "MsList has not been initialized, "
          "this should not be reached");
    }
    //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
    else if(OB_SUCCESS != (ret = serialization::encode_vi32(data_buff.get_data(), data_buff.get_capacity(),
                                                            data_buff.get_position(), cluster_id)))
    {
      TBSYS_LOG(WARN, "failed to encode cluster_id, ret=%d", ret);
    }
    //add:e

    if (OB_SUCCESS == ret)
    {
      ret = client_->send_request(rs_, OB_GET_MS_LIST, MY_VERSION, timeout, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      data_buff.get_position() = 0;
      ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
      }
      else if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
        TBSYS_LOG(WARN, "OB_GET_MS_LIST error, result_code_=%d", ret);
      }
      else
      {
        ret = serialization::decode_vi32(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), &ms_num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize ms_num error, ret=%d", ret);
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      new_ms.resize(ms_num);
      for (int32_t i = 0; i < ms_num && OB_SUCCESS == ret; i++)
      {
        ret = new_ms[i].deserialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize %dth MS error, ret=%d", i, ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "Get %dth Mergeserver %s", i, ob_server_to_string(new_ms[i]));
          int64_t reserve;
          ret = serialization::decode_vi64(data_buff.get_data(), data_buff.get_capacity(),
              data_buff.get_position(), &reserve);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize reserve field error, ret=%d", ret);
          }
        }
      }
    }
    //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
    if(OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(),
                    in_buff.get_position(), &master_cluster_id)))
      {
        TBSYS_LOG(WARN, "failed to decode master_cluster_id, err=%d", ret);
      }
    }
    //add:e
    if (OB_SUCCESS == ret)
    {
      if (manual)
      {
        last_update_time_ = tbsys::CTimeUtil::getTime();
      }
      has_updated = true;
    }
  }

  update_mutex_.unlock();

  if (OB_SUCCESS == ret && has_updated)
  {
    rwlock_.wrlock();
    if (!list_equal_(new_ms))
    {
      TBSYS_LOG(INFO, "Mergeserver List is modified, get the most updated, "
          "MS num=%zu", new_ms.size());
      list_copy_(new_ms);
    }
    else
    {
      TBSYS_LOG(DEBUG, "Mergeserver List do not change, MS num=%zu",
          new_ms.size());
    }
    rwlock_.unlock();
  }

  if (!has_updated)
  {
    const int RANDOM_SLEEP = 10000;
    usleep(RANDOM_SLEEP);
  }

  return ret;
}

bool MsList::list_equal_(const std::vector<ObServer> &list)
{
  bool ret = true;
  if (list.size() != ms_list_.size())
  {
    ret = false;
  }
  else
  {
    for (unsigned i = 0; i < ms_list_.size(); i++)
    {
      if (!(list[i] == ms_list_[i]))
      {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

void MsList::list_copy_(const std::vector<ObServer> &list)
{
  ms_list_.clear();
  std::vector<ObServer>::const_iterator iter;
  for (iter = list.begin(); iter != list.end(); iter++)
  {
    ms_list_.push_back(*iter);
    TBSYS_LOG(INFO, "Add Mergeserver %s", ob_server_to_string(*iter));
  }
}

UpsMaster::UpsMaster()
    : ups_(), buff_(), rwlock_()
{
}

UpsMaster::~UpsMaster()
{
}

const ObServer UpsMaster::get_ups()
{
  ObServer ret;
  rwlock_.rdlock();
  ret = ups_;
  rwlock_.unlock();
  return ret;
}

int UpsMaster::periodic_update()
{
  const bool manually_update = false;
  return update_(manually_update);
}

int UpsMaster::manual_update()
{
  const bool manually_update = true;
  return update_(manually_update);
}

int UpsMaster::update_(bool manual)
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int64_t timeout = 1000000;
  bool has_updated = false;
  common::ObServer new_ups_master;

  update_mutex_.lock();

  if (!manual || tbsys::CTimeUtil::getTime() - last_update_time_ > refresh_interval_)
  {
    ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
    ObResultCode res;
    common::ObUpsList ups_list;

    if (!initialized_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "UpsMaster has not been initialized, "
          "this should not be reached");
    }

    if (OB_SUCCESS == ret)
    {
      ret = client_->send_request(rs_, OB_GET_UPS, MY_VERSION, timeout, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      data_buff.get_position() = 0;
      ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
      }
      else if (OB_SUCCESS != res.result_code_)
      {
        ret = res.result_code_;
        TBSYS_LOG(WARN, "OB_GET_UPS error, result_code_=%d", ret);
      }
      else
      {
        ret = ups_list.deserialize(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize ups_list error, ret=%d", ret);
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      for (int32_t i = 0; i < ups_list.ups_count_; i++)
      {
        if (UPS_MASTER == ups_list.ups_array_[i].stat_)
        {
          new_ups_master = ups_list.ups_array_[i].addr_;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (manual)
      {
        last_update_time_ = tbsys::CTimeUtil::getTime();
      }
      has_updated = true;
    }
  }

  update_mutex_.unlock();

  if (OB_SUCCESS == ret && has_updated)
  {
    rwlock_.wrlock();
    if (!(ups_ == new_ups_master))
    {
      ups_ = new_ups_master;
      TBSYS_LOG(INFO, "UpdateServer Master is modified, new one is %s",
          ob_server_to_string(ups_));
    }
    else
    {
      TBSYS_LOG(DEBUG, "UpdateServer Master does not change, addr=%s",
          ob_server_to_string(ups_));
    }
    rwlock_.unlock();
  }

  if (!has_updated)
  {
    const int RANDOM_SLEEP = 10000;
    usleep(RANDOM_SLEEP);
  }

  return ret;
}

ObClient::ObClient() : connmgr_(&transport_, &streamer_, NULL),
                       buff_(OB_MAX_PACKET_LENGTH)
{
  reqs_ = NULL;
}

ObClient::~ObClient()
{
  close();
}

int ObClient::connect(const char* rs_addr, const int rs_port,
                      const char* user, const char* passwd)
{
  (void)user;
  (void)passwd;
  int ret = OB_SUCCESS;

  rs_ = ObServer(ObServer::IPV4, rs_addr, rs_port);
  streamer_.setPacketFactory(&factory_);
  client_.initialize(&transport_, &streamer_);
  transport_.start();

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = ms_list_.init(rs_, &client_,
            cntl_.refresh_interval)))
    {
      TBSYS_LOG(WARN, "MsList init error, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ups_master_.init(rs_, &client_,
            cntl_.refresh_interval)))
    {
      TBSYS_LOG(WARN, "UpsMaster init error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = init_reqs()))
    {
      TBSYS_LOG(WARN, "init_free_req error, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = timer_.init();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObTimer init error");
    }
    else
    {
      const bool REPEATED = true;
      ret = timer_.schedule(ms_list_,
          cntl_.refresh_interval, REPEATED);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObTimer schedule error");
      }
      else
      {
        ret = timer_.schedule(ups_master_,
            cntl_.refresh_interval, REPEATED);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObTimer schedule error");
        }
      }
    }
  }

  return ret;
}

int ObClient::submit(int reqs_num, OB_REQ *reqs[])
{
  int ret = OB_SUCCESS;

  if (reqs_num <= 0 || NULL == reqs)
  {
    TBSYS_LOG(WARN, "Parameter is invalid, reqs_num=%d reqs=%p",
        reqs_num, reqs);
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret)
  {
    for (int i = 0; OB_SUCCESS == ret && i < reqs_num; ++i)
    {
      if (NULL == reqs[i])
      {
        TBSYS_LOG(WARN, "The %dth req is NULL", i);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = check_req_integrity(reqs[i])))
      {
        TBSYS_LOG(WARN, "The %dth req is not legal", i);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = dispatch_req(reqs[i])))
      {
        TBSYS_LOG(WARN, "dispatch req error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObClient::cancel(int reqs_num, OB_REQ *reqs[])
{
  UNUSED(reqs_num);
  UNUSED(reqs);
  return 0;
}

int ObClient::get_results(int min_num, int max_num, int64_t timeout,
                          int64_t &num, OB_REQ* reqs[])
{
  int ret = OB_SUCCESS;

  if (-1 == timeout)
  {
    timeout = INT64_MAX;
  }
  int64_t start_time = tbsys::CTimeUtil::getMonotonicTime();

  num = 0;
  while (num < max_num)
  {
    ObInnerReq* i_req = NULL;
    ret = ready_list_.pop_front(i_req);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ReqList pop_front error, ret=%d, "
          "this should not happened", ret);
      break;
    }
    else if (NULL == i_req)
    {
      if (num >= min_num)
      {
        break;
      }
      else
      {
        if (tbsys::CTimeUtil::getMonotonicTime() - start_time < timeout)
        {
          usleep(1000);
        }
        else
        {
          ret = OB_ERR_TIMEOUT;
          break;
        }
      }
    }
    else
    {
      reqs[num++] = (OB_REQ*)i_req;
    }
  }

  return ret;
}

OB_RES* ObClient::scan(const ObScanParam& scan)
{
  OB_RES* res = acquire_res_st();
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  if (NULL == res)
  {
    TBSYS_LOG(ERROR, "acquire OB_RES error");
  }
  else
  {
    ObDataBuffer data_buff;
    err_code = get_thread_data_buffer(data_buff);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "get_thread_data_buffer error, err_code=%d", err_code);
    }
    else
    {
      err_code = serialize_scan(scan, data_buff);
      if (OB_ERR_SUCCESS != err_code)
      {
        TBSYS_LOG(ERROR, "serialize_scan error, err_code=%d", err_code);
      }
      else
      {
        int retry_counter = 0;
        ObServer ms_server = ms_list_.get_one();
        err_code = scan_ms(ms_server, data_buff, cntl_.timeout_scan, res->scanner);
        while (OB_ERR_NOT_MASTER == err_code
          && ++retry_counter <= OB_DEFAULT_RETRY)
        {
          TBSYS_LOG(WARN, "MergeServer(%s) returned OB_ERR_NOT_MASTER, "
              "retry %d...", ob_server_to_string(ms_server), retry_counter);
          ms_list_.manual_update();
          ms_server = ms_list_.get_one();
          res->scanner.clear();
          data_buff.get_position() = 0;
          err_code = serialize_scan(scan, data_buff);
          if (OB_ERR_SUCCESS != err_code)
          {
            TBSYS_LOG(ERROR, "serialize_scan error, err_code=%d", err_code);
          }
          else
          {
            int64_t st = tbsys::CTimeUtil::getTime();
            err_code = scan_ms(ms_server, data_buff, cntl_.timeout_scan,
                res->scanner);
            int64_t dt = OB_DEFAULT_MINIMUM_RETRY_INTERVAL
                - (tbsys::CTimeUtil::getTime() - st);
            if (dt > 0)
            {
              usleep(static_cast<__useconds_t> (dt));
            }
          }
        }
        if (OB_ERR_SUCCESS != err_code)
        {
          TBSYS_LOG(ERROR, "scan_ms error, MS=%s err_code=%d",
              ob_server_to_string(ms_server), err_code);
        }
        else
        {
          res->cur_scanner = res->scanner.begin();
        }
      }
    }
  }
  if (OB_ERR_SUCCESS != err_code)
  {
    ob_set_errno(err_code);
    release_res_st(res);
    res = NULL;
  }
  return res;
}

OB_RES* ObClient::get(const ObGetParam& get)
{
  OB_RES* res = acquire_res_st();
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  if (NULL == res)
  {
    TBSYS_LOG(ERROR, "acquire OB_RES error");
  }
  else
  {
    ObDataBuffer data_buff;
    err_code = get_thread_data_buffer(data_buff);

    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "get_thread_data_buffer error, err_code=%d", err_code);
    }
    else
    {
      err_code = serialize_get(get, data_buff);
      if (OB_ERR_SUCCESS != err_code)
      {
        TBSYS_LOG(ERROR, "serialize_get error, err_code=%d", err_code);
      }
      else
      {
        int retry_counter = 0;
        ObServer ms_server = ms_list_.get_one();
        err_code = get_ms(ms_server, data_buff, cntl_.timeout_get,
            res->scanner);
        while (OB_ERR_NOT_MASTER == err_code
          && ++retry_counter <= OB_DEFAULT_RETRY)
        {
          TBSYS_LOG(WARN, "MergeServer(%s) returned OB_ERR_NOT_MASTER, "
              "retry %d...", ob_server_to_string(ms_server), retry_counter);
          ms_list_.manual_update();
          ms_server = ms_list_.get_one();
          res->scanner.clear();
          data_buff.get_position() = 0;
          err_code = serialize_get(get, data_buff);
          if (OB_ERR_SUCCESS != err_code)
          {
            TBSYS_LOG(ERROR, "serialize_get error, err_code=%d", err_code);
          }
          else
          {
            int64_t st = tbsys::CTimeUtil::getTime();
            err_code = get_ms(ms_server, data_buff, cntl_.timeout_get,
                res->scanner);
            int64_t dt = OB_DEFAULT_MINIMUM_RETRY_INTERVAL
                - (tbsys::CTimeUtil::getTime() - st);
            if (dt > 0)
            {
              usleep(static_cast<__useconds_t> (dt));
            }
          }
        }
        if (OB_ERR_SUCCESS != err_code)
        {
          TBSYS_LOG(ERROR, "get_ms error, MS:%s err_code=%d",
              ob_server_to_string(ms_server), err_code);
        }
        else
        {
          res->cur_scanner = res->scanner.begin();
        }
      }
    }

    if (OB_ERR_SUCCESS != err_code)
    {
      ob_set_errno(err_code);
      release_res_st(res);
      res = NULL;
    }
  }
  return res;
}

OB_ERR_CODE ObClient::set(const ObMutator& mut)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  ObDataBuffer data_buff;
  err_code = get_thread_data_buffer(data_buff);

  if (OB_ERR_SUCCESS != err_code)
  {
    TBSYS_LOG(ERROR, "get_thread_data_buffer error, err_code=%d", err_code);
  }
  else
  {
    err_code = serialize_set(mut, data_buff);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "serialize_set error, err_code=%d", err_code);
    }
    else
    {
      int retry_counter = 0;
      ObServer ups = ups_master_.get_ups();
      err_code = set_us(ups, data_buff, cntl_.timeout_set);
      while (OB_ERR_NOT_MASTER == err_code
          && ++retry_counter <= OB_DEFAULT_RETRY)
      {
        TBSYS_LOG(WARN, "UpdateServer(%s) returned OB_ERR_NOT_MASTER, "
            "retry %d...", ob_server_to_string(ups), retry_counter);
        ups_master_.manual_update();
        ups = ups_master_.get_ups();
        data_buff.get_position() = 0;
        err_code = serialize_set(mut, data_buff);
        if (OB_ERR_SUCCESS != err_code)
        {
          TBSYS_LOG(ERROR, "serialize_set error, err_code=%d", err_code);
        }
        else
        {
          int64_t st = tbsys::CTimeUtil::getTime();
          err_code = set_us(ups, data_buff, cntl_.timeout_set);
          int64_t dt = OB_DEFAULT_MINIMUM_RETRY_INTERVAL
              - (tbsys::CTimeUtil::getTime() - st);
          if (dt > 0)
          {
            usleep(static_cast<__useconds_t> (dt));
          }
        }
      }
      if (OB_ERR_SUCCESS != err_code)
      {
        TBSYS_LOG(ERROR, "set_us error, US:%s err_code=%d",
            ob_server_to_string(ups), err_code);
      }
    }
  }

  if (OB_ERR_SUCCESS != err_code)
  {
    ob_set_errno(err_code);
  }

  return err_code;
}

OB_REQ* ObClient::acquire_get_req()
{
  OB_REQ* req = acquire_req();
  if (NULL != req)
  {
    req->opcode = OB_OP_GET;
    req->timeout = cntl_.timeout_get;
    if (NULL == req->ob_get)
    {
      req->ob_get = acquire_get_st();
      if (NULL == req->ob_get)
      {
        TBSYS_LOG(ERROR, "new OB_REQ error, NOT ENOUGH MEMORY");
        release_req(req, true);
        req = NULL;
      }
    }
    else
    {
      req->ob_get->get_param.reset(true);
    }
  }

  return req;
}

OB_REQ* ObClient::acquire_scan_req()
{
  OB_REQ* req = acquire_req();
  if (NULL != req)
  {
    req->opcode = OB_OP_SCAN;
    req->timeout = cntl_.timeout_scan;
    if (NULL == req->ob_scan)
    {
      if (NULL == (req->ob_scan = acquire_scan_st()))
      {
        TBSYS_LOG(ERROR, "new OB_REQ error, NOT ENOUGH MEMORY");
        release_req(req, true);
        req = NULL;
      }
    }
    else
    {
      req->ob_scan->scan_param.reset();
      st_ob_groupby_param *groupby_param_st = req->ob_scan->ob_groupby_param;
      if (NULL != groupby_param_st)
      {
        groupby_param_st->groupby_param.reset(true);
      }
      else
      {
        TBSYS_LOG(ERROR, "new OB_REQ error, this should not be reached");
        release_req(req, true);
        req = NULL;
      }
    }
  }

  return req;
}

OB_REQ* ObClient::acquire_set_req()
{
  OB_REQ* req = acquire_req();
  if (NULL != req)
  {
    req->opcode = OB_OP_SET;
    req->timeout = cntl_.timeout_set;
    if (NULL == req->ob_set)
    {
      if (NULL == (req->ob_set = acquire_set_st()))
      {
        TBSYS_LOG(ERROR, "new OB_REQ error, NOT ENOUGH MEMORY");
        release_req(req, true);
        req = NULL;
      }
    }
    else
    {
      req->ob_set->mutator.reset();
      st_ob_set_cond *set_cond_st = req->ob_set->ob_set_cond;
      if (NULL != set_cond_st)
      {
        set_cond_st->update_condition.reset();
      }
      else
      {
        TBSYS_LOG(ERROR, "new OB_REQ error, this should not be reached");
        release_req(req, true);
        req = NULL;
      }
    }
  }

  return req;
}

void ObClient::release_req(OB_REQ* req, bool free_mem)
{
  if (NULL != req)
  {
    if (free_mem)
    {
      release_get_st(req->ob_get); req->ob_get = NULL;
      release_scan_st(req->ob_scan); req->ob_scan = NULL;
      release_set_st(req->ob_set); req->ob_set = NULL;
      release_res_st(req->ob_res); req->ob_res = NULL;
    }
    int err = free_list_.push_back((ObInnerReq*)req);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ReqList push_back error, err=%d, "
          "this should not be happened", err);
    }
  }
}


OB_GET* ObClient::acquire_get_st()
{
  OB_GET* get_st = new (std::nothrow) OB_GET;
  if (NULL == get_st)
  {
    ob_set_errno(OB_ERR_MEM_NOT_ENOUGH);
    TBSYS_LOG(ERROR, "new OB_GET error, NOT ENOUGH MEMORY");
  }
  return get_st;
}

OB_SCAN* ObClient::acquire_scan_st()
{
  OB_SCAN* ob_scan = new (std::nothrow) OB_SCAN();
  OB_GROUPBY_PARAM* groupby_param = NULL;
  if (NULL != ob_scan)
  {
    groupby_param = new (std::nothrow) OB_GROUPBY_PARAM(
        ob_scan->scan_param.get_group_by_param());
  }

  if (NULL != ob_scan && NULL != groupby_param)
  {
    ob_scan->ob_groupby_param = groupby_param;
    reset_scan_st(ob_scan);
  }
  else
  {
    TBSYS_LOG(ERROR, "new OB_SCAN error, NOT ENOUGH MEMORY");
    ob_set_errno(OB_ERR_MEM_NOT_ENOUGH);
    if (NULL != ob_scan)
    {
      SAFE_DELETE(ob_scan);
    }
    if (NULL != groupby_param)
    {
      SAFE_DELETE(groupby_param);
    }
  }
  return ob_scan;
}

OB_SET* ObClient::acquire_set_st()
{
  OB_SET* ob_set = new (std::nothrow) OB_SET();
  OB_SET_COND* set_cond = NULL;
  if (NULL != ob_set)
  {
    set_cond = new (std::nothrow) OB_SET_COND(
        ob_set->mutator.get_update_condition());
  }

  if (NULL != ob_set && NULL != set_cond)
  {
    ob_set->ob_set_cond = set_cond;
  }
  else
  {
    TBSYS_LOG(ERROR, "new OB_SET error, NOT ENOUGH MEMORY");
    ob_set_errno(OB_ERR_MEM_NOT_ENOUGH);
    if (NULL != ob_set)
    {
      SAFE_DELETE(ob_set);
    }
    if (NULL != set_cond)
    {
      SAFE_DELETE(set_cond);
    }
  }
  return ob_set;
}

OB_RES* ObClient::acquire_res_st()
{
  OB_RES* ob_res = new (std::nothrow) OB_RES();
  if (NULL == ob_res)
  {
    TBSYS_LOG(ERROR, "new OB_RES error, NOT ENOUGH MEMORY");
    ob_set_errno(OB_ERR_MEM_NOT_ENOUGH);
  }
  return ob_res;
}

void ObClient::release_get_st(OB_GET* ob_get)
{
  if (NULL != ob_get)
  {
    SAFE_DELETE(ob_get);
  }
}

void ObClient::release_scan_st(OB_SCAN* ob_scan)
{
  if (NULL != ob_scan)
  {
    OB_GROUPBY_PARAM* groupby_param = ob_scan->ob_groupby_param;
    if (NULL != groupby_param)
    {
      SAFE_DELETE(groupby_param);
    }
    SAFE_DELETE(ob_scan);
  }
}

void ObClient::release_set_st(OB_SET* ob_set)
{
  if (NULL != ob_set)
  {
    OB_SET_COND* set_cond = ob_set->ob_set_cond;
    if (NULL != set_cond)
    {
      SAFE_DELETE(set_cond);
    }
    SAFE_DELETE(ob_set);
  }
}

void ObClient::reset_get_st(OB_GET* ob_get)
{
  if (NULL != ob_get)
  {
    ob_get->get_param.reset();
  }
}

void ObClient::reset_scan_st(OB_SCAN* ob_scan)
{
  if (NULL != ob_scan)
  {
    ob_scan->scan_param.reset();
    ob_scan->scan_param.set_is_result_cached(true);
    ObScanParam::ReadMode read_mode = ObScanParam::PREREAD;
    if (ObScanParam::SYNCREAD == cntl_.read_mode)
    {
      read_mode = ObScanParam::SYNCREAD;
    }
    else
    {
      read_mode = ObScanParam::PREREAD;
    }
    ObScanParam::ScanFlag scan_flag(ObScanParam::FORWARD, read_mode);
    ob_scan->scan_param.set_scan_flag(scan_flag);
  }
}

void ObClient::reset_set_st(OB_SET* ob_set)
{
  if (NULL != ob_set)
  {
    ob_set->mutator.reset();
  }
}

void ObClient::release_res_st(OB_RES* res)
{
  if (NULL != res)
  {
    SAFE_DELETE(res);
  }
}

void ObClient::set_resfd(int32_t fd, OB_REQ* cb)
{
  if (NULL != cb)
  {
    cb->flags = FD_INVOCATION;
    cb->resfd = fd;
  }
}

OB_ERR_CODE ObClient::api_cntl(int32_t cmd, va_list args)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  int32_t tmp = 0;
  switch (cmd)
  {
    case OB_S_MAX_REQ:
      tmp = va_arg(args, int32_t);
      if (MAX_REQ_MINIMUM <= tmp && tmp <= MAX_REQ_MAXIMUM)
      {
        cntl_.max_req = tmp;
      }
      else
      {
        err_code = OB_ERR_INVALID_PARAM;
      }
      break;
    case OB_G_MAX_REQ:
      *(va_arg(args, int32_t*)) = cntl_.max_req;
      break;
    case OB_S_TIMEOUT:
      tmp = va_arg(args, int32_t);
      atomic_exchange((uint32_t*)&(cntl_.timeout_get), tmp);
      atomic_exchange((uint32_t*)&(cntl_.timeout_scan), tmp);
      atomic_exchange((uint32_t*)&(cntl_.timeout_set), tmp);
      break;
    case OB_S_TIMEOUT_GET:
      atomic_exchange((uint32_t*)&(cntl_.timeout_get), va_arg(args, int32_t));
      break;
    case OB_G_TIMEOUT_GET:
      *(va_arg(args, int32_t*)) = cntl_.timeout_get;
      break;
    case OB_S_TIMEOUT_SCAN:
      atomic_exchange((uint32_t*)&(cntl_.timeout_scan), va_arg(args, int32_t));
      break;
    case OB_G_TIMEOUT_SCAN:
      *(va_arg(args, int32_t*)) = cntl_.timeout_scan;
      break;
    case OB_S_TIMEOUT_SET:
      atomic_exchange((uint32_t*)&(cntl_.timeout_set), va_arg(args, int32_t));
      break;
    case OB_G_TIMEOUT_SET:
      *(va_arg(args, int32_t*)) = cntl_.timeout_set;
      break;
    case OB_S_READ_MODE:
      atomic_exchange((uint32_t*)&(cntl_.read_mode), va_arg(args, int32_t));
      break;
    case OB_G_READ_MODE:
      *(va_arg(args, int32_t*)) = cntl_.read_mode;
      break;
    case OB_S_REFRESH_INTERVAL:
      atomic_exchange((uint32_t*)&(cntl_.refresh_interval),
          va_arg(args, int32_t));
      break;
    case OB_G_REFRESH_INTERVAL:
      *(va_arg(args, int32_t*)) = cntl_.refresh_interval;
      break;
  }
  return err_code;
}

OB_ERR_CODE ObClient::ob_debug(int32_t cmd, va_list args)
{
  (void)(args);
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  switch (cmd)
  {
    case OB_DEBUG_CLEAR_ACTIVE_MEMTABLE:
      err_code = clear_active_memtable();
      break;
  }
  return err_code;
}

int ObClient::get_ms_list(ObScanner &scanner)
{
  int ret = OB_SUCCESS;

  const char* res_table_name = "MergeServer List";
  const char* port_column_name = "Port";
  ObCellInfo cell;
  cell.table_name_.assign(const_cast<char*>(res_table_name), static_cast<int32_t>(strlen(res_table_name)));
  cell.column_name_.assign(const_cast<char*>(port_column_name), static_cast<int32_t>(strlen(port_column_name)));
  char buf[BUFSIZ];

  std::vector<ObServer> ms_list = ms_list_.get_whole_list();
  std::vector<ObServer>::iterator iter = ms_list.begin();
  for (; iter != ms_list.end() && OB_SUCCESS == ret; iter++)
  {
    iter->ip_to_string(buf, BUFSIZ);
    cell.row_key_.assign(buf, static_cast<int32_t>(strlen(buf)));
    cell.value_.set_int(iter->get_port());
    ret = scanner.add_cell(cell);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "add MergeServer to result error, ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "get_ms_list add a ms %s:%d", buf, iter->get_port());
    }
  }

  return ret;
}

int ObClient::init_reqs()
{
  int ret = OB_SUCCESS;

  reqs_ = new (std::nothrow) ObInnerReq[cntl_.max_req];
  if (NULL == reqs_)
  {
    TBSYS_LOG(ERROR, "new req_list_node error, NOT ENOUGH MEMORY");
    ret = OB_ERROR;
  }
  else
  {
    memset(reqs_, 0x00, cntl_.max_req * sizeof(ObInnerReq));
    for (int i = 0; i < cntl_.max_req; ++i)
    {
      free_list_.push_back(reqs_ + i);
    }
  }

  return ret;
}

void ObClient::close()
{
  if (!(*transport_.getStop()))
  {
    transport_.stop();
    transport_.wait();
  }
  if (NULL != reqs_)
  {
    delete[] reqs_;
    reqs_ = NULL;
  }
  free_list_.clear();
  ready_list_.clear();
  retry_list_.clear();
  ms_list_.clear();
  timer_.destroy();
}

OB_REQ* ObClient::acquire_req()
{
  OB_REQ* req = NULL;

  ObInnerReq* i_req = NULL;
  int err = free_list_.pop_front(i_req);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "ReqList pop_front error, err=%d, "
        "this should not happened", err);
  }
  else if (NULL == i_req)
  {
    TBSYS_LOG(INFO, "no req left");
  }
  else
  {
    req = (OB_REQ*)i_req;
  }

  return req;
}

int ObClient::submit_set(const common::ObServer &server, OB_REQ *req)
{
  int ret = OB_SUCCESS;

  if (NULL == req || NULL == req->ob_set)
  {
    OB_SET* ob_set = req == NULL ? NULL : req->ob_set;
    TBSYS_LOG(WARN, "Parameter is invalid, req=%p ob_set=%p", req, ob_set);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    common::ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());

    ret = req->ob_set->mutator.serialize(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObGetParam serialize error, err=%d", ret);
    }

    if (OB_SUCCESS == ret)
    {
      ret = send_request(server, OB_WRITE, req->timeout, data_buff, req);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request to server=%s, ret=%d",
            ob_server_to_string(server), ret);
      }
    }
  }

  return ret;
}

int ObClient::submit_scan(const common::ObServer &server, OB_REQ *req)
{
  int ret = OB_SUCCESS;

  if (NULL == req || NULL == req->ob_scan)
  {
    OB_SCAN* ob_scan = req == NULL ? NULL : req->ob_scan;
    TBSYS_LOG(WARN, "Parameter is invalid, req=%p ob_scan=%p", req, ob_scan);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    common::ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());

    ret = req->ob_scan->scan_param.serialize(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObScanParam serialize error, err=%d", ret);
    }

    if (OB_SUCCESS == ret)
    {
      ret = send_request(server, OB_SCAN_REQUEST, req->timeout, data_buff, req);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request to server=%s, ret=%d",
            ob_server_to_string(server), ret);
      }
    }
  }

  return ret;
}

int ObClient::submit_get(const common::ObServer &server, OB_REQ *req)
{
  int ret = OB_SUCCESS;

  if (NULL == req || NULL == req->ob_get)
  {
    OB_GET* ob_get = req == NULL ? NULL : req->ob_get;
    TBSYS_LOG(WARN, "Parameter is invalid, req=%p ob_get=%p", req, ob_get);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    common::ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());

    ret = req->ob_get->get_param.serialize(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObGetParam serialize error, err=%d", ret);
    }

    if (OB_SUCCESS == ret)
    {
      ret = send_request(server, OB_GET_REQUEST, req->timeout, data_buff, req);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request to server=%s, ret=%d",
            ob_server_to_string(server), ret);
      }
    }
  }

  return ret;
}

void ObClient::clear_req(OB_REQ *r)
{
  if (NULL != r)
  {
    r->opcode = (OB_OPERATION_CODE)0;
    r->flags = (OB_REQ_FLAG)0;
    r->resfd = r->timeout = r->err_code = r->req_id = 0;
    r->data = r->arg = NULL;
    if (NULL != r->ob_get)
    {
      r->ob_get->get_param.reset();
    }
    if (NULL != r->ob_scan)
    {
      r->ob_scan->scan_param.reset();
    }
    if (NULL != r->ob_set)
    {
      r->ob_set->mutator.reset();
    }
    if (NULL != r->ob_res)
    {
      for (OB_RES::ScannerListIter i = r->ob_res->scanner.begin();
          i != r->ob_res->scanner.end(); i++)
      {
        //r->ob_res->scanner.reset();
        i->reset();
      }
      r->ob_res->scanner.clear();
      r->ob_res->scanner.resize(1);
      r->ob_res->cur_scanner = r->ob_res->scanner.begin();
    }
  }
}

void ObClient::print_req(OB_REQ *r, const char* TARGET)
{
  if (r != NULL && TARGET != NULL)
  {
#define REQ_FORMAT "opcode=%d flags=%d resfd=%d timeout=%d err_code=%d " \
                   "req_id=%d data=%p arg=%p " \
                   "ob_get=%p ob_scan=%p ob_set=%p ob_res=%p"
    if (strcmp(TARGET, "stdout") == 0)
    {
      fprintf(stdout, REQ_FORMAT "\n",
          r->opcode, r->flags, r->resfd, r->timeout, r->err_code,
          r->req_id, r->data, r->arg,
          r->ob_get, r->ob_scan, r->ob_set, r->ob_res);
    }
    else if (strcmp(TARGET, "stderr") == 0)
    {
      fprintf(stderr, REQ_FORMAT "\n",
          r->opcode, r->flags, r->resfd, r->timeout, r->err_code,
          r->req_id, r->data, r->arg,
          r->ob_get, r->ob_scan, r->ob_set, r->ob_res);
    }
    else
    {
      TBSYS_LOG(INFO, REQ_FORMAT,
          r->opcode, r->flags, r->resfd, r->timeout, r->err_code,
          r->req_id, r->data, r->arg,
          r->ob_get, r->ob_scan, r->ob_set, r->ob_res);
    }
  }
}

tbnet::IPacketHandler::HPRetCode
    ObClient::handlePacket(tbnet::Packet* packet, void * args)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ObPacket *rsps = dynamic_cast<ObPacket*>(packet);
  OB_REQ *creq = static_cast<OB_REQ*>(args);
  const int REGULAR_PACKET = 1;
  const int CONTROL_PACKET = 2;
  int packet_type = 0;
  if (NULL != packet)
  {
    if (NULL != args)
    {
      if (packet->isRegularPacket())
      {
        packet_type = REGULAR_PACKET;
      }
      else
      {
        packet_type = CONTROL_PACKET;
      }
    }
    else
    {
      err_code = OB_ERR_ERROR;
      TBSYS_LOG(ERROR, "Unexpected: receive a packet with "
          "args=NULL pcode=%d, discarding", packet->getPCode());
    }
  }
  else
  {
    err_code = OB_ERR_ERROR;
    if (NULL == creq)
    {
      TBSYS_LOG(ERROR, "Unexpected: receive a NULL packet with "
          "args=%p creq=%p", args, creq);
    }
    else
    {
      TBSYS_LOG(ERROR, "Unexpected: receive a NULL packet with "
          "args=%p creq.opcode=%d creq.flags=%d creq->resfd=%d creq.timeout=%d "
          "creq.err_code=%d creq.req_id=%d creq.data=%p creq.arg=%p"
          "creq.ob_get=%p creq.ob_scan=%p creq.ob_set=%p creq.ob_res=%p",
          args, creq->opcode, creq->flags, creq->resfd, creq->timeout,
          creq->err_code, creq->req_id, creq->data, creq->arg,
          creq->ob_get, creq->ob_scan, creq->ob_set, creq->ob_res);
    }
  }

  if (OB_ERR_SUCCESS == err_code && REGULAR_PACKET == packet_type)
  {
    int err = OB_SUCCESS;
    if (OB_SUCCESS != (err = rsps->deserialize()))
    {
      TBSYS_LOG(WARN, "response packet deserialize error, err=%d", err);
    }
    else
    {
      ObDataBuffer *data_buff = rsps->get_buffer();
      const char* data = data_buff->get_data() + data_buff->get_position();
      int64_t data_len = rsps->get_data_length();
      int64_t pos = 0;
      ObResultCode result_code;
      if (OB_SUCCESS !=
          (err = result_code.deserialize(data, data_len, pos)))
      {
        TBSYS_LOG(WARN, "deserialize result_code failed: "
                         "data_len=%ld pos=%ld, err=%d",
                         data_len, pos, err);
      }

      if (OB_SUCCESS == err
          && (OB_OP_GET == creq->opcode || OB_OP_SCAN == creq->opcode))
      {
        if (NULL == creq->ob_res)
        {
          creq->ob_res = acquire_res_st();
          if (NULL == creq->ob_res)
          {
            err = OB_ALLOCATE_MEMORY_FAILED;
          }
        }

        if (OB_SUCCESS == err)
        {
          creq->ob_res->scanner.resize(1);
          creq->ob_res->cur_scanner = creq->ob_res->scanner.begin();
          err = creq->ob_res->cur_scanner->deserialize(data, data_len, pos);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "deserialize scanner from buff failed, "
                "data_len=%ld pos=%ld, err=%d", data_len, pos, err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        creq->err_code = result_code.result_code_;
        err = ready_list_.push_back((ObInnerReq*)creq);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "ReqList push_back error, err=%d, "
              "this should not be happened", err);
        }
      }
    }

    if (OB_SUCCESS == err)
    {
      if (creq->flags == FD_INVOCATION)
      {
        int64_t num1 = 1;
        ::write(creq->resfd, &num1, sizeof(num1));
      }
    }
  }

  if (OB_ERR_SUCCESS == err_code && CONTROL_PACKET == packet_type)
  {
    tbnet::ControlPacket *cpkt = static_cast<tbnet::ControlPacket*>(packet);
    int cmd = cpkt->getCommand();
    const char *cmd_str = NULL;
    switch (cmd)
    {
      case tbnet::ControlPacket::CMD_BAD_PACKET:
        cmd_str = "BAD_PACKET";
        break;
      case tbnet::ControlPacket::CMD_TIMEOUT_PACKET:
        cmd_str = "TIMEOUT_PACKET";
        break;
      case tbnet::ControlPacket::CMD_DISCONN_PACKET:
        cmd_str = "DISCONN_PACKET";
        break;
      default:
        cmd_str = "UNKNOWN_PACKET";
    }
    TBSYS_LOG(WARN, "control packet: %s", cmd_str);
  }

  return tbnet::IPacketHandler::FREE_CHANNEL;
}

int ObClient::send_request(const ObServer& server,  const int32_t pcode,
                           const int64_t timeout, const ObDataBuffer& in_buffer,
                           OB_REQ *req)
{
  int ret = OB_SUCCESS;
  const int32_t version = 1;
  ObPacket* packet = new (std::nothrow) ObPacket();
  if (NULL == packet)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    packet->set_packet_code(pcode);
    packet->setChannelId(0);
    packet->set_api_version(version);
    packet->set_data(in_buffer);
    packet->set_source_timeout(timeout);
    ret = packet->serialize();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "packet serialize error");
      packet->free();
      packet = NULL;
    }
    else
    {
      if (NULL != req)
      {
        ((ObInnerReq*)req)->submit_time = tbsys::CTimeUtil::getMonotonicTime();
      }
      bool send_ok = connmgr_.sendPacket(server.get_ipv4_server_id(), packet,
                                         this, req);
      if (!send_ok)
      {
        ret = OB_PACKET_NOT_SENT;
        TBSYS_LOG(INFO, "cannot send packet, disconnected or queue is full");
        packet->free();
      }
    }
  }

  return ret;
}

int ObClient::check_req_integrity(OB_REQ *req)
{
  int ret = OB_SUCCESS;

  if (NULL == req)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    if (OB_OP_GET != req->opcode
        && OB_OP_SCAN != req->opcode
        && OB_OP_SET != req->opcode)
    {
      TBSYS_LOG(WARN, "req's opcode is not supported, opcode=%d", req->opcode);
      ret = OB_ERROR;
    }
    else if (req->timeout == 0)
    {
      TBSYS_LOG(WARN, "req's timeout cannot be zero");
      ret = OB_ERROR;
    }
    else
    {
      switch (req->opcode)
      {
        case OB_OP_GET:
          if (NULL == req->ob_get)
          {
            TBSYS_LOG(WARN, "req's ob_get is NULL");
            ret = OB_ERROR;
          }
          break;
        case OB_OP_SCAN:
          if (NULL == req->ob_scan)
          {
            TBSYS_LOG(WARN, "req's ob_scan is NULL");
            ret = OB_ERROR;
          }
          break;
        case OB_OP_SET:
          if (NULL == req->ob_set)
          {
            TBSYS_LOG(WARN, "req's ob_set is NULL");
            ret = OB_ERROR;
          }
          break;
      }
    }
  }

  return ret;
}

int ObClient::dispatch_req(OB_REQ *req)
{
  int ret = OB_SUCCESS;

  if (NULL == req)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObServer server;
    switch (req->opcode)
    {
      case OB_OP_GET:
        if (OB_SUCCESS != (ret = get_ms(req, server)))
        {
          TBSYS_LOG(WARN, "get_ms error, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = submit_get(server, req)))
        {
          TBSYS_LOG(WARN, "submit_get req error, ret=%d", ret);
        }
        break;
      case OB_OP_SCAN:
        if (OB_SUCCESS != (ret = get_ms(req, server)))
        {
          TBSYS_LOG(WARN, "get_ms error, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = submit_scan(server, req)))
        {
          TBSYS_LOG(WARN, "submit_scan req error, ret=%d", ret);
        }
        break;
      case OB_OP_SET:
        if (OB_SUCCESS != (ret = get_us(req, server)))
        {
          TBSYS_LOG(WARN, "get_us error, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = submit_set(server, req)))
        {
          TBSYS_LOG(WARN, "submit_set req error, ret=%d", ret);
        }
        break;
      default:
        TBSYS_LOG(WARN, "req's opcode is not supported, opcode=%d",
            req->opcode);
        ret = OB_ERROR;
    }
  }

  return ret;
}

int ObClient::get_us(OB_REQ *req, ObServer &server)
{
  UNUSED(req);
  server = ups_master_.get_ups();
  return OB_SUCCESS;
}

int ObClient::get_ms(OB_REQ *req, ObServer &server)
{
  UNUSED(req);
  server = ms_list_.get_one();
  return OB_SUCCESS;
}

OB_ERR_CODE ObClient::serialize_get(const ObGetParam& get,
                                    ObDataBuffer& data_buff)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  int ret = get.serialize(data_buff.get_data(),
      data_buff.get_capacity(), data_buff.get_position());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObGetParam serialize error, err=%d", ret);
    err_code = OB_ERR_ERROR;
  }

  return err_code;
}

OB_ERR_CODE ObClient::serialize_scan(const ObScanParam& scan,
                                     ObDataBuffer& data_buff)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  int ret = scan.serialize(data_buff.get_data(),
      data_buff.get_capacity(), data_buff.get_position());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObScanParam serialize error, ret=%d", ret);
    err_code = OB_ERR_ERROR;
  }
  else
  {
    TBSYS_LOG(DEBUG, "scan ObScanParam table_id=%lu table_name=%.*s",
        scan.get_table_id(), scan.get_table_name().length(),
        scan.get_table_name().ptr());
  }

  return err_code;
}

OB_ERR_CODE ObClient::serialize_set(const ObMutator& mut,
                                    ObDataBuffer& data_buff)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  int ret = mut.serialize(data_buff.get_data(),
      data_buff.get_capacity(), data_buff.get_position());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObGetParam serialize error, err=%d", ret);
    err_code = OB_ERR_ERROR;
  }

  return err_code;
}

OB_ERR_CODE ObClient::deserialize_get(const char* data,
                                      int64_t data_len,
                                      ObResultCode& res_code,
                                      ObScanner& scanner)
{
  return deserialize_scan(data, data_len, res_code, scanner);
}

OB_ERR_CODE ObClient::deserialize_scan(const char* data,
                                       int64_t data_len,
                                       ObResultCode& res_code,
                                       ObScanner& scanner)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  int64_t pos = 0;
  int ret = res_code.deserialize(data, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "deserialize result_code failed, ret=%d", ret);
    err_code = OB_ERR_ERROR;
  }
  else if (OB_SUCCESS == res_code.result_code_)
  {
    ret = scanner.deserialize(data, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner failed, ret=%d", ret);
      err_code = OB_ERR_ERROR;
    }
  }

  return err_code;
}

OB_ERR_CODE ObClient::deserialize_set(const char* data,
                                      int64_t data_len,
                                      ObResultCode& res_code)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  int64_t pos = 0;
  int ret = res_code.deserialize(data, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "deserialize result_code failed, ret=%d", ret);
    err_code = OB_ERR_ERROR;
  }

  return err_code;
}

OB_ERR_CODE ObClient::get_ms(const ObServer& server,
                             ObDataBuffer& data_buff,
                             int32_t timeout,
                             OB_RES::ScannerList& scanner)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  static const int32_t MY_VERSION = 1;
  int64_t session_id = 0;

  int ret = client_.send_request(server, OB_GET_REQUEST, MY_VERSION, timeout,
      data_buff, session_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    err_code = err_code_map(ret);
  }
  else
  {
    scanner.resize(scanner.size() + 1);
    ObResultCode result_code;
    err_code = deserialize_get(data_buff.get_data(), data_buff.get_position(),
                               result_code, scanner.back());
    if (OB_ERR_SUCCESS == err_code)
    {
      err_code = err_code_map(result_code.result_code_);
      ob_set_error(result_code.message_.ptr());
      if (OB_ERR_SUCCESS == err_code)
      {
        TBSYS_LOG(DEBUG, "get_ms receives a scanner, row_num=%ld",
            scanner.back().get_row_num());
      }
    }
  }

  if (OB_ERR_SUCCESS == err_code)
  {
    bool is_end = false;
    int64_t tmp = 0;
    scanner.back().get_is_req_fullfilled(is_end, tmp);
    while (!is_end && session_id != 0 && OB_ERR_SUCCESS == err_code)
    {
      ret = client_.get_next(server, session_id, timeout, data_buff, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
        err_code = err_code_map(ret);
      }
      else
      {
        scanner.resize(scanner.size() + 1);
        ObResultCode result_code;
        err_code = deserialize_scan(data_buff.get_data(),
            data_buff.get_position(), result_code, scanner.back());
        if (OB_ERR_SUCCESS == err_code)
        {
          err_code = err_code_map(result_code.result_code_);
          ob_set_error(result_code.message_.ptr());
          if (OB_ERR_SUCCESS == err_code)
          {
            TBSYS_LOG(DEBUG, "get_ms receives a scanner, row_num=%ld",
                scanner.back().get_row_num());
          }
        }
      }
      scanner.back().get_is_req_fullfilled(is_end, tmp);
    }
  }

  return err_code;
}

OB_ERR_CODE ObClient::scan_ms(const ObServer& server,
                              ObDataBuffer& data_buff,
                              int32_t timeout,
                              OB_RES::ScannerList& scanner)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  static const int32_t MY_VERSION = 1;
  int64_t session_id = 0;

  int ret = client_.send_request(server, OB_SCAN_REQUEST, MY_VERSION, timeout,
      data_buff, session_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    err_code = err_code_map(ret);
  }
  else
  {
    scanner.resize(scanner.size() + 1);
    ObResultCode result_code;
    err_code = deserialize_scan(data_buff.get_data(), data_buff.get_position(),
        result_code, scanner.back());
    if (OB_ERR_SUCCESS == err_code)
    {
      err_code = err_code_map(result_code.result_code_);
      ob_set_error(result_code.message_.ptr());
      if (OB_ERR_SUCCESS == err_code)
      {
        TBSYS_LOG(DEBUG, "scan_ms receives a scanner, row_num=%ld",
            scanner.back().get_row_num());
      }
    }
  }

  if (OB_ERR_SUCCESS == err_code)
  {
    bool is_end = false;
    int64_t tmp = 0;
    scanner.back().get_is_req_fullfilled(is_end, tmp);
    while (!is_end && session_id != 0 && OB_ERR_SUCCESS == err_code)
    {
      data_buff.get_position() = 0;
      ret = client_.get_next(server, session_id, timeout, data_buff, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
        err_code = err_code_map(ret);
      }
      else
      {
        scanner.resize(scanner.size() + 1);
        ObResultCode result_code;
        err_code = deserialize_scan(data_buff.get_data(),
            data_buff.get_position(), result_code, scanner.back());
        if (OB_ERR_SUCCESS == err_code)
        {
          err_code = err_code_map(result_code.result_code_);
          ob_set_error(result_code.message_.ptr());
          if (OB_ERR_SUCCESS == err_code)
          {
            TBSYS_LOG(DEBUG, "scan_ms receives a scanner, row_num=%ld",
                scanner.back().get_row_num());
          }
        }
      }
      scanner.back().get_is_req_fullfilled(is_end, tmp);
    }
  }

  return err_code;
}

OB_ERR_CODE ObClient::set_us(const ObServer& server,
                             ObDataBuffer& data_buff,
                             int32_t timeout)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  static const int32_t MY_VERSION = 1;

  int ret = client_.send_request(server, OB_WRITE, MY_VERSION, timeout, data_buff);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    err_code = err_code_map(ret);
  }
  else
  {
    ObResultCode result_code;
    err_code = deserialize_set(data_buff.get_data(), data_buff.get_position(), result_code);
    if (OB_ERR_SUCCESS == err_code)
    {
      err_code = err_code_map(result_code.result_code_);
      ob_set_error(result_code.message_.ptr());
    }
  }

  return err_code;
}

OB_ERR_CODE ObClient::request(const ObServer& server,
                              int command,
                              ObDataBuffer& data_buff,
                              int32_t timeout)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  static const int32_t MY_VERSION = 1;

  int ret = client_.send_request(server, command, MY_VERSION, timeout, data_buff);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    err_code = err_code_map(ret);
  }
  else
  {
    ObResultCode result_code;
    int64_t pos = 0;
    int ret = result_code.deserialize(data_buff.get_data(),
        data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed, ret=%d", ret);
      err_code = OB_ERR_ERROR;
    }

    if (OB_ERR_SUCCESS == err_code)
    {
      err_code = err_code_map(result_code.result_code_);
      ob_set_error(result_code.message_.ptr());
    }
  }

  return err_code;
}

OB_ERR_CODE ObClient::clear_active_memtable()
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  ObDataBuffer data_buff;
  err_code = get_thread_data_buffer(data_buff);

  if (OB_ERR_SUCCESS != err_code)
  {
    TBSYS_LOG(ERROR, "get_thread_data_buffer error, err_code=%d", err_code);
  }
  else
  {
    err_code = request(ups_master_.get_ups(), OB_UPS_CLEAR_ACTIVE_MEMTABLE,
        data_buff, cntl_.timeout_set);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "clear_active_memtable error, err_code=%d", err_code);
    }
  }

  return err_code;
}

OB_ERR_CODE ObClient::get_thread_data_buffer(ObDataBuffer &data_buffer)
{
  OB_ERR_CODE ret = OB_ERR_SUCCESS;
  if (NULL == tbuff_.get())
  {
    ret = OB_ERR_ERROR;
    TBSYS_LOG(ERROR, "thread buffer return null, ret=%d", ret);
  }
  else
  {
    data_buffer.set_data(tbuff_.get()->ptr(), tbuff_.get()->capacity());
  }
  return ret;
}

