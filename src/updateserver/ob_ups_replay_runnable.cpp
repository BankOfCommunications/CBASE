/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_ups_replay_runnable.h"

#include "ob_ups_log_mgr.h"
#include "common/utility.h"
#include "ob_update_server.h"
#include "ob_update_server_main.h"
using namespace oceanbase::common;
using namespace oceanbase::updateserver;

//add wangdonghui [ups_replication] 20170527:b
#define UPS ObUpdateServerMain::get_instance()->get_update_server()
//add :e

ObUpsReplayRunnable::ObUpsReplayRunnable()
{
  is_initialized_ = false;
  replay_wait_time_us_ = DEFAULT_REPLAY_WAIT_TIME_US;
  fetch_log_wait_time_us_ = DEFAULT_FETCH_LOG_WAIT_TIME_US;
}

ObUpsReplayRunnable::~ObUpsReplayRunnable()
{
}

int ObUpsReplayRunnable::init(ObUpsLogMgr* log_mgr, ObiRole *obi_role, ObUpsRoleMgr *role_mgr)
{
  int ret = OB_SUCCESS;
  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogReplayRunnable has been initialized"); 
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == log_mgr || NULL == role_mgr || NULL == obi_role)
    {
      TBSYS_LOG(ERROR, "Parameter is invalid[obi_role=%p][role_mgr=%p]", obi_role, role_mgr);
    }
  }
  if (OB_SUCCESS == ret)
  {
    log_mgr_ = log_mgr;
    role_mgr_ = role_mgr;
    obi_role_ = obi_role;
    is_initialized_ = true;
  }
  return ret;
}

void ObUpsReplayRunnable::clear()
{
  if (NULL != _thread)
  {
    delete[] _thread;
    _thread = NULL;
  }
}

void ObUpsReplayRunnable::stop()
{
  _stop = true;
}

bool ObUpsReplayRunnable::wait_stop()
{
  return switch_.wait_off();
}

bool ObUpsReplayRunnable::wait_start()
{
  return switch_.wait_on();
}

void ObUpsReplayRunnable::run(tbsys::CThread* thread, void* arg)
{
  int err = OB_SUCCESS;
  UNUSED(thread);
  UNUSED(arg);

  TBSYS_LOG(INFO, "ObUpsLogReplayRunnable start to run");
  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsLogReplayRunnable has not been initialized");
    err = OB_NOT_INIT;
  }
  while (!_stop && (OB_SUCCESS == err || OB_NEED_RETRY == err || OB_NEED_WAIT == err))
  {
    //mod wangdonghui maybe slave ups has nothing to replay, but need to fetch log from master
    //if (switch_.check_off(log_mgr_->has_nothing_in_buf_to_replay()))
    if (switch_.check_off(log_mgr_->has_nothing_to_replay()))//在括号里的条件成立且有人要wait off这个线程，才会进入usleep
    //mod :e
    {
      usleep(static_cast<useconds_t>(replay_wait_time_us_)); //default 50ms
    }
    else if (OB_SUCCESS != (err = log_mgr_->replay_log())
        && OB_NEED_RETRY != err && OB_NEED_WAIT != err)
    {
      if (OB_CANCELED != err)
      {
        TBSYS_LOG(ERROR, "log_mgr.replay()=>%d", err);
      }
    }
    else if (OB_NEED_RETRY == err)
    {
      log_mgr_->wait_new_log_to_replay(replay_wait_time_us_);
    }
    else if (OB_NEED_WAIT == err)
    {
      TBSYS_LOG(WARN, "slave need wait for master ready %ld, %s", fetch_log_wait_time_us_, to_cstring(*log_mgr_));
      usleep(static_cast<useconds_t>(fetch_log_wait_time_us_));
    }
  }
  TBSYS_LOG(INFO, "ObLogReplayRunnable finished[stop=%d ret=%d]", _stop, err); 
}


//add wangdonghui [paxos ups_replication_optimize] 20161009:b
ObUpsWaitFlushRunnable::ObUpsWaitFlushRunnable():task_queue_(),allocator_()
{
  is_initialized_ = false;
}

ObUpsWaitFlushRunnable::~ObUpsWaitFlushRunnable()
{
    allocator_.destroy();
}

int ObUpsWaitFlushRunnable::init(ObUpsLogMgr* log_mgr, ObiRole *obi_role, ObUpsRoleMgr *role_mgr,
                                 ObUpdateServer* updateserver, ObLogReplayWorker* replay_worker)
{
  int ret = OB_SUCCESS;
  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsWaitFlushRunnable has been initialized");
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == log_mgr || NULL == role_mgr || NULL == obi_role || NULL == replay_worker)
    {
      TBSYS_LOG(ERROR, "Parameter is invalid[obi_role=%p][role_mgr=%p] [replay_worker=%p]", obi_role, role_mgr, replay_worker);
    }
  }
  if (OB_SUCCESS != (ret = task_queue_.init(10240)))
  {
    TBSYS_LOG(WARN, "task_queue_ init fail=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = allocator_.init(ALLOCATOR_TOTAL_LIMIT, ALLOCATOR_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
  {
    TBSYS_LOG(WARN, "init allocator fail ret=%d", ret);
  }
  else
  {
    log_mgr_ = log_mgr;
    role_mgr_ = role_mgr;
    obi_role_ = obi_role;
    updateserver_ = updateserver;
    replay_worker_ = replay_worker;
    is_initialized_ = true;
  }
  return ret;
}

void ObUpsWaitFlushRunnable::clear()
{
  if (NULL != _thread)
  {
    delete[] _thread;
    _thread = NULL;
  }
}

void ObUpsWaitFlushRunnable::stop()
{
  _stop = true;
}

bool ObUpsWaitFlushRunnable::wait_stop()
{
  return switch_.wait_off();
}

bool ObUpsWaitFlushRunnable::wait_start()
{
  return switch_.wait_on();
}

void ObUpsWaitFlushRunnable::run(tbsys::CThread* thread, void* arg)
{
  int err = OB_SUCCESS;
  UNUSED(thread);
  UNUSED(arg);

  TBSYS_LOG(INFO, "ObUpsWaitFlushRunnable start to run");
  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsWaitFlushRunnable has not been initialized");
    err = OB_NOT_INIT;
  }
  while (!_stop)
  {
    if (0 == task_queue_.size())
    {
      usleep(static_cast<useconds_t>(100)); //default 0.1ms
    }
    else if (OB_SUCCESS != (err = process_head_task()))
    {
      TBSYS_LOG(WARN, "process task=>%d", err);
    }
  }
  TBSYS_LOG(INFO, "ObUpsWaitFlushRunnable finished[stop=%d ret=%d]", _stop, err);
}

int ObUpsWaitFlushRunnable::process_head_task()
{
  int ret = OB_SUCCESS;
  TransExecutor::Task *task = NULL;
  _cond.lock();
  task_queue_.pop(task);
  _cond.unlock();
  if(NULL == task)
  {
    TBSYS_LOG(WARN, "task is NULL.");
  }
  else
  {
    int response_err = OB_SUCCESS;
    int64_t wait_sync_time_us =  updateserver_->get_wait_sync_time();
    int64_t wait_event_type = updateserver_->get_wait_sync_type();
    int64_t start_id = 0;
    int64_t end_id = 0;
    int64_t last_log_term = OB_INVALID_LOG;
    common::ObDataBuffer* in_buff = task->pkt.get_buffer();
    char* buf = in_buff->get_data() + in_buff->get_position();
    int64_t len = in_buff->get_capacity() - in_buff->get_position();
    //int64_t next_commit_log_id;
    int64_t next_flush_log_id;
    easy_request_t* req = task->pkt.get_request();
    uint32_t channel_id = task->pkt.get_channel_id();
    if( task->pkt.get_api_version() != MY_VERSION)
    {
      ret = OB_ERROR_FUNC_VERSION;
    }
    else if (OB_SUCCESS != (ret = parse_log_buffer(buf, len, start_id, end_id, last_log_term))) //mod1 wangjiahao [Paxos ups_replication_tmplog] 20150804 add last_log_term
    {
      TBSYS_LOG(ERROR, "parse_log_buffer(log_data=%p[%ld])=>%d", buf, len, ret);
    }
    else if (wait_sync_time_us <= 0 || ObUpsLogMgr::WAIT_NONE == wait_event_type)
    {}
    else if (ObUpsRoleMgr::ACTIVE != role_mgr_->get_state())
    {
      //mod wangjiahao [Paxos ups_replication_bugfix] 20150730 :b
      TBSYS_LOG(WARN, "recv log(log=[%ld,%ld)). But slave state not ACTIVE, do not wait flush until log catch up master", start_id, end_id);
      //add wangjiahao [Paxos ups_replication_bugfix] 20150730 :b
      if (ObUpsRoleMgr::STOP == role_mgr_->get_state() || ObUpsRoleMgr::FATAL == role_mgr_->get_state())
      {
        ret = OB_ERR_UNEXPECTED;
      }
      ret = OB_LOG_NOT_SYNC;
      //add :e
    }
    //del pangtianze [Paxos replication] 20161221:b remove waitcommit model
    /*else if (ObUpsLogMgr::WAIT_COMMIT == wait_event_type)
    {
      if (end_id > (next_commit_log_id = replay_worker_->wait_next_commit_log_id(end_id, wait_sync_time_us)))
      {
        TBSYS_LOG(WARN, "wait_flush_log_id(end_id=%ld, next_flush_log_id=%ld, timeout=%ld) Fail.",
                  end_id, next_commit_log_id, wait_sync_time_us);
      }
    }*/
    //del:e
    else if (ObUpsLogMgr::WAIT_FLUSH == wait_event_type)
    {
      if (end_id > (next_flush_log_id = replay_worker_->wait_next_flush_log_id(end_id, wait_sync_time_us)))
      {
        TBSYS_LOG(WARN, "wait_flush_log_id(end_id=%ld, next_flush_log_id=%ld, timeout=%ld) Fail.",
                  end_id, next_flush_log_id, wait_sync_time_us);
        //add wangjiahao [Paxos ups_replication_tmplog] 20150716 :b
        ret = OB_LOG_NOT_SYNC;
        //add :e
      }
      else
      {
        //TBSYS_LOG(INFO, "WDH_TEST:: slave_process_time(%s): %ld log: %ld %ld", updateserver_->get_self().to_cstring(),
        //          tbsys::CTimeUtil::getTime()-packet->get_receive_ts(), start_id, end_id);
      }
    }
    int64_t message_residence_time_us = tbsys::CTimeUtil::getTime()-task->pkt.get_receive_ts();
    if (OB_SUCCESS != (response_err = updateserver_->response_result1_(ret, OB_SEND_LOG_RES, MY_VERSION, req, channel_id, message_residence_time_us)))
    {
      ret = response_err;
      TBSYS_LOG(ERROR, "response_result_()=>%d", ret);
    }
    UPS.get_trans_executor().get_allocator()->free(task);
  }

  return ret;
}

int ObUpsWaitFlushRunnable::push(TransExecutor::Task* task)
{
  int ret = OB_SUCCESS;
  /*int64_t packet_size = sizeof(ObPacket) + packet->get_buffer()->get_capacity();
  ObPacket *req = (ObPacket *)allocator_.alloc(packet_size);
  if (NULL == req)
  {
    ret = OB_MEM_OVERFLOW;
  }
  else
  {
    req->set_api_version(packet->get_api_version());
    req->set_channel_id(packet->get_channel_id());
    req->set_receive_ts(packet->get_receive_ts());
    req->set_request(packet->get_request());
    char *data_buffer = (char*)req + sizeof(ObPacket);
    memcpy(data_buffer, packet->get_buffer()->get_data(), packet->get_buffer()->get_capacity());
    req->get_buffer()->set_data(data_buffer, packet->get_buffer()->get_capacity());
    req->get_buffer()->get_position() = packet->get_buffer()->get_position();
    TBSYS_LOG(DEBUG, "packet_size=%ld data_size=%ld pos=%ld",
                packet_size, req->get_buffer()->get_capacity(), req->get_buffer()->get_position());
    */
    _cond.lock();
    task_queue_.push(task);
    _cond.unlock();
  //}
  return ret;
//  TBSYS_LOG(INFO, "WDH_INFO: push wait_flush_thread succ, size: %d", packet_queue_.size());
}

//add :e
