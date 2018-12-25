/*
 * =====================================================================================
 *
 *       Filename:  ob_export_monitor.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年12月06日 15时09分19秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#include "ob_export_monitor.h"
#include "ob_export_producer.h"

ExportMonitor* ExportMonitor::s_export_monitor = NULL;

void ExportMonitor::handler_signal(const int sig)
{
  switch(sig)
  {
  case SIGINT:
    if(NULL != s_export_monitor)
    {
      s_export_monitor->set_exit_signal(true);
      sleep(2);
    }
    exit(OB_ERROR);
    break;
  default:
    break;
  }
}

ExportMonitor::ExportMonitor(OceanbaseDb *db, ErrorCode *err_code)
{
  db_ = db;
  err_code_ = err_code;
  exit_signal_ = false;
  s_export_monitor = this;
  signal(SIGINT, handler_signal);
}

void ExportMonitor::set_exit_signal(bool exit_signal)
{
  exit_signal_ = exit_signal;
}

void ExportMonitor::run(tbsys::CThread *thread, void *arg)
{
  UNUSED(thread);
  UNUSED(arg);
  int ret = OB_SUCCESS;
  // 每100ms检查一次当前系统的状态，如果是异常状态或者接收到了SIGINT信号
  // 则结束循环，向所有有效的server，session_id发送end_next的请求
  // 避免ms的线程等待直到超时才结束
  while(err_code_->get_err_code() == OB_SUCCESS && !exit_signal_)
  {
    usleep(100000);
  }
  if(OB_SUCCESS != (ret = post_end_session_all()))
  {
    TBSYS_LOG(WARN, "post end session failed!");
  }
  TBSYS_LOG(INFO, "monitor work done!");
}

int ExportMonitor::post_end_session_all()
{
  ObServer server;
  int64_t session_id;
  int ret = OB_SUCCESS;

  ActiveServerSession& ac_server_session = ExportProducer::ac_server_session;

  while(OB_EXPORT_EMPTY != (ret = ac_server_session.pop_one(server, session_id)))
  {
    if(OB_SUCCESS != (ret = db_->post_end_next(server, session_id)))
    {
      TBSYS_LOG(WARN, "post end next failed! server=%s, session_id=%ld, ret=%d", to_cstring(server), session_id, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "post end next success! server=%s, session_id=%ld.", to_cstring(server), session_id);
    }
  }
  if(OB_SUCCESS != ret && OB_EXPORT_EMPTY != ret)
  {
    TBSYS_LOG(WARN, "post end next failed!");
  }
  else
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ExportMonitor::start_monitor()
{
  this->setThreadCount(1);
  this->start();
  return OB_SUCCESS;
}

