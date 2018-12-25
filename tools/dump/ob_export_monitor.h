/*
 * =====================================================================================
 *
 *       Filename:  ob_export_monitor.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年12月06日 15时04分36秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef OB_EXPORT_MONITOR_H
#define OB_EXPORT_MONITOR_H
#include "tbsys.h"
#include "oceanbase_db.h"

class ErrorCode;


/*
 * =====================================================================================
 *        Class:  ExportMonitor
 *  Description:  监控当前进程的SIGINT信号，如果发生了则执行post_end_next
 *                到所有当前有效的server上
 * =====================================================================================
 */
class ExportMonitor: public tbsys::CDefaultRunnable
{
public:
  ExportMonitor(oceanbase::api::OceanbaseDb *db, ErrorCode *err_code);
  virtual void run(tbsys::CThread *thread, void *arg);
  void set_exit_signal(bool exit_signal);
  int post_end_session_all();
  int start_monitor();
  static void handler_signal(const int sig);

private:
  ErrorCode *err_code_;
  bool exit_signal_;
  oceanbase::api::OceanbaseDb *db_;
  static ExportMonitor *s_export_monitor;
};

#endif
