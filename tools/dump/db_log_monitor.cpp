/*
 * =====================================================================================
 *
 *       Filename:  db_log_monitor.cpp
 *
 *    Description:  log monitor implementation
 *
 *        Version:  1.0
 *        Created:  06/11/2011 12:52:34 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_log_monitor.h"
#include "db_file_utils.h"
#include "db_msg_report.h"
#include "db_dumper_config.h"
#include "db_utils.h"
#include "common/ob_define.h"
#include "common/file_utils.h"
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <sstream>

using namespace oceanbase::common;
const char* kLogIdFile = "file_max";
const int kLogIdMaxLen = 32;

DbLogMonitor::DbLogMonitor(std::string path, std::string local_path, int64_t interval) :
  path_(path), local_path_(local_path), interval_(interval), 
  file_util_(path)
{
 running_ = false;
 max_nolog_interval_ = 0;
 last_nolog_time_ = 0;
 last_max_logid_ = -1;
}

DbLogMonitor::~DbLogMonitor()
{
  if (running_ == true)
    stop_service();
}

void DbLogMonitor::set_max_nolog_interval(int64_t max_nolog_interval)
{
  TBSYS_LOG(INFO, "set max nolog interval");
  max_nolog_interval_ = max_nolog_interval;
}

int64_t DbLogMonitor::get_max_avail_log()
{
  int64_t logid = -1;
  std::string file_name = path_ + "/";
  file_name += kLogIdFile;

  FileUtils file;
  if (file.open(file_name.c_str(), O_RDONLY) < 0) {
    TBSYS_LOG(ERROR, "can't open %s", file_name.c_str());
  } else {
    char buf[kLogIdMaxLen];
    if (file.read(buf, kLogIdMaxLen) <= 0) {
      TBSYS_LOG(ERROR, "can't read %s", file_name.c_str());
    } else {                                    /* we got max log avail id */
      logid = atol(buf);
    }
  }

  return logid;
}

int DbLogMonitor::fetch_log(int64_t &log)
{
  int ret = OB_SUCCESS;

//  tbsys::CThreadGuard guard(&logs_mutex_);
  logs_cond_.lock();
  while (waiting_logs_.empty() && running_) {
    logs_cond_.wait();
  } 
  
  if (running_ || !waiting_logs_.empty()) {
    log = waiting_logs_.front().log_id;
    waiting_logs_.pop_front();
  } else {
    //when encountered OB_ERROR,or no more records available
    ret = OB_ERROR;
  }

  logs_cond_.unlock();

  return ret;
}

bool DbLogMonitor::is_lastlog_of_day(int64_t logid)
{
  char date_buf[MAX_TIME_LEN];
  char file_path[1024];
  struct stat log_stat;
  bool ret = true;

  int len = snprintf(file_path, 1024, "%s/%ld", path_.c_str(), logid);
  if (len < 0) {
    TBSYS_LOG(ERROR, "%s is too long", path_.c_str());
    return false;
  }

  if (stat(file_path, &log_stat) == 0) {
    //setup file date
    tm file_tm;
    localtime_r(&log_stat.st_mtime, &file_tm);
    strftime(date_buf, MAX_TIME_LEN, "%Y-%m-%d", &file_tm);

    if (today_.compare(date_buf) < 0) {       /* last log file */
      ret = true;
      today_ = date_buf;
    } else {
      ret = false;                              /* not last file */
    }
  } else {
    ret = false;
    TBSYS_LOG(ERROR, "stat file error, path=%s, retcode=%d", file_path, errno);
  }

  return ret;
}

void DbLogMonitor::start_servic(int64_t start_log)
{

  running_ = true;
  log_current_ = start_log;
  TBSYS_LOG(INFO, "start log is %ld", start_log);
  interval_ = DUMP_CONFIG->get_monitor_interval();
  max_nolog_interval_ = DUMP_CONFIG->max_nolog_interval();
  last_nolog_time_ = time(NULL);
  TBSYS_LOG(INFO, "Max Nolog Interval = [%ld]", max_nolog_interval_);

  //init today
  if (DUMP_CONFIG->init_date() == NULL) {
    char buf[MAX_TIME_LEN];

    get_current_date(buf, MAX_TIME_LEN);
    today_ = buf;
  } else {
    today_ = DUMP_CONFIG->init_date();
  }

  TBSYS_LOG(INFO, "[LOG MONITOR]:today = %s", today_.c_str());
  start();
}

void DbLogMonitor::stop_service()
{
  logs_cond_.lock();
  running_ = false;
  logs_cond_.broadcast();
  logs_cond_.unlock();

  stop();
  wait();
}

int DbLogMonitor::monitor_log_dir()
{
  return 0;
}

void DbLogMonitor::run(tbsys::CThread *thread, void *arg)
{
  UNUSED(thread);
  UNUSED(arg);
  while(running_) {
    TBSYS_LOG(INFO, "processing logs....., %ld", interval_);
    process_logs();
    sleep(static_cast<int32_t>(interval_));
  }
}

int DbLogMonitor::get_nas_log(int64_t log_id)
{
  int ret = OB_SUCCESS;
  char cmd[1024];

  int len = snprintf(cmd, 1024, "cp %s/%ld %s/%ld", path_.c_str(), 
           log_id, local_path_.c_str(), log_id);
  if (len < 0) {
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    if (file_util_.system(cmd) != 0) {
      TBSYS_LOG(ERROR, "error when get log from nas");
      ret = OB_ERROR;
    }
  }

  return ret;
}

void DbLogMonitor::copy_logs(std::vector<int64_t> &logs)
{
  char cmd[1024];
  int ret = 0;

  for(size_t i = 0;i < logs.size(); i++) {
    snprintf(cmd, 1024, "cp %s/%ld %s/%ld", path_.c_str(), 
             logs[i], local_path_.c_str(), logs[i]);

    int retries = 3;

    while (retries--) {
      ret = file_util_.system(cmd);
      if (ret == 0)
        break;
    }

    if (retries <= 0) {
      TBSYS_LOG(ERROR, "copy logs failed, check %s", cmd);
      report_msg(MSG_ERROR, "error when copy logs [%d]", logs[i]);
    }
  }
}

void DbLogMonitor::append_logs(std::vector<int64_t> &logs)
{
  CommitlogInfo log_info;
//  tbsys::CThreadGuard guard(&logs_cond_);

  logs_cond_.lock();
  std::vector<int64_t>::iterator itr = logs.begin();
  while(itr != logs.end()) {
    log_info.log_id = *itr;
    waiting_logs_.push_back(log_info);
    TBSYS_LOG(INFO, "appending log=[%ld]", log_info.log_id);
    itr++;
  }
  //logs_cond_.broadcast();
  logs_cond_.signal();
  logs_cond_.unlock();
}

int DbLogMonitor::process_logs()
{
  //1.list all available log files
  std::vector<int64_t> logs;
  int max_avail_log = get_max_avail_log();
  FileProcessor processor(logs, max_avail_log);

  if (max_avail_log > last_max_logid_) {
    last_max_logid_ = max_avail_log;
    last_nolog_time_ = time(NULL);
  } else if (max_nolog_interval_ != 0) {
    time_t now = time(NULL);

    if (now - last_nolog_time_ > max_nolog_interval_) {
      TBSYS_LOG(ERROR, "waiting too long for commitlog");
    }
  }

  TBSYS_LOG(INFO, "max avail log id = %d", max_avail_log);

  int ret = file_util_.search_files(processor);
  if (ret == OB_SUCCESS) {
    std::sort(logs.begin(), logs.end());
    //find next log to parse
    std::vector<int64_t>::iterator low_itr = std::upper_bound(logs.begin(), logs.end(), log_current_);

    if (low_itr != logs.end()) {
      logs.erase(logs.begin(), low_itr);
      //check if the log_id is continous
      check_continuity(logs, log_current_ + 1);

      copy_logs(logs);
      append_logs(logs);

      //update log_current_
      log_current_ = logs.back();
    } //else no newly log

  } else {
//    printf("search files failed\n");
    TBSYS_LOG(ERROR, "search files in nas failed");
    ret = OB_ERROR;
  }

  return ret;
}

bool DbLogMonitor::check_continuity(std::vector<int64_t> &vec, int64_t start)
{
  bool ret = true;
  std::vector<int64_t>::iterator itr = vec.begin();

  while (itr != vec.end()) {
    if (*itr != start++) {
      TBSYS_LOG(WARN, "continuity violated expected=%ld, found=%ld", start - 1, *itr);
      report_msg(MSG_WARN, "continuity violated expected=%ld, found=%ld", start - 1, *itr);
      start = *itr;
      ret = false;
      continue;
    }

    itr++;
  }

  return ret;
}

void DbLogMonitor::wait()
{
  CDefaultRunnable::wait(); 
}
