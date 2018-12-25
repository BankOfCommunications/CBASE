/*
 * =====================================================================================
 *
 *       Filename:  db_log_monitor.h
 *
 *    Description:  commit log monitor
 *
 *        Version:  1.0
 *        Created:  06/10/2011 01:37:36 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef __DB_LOG_MONITOR_H__
#define  __DB_LOG_MONITOR_H__

#include "tbsys.h"
#include "db_file_utils.h"
#include <string>
#include <vector>
#include <deque>

bool is_number(const char *s);

struct CommitlogInfo {
  int64_t log_id;
};

class DbLogMonitor : public tbsys::CDefaultRunnable {
  public:
    DbLogMonitor(std::string path, std::string local_path, int64_t interval);

    ~DbLogMonitor();

    //fecth one commitlog ready for parsing
    //if no log availabe, call thread will be blocked
    int fetch_log(int64_t &log_id);

    //thread function 
    virtual void run(tbsys::CThread *thread, void *arg);

    //call to period monitoring commitlog
    //start_log_id specifies the start log num
    void start_servic(int64_t start_log_id);
    void stop_service();
    void wait();

    //check if it is the last log of day
    bool is_lastlog_of_day(int64_t logid);

    int get_nas_log(int64_t log_id);
    //call to stop monitoring 

    void set_max_nolog_interval(int64_t max_nolog_interval);
  private:
    bool running_;
    int monitor_log_dir();

    std::string path_;
    std::string local_path_;
    int64_t interval_;

    int64_t max_nolog_interval_;
    time_t last_nolog_time_;
    int64_t last_max_logid_;

    //used to caculate next log
    int64_t log_current_;

    std::deque<CommitlogInfo> waiting_logs_;
    tbsys::CThreadMutex logs_mutex_;
    tbsys::CThreadCond logs_cond_;

    //today
    std::string today_;

    //commitlog filter, escape useless logs
    struct FileProcessor {
      FileProcessor(std::vector<int64_t> &vec, int64_t log_id) 
        : logs(vec), max_avail_log(log_id) { }

      std::vector<int64_t> &logs;
      int64_t max_avail_log;

      bool operator()(const char *dir, const char *file) {
        bool ret = false;
        UNUSED(dir);
        if ((ret = is_number(file)) == true) {
          int64_t file_id = atol(file);

          if (file_id <= max_avail_log)
            logs.push_back(file_id);
        }

        return ret;
      }
    };

    int64_t get_max_avail_log();

    //append new log into waiting logs
    void append_logs(std::vector<int64_t> &logs);
    void copy_logs(std::vector<int64_t> &logs);

    //check contiunity of newly generated logs
    bool check_continuity(std::vector<int64_t> &logs, int64_t start);

    int process_logs();
    DbFileUtils file_util_;
};

#endif
