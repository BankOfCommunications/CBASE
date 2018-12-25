/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_stat.cpp for define syschecker statistic API. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_syschecker_stat.h"

namespace oceanbase 
{ 
  namespace syschecker 
  {
    using namespace common;

    ObRespStat::ObRespStat()
    {
      memset(this, 0, sizeof(ObRespStat));
    }

    ObRespStat::~ObRespStat()
    {

    }

    void ObRespStat::init(const char *name)
    {
      name_            = name;
      min_time_        = (uint64_t) -1;
      period_min_time_ = (uint64_t) -1;
    }

    int64_t ObRespStat::local_log2(uint64_t value)
    {
      int64_t result = 0;
  
      while (result <= 63 && ((uint64_t) 1 << result) < value) {
          result++;
      }
  
      return result;
    }

    uint64_t ObRespStat::get_events()
    {
      uint64_t events = 0;
  
      for (int i = 0; i < DIST_CNT; i++) {
          events += dist_[i];
      }
  
      return events;
    }

    void ObRespStat::record_event(uint64_t time, bool fail)
    {
      total_time_ += time;

      if (time < min_time_) 
      {
        min_time_ = time;
      }

      if (time > max_time_) 
      {
        max_time_ = time;
      }

      if (time < period_min_time_) 
      {
        period_min_time_ = time;
      }

      if (time > period_max_time_) 
      {
        period_max_time_ = time;
      }

      if (fail) 
      {
        fail_++;
      }

      dist_[local_log2(time)]++;
      squares_ += (double) time * (double)time;

      if (time != 0) {
        log_product_ += log((double)time);
      }

      return;
    }

    void ObRespStat::dump_stats()
    {
      uint64_t events         = 0;
      int      max_non_zero   = 0;
      int      min_non_zero   = 0;
      double   average        = 0;
      int64_t  pos            = 0;
      char buffer[1024];
      char tmp_buf[32];

      for (int i = 0; i < DIST_CNT; i++) 
      {
        events += dist_[i];
        if (dist_[i] != 0) {
            max_non_zero = i;
        }
      }

      pos += sprintf(buffer, "%s Response Time Statistics (%lu events)\n", 
        name_, events);

      if (events == 0) 
      {
        return;
      }
      average = (double) total_time_ / (double)events;

      pos += sprintf(buffer + pos, "   Min:  %8lu\n"
                                   "   Max:  %8lu\n"
                                   "   Avg:  %8lu\n"
                                   "   Geo:  %8.2lf\n", 
        min_time_, max_time_, total_time_ / events, exp((double)log_product_ / (double)events));

      if (events > 1) {
        pos += sprintf(buffer + pos, "   Std:  %8.2lf\n",
               sqrt((squares_ - (double)events * average * average) / (double)(events - 1)));
      }
      pos += sprintf(buffer + pos, "   Log2 Dist:");

      for (int i = 0; i <= max_non_zero - 4; i += 4) {
        if (dist_[i + 0] != 0
            || dist_[i + 1] != 0
            || dist_[i + 2] != 0
            || dist_[i + 3] != 0) {
            min_non_zero = i;
            break;
        }
      }

      for (int i = min_non_zero; i <= max_non_zero; i++) {
        if ((i % 4) == 0) {
            pos += sprintf(buffer + pos, "\n    %6ld:", (int64_t)1 << i);
        }
        else 
        {
          pos += sprintf(buffer + pos, " %6ld:", (int64_t)1 << i);
        }
        sprintf(tmp_buf, " %lu(%.2f%%)", dist_[i], 
                (double)dist_[i] / (double)events * 100);
        pos += sprintf(buffer + pos, "%-15s", tmp_buf);
      }

      pos += sprintf(buffer + pos, "\n\n");
      printf("%s", buffer);

      return;
    }

    void ObRespStat::dump_resp_stats(int64_t run_time)
    {
      uint64_t events         = 0;
      double   global_average = 0;
      double   global_std     = 0;
  
      if ((events = get_events()) == 0) {
          return;
      }
  
      global_average = (double)total_time_ / (double)events;
      global_std = sqrt((squares_ - (double)events * global_average
                         * global_average) / (double)(events - 1));
  
      printf("\ntime(s): %-6ld avg_resp(us): %-10.2f min_resp(us): %-8lu "
             "max_resp(us): %-8lu std_dev: %.2f\n",
             run_time, global_average, min_time_, max_time_, global_std);
  
      return;
    }

    void ObRespStat::dump_format_stats(int64_t run_time, int64_t freq)
    {
      uint64_t events             = 0;
      double   global_average     = 0;
      uint64_t global_tps         = 0;
      double   global_std         = 0;
      double   global_log         = 0;

      uint64_t diff_time          = 0;
      uint64_t diff_events        = 0;
      double   diff_squares       = 0;
      double   diff_log_product   = 0;
      double   period_average     = 0;
      uint64_t period_tps         = 0;
      double   period_std         = 0;
      double   period_log         = 0;
      int64_t  pos                = 0;
      char buffer[1024];

      if ((events = get_events()) == 0) 
      {
        return;
      }

      global_average = (double)total_time_ / (double)events;
      global_tps = events / run_time;
      global_std = sqrt((squares_ - (double)events * global_average
                         * global_average) / (double)(events - 1));
      global_log = exp((double)log_product_ / (double)events);

      diff_time = total_time_ - pre_total_time_;
      diff_events = events - pre_events_;
      if (diff_events >= 1) {
          period_average = (double)diff_time / (double)diff_events;
          period_tps = diff_events / freq;
          diff_squares = squares_ - pre_squares_;
          period_std = sqrt((diff_squares - (double)diff_events * period_average
                             * period_average) / (double)(diff_events - 1));
          diff_log_product = log_product_ - pre_log_product_;
          period_log = exp((double)diff_log_product / (double)diff_events);
      }

      pos += sprintf(buffer + pos, "%s Statistics\n", name_);
      pos += sprintf(buffer + pos, "%-8s %-8s %-10s %-8s %-8s %-8s %-10s %-10s %-10s %-10s\n", 
              "Type", "Time(s)", "Ops", "QPS", "Fail", "Min(us)", "Max(us)",
             "Avg(us)", "Std_dev", "Geo_dist");

      pos += sprintf(buffer + pos, "%-8s %-8ld %-10lu %-8lu %-8lu %-8lu %-10lu %-10.2f %-10.2f %.2f\n",
             "Period", freq, diff_events, period_tps, 
             fail_ - pre_fail_, period_min_time_, period_max_time_,
             period_average, period_std, period_log);

      pos += sprintf(buffer + pos, "%-8s %-8ld %-10lu %-8lu %-8lu %-8lu %-10lu %-10.2f %-10.2f %.2f\n\n",
             "Global", run_time, events, global_tps, 
             fail_, min_time_, max_time_, global_average, global_std, global_log);
      printf("%s", buffer);

      pre_events_ = events;
      pre_squares_ = squares_;
      pre_total_time_ = total_time_;
      pre_log_product_ = log_product_;
      period_min_time_ = (uint64_t)-1;
      period_max_time_ = 0;
      pre_fail_ = fail_;

      return;
    }

    ObSyscheckerStat::ObSyscheckerStat() 
    : write_cell_(0), write_cell_fail_(0), read_cell_(0), read_cell_fail_(0),
      mix_opt_(0), add_opt_(0), add_cell_(0), add_cell_fail_(0), update_opt_(0),
      update_cell_(0), update_cell_fail_(0), insert_opt_(0), insert_cell_(0),
      insert_cell_fail_(0), delete_row_opt_(0), delete_row_(0), delete_row_fail_(0),
      get_opt_(0), get_cell_(0), get_cell_fail_(0), scan_opt_(0), scan_cell_(0),
      scan_cell_fail_(0), total_time_(0), prev_write_cell_(0), prev_write_cell_fail_(0),
      prev_read_cell_(0), prev_get_cell_(0), prev_scan_cell_(0), 
      stat_dump_interval_(0), stat_dumper_(this)
    {

    }

    ObSyscheckerStat::~ObSyscheckerStat()
    {
      pthread_mutex_destroy(&resp_mutex_);
    }

    int ObSyscheckerStat::init(int64_t stat_dump_interval, bool is_check_result)
    {
      int ret = OB_SUCCESS;

      stat_dump_interval_ = stat_dump_interval;
      is_check_result_ = is_check_result;
      ret = timer_.init();
      if (OB_SUCCESS == ret)
      {
        ret = timer_.schedule(stat_dumper_, stat_dump_interval_, false);
      }

      if (OB_SUCCESS == ret)
      {
        pthread_mutex_init(&resp_mutex_, NULL);
        get_resp_dist_.init("Get");
        scan_resp_dist_.init("Scan");
        update_resp_dist_.init("Update");
        total_resp_dist_.init("Total");
      }

      return ret;
    }

    void ObSyscheckerStat::add_write_cell(const uint64_t write_cell)
    {
      if (write_cell > 0)
      {
        atomic_add(&write_cell_, write_cell);
      }
    }

    void ObSyscheckerStat::add_write_cell_fail(const uint64_t write_cell_fail)
    {
      if (write_cell_fail > 0)
      {
        atomic_add(&write_cell_fail_, write_cell_fail);
      }
    }

    void ObSyscheckerStat::add_read_cell(const uint64_t read_cell)
    {
      if (read_cell > 0)
      {
        atomic_add(&read_cell_, read_cell);
      }
    }

    void ObSyscheckerStat::add_read_cell_fail(const uint64_t read_cell_fail)
    {
      if (read_cell_fail > 0)
      {
        atomic_add(&read_cell_fail_, read_cell_fail);
      }
    }

    void ObSyscheckerStat::add_mix_opt(const uint64_t mix_opt)
    {
      if (mix_opt > 0)
      {
        atomic_add(&mix_opt_, mix_opt);
      }
    }

    void ObSyscheckerStat::add_add_opt(const uint64_t add_opt)
    {
      if (add_opt > 0)
      {
        atomic_add(&add_opt_, add_opt);
      }
    }

    void ObSyscheckerStat::add_add_cell(const uint64_t add_cell)
    {
      if (add_cell > 0)
      {
        atomic_add(&add_cell_, add_cell);
      }
    }

    void ObSyscheckerStat::add_add_cell_fail(const uint64_t add_cell_fail)
    {
      if (add_cell_fail > 0)
      {
        atomic_add(&add_cell_fail_, add_cell_fail);
      }
    }

    void ObSyscheckerStat::add_update_opt(const uint64_t update_opt)
    {
      if (update_opt > 0)
      {
        atomic_add(&update_opt_, update_opt);
      }
    }

    void ObSyscheckerStat::add_update_cell(const uint64_t update_cell)
    {
      if (update_cell > 0)
      {
        atomic_add(&update_cell_, update_cell);
      }
    }

    void ObSyscheckerStat::add_update_cell_fail(const uint64_t update_cell_fail)
    {
      if (update_cell_fail > 0)
      {
        atomic_add(&update_cell_fail_, update_cell_fail);
      }
    }

    void ObSyscheckerStat::add_insert_opt(const uint64_t insert_opt)
    {
      if (insert_opt > 0)
      {
        atomic_add(&insert_opt_, insert_opt);
      }
    }

    void ObSyscheckerStat::add_insert_cell(const uint64_t insert_cell)
    {
      if (insert_cell > 0)
      {
        atomic_add(&insert_cell_, insert_cell);
      }
    }

    void ObSyscheckerStat::add_insert_cell_fail(const uint64_t insert_cell_fail)
    {
      if (insert_cell_fail > 0)
      {
        atomic_add(&insert_cell_fail_, insert_cell_fail);
      }
    }

    void ObSyscheckerStat::add_delete_row_opt(const uint64_t delete_row_opt)
    {
      if (delete_row_opt > 0)
      {
        atomic_add(&delete_row_opt_, delete_row_opt);
      }
    }

    void ObSyscheckerStat::add_delete_row(const uint64_t delete_row)
    {
      if (delete_row > 0)
      {
        atomic_add(&delete_row_, delete_row);
      }
    }

    void ObSyscheckerStat::add_delete_row_fail(const uint64_t delete_row_fail)
    {
      if (delete_row_fail > 0)
      {
        atomic_add(&delete_row_fail_, delete_row_fail);
      }
    }

    void ObSyscheckerStat::add_get_opt(const uint64_t get_opt)
    {
      if (get_opt > 0)
      {
        atomic_add(&get_opt_, get_opt);
      }
    }

    void ObSyscheckerStat::add_get_cell(const uint64_t get_cell)
    {
      if (get_cell > 0)
      {
        atomic_add(&get_cell_, get_cell);
      }
    }

    void ObSyscheckerStat::add_get_cell_fail(const uint64_t get_cell_fail)
    {
      if (get_cell_fail > 0)
      {
        atomic_add(&get_cell_fail_, get_cell_fail);
      }
    }

    void ObSyscheckerStat::add_scan_opt(const uint64_t scan_opt)
    {
      if (scan_opt > 0)
      {
        atomic_add(&scan_opt_, scan_opt);
      }
    }

    void ObSyscheckerStat::add_scan_cell(const uint64_t scan_cell)
    {
      if (scan_cell > 0)
      {
        atomic_add(&scan_cell_, scan_cell);
      }
    }

    void ObSyscheckerStat::add_scan_cell_fail(const uint64_t scan_cell_fail)
    {
      if (scan_cell_fail > 0)
      {
        atomic_add(&scan_cell_fail_, scan_cell_fail);
      }
    }

    void ObSyscheckerStat::record_resp_event(int cmd_type, uint64_t time, bool fail)
    {
      pthread_mutex_lock(&resp_mutex_);
      switch (cmd_type)
      {
        case CMD_GET:
          get_resp_dist_.record_event(time, fail);
          break;

        case CMD_SCAN:
          scan_resp_dist_.record_event(time, fail);
          break;

        case CMD_UPDATE:
          update_resp_dist_.record_event(time, fail);
          break;

        default:
          TBSYS_LOG(WARN, "unknow cmd_type=%d", cmd_type);
          break;
      }
      total_resp_dist_.record_event(time, fail);
      pthread_mutex_unlock(&resp_mutex_);
    }

    void ObSyscheckerStat::dump_stat()
    {
      if (is_check_result_)
      {
        printf("run_time: %lus \n"
               "stat_dump_interval: %lds \n"
               "write_cell: %lu \n"
               "write_cell_fail: %lu \n"
               "read_cell: %lu \n"
               "read_cell_miss: %lu \n"
               "mix_opt: %ld \n"
               "add_opt: %lu \n"
               "add_cell: %lu \n"
               "add_cell_fail: %lu \n"
               "update_opt: %lu \n"
               "update_cell: %lu \n"
               "update_cell_fail: %lu \n"
               "insert_opt: %lu \n"
               "insert_cell: %lu \n"
               "insert_cell_fail: %lu \n"
               "delete_row_opt: %lu \n"
               "delete_row: %lu \n"
               "delete_row_fail: %lu \n"
               "get_opt: %lu \n"
               "get_cell: %lu \n"
               "get_cell_fail: %lu \n"
               "scan_opt: %lu \n"
               "scan_cell: %lu \n"
               "scan_cell_fail: %lu \n",
               total_time_,
               stat_dump_interval_ / 1000000,
               write_cell_,
               write_cell_fail_,
               read_cell_,
               read_cell_fail_,
               mix_opt_,
               add_opt_,
               add_cell_,
               add_cell_fail_,
               update_opt_,
               update_cell_,
               update_cell_fail_,
               insert_opt_,
               insert_cell_,
               insert_cell_fail_,
               delete_row_opt_,
               delete_row_,
               delete_row_fail_,
               get_opt_,
               get_cell_,
               get_cell_fail_,
               scan_opt_,
               scan_cell_,
               scan_cell_fail_);
      }
      total_resp_dist_.dump_resp_stats(total_time_);
      get_resp_dist_.dump_stats();
      scan_resp_dist_.dump_stats();
      update_resp_dist_.dump_stats();
    }

    void ObSyscheckerStat::dump_stat_cycle()
    {
      uint64_t write_cell_diff = 0;
      uint64_t write_cell_fail_diff = 0;
      uint64_t read_cell_diff = 0;
      uint64_t get_cell_diff = 0;
      uint64_t scan_cell_diff = 0;
      uint64_t interval_time = stat_dump_interval_ / 1000000;
      char buffer[1024];
      int64_t pos = 0;

      printf("\n\033[1;1H\033[2J\n");
      total_time_ += interval_time;
      if (is_check_result_)
      {
        write_cell_diff = write_cell_ - prev_write_cell_;
        prev_write_cell_ = write_cell_;
        write_cell_fail_diff = write_cell_fail_ - prev_write_cell_fail_;
        prev_write_cell_fail_ = write_cell_fail_;
        read_cell_diff = read_cell_ - prev_read_cell_;
        prev_read_cell_ = read_cell_;
        get_cell_diff = get_cell_ - prev_get_cell_;
        prev_get_cell_ = get_cell_;
        scan_cell_diff = scan_cell_ - prev_scan_cell_;
        prev_scan_cell_ = scan_cell_;
  
        pos += sprintf(buffer, "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
                       "write_i", "write_g", "write_fi", "write_fg", 
                       "read_i", "read_g", "get_i", "get_g", "scan_i", "scan_g");
        pos += sprintf(buffer + pos, "%-10lu %-10lu %-10lu %-10lu %-10lu "
                                      "%-10lu %-10lu %-10lu %-10lu %-10lu\n\n",
                       write_cell_diff / interval_time, write_cell_ / total_time_,
                       write_cell_fail_diff /interval_time, write_cell_fail_ / total_time_,
                       read_cell_diff / interval_time, read_cell_ / total_time_,
                       get_cell_diff / interval_time, get_cell_ /total_time_,
                       scan_cell_diff / interval_time, scan_cell_ / total_time_);
  
        printf("%s", buffer);
      }

      get_resp_dist_.dump_format_stats(total_time_, interval_time);
      scan_resp_dist_.dump_format_stats(total_time_, interval_time);
      update_resp_dist_.dump_format_stats(total_time_, interval_time);
      total_resp_dist_.dump_format_stats(total_time_, interval_time);

      if (is_check_result_)
      {
        printf("run_time:%lus   get_qps:%ld   scan_qps:%ld   update_tps:%ld\n",
          total_time_, get_opt_ / total_time_, scan_opt_ / total_time_,
          (mix_opt_ + add_opt_ + update_opt_ + insert_opt_ + delete_row_opt_) / total_time_);
      }
    }

    void ObSyscheckerStat::ObStatDumper::runTimerTask()
    {
      if (NULL != stat_)
      {
        stat_->dump_stat_cycle();

        // reschedule
        stat_->timer_.schedule(*this, stat_->get_stat_dump_interval(), false);
      }
    }
  } // end namespace syschecker
} // end namespace oceanbase
