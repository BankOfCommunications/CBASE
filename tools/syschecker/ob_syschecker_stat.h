/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_stat.h for define syschecker statistic 
 * structure. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_STAT_H_
#define OCEANBASE_SYSCHECKER_STAT_H_

#include <math.h>
#include <stdint.h>
#include <pthread.h>
#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "common/ob_timer.h"

namespace oceanbase 
{ 
  namespace syschecker
  {
    enum ObSyscheckerCmd
    {
      CMD_GET,
      CMD_SCAN,
      CMD_UPDATE,
    };

    class ObRespStat
    {
    public:
        ObRespStat();
        ~ObRespStat();

        void init(const char *name);
        void record_event(uint64_t time, bool fail);
        int64_t local_log2(uint64_t value);
        uint64_t get_events();
        void dump_stats();
        void dump_resp_stats(int64_t run_time);
        void dump_format_stats(int64_t run_time, int64_t freq);
    private:
        static const int64_t DIST_CNT = 64;

        const char* name_;
        uint64_t    total_time_;
        uint64_t    min_time_;
        uint64_t    max_time_;
        uint64_t    fail_;
        uint64_t    dist_[DIST_CNT];
        double      squares_;
        double      log_product_;
        
        uint64_t    period_min_time_;
        uint64_t    period_max_time_;
        uint64_t    pre_fail_;
        uint64_t    pre_events_;
        uint64_t    pre_total_time_;
        double    pre_squares_;
        double      pre_log_product_;
    };

    class ObSyscheckerStat
    {
    public:
      ObSyscheckerStat();
      ~ObSyscheckerStat();

      int init(int64_t stat_dump_interval, bool is_check_result = true);

      void add_write_cell(const uint64_t write_cell);
      void add_write_cell_fail(const uint64_t write_cell_fail);

      void add_read_cell(const uint64_t read_cell);
      void add_read_cell_fail(const uint64_t read_cell_fail);

      void add_mix_opt(const uint64_t mix_opt);

      void add_add_opt(const uint64_t add_opt);
      void add_add_cell(const uint64_t add_cell);
      void add_add_cell_fail(const uint64_t add_cell_fail);

      void add_update_opt(const uint64_t update_opt);
      void add_update_cell(const uint64_t update_cell);
      void add_update_cell_fail(const uint64_t update_cell_fail);

      void add_insert_opt(const uint64_t insert_opt);
      void add_insert_cell(const uint64_t insert_cell);
      void add_insert_cell_fail(const uint64_t insert_cell_fail);

      void add_delete_row_opt(const uint64_t delete_row_opt);
      void add_delete_row(const uint64_t delete_row);
      void add_delete_row_fail(const uint64_t delete_row_fail);

      void add_get_opt(const uint64_t get_opt);
      void add_get_cell(const uint64_t get_cell);
      void add_get_cell_fail(const uint64_t get_cell_fail);

      void add_scan_opt(const uint64_t scan_opt);
      void add_scan_cell(const uint64_t scan_cell);
      void add_scan_cell_fail(const uint64_t scan_cell_fail);

      void record_resp_event(int cmd_type, uint64_t time, bool fail);

      void dump_stat();
      void dump_stat_cycle();

      inline const int64_t get_stat_dump_interval() const 
      {
        return stat_dump_interval_;
      }

    private:
      class ObStatDumper : public common::ObTimerTask 
      {
        public:
          ObStatDumper(ObSyscheckerStat* stat) : stat_(stat) {}
          ~ObStatDumper() {}
        public:
          virtual void runTimerTask();
        private:
          ObSyscheckerStat* stat_;
      };

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSyscheckerStat);

      uint64_t write_cell_;
      uint64_t write_cell_fail_;

      uint64_t read_cell_;
      uint64_t read_cell_fail_;

      uint64_t mix_opt_;

      uint64_t add_opt_;
      uint64_t add_cell_;
      uint64_t add_cell_fail_;

      uint64_t update_opt_;
      uint64_t update_cell_;
      uint64_t update_cell_fail_;

      uint64_t insert_opt_;
      uint64_t insert_cell_;
      uint64_t insert_cell_fail_;

      uint64_t delete_row_opt_;
      uint64_t delete_row_;
      uint64_t delete_row_fail_;

      uint64_t get_opt_;
      uint64_t get_cell_;
      uint64_t get_cell_fail_;

      uint64_t scan_opt_;
      uint64_t scan_cell_;
      uint64_t scan_cell_fail_;

      uint64_t total_time_;  //unit s
      uint64_t prev_write_cell_;
      uint64_t prev_write_cell_fail_;
      uint64_t prev_read_cell_;
      uint64_t prev_get_cell_;
      uint64_t prev_scan_cell_;

      int64_t stat_dump_interval_;  //unit us
      common::ObTimer timer_;
      ObStatDumper stat_dumper_;

      pthread_mutex_t resp_mutex_; /* synchronize the following members */
      ObRespStat get_resp_dist_;
      ObRespStat scan_resp_dist_;
      ObRespStat update_resp_dist_;
      ObRespStat total_resp_dist_;
      bool is_check_result_;
    };
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_STAT_H_
