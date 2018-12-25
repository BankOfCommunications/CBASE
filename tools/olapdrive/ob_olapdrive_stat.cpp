/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_stat.cpp for define olapdrive statistic API. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_olapdrive_stat.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace common;

    ObOlapdriveStat::ObOlapdriveStat() 
    : insert_opt_(0), insert_row_(0),
      scan_opt_(0), scan_row_(0),
      total_time_(0), prev_insert_row_(0), 
      prev_scan_row_(0), stat_dump_interval_(0), 
      stat_dumper_(this)
    {

    }

    ObOlapdriveStat::~ObOlapdriveStat()
    {

    }

    int ObOlapdriveStat::init(int64_t stat_dump_interval)
    {
      int ret = OB_SUCCESS;

      stat_dump_interval_ = stat_dump_interval;
      ret = timer_.init();
      if (OB_SUCCESS == ret)
      {
        ret = timer_.schedule(stat_dumper_, stat_dump_interval_, false);
      }

      return ret;
    }

    void ObOlapdriveStat::add_insert_opt(const uint64_t insert_opt)
    {
      if (insert_opt > 0)
      {
        atomic_add(&insert_opt_, insert_opt);
      }
    }

    void ObOlapdriveStat::add_insert_row(const uint64_t insert_row)
    {
      if (insert_row > 0)
      {
        atomic_add(&insert_row_, insert_row);
      }
    }

    void ObOlapdriveStat::add_scan_opt(const uint64_t scan_opt)
    {
      if (scan_opt > 0)
      {
        atomic_add(&scan_opt_, scan_opt);
      }
    }

    void ObOlapdriveStat::add_scan_row(const uint64_t scan_row)
    {
      if (scan_row > 0)
      {
        atomic_add(&scan_row_, scan_row);
      }
    }

    void ObOlapdriveStat::dump_stat()
    {
      printf("run_time: %lus \n"
             "stat_dump_interval: %lds \n"
             "insert_opt: %lu \n"
             "insert_row: %lu \n"
             "scan_opt: %lu \n"
             "scan_row: %lu \n",
             total_time_,
             stat_dump_interval_ / 1000000,
             insert_opt_,
             insert_row_,
             scan_opt_,
             scan_row_);
    }

    void ObOlapdriveStat::dump_stat_cycle()
    {
      uint64_t insert_opt_diff = 0;
      uint64_t insert_row_diff = 0;
      uint64_t scan_opt_diff = 0;
      uint64_t scan_row_diff = 0;
      uint64_t interval_time = stat_dump_interval_ / 1000000;
      char buffer[1024];
      int64_t pos = 0;

      total_time_ += interval_time;
      insert_opt_diff = insert_opt_ - prev_insert_opt_;
      prev_insert_opt_ = insert_opt_;
      insert_row_diff = insert_row_ - prev_insert_row_;
      prev_insert_row_ = insert_row_;
      scan_opt_diff = scan_opt_ - prev_scan_opt_;
      prev_scan_opt_ = scan_opt_;
      scan_row_diff = scan_row_ - prev_scan_row_;
      prev_scan_row_ = scan_row_;

      pos += sprintf(buffer, "%-15s %-15s %-15s %-15s %-12s %-12s %-12s %-12s\n",
                     "insert_opt_i", "insert_opt_g", "insert_row_i", "insert_row_g", 
                     "scan_opt_i", "scan_opt_g", "scan_row_i", "scan_row_g");
      pos += sprintf(buffer + pos, "%-15lu %-15lu %-15lu %-15lu %-12lu "
                                    "%-12lu %-12lu %-12lu\n\n",
                     insert_opt_diff / interval_time, insert_opt_ / total_time_,
                     insert_row_diff / interval_time, insert_row_ / total_time_,
                     scan_opt_diff / interval_time, scan_opt_ / total_time_, 
                     scan_row_diff / interval_time, scan_row_ / total_time_);

      printf("\33[2J");
      printf("%s", buffer);
      dump_stat();    
    }

    void ObOlapdriveStat::ObStatDumper::runTimerTask()
    {
      if (NULL != stat_)
      {
        stat_->dump_stat_cycle();

        // reschedule
        stat_->timer_.schedule(*this, stat_->get_stat_dump_interval(), false);
      }
    }
  } // end namespace olapdrive
} // end namespace oceanbase
