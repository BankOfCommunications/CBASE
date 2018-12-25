/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_stat.h for define olapdrive statistic structure.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_STAT_H
#define OCEANBASE_OLAPDRIVE_STAT_H

#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "common/ob_timer.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObOlapdriveStat
    {
    public:
      ObOlapdriveStat();
      ~ObOlapdriveStat();

      int init(int64_t stat_dump_interval);

      void add_insert_opt(const uint64_t insert_opt);
      void add_insert_row(const uint64_t insert_row);

      void add_scan_opt(const uint64_t scan_opt);
      void add_scan_row(const uint64_t scan_row);

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
          ObStatDumper(ObOlapdriveStat* stat) : stat_(stat) {}
          ~ObStatDumper() {}
        public:
          virtual void runTimerTask();
        private:
          ObOlapdriveStat* stat_;
      };

    private:
      DISALLOW_COPY_AND_ASSIGN(ObOlapdriveStat);

      uint64_t insert_opt_;
      uint64_t insert_row_;

      uint64_t scan_opt_;
      uint64_t scan_row_;

      uint64_t total_time_;  //unit s
      uint64_t prev_insert_opt_;
      uint64_t prev_insert_row_;
      uint64_t prev_scan_opt_;
      uint64_t prev_scan_row_;

      int64_t stat_dump_interval_;  //unit us
      common::ObTimer timer_;
      ObStatDumper stat_dumper_;
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_STAT_H
