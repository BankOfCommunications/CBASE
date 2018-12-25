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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_LOG_SYNC_DELAY_STAT_H_
#define OCEANBASE_UPDATESERVER_OB_LOG_SYNC_DELAY_STAT_H_
#include "common/ob_define.h"
#include "updateserver/ob_ups_log_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    struct ObEventAccumulator
    {
      int64_t last_event_id_;
      int64_t event_counter_;
      int64_t attr_accumulator_;
      int64_t max_attr_;
      ObEventAccumulator();
      ~ObEventAccumulator();
      int reset();
      int add_event(const int64_t event_id, const int64_t attr);
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObLogSyncDelayStat
    {
      const static int64_t DEFAULT_LOG_SYNC_DELAY_WARN_TIME_THRESHOLD_US = 100 * 1000;
      const static int64_t DEFAULT_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US = 10 * 1000 * 1000;
      ObLogSyncDelayStat();
      ~ObLogSyncDelayStat();
      int reset();
      int reset_max_delay();
      int set_delay_warn_time_us(const int64_t delay_warn_time_us);
      int set_delay_tolerable_time_us(const int64_t delay_tolerable_time_us);
      int set_report_interval_us(const int64_t report_interval_us);
      int set_max_n_lagged_log_allowed(const int64_t max_n_lagged_log_allowed);
      int add_log_replay_event(const int64_t log_id, const int64_t mutation_time, const int64_t master_log_id, ReplayType replay_type);
      int64_t get_last_delay_us() const;
      int64_t get_last_log_id() const;
      int64_t get_mutator_count() const;
      int64_t get_total_delay_us() const;
      int64_t get_max_delay_us() const;
      int64_t get_last_replay_time_us() const;
      NEED_SERIALIZE_AND_DESERIALIZE;
      bool have_ever_catchup_;
      ObEventAccumulator delay_accumulator_;
      int64_t delay_warn_time_us_;
      int64_t delay_tolerable_time_us_;
      int64_t last_mutation_time_us_;
      int64_t last_replay_time_us_;
      int64_t max_n_lagged_log_allowed_;
      int64_t n_lagged_mutator_;
      int64_t n_lagged_mutator_since_last_report_;
      int64_t report_interval_us_;
      int64_t last_report_time_us_;
    };
  }
}
#endif // OCEANBASE_UPDATESERVER_OB_LOG_SYNC_DELAY_STAT_H_
