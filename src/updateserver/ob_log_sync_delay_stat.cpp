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

#include "ob_log_sync_delay_stat.h"
#include "common/serialization.h"
#include "Time.h"
#include "ob_update_server_main.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    int set_state_as_replaying_log()
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsRoleMgr& role_mgr = main->get_update_server().get_role_mgr();
        if (role_mgr.get_state() != ObUpsRoleMgr::REPLAYING_LOG)
        {
          role_mgr.set_state(ObUpsRoleMgr::REPLAYING_LOG);
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
          {
            TBSYS_LOG(INFO, "slave lagged, set state to REPLAYING_LOG");
          }
        }
      }
      return ret;
    }

    int set_state_as_active()
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsRoleMgr& role_mgr = main->get_update_server().get_role_mgr();
        if (role_mgr.get_state() != ObUpsRoleMgr::ACTIVE)
        {
          role_mgr.set_state(ObUpsRoleMgr::ACTIVE);
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
          {
            TBSYS_LOG(INFO, "slave catchup, set state to ACTIVE");
          }
        }
      }
      return ret;
    }

    ObEventAccumulator::ObEventAccumulator(): last_event_id_(0), event_counter_(0), attr_accumulator_(0), max_attr_(0)
    {}

    ObEventAccumulator::~ObEventAccumulator()
    {}
    
    int ObEventAccumulator::reset()
    {
      int err = OB_SUCCESS;
      max_attr_ = 0;
      return err;
    }
    
    int ObEventAccumulator::add_event(const int64_t event_id, const int64_t attr)
    {
      int err = OB_SUCCESS;
      last_event_id_ = event_id;
      event_counter_ ++;
      attr_accumulator_ += attr;
      if (max_attr_ < attr)
      {
        max_attr_ = attr;
      }
      return err;
    }

    DEFINE_SERIALIZE(ObEventAccumulator)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || (buf_len < pos))
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "ObEventAccumulator.serialize(buf=%p, len=%ld, pos=%ld)=>%d",
                  buf, buf_len, pos, err);
      }
      else if ((OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, last_event_id_)))
               || (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, event_counter_)))
               || (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, attr_accumulator_)))
               || (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, max_attr_))))
      {
        TBSYS_LOG(ERROR, "ObEventAccumulator.serialize()=>%d", err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    DEFINE_DESERIALIZE(ObEventAccumulator)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;

      if (NULL == buf || (data_len < pos))
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "ObEventAccumulator.deserialize(buf=%p, len=%ld, pos=%ld)=>%d",
                  buf, data_len, pos, err);
      }
      else if ((OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&last_event_id_)))
               || (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&event_counter_)))
               || (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&attr_accumulator_)))
               || (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&max_attr_))))
      {
        TBSYS_LOG(ERROR, "ObEventAccumulator.deserialize()=>%d", err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    ObLogSyncDelayStat::ObLogSyncDelayStat(): have_ever_catchup_(false), delay_accumulator_(), delay_warn_time_us_(-1), delay_tolerable_time_us_(-1),
                                              last_mutation_time_us_(-1), last_replay_time_us_(-1),
                                              max_n_lagged_log_allowed_(0),
                                              n_lagged_mutator_(0), n_lagged_mutator_since_last_report_(0),
                                              report_interval_us_(DEFAULT_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US),
                                              last_report_time_us_(0)
    {}
    
    ObLogSyncDelayStat::~ObLogSyncDelayStat()
    {}

    int ObLogSyncDelayStat::reset()
    {
      int err = OB_SUCCESS;
      err = delay_accumulator_.reset();
      last_mutation_time_us_ = -1;
      last_replay_time_us_ = -1;
      return err;
    }

    int ObLogSyncDelayStat::reset_max_delay()
    {
      int err = OB_SUCCESS;
      delay_accumulator_.max_attr_ = 0;
      return err;
    }

    int ObLogSyncDelayStat::set_delay_warn_time_us(const int64_t delay_warn_time_us)
    {
      int err = OB_SUCCESS;
      delay_warn_time_us_ = delay_warn_time_us;
      return err;
    }
    
    int ObLogSyncDelayStat::set_delay_tolerable_time_us(const int64_t delay_tolerable_time_us)
    {
      int err = OB_SUCCESS;
      delay_tolerable_time_us_ = delay_tolerable_time_us;
      return err;
    }

    int ObLogSyncDelayStat::set_report_interval_us(const int64_t report_interval_us)
    {
      int err = OB_SUCCESS;
      report_interval_us_ = report_interval_us;
      return err;
    }

    int ObLogSyncDelayStat::set_max_n_lagged_log_allowed(const int64_t max_n_lagged_log_allowed)
    {
      int err = OB_SUCCESS;
      max_n_lagged_log_allowed_ = max_n_lagged_log_allowed;
      return err;
    }

    int ObLogSyncDelayStat::add_log_replay_event(const int64_t log_id, const int64_t mutation_time, const int64_t master_log_id, ReplayType replay_type)
    {
      int err = OB_SUCCESS;
      int64_t now_us = tbsys::CTimeUtil::getTime();
      if ((delay_warn_time_us_ > 0 && mutation_time + delay_warn_time_us_ < now_us)
          || (master_log_id > 0 && log_id + max_n_lagged_log_allowed_ < master_log_id))
      {
        OB_STAT_INC(UPDATESERVER, UPS_STAT_SLOW_CLOG_SYNC_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_SLOW_CLOG_SYNC_DELAY, now_us - mutation_time);
        n_lagged_mutator_ ++;
        if (delay_tolerable_time_us_ < 0 || mutation_time + delay_tolerable_time_us_ < now_us)
        {
          set_state_as_replaying_log();
        }
      }
      else if (replay_type == RT_LOCAL || log_id + 1 != master_log_id) // 没追上
      {}
      else
      {
        if (!have_ever_catchup_)
        {
          have_ever_catchup_ = true;
        }
        set_state_as_active();
      }

      if (last_report_time_us_ + report_interval_us_ < now_us
          && n_lagged_mutator_ != n_lagged_mutator_since_last_report_
          && have_ever_catchup_) // 第一次启动时肯定处于not sync状态，但是这时不打印警告
      {
        TBSYS_LOG(WARN, "mutator sync delay warn:"
                  "delay_warn_time=%ld us, n_lagged_mutator[%ld] - n_lagged_mutator_since_last_report[%ld] = %ld "
                  "log_id=%ld, master_log_id=%ld",
                  delay_warn_time_us_, n_lagged_mutator_,
                  n_lagged_mutator_since_last_report_, n_lagged_mutator_-n_lagged_mutator_since_last_report_,
                  log_id, master_log_id);
        if (REACH_TIME_INTERVAL_RANGE(5L * 60L * 1000000L, 60L * 60L * 1000000L))
        {
          TBSYS_LOG(ERROR, "[PER_5_MIN] mutator sync delay warn:"
                    "delay_warn_time=%ld us, n_lagged_mutator[%ld] - n_lagged_mutator_since_last_report[%ld] = %ld "
                    "log_id=%ld, master_log_id=%ld",
                    delay_warn_time_us_, n_lagged_mutator_,
                    n_lagged_mutator_since_last_report_, n_lagged_mutator_-n_lagged_mutator_since_last_report_,
                    log_id, master_log_id);
        }
        last_report_time_us_ = now_us;
        n_lagged_mutator_since_last_report_ = n_lagged_mutator_;
      }
      last_mutation_time_us_ = mutation_time;
      last_replay_time_us_ = now_us;
      delay_accumulator_.add_event(log_id, last_replay_time_us_ - last_mutation_time_us_);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_CLOG_SYNC_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_CLOG_SYNC_DELAY, now_us - mutation_time);
      OB_STAT_SET(UPDATESERVER, UPS_STAT_LAST_REPLAY_CLOG_TIME, last_replay_time_us_);
      return err;
    }
    
    int64_t ObLogSyncDelayStat::get_last_log_id() const
    {
      return delay_accumulator_.last_event_id_;
    }
    
    int64_t ObLogSyncDelayStat::get_mutator_count() const
    {
      return delay_accumulator_.event_counter_;
    }
    
    int64_t ObLogSyncDelayStat::get_total_delay_us() const
    {
      return delay_accumulator_.attr_accumulator_;
    }

    int64_t ObLogSyncDelayStat::get_last_delay_us() const
    {
      return 0 >= last_replay_time_us_? -1: last_replay_time_us_ - last_mutation_time_us_;
    }

    int64_t ObLogSyncDelayStat::get_max_delay_us() const
    {
      return delay_accumulator_.max_attr_;
    }
    
    int64_t ObLogSyncDelayStat::get_last_replay_time_us() const
    {
      return last_replay_time_us_;
    }

    DEFINE_SERIALIZE(ObLogSyncDelayStat)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || (buf_len < pos))
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "ObEventAccumulator.serialize(buf=%p, len=%ld, pos=%ld)=>%d",
                  buf, buf_len, pos, err);
      }
      else if (OB_SUCCESS != (err = delay_accumulator_.serialize(buf, buf_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "delay_stat.serialize(buf=%p, data_len=%ld, new_pos=%ld)=>%d", buf, buf_len, new_pos, err);
      }
      else if ((OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, delay_warn_time_us_)))
               || (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, last_mutation_time_us_)))
               || (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, last_replay_time_us_)))
               || (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, n_lagged_mutator_))))
      {
        TBSYS_LOG(ERROR, "ObLogSyncDelayStat.serialize()=>%d", err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    DEFINE_DESERIALIZE(ObLogSyncDelayStat)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || (data_len < pos))
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "ObEventAccumulator.deserialize(buf=%p, len=%ld, pos=%ld)=>%d",
                  buf, data_len, pos, err);
      }
      else if (OB_SUCCESS != (err = delay_accumulator_.deserialize(buf, data_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "delay_stat.deserialize(buf=%p, data_len=%ld, new_pos=%ld)=>%d", buf, data_len, new_pos, err);
      }
      else if ((OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&delay_warn_time_us_)))
               || (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&last_mutation_time_us_)))
               || (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&last_replay_time_us_)))
               || (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&n_lagged_mutator_))))
      {
        TBSYS_LOG(ERROR, "ObLogSyncDelayStat.deserialize()=>%d", err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

  };
};
