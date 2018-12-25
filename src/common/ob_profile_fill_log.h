/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Author:
 *   lide.wd <lide.wd@alipay.com>
 * Description:
 *
 */

#ifndef OB_PROFILE_FILL_LOG_H

#define OB_PROFILE_FILL_LOG_H

#include "common/ob_request_profile_data.h"
#include "common/ob_trace_id.h"

#define PROFILE_SAMPLE_INTERVAL "PROFILE_SAMPLE_INTERVAL"
#define PFILL_RPC_START(pcode,channel_id) \
  if (ObProfileFillLog::is_open()) \
{\
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    int64_t st = tbsys::CTimeUtil::getTime();\
    ObRequestProfileData &profile_data = ObProfileFillLog::get_request_profile_data();\
    profile_data.rpc_latency_arr_[profile_data.count_].pcode_ = pcode;\
    profile_data.rpc_latency_arr_[profile_data.count_].channel_id_ = channel_id;\
    profile_data.rpc_latency_arr_[profile_data.count_].rpc_start_ = st;\
    profile_data.rpc_latency_arr_[profile_data.count_].rpc_end_ = 0;\
    __sync_fetch_and_add(&profile_data.count_, 1);\
  }\
}
#define GET_TRACE_SEQ()\
  ((reinterpret_cast<TraceId*>(&(ObProfileFillLog::get_request_profile_data().trace_id_)))->id.seq_)

#define PFILL_RPC_END(channel_id) \
  if (ObProfileFillLog::is_open()) \
{\
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    int64_t ed = tbsys::CTimeUtil::getTime();\
    ObRequestProfileData &profile_data = ObProfileFillLog::get_request_profile_data();\
    for (int64_t i = 0;i < profile_data.count_; ++i) \
    {\
      if (profile_data.rpc_latency_arr_[i].channel_id_ == channel_id)\
      {\
        profile_data.rpc_latency_arr_[i].rpc_end_ = ed;\
        break;\
      }\
    }\
  }\
}

#define PFILL_ITEM_START(target) \
  if (ObProfileFillLog::is_open()) \
{ \
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    int64_t st = tbsys::CTimeUtil::getTime();\
    ObProfileFillLog::get_request_profile_data().target##_start_ = st;\
  }\
}
#define PFILL_ITEM_END(target) \
  if (ObProfileFillLog::is_open()) \
{ \
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    int64_t ed = tbsys::CTimeUtil::getTime();\
    ObProfileFillLog::get_request_profile_data().target##_end_ = ed;\
  }\
}

#define PFILL_SET_TRACE_ID(trace_id) \
  if (ObProfileFillLog::is_open()) \
{ \
  ObProfileFillLog::set_trace_id(ObProfileFillLog::get_request_profile_data(),trace_id);\
}
#define PFILL_SET_QUEUE_SIZE(queue_size)\
  if (ObProfileFillLog::is_open())\
{\
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    ObProfileFillLog::set_sql_queue_size(ObProfileFillLog::get_request_profile_data(),queue_size);\
  }\
}

#define PFILL_SET_WAIT_SQL_QUEUE_TIME(wait_sql_queue_time)\
  if (ObProfileFillLog::is_open())\
{\
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    ObProfileFillLog::set_wait_sql_queue_time(ObProfileFillLog::get_request_profile_data(),wait_sql_queue_time);\
  }\
}
#define PFILL_SET_PCODE(pcode)\
  if (ObProfileFillLog::is_open())\
{\
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    ObProfileFillLog::set_pcode(ObProfileFillLog::get_request_profile_data(),pcode);\
  }\
}

#define PFILL_INIT_PROFILE_ITEM(profile_data,target)\
  if (ObProfileFillLog::is_open())\
{\
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    profile_data.target##_start_ = 0;\
    profile_data.target##_end_ = 0;\
  }\
}

#define PFILL_CLEAR_LOG() \
  if (ObProfileFillLog::is_open()) \
{ \
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    ObProfileFillLog::clear_log(ObProfileFillLog::get_request_profile_data());\
  }\
}

#define PFILL_PRINT() \
  if (ObProfileFillLog::is_open()) \
{ \
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    int64_t print_time = tbsys::CTimeUtil::getTime();\
    int64_t pos = 0;\
    ObRequestProfileData &profile_data = ObProfileFillLog::get_request_profile_data();\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "trace_id=[%ld] ", profile_data.trace_id_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "sql=[%.*s] ", profile_data.sql_len_, profile_data.sql_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "sql_to_logicalplan=[%ld] ", profile_data.sql_to_logicalplan_end_ - profile_data.sql_to_logicalplan_start_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "logicalplan_to_physicalplan=[%ld] ", profile_data.logicalplan_to_physicalplan_end_ - profile_data.logicalplan_to_physicalplan_start_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "handle_sql_time=[%ld] ", profile_data.handle_sql_time_end_ - profile_data.handle_sql_time_start_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "wait_sql_queue_time=[%ld] ",profile_data.wait_sql_queue_time_);\
    int64_t i = 0;\
    for (i = 0 ;i < profile_data.count_;++i)\
    {\
      databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "rpc:channel_id=[%ld] rpc_start=[%ld] latency=[%ld] pcode=[%ld] ", profile_data.rpc_latency_arr_[i].channel_id_,  profile_data.rpc_latency_arr_[i].rpc_start_, profile_data.rpc_latency_arr_[i].rpc_end_ - profile_data.rpc_latency_arr_[i].rpc_start_, profile_data.rpc_latency_arr_[i].pcode_);\
    }\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "sql_queue_size=[%d] ",profile_data.sql_queue_size_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "print_time=[%ld]",print_time);\
    ObProfileFillLog::get_log_buf()[pos] = '\n';\
    ::write(ObProfileFillLog::log_fd_, ObProfileFillLog::get_log_buf(), pos + 1);\
  }\
}

#define PFILL_CS_PRINT() \
  if (ObProfileFillLog::is_open()) \
{ \
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    int64_t pos = 0;\
    ObRequestProfileData &profile_data = ObProfileFillLog::get_request_profile_data();\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "trace_id=[%ld] ", profile_data.trace_id_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "handle_request_start=[%ld] ", profile_data.handle_request_time_start_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "handle_request_time=[%ld] ", profile_data.handle_request_time_end_ - profile_data.handle_request_time_start_);\
    databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "wait_queue_time=[%ld] ",profile_data.wait_sql_queue_time_);\
    int64_t i = 0;\
    for (i = 0 ;i < profile_data.count_;++i)\
    {\
      databuff_printf(ObProfileFillLog::get_log_buf(), 1024, pos, "rpc:channel_id=[%ld] rpc_start=[%ld] latency=[%ld] pcode=[%ld] ", profile_data.rpc_latency_arr_[i].channel_id_,  profile_data.rpc_latency_arr_[i].rpc_start_, profile_data.rpc_latency_arr_[i].rpc_end_ - profile_data.rpc_latency_arr_[i].rpc_start_, profile_data.rpc_latency_arr_[i].pcode_);\
    }\
    ObProfileFillLog::get_log_buf()[pos] = '\n';\
    ::write(ObProfileFillLog::log_fd_, ObProfileFillLog::get_log_buf(), pos + 1);\
  }\
}

#define PFILL_SET_SQL(ptr,len)\
  if (ObProfileFillLog::is_open())\
{\
  if ( GET_TRACE_SEQ()% ObProfileFillLog::profile_sample_interval_ == 0)\
  {\
    int64_t pos = 0;\
    databuff_printf(ObProfileFillLog::get_request_profile_data().sql_, 512, pos, "%.*s", len, ptr);\
    ObProfileFillLog::get_request_profile_data().sql_len_ = static_cast<int32_t>(pos);\
  }\
}
  

namespace oceanbase
{
  namespace common
  {
    class ObProfileFillLog
    {
      public:
        static void set_trace_id(ObRequestProfileData &profile_data, int64_t trace_id);
        static void set_sql_queue_size(ObRequestProfileData &profile_data, int size);
        static void set_wait_sql_queue_time(ObRequestProfileData &profile_data, int64_t wait_sql_queue_time);
        static void set_pcode(ObRequestProfileData &profile_data, int64_t pcode);
        static void clear_log(ObRequestProfileData &profile_data);
        static inline ObRequestProfileData& get_request_profile_data()
        {
          static __thread ObRequestProfileData profile_data;
          static __thread bool init = false;
          if (!init)
          {
            TBSYS_LOG(INFO, "init profile fill log, tid=%ld",
                syscall(__NR_gettid));
            init = true;
          }
          return profile_data;
        }
        static inline char* get_log_buf()
        {
          static __thread char log_buf_[1024];
          return log_buf_;
        }
        static void setLogDir(const char *log_dir);
        static void setFileName(const char *log_filename);
        static int init();
        static void open();
        static void close();
        static inline bool is_open()
        {
          return is_open_;
        }
        static int is_open_;
        static char *log_dir_;
        static char *log_filename_;
        static int log_fd_;
        static int profile_sample_interval_;
    };
  }
}
#endif
