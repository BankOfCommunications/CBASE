////===================================================================
 //
 // ob_trace_log.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2010-09-01 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_COMMON_FILL_LOG_H_
#define  OCEANBASE_COMMON_FILL_LOG_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include <sys/syscall.h>
#include "ob_define.h"
#include "tblog.h"
#include "tbtimeutil.h"

#define GETTID() syscall(__NR_gettid)
#define INC_TRACE_LOG_LEVEL() oceanbase::common::TraceLog::inc_log_level()
#define DEC_TRACE_LOG_LEVEL() oceanbase::common::TraceLog::dec_log_level()
#define SET_TRACE_LOG_LEVEL(level) oceanbase::common::TraceLog::set_log_level(level)
#define SET_FORCE_TRACE_LOG(force) oceanbase::common::TraceLog::get_logbuffer().set_force_log(force)

#define CLEAR_TRACE_LOG() oceanbase::common::TraceLog::clear_log(oceanbase::common::TraceLog::get_logbuffer())
#define FILL_TRACE_LOG(_fmt_, args...) \
    if (oceanbase::common::TraceLog::get_log_level() <= TBSYS_LOGGER._level || oceanbase::common::TraceLog::get_logbuffer().force_log_) \
    { \
      oceanbase::common::TraceLog::fill_log(oceanbase::common::TraceLog::get_logbuffer(), "f=[%s] " _fmt_, __FUNCTION__, ##args); \
    }
#define FORCE_PRINT_TRACE_LOG()                                         \
  {                                                                     \
    TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(TBSYS_LOGGER._level), "[%ld][%ld][%ld][%ld] %stotal_timeu=%ld", \
                            pthread_self(), GETTID(), tbsys::CLogger::get_cur_tv().tv_sec, tbsys::CLogger::get_cur_tv().tv_usec, \
                            oceanbase::common::TraceLog::get_logbuffer().buffer, \
                            tbsys::CTimeUtil::getTime() - oceanbase::common::TraceLog::get_logbuffer().start_timestamp); \
    CLEAR_TRACE_LOG();                                                  \
  }
#define PRINT_TRACE_LOG() \
  { \
    if (oceanbase::common::TraceLog::get_log_level() <= TBSYS_LOGGER._level) \
    { \
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(oceanbase::common::TraceLog::get_log_level()), "[%ld][%ld][%ld][%ld] %stotal_timeu=%ld", \
                            pthread_self(), GETTID(), tbsys::CLogger::get_cur_tv().tv_sec, tbsys::CLogger::get_cur_tv().tv_usec, \
                            oceanbase::common::TraceLog::get_logbuffer().buffer, \
                            tbsys::CTimeUtil::getTime() - oceanbase::common::TraceLog::get_logbuffer().start_timestamp); \
    }\
    CLEAR_TRACE_LOG(); \
  }
#define JUST_PRINT_TRACE_LOG() \
  { \
    if (oceanbase::common::TraceLog::get_log_level() <= TBSYS_LOGGER._level) \
    { \
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(oceanbase::common::TraceLog::get_log_level()), "[%ld][%ld][%ld][%ld] %stotal_timeu=%ld", \
                            pthread_self(), GETTID(), tbsys::CLogger::get_cur_tv().tv_sec, tbsys::CLogger::get_cur_tv().tv_usec, \
                            oceanbase::common::TraceLog::get_logbuffer().buffer, \
                            tbsys::CTimeUtil::getTime() - oceanbase::common::TraceLog::get_logbuffer().start_timestamp); \
    } \
  }
#define RESET_LOG_BUFFER() oceanbase::common::TraceLog::reset_log_buffer()
#define GET_TRACE_TIMEU() (tbsys::CTimeUtil::getTime() - oceanbase::common::TraceLog::get_logbuffer().start_timestamp)
#define TBSYS_TRACE_LOG(_fmt_, args...) \
    if (oceanbase::common::TraceLog::get_log_level() <= TBSYS_LOGGER._level) \
    { \
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(oceanbase::common::TraceLog::get_log_level()), "[%ld][%ld][%ld][%ld] " _fmt_, \
                             pthread_self(), GETTID(), tbsys::CLogger::get_cur_tv().tv_sec, tbsys::CLogger::get_cur_tv().tv_usec, ##args); \
    }

#define CLEAR_TRACE_BUF(log_buffer) oceanbase::common::TraceLog::clear_log(log_buffer)
#define FILL_TRACE_BUF(log_buffer, _fmt_, args...) \
    if (oceanbase::common::TraceLog::get_log_level() <= TBSYS_LOGGER._level || log_buffer.force_log_) \
    { \
      oceanbase::common::TraceLog::fill_log(log_buffer, "f=[%s] " _fmt_, __FUNCTION__, ##args); \
    }
#define FILL_OBJ_TO_TRACE_BUF(log_buffer, obj) \
    if (oceanbase::common::TraceLog::get_log_level() <= TBSYS_LOGGER._level || log_buffer.force_log_) \
    { \
      oceanbase::common::TraceLog::fill_log(log_buffer, "f=[%s] ", __FUNCTION__); \
      oceanbase::common::TraceLog::fill_obj(log_buffer, obj); \
    }
#define PRINT_TRACE_BUF(log_buffer) \
  { \
    if (oceanbase::common::TraceLog::get_log_level() <= TBSYS_LOGGER._level || (log_buffer.force_log_ && log_buffer.force_print_)) \
    { \
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(oceanbase::common::TraceLog::get_log_level()), "[%ld][%ld][%ld][%ld] %stotal_timeu=%ld", \
                            pthread_self(), GETTID(), tbsys::CLogger::get_cur_tv().tv_sec, tbsys::CLogger::get_cur_tv().tv_usec, \
                            log_buffer.buffer, \
                            tbsys::CTimeUtil::getTime() - log_buffer.start_timestamp); \
    } \
    CLEAR_TRACE_BUF(log_buffer); \
  }
#define GET_BUF_TRACE_TIMEU(log_buffer) (tbsys::CTimeUtil::getTime() - log_buffer.start_timestamp)

namespace oceanbase
{
  namespace common
  {
    class TraceLog
    {
      static const char *const LOG_LEVEL_ENV_KEY;
      public:
        struct LogBuffer
        {
          static const int64_t LOG_BUFFER_SIZE = 4 * 1024;
          int64_t cur_pos;
          int64_t prev_timestamp;
          int64_t start_timestamp;
          bool force_log_;
          bool force_print_;
          char buffer[LOG_BUFFER_SIZE];

          void set_force_log(bool force) {force_log_ = force;};
        };
      public:
        static void clear_log(LogBuffer &log_buffer);
        static void fill_log(LogBuffer &log_buffer, const char *fmt, ...);
        template<typename type>
        static void fill_obj(LogBuffer &log_buffer, type& obj);
        static void print_log();
        static LogBuffer &get_logbuffer();
        static int set_log_level(const char *log_level_str);
        static int get_log_level();
        static int dec_log_level();
        static int inc_log_level();
        //add dragon [trace_log] 2016-11-4 b
        /**
         * @brief next
         * 用于处理缓冲区不足的情况
         *  print-->clear-->write trace log at begin
         */
        static void next(LogBuffer &log_buffer, const char* fmt, va_list vl);
        /**
         * @brief reset_log_buffer
         * 仅仅reset缓冲区,时间戳并不重置
         */
        static void reset_log_buffer();
        //add dragon [trace_log] e
      private:
        static const char *const level_strs_[];
        static volatile int log_level_;
        static bool got_env_;
    };
    template<typename type>
    void TraceLog::fill_obj(LogBuffer &log_buffer, type& obj)
    {
      if (get_log_level() > TBSYS_LOGGER._level)
      {
        if (0 == log_buffer.prev_timestamp)
        {
          log_buffer.start_timestamp = tbsys::CTimeUtil::getTime();
          log_buffer.prev_timestamp = log_buffer.start_timestamp;
        }
      }
      else
      {
        log_buffer.cur_pos += obj.to_string(log_buffer.buffer + log_buffer.cur_pos,
                                            sizeof(log_buffer.buffer) - log_buffer.cur_pos);
      }
    }
  }
}

#endif //OCEANBASE_COMMON_FILL_LOG_H_
