////===================================================================
 //
 // ob_trace_log.cpp / hash / common / Oceanbase
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

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "ob_trace_log.h"
#include "ob_define.h"
#include "tblog.h"
#include "tbtimeutil.h"

namespace oceanbase
{
  namespace common
  {
    const char *const TraceLog::LOG_LEVEL_ENV_KEY = "_OB_TRACE_LOG_LEVEL_";
    const char *const TraceLog::level_strs_[] = {"ERROR","USER_ERR", "WARN", "INFO", "TRACE", "DEBUG"};
    volatile int TraceLog::log_level_ = TBSYS_LOG_LEVEL_TRACE;
    bool TraceLog::got_env_ = false;

    TraceLog::LogBuffer &TraceLog::get_logbuffer()
    {
      static __thread LogBuffer log_buffer;
      static __thread bool inited = false;
      if (!inited)
      {
        TBSYS_LOG(INFO, "init trace log buffer tid=%ld buffer=%p", GETTID(), &log_buffer);
        memset(&log_buffer, 0, sizeof(log_buffer));
        inited = true;
      }
      return log_buffer;
    }

    int TraceLog::dec_log_level()
    {
      int prev_log_level = log_level_;
      int modify_log_level = log_level_ - 1;
      if (TBSYS_LOG_LEVEL_DEBUG >= modify_log_level
          && TBSYS_LOG_LEVEL_ERROR <= modify_log_level)
      {
        log_level_ = modify_log_level;
      }
      TBSYS_LOG(INFO, "dec_log_level prev_log_level=%d cur_log_level=%d", prev_log_level, log_level_);
      return log_level_;
    }

    int TraceLog::inc_log_level()
    {
      int prev_log_level = log_level_;
      int modify_log_level = log_level_ + 1;
      if (TBSYS_LOG_LEVEL_DEBUG >= modify_log_level
          && TBSYS_LOG_LEVEL_ERROR <= modify_log_level)
      {
        log_level_ = modify_log_level;
      }
      TBSYS_LOG(INFO, "inc_log_level prev_log_level=%d cur_log_level=%d", prev_log_level, log_level_);
      return log_level_;
    }

    int TraceLog::set_log_level(const char *log_level_str)
    {
      if (NULL != log_level_str)
      {
        int level_num = sizeof(level_strs_) / sizeof(const char*);
        for (int i = 0; i < level_num; ++i)
        {
          if (0 == strcasecmp(level_strs_[i], log_level_str))
          {
            log_level_ = i;
            break;
          }
        }
        got_env_ = true;
      }
      return log_level_;
    }

    int TraceLog::get_log_level()
    {
      if (!got_env_)
      {
        const char *log_level_str = getenv(LOG_LEVEL_ENV_KEY);
        set_log_level(log_level_str);
      }
      return log_level_;
    }

    void TraceLog::fill_log(LogBuffer &log_buffer, const char *fmt, ...)
    {
      if (get_log_level() > TBSYS_LOGGER._level && !log_buffer.force_log_)
      {
        if (0 == log_buffer.prev_timestamp)
        {
          log_buffer.start_timestamp = tbsys::CTimeUtil::getTime();
          log_buffer.prev_timestamp = log_buffer.start_timestamp;
        }
      }
      else
      {
        va_list args;
        va_start(args, fmt);
        int64_t valid_buffer_size = LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos;
        if (0 < valid_buffer_size)
        {
          int64_t ret = 0;
          ret = vsnprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, fmt, args);
          log_buffer.cur_pos += ((0 < ret && valid_buffer_size > ret) ? ret : 0);
          if (-1 < ret
              && valid_buffer_size > ret)
          {
            valid_buffer_size -= ret;
            int64_t cur_time = tbsys::CTimeUtil::getTime();
            if (0 < valid_buffer_size)
            {
              if (0 != log_buffer.prev_timestamp)
              {
                ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, " timeu=%lu | ",
                              cur_time - log_buffer.prev_timestamp);
              }
              else
              {
                ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, " | ");
                log_buffer.start_timestamp = cur_time;
              }
              log_buffer.cur_pos += ((0 < ret && valid_buffer_size > ret) ? ret : 0);
            }
            log_buffer.prev_timestamp = cur_time;
          }
          //del
          /*
          //add dragon [trace_log] 2016-11-14 b
          else
          {
            //要将写入的一半的数据从buffer中移除
            memset(log_buffer.buffer + log_buffer.cur_pos, 0, LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos);
            //说明buffer空间不足，或者写入失败了
            va_start(args, fmt);
            next(log_buffer, fmt, args);
          }
          //add dragon e
          */
          //del
          log_buffer.buffer[log_buffer.cur_pos] = '\0';
        }
        va_end(args);
      }
    }

    void TraceLog::clear_log(LogBuffer &log_buffer)
    {
      log_buffer.cur_pos = 0;
      log_buffer.prev_timestamp = 0;
      log_buffer.start_timestamp = 0;
      log_buffer.force_log_ = false;
      log_buffer.force_print_ = false;
    }

    void TraceLog::next(LogBuffer &log_buffer, const char* fmt, va_list vl)
    {
      //重置
      TBSYS_LOG(DEBUG, "trace buff not enough, will reset trace buffer total[%ld] used[%ld]",
                LogBuffer::LOG_BUFFER_SIZE, log_buffer.cur_pos);
      JUST_PRINT_TRACE_LOG();
      RESET_LOG_BUFFER();
      int64_t buff_sz = LogBuffer::LOG_BUFFER_SIZE;
      int64_t ret = 0;
      //重置完成后再将此次写入此次trace log
      ret = vsnprintf(log_buffer.buffer + log_buffer.cur_pos, buff_sz, fmt, vl);
      log_buffer.cur_pos += ((0 < ret && buff_sz > ret) ? ret : 0);
      if (-1 < ret
          && buff_sz > ret)
      {
        buff_sz -= ret;
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        if (0 < buff_sz)
        {
          if (0 != log_buffer.prev_timestamp)
          {
            ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, buff_sz, " timeu=%lu | ",
                          cur_time - log_buffer.prev_timestamp);
          }
          else
          {
            ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, buff_sz, " | ");
            log_buffer.start_timestamp = cur_time;
          }
          log_buffer.cur_pos += ((0 < ret && buff_sz > ret) ? ret : 0);
        }
        log_buffer.prev_timestamp = cur_time;
      }
      log_buffer.buffer[log_buffer.cur_pos] = '\0';
    }

    void TraceLog::reset_log_buffer()
    {
      LogBuffer &log_buffer = get_logbuffer();
      log_buffer.cur_pos = 0;
      memset(log_buffer.buffer, 0, LogBuffer::LOG_BUFFER_SIZE);
    }
  }
}
