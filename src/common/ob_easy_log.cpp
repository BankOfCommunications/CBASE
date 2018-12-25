/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_easy_log.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_easy_log.h"
#include "common/ob_define.h"
#include <easy_log.h>
#include <easy_define.h>
#include <easy_string.h>
#include <easy_baseth_pool.h>
#include <easy_time.h>
#include <easy_io.h>
#include <pthread.h>

extern const char* const OB_EASY_LOG_LEVEL_STR[];

void ob_easy_log_format(int level, const char *file, int line, const char *function, const char *fmt, ...)
{
  static __thread ev_tstamp   oldtime = 0.0;
  static __thread char        time_str[32];
  ev_tstamp                   now;
  int                         len, vlen;
  char                        buffer[4096];
  UNUSED(function);

  // 从loop中取
  if (easy_baseth_self && easy_baseth_self->loop) {
    now = ez_now(easy_baseth_self->loop);
  } else {
    now = static_cast<ev_tstamp>(time(NULL));
  }

  if (oldtime != now) {
    time_t              t;
    struct tm           tm;
    oldtime = now;
    t = (time_t) now;
    easy_localtime((const time_t *)&t, &tm);
    snprintf(time_str, 32, "[%04d-%02d-%02d %02d:%02d:%02d.%06d]",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, (int)((now - static_cast<ev_tstamp>(t)) * 1000 * 1000));
  }

  // print
  len = lnprintf(buffer, 128, "%s %s %s:%d [%ld] ", time_str, OB_EASY_LOG_LEVEL_STR[level], file, line, pthread_self());
  va_list args;
  va_start(args, fmt);
  vlen = vsnprintf(buffer + len, 4090 - len,  fmt, args);
  len += easy_min(vlen, 4090 - len - 1);
  va_end(args);

  // 去掉最后'\n'
  while(buffer[len - 1] == '\n') len --;

  buffer[len++] = '\n';
  buffer[len] = '\0';
  // @todo use easy_log_print instead
  easy_log_print_default(buffer);
}

// @see easy_log_level_t in easy_log.h
const char* const OB_EASY_LOG_LEVEL_STR[] = {"UNKNOWN","OFF"/*offset 1*/,"FATAL","ERROR","WARN","INFO","DEBUG","TRACE","ALL"};
