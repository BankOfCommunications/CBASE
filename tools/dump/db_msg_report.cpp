/*
 * =====================================================================================
 *
 *       Filename:  db_msg_report.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/12/2011 02:42:07 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include <iostream>
#include <string>
#include <stdarg.h>
#include <stdio.h>
#include "db_msg_report.h"

using namespace std;


static const char *msg_type[MSG_MAX] = {
  "INFO",
  "WARN",
  "ERROR"
};

//TODO: report error to server by thrift
void report_msg(MsgType type, const char *msg, ...)
{
  size_t size;
  time_t t;
  time(&t);
  struct tm tm;
  ::localtime_r((const time_t*)&t, &tm);

  char msg_buf[4096];
  char data1[4096];

  va_list args;
  va_start(args, msg);
  vsnprintf(data1, 4096, msg, args);
  va_end(args);

  size = snprintf(msg_buf, 4096, "[%04d-%02d-%02d %02d:%02d:%02d] %s : %s\n",
                  tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec,
                  msg_type[type], data1);

  while (msg_buf[size-2] == '\n') size --;
  msg_buf[size] = '\0';

  cout << msg_buf;
}
