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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include "common/ob_profile_fill_log.h"
#include "tblog.h"

using namespace oceanbase;
using namespace oceanbase::common;

int ObProfileFillLog::is_open_ = false;
char* ObProfileFillLog::log_dir_ = strdup("./");
char* ObProfileFillLog::log_filename_ = strdup("");
int ObProfileFillLog::log_fd_ = -1;
int ObProfileFillLog::profile_sample_interval_ = 1;
//char ObProfileFillLog::log_buf_[1024];
void ObProfileFillLog::setLogDir(const char *log_dir)
{
  if (log_dir != NULL)
  {
    free(log_dir_);
    log_dir_ = strdup(log_dir);
  }
}
void ObProfileFillLog::setFileName(const char *log_filename)
{
  if (log_filename != NULL)
  {
    free(log_filename_);
    log_filename_ = strdup(log_filename);
  }
}

int ObProfileFillLog::init()
{
  int ret = OB_SUCCESS;
  char buf[128];
  strncpy(buf, log_dir_, 128);
  strncat(buf, log_filename_, strlen(log_filename_));
  char *interval = getenv(PROFILE_SAMPLE_INTERVAL);
  if (NULL != interval)
  {
    profile_sample_interval_ = atoi(interval);
    if (profile_sample_interval_ < 1)
    {
      profile_sample_interval_ = 1;
    }
  }
  int fd = ::open(buf, O_RDWR | O_CREAT | O_APPEND, 0644);
  if (fd != -1)
  {
    log_fd_ = fd;
  }
  else
  {
    fprintf(stderr, "open profile fill log error\n");
    ret = OB_ERROR;
  }
  return ret;
}
void ObProfileFillLog::open()
{
  is_open_ = true;
}
void ObProfileFillLog::close()
{
  is_open_ = false;
}
void ObProfileFillLog::set_trace_id(ObRequestProfileData &profile_data, int64_t trace_id)
{
  profile_data.trace_id_ = trace_id;
}
void ObProfileFillLog::set_sql_queue_size(ObRequestProfileData &profile_data, int size)
{
  profile_data.sql_queue_size_ = size;
}
void ObProfileFillLog::set_pcode(ObRequestProfileData &profile_data, int64_t pcode)
{
  profile_data.pcode_ = pcode;
}
void ObProfileFillLog::set_wait_sql_queue_time(ObRequestProfileData &profile_data, int64_t wait_sql_queue_time)
{
  profile_data.wait_sql_queue_time_ = wait_sql_queue_time;
}

void ObProfileFillLog::clear_log(ObRequestProfileData &profile_data)
{
  profile_data.trace_id_ = 0;
  profile_data.count_ = 0;
  profile_data.wait_sql_queue_time_ = 0;
  PFILL_INIT_PROFILE_ITEM(profile_data,sql_to_logicalplan);
  PFILL_INIT_PROFILE_ITEM(profile_data,logicalplan_to_physicalplan);
  PFILL_INIT_PROFILE_ITEM(profile_data,handle_sql_time);
}
