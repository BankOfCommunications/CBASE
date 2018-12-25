#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <new>
#include <pthread.h>
#include "ob_profile_log.h"
#include "ob_define.h"

using namespace oceanbase;
using namespace oceanbase::common;

const char *const ObProfileLogger::errmsg_[2] = {"INFO","DEBUG"};
ObProfileLogger* ObProfileLogger::logger_ = NULL;
ObProfileLogger::LogLevel ObProfileLogger::log_levels_[2] = {INFO,DEBUG};

ObProfileLogger::ObProfileLogger()
  :log_dir_(strdup("./")), log_filename_(strdup(""))
  ,log_level_(INFO), log_fd_(2)
{
}
ObProfileLogger* ObProfileLogger::getInstance()
{
  if (NULL == logger_)
  {
    logger_ = new (std::nothrow) ObProfileLogger();
  }
  return logger_;
}

ObProfileLogger::~ObProfileLogger()
{
  close(log_fd_);
  free(log_dir_);
}


void ObProfileLogger::setLogLevel(const char *log_level)
{
  for(int i = 0;i < 2;++i)
  {
    if(!strcasecmp(log_level, errmsg_[i]))
    {
      log_level_ = log_levels_[i];
      break;
    }
  }
}
void ObProfileLogger::setLogDir(const char *log_dir)
{
  if(log_dir != NULL)
  {
    free(log_dir_);
    log_dir_ = strdup(log_dir);
  }
}
void ObProfileLogger::setFileName(const char *log_filename)
{
  if(log_filename != NULL)
  {
    free(log_filename_);
    log_filename_ = strdup(log_filename);
  }
}
int ObProfileLogger::init()
{
  char buf[128];
  strncpy(buf, log_dir_, 128);
  strncat(buf, log_filename_, strlen(log_filename_));
  int fd = open(buf, O_RDWR | O_CREAT | O_APPEND, 0644);
  if(fd != -1)
  {
    // stderr
    //dup2(fd,2);
    log_fd_ = fd;
    return OB_SUCCESS;
  }
  else
  {
    fprintf(stderr, "open log file error\n");
    return OB_ERROR;
  }
}

void ObProfileLogger::printlog(LogLevel level, const char *file, int line, const char *function, pthread_t tid, const char *fmt,...)
{
  if(level > log_level_)
    return;


  time_t t;
  time(&t);
  struct tm tm;
  localtime_r(&t,&tm);
  char log[512];
  char buf[256];
  va_list args;
  va_start(args,fmt);
  int written = vsnprintf(log, 512, fmt, args);
  written = written >= 511 ? 511 : written;
  log[written] = '\n';
  va_end(args);
  ssize_t size = static_cast<ssize_t>(snprintf(buf,256,"[%04d-%02d-%02d %02d:%02d:%02d] %-5s %s (%s:%d) [%ld] trace_id=[%lu] source_chid=[%u] chid=[%u]", tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,tm.tm_hour, tm.tm_min, tm.tm_sec, errmsg_[level], function, file, line, tid, TRACEID, SRC_CHID, CHID));
  struct iovec iov[2];
  iov[0].iov_base = buf;
  iov[0].iov_len = size;
  iov[1].iov_base = log;
  iov[1].iov_len = written + 1;
  writev(log_fd_, iov, 2);
}

void ObProfileLogger::printlog(LogLevel level, const char *file, int line, const char *function, pthread_t tid, 
    const int64_t consume, const char *fmt, ...)
{
  if(level > log_level_)
    return;


  time_t t;
  time(&t);
  struct tm tm;
  localtime_r(&t,&tm);
  char log[512];
  char buf[256];
  va_list args;
  va_start(args,fmt);
  int written = vsnprintf(log, 512, fmt, args);
  log[written] = '\n';
  va_end(args);
  ssize_t size = static_cast<ssize_t>(snprintf(buf,256,"[%04d-%02d-%02d %02d:%02d:%02d] %-5s %s (%s:%d) [%ld] trace_id=[%lu] source_chid=[%u] chid=[%u], consume=[%ld] ", tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,tm.tm_hour, tm.tm_min, tm.tm_sec, errmsg_[level], function, file, line, tid, TRACEID, SRC_CHID, CHID, consume));
  struct iovec iov[2];
  iov[0].iov_base = buf;
  iov[0].iov_len = size;
  iov[1].iov_base = log;
  iov[1].iov_len = written + 1;
  writev(log_fd_, iov, 2);
}

