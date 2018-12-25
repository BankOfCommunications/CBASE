#include "han.h"

int64_t microseconds()
{
  struct timeval tv; 
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000L + tv.tv_usec;
}

ErrMsgException::ErrMsgException(const char * pszFile, int iLine, const char * pszFunction,
                pthread_t iTID, int iErrCode, const char* pszMsg, ...)
  : pszFile_(pszFile), iLine_(iLine), pszFunction_(pszFunction),
    iTID_(iTID), iErrCode_(iErrCode)
{
  gettimeofday(&tTime_, NULL);
  va_list arg;
  va_start(arg, pszMsg);
  vsnprintf(pszMsg_, BUFSIZ, pszMsg, arg);
  va_end(arg);
}

ErrMsgException::ErrMsgException(const ErrMsgException & e)
  : tTime_(e.tTime_), pszFile_(e.pszFile_), iLine_(e.iLine_),
    pszFunction_(e.pszFunction_), iTID_(e.iTID_), iErrCode_(e.iErrCode_)
{
  strcpy(pszMsg_, e.pszMsg_);
}

ErrMsgException::~ErrMsgException()
{
}

const char * ErrMsgException::what() const
{
  return pszMsg_;
}

void ErrMsgException::logException() const
{
  struct tm tm;
  localtime_r((const time_t*)&tTime_.tv_sec, &tm);
  TBSYS_LOG(ERROR, "Exception: %04d%02d%02d%02d%02d%02d%06ld "
      "%ld %s:%d %s ErrCode: %d ErrMsg: %s",
      tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
      tm.tm_hour, tm.tm_min, tm.tm_sec, tTime_.tv_usec,
      iTID_, pszFile_, iLine_, pszFunction_,
      iErrCode_, pszMsg_);
}

int ErrMsgException::getErrorCode() const
{
  return iErrCode_;
}

__thread uint64_t Random::seed_;

uint64_t Random::rand()
{
  return seed_ = (seed_ * 214013L + 2531011L) >> 16;
}

uint64_t Random::srand(uint64_t seed)
{
  seed_ = seed;
  return seed_;
}

