#ifndef _HAN_H_
#define _HAN_H_

#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <tbsys/tbsys.h>

const int ERR_FATAL = 1;
const int ERR_ASSERT = 2;
const int ERR_ARG = 3;
const int ERR_NO_DATA = -1;

int64_t microseconds();

class ErrMsgException
{
  public:
    ErrMsgException(const char * pszFile, int iLine, const char * pszFunction,
                    pthread_t iTID, int iErrCode, const char* pszMsg, ...);
    ErrMsgException(const ErrMsgException & e);
    ~ErrMsgException();
    const char * what() const;
    void logException() const;
    int getErrorCode() const;

  private:
    struct timeval       tTime_;
    const char *         pszFile_;
    int                  iLine_;
    const char *         pszFunction_;
    pthread_t            iTID_;
    int                  iErrCode_;
    char                 pszMsg_[BUFSIZ];
};
#define THROW_EXCEPTION(iErrCode, pszMsg, ...) \
throw ErrMsgException(__FILE__, __LINE__, __FUNCTION__, pthread_self(), \
                      iErrCode, pszMsg, ##__VA_ARGS__)
#define THROW_EXCEPTION_AND_LOG(iErrCode, pszMsg, ...) \
TBSYS_LOG(ERROR, "Throw Exception: ErrCode: %d ErrMsg: " pszMsg, \
    iErrCode, ##__VA_ARGS__); \
throw ErrMsgException(__FILE__, __LINE__, __FUNCTION__, pthread_self(), \
                      iErrCode, pszMsg, ##__VA_ARGS__)
#define RETHROW(ex) \
TBSYS_LOG(ERROR, "Rethrow Exception: ErrCode: %d ErrMsg: %s", \
    ex.getErrorCode(), ex.what()); \
throw

class Random
{
  public:
    static uint64_t rand();
    static uint64_t srand(uint64_t seed);
  private:
    static __thread uint64_t seed_;
};


#endif // _HAN_H_
