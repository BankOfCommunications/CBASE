#ifndef _OB_PROFILE_LOG_H_
#define _OB_PROFILE_LOG_H_

#define PROFILE_LOGGER oceanbase::common::ObProfileLogger::getInstance()
#define PROFILE_LOG(level,fmt,args...) if (ObProfileLogger::level <= PROFILE_LOGGER->getLogLevel()) PROFILE_LOGGER->printlog(ObProfileLogger::level, __FILE__, __LINE__, __FUNCTION__, pthread_self(), fmt,##args)
#define TRACEID (GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1))->uval_
#define SRC_CHID *(GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_SOURCE_CHID_1))
#define CHID *(GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_CHID_1))

#define INIT_PROFILE_LOG_TIMER() \
  int64_t __PREV_TIME__ = tbsys::CTimeUtil::getTime();

#define PROFILE_LOG_TIME(level, fmt, args...) \
  do{ \
    int64_t now = tbsys::CTimeUtil::getTime(); \
    if (ObProfileLogger::level <= PROFILE_LOGGER->getLogLevel()) { \
      PROFILE_LOGGER->printlog(ObProfileLogger::level, __FILE__, __LINE__, __FUNCTION__, pthread_self(), now - __PREV_TIME__, fmt, ##args); } \
    __PREV_TIME__ = now; \
  } while(0);



#include "ob_trace_id.h"
#include "ob_tsi_factory.h"
namespace oceanbase
{
  namespace common
  {
    // 打印ip, port, seq, chid,default
    class ObProfileLogger
    {
      public:
        enum LogLevel
        {
          INFO = 0,
          DEBUG
        };
        ~ObProfileLogger();
        void printlog(LogLevel level, const char *file, int line, const char *function, pthread_t tid, const char *fmt, ...);
        void printlog(LogLevel level, const char *file, int line, const char *function, pthread_t tid, 
            const int64_t time, const char *fmt, ...);
      public:
        static ObProfileLogger* getInstance();
        LogLevel getLogLevel() { return log_level_; }
        void setLogLevel(const char *log_level);
        void setLogDir(const char *log_dir);
        void setFileName(const char *log_filename);
        int init();
      private:
        static const char *const errmsg_[2];
        static LogLevel log_levels_[2];
      private:
        static ObProfileLogger *logger_;
        char *log_dir_;
        char *log_filename_;
        LogLevel log_level_;
        int log_fd_;
      private:
        ObProfileLogger();
        ObProfileLogger(const ObProfileLogger&);
        ObProfileLogger& operator=(const ObProfileLogger&);

    };
  }// common
}// oceanbase


#endif
