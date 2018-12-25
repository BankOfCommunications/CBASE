////===================================================================
 //
 // ob_log_commonc.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-06-07 by Fusheng Han (yanran.hfs@alipay.com) 
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

#ifndef  OCEANBASE_LIBOBLOG_COMMON_H_
#define  OCEANBASE_LIBOBLOG_COMMON_H_

#include "liboblog.h"
#include "common/hash/ob_hashmap.h"   // ObHashMap
#include "common/ob_array.h"          // ObArray
#include "common/ob_resource_pool.h"  // ObResourcePool

#define LOG_AND_ERR(level, format_str, ...) \
  { \
    TBSYS_LOG(level, format_str, ##__VA_ARGS__); \
    bool is_err = false; \
    const char * prefix_str = NULL; \
    if (TBSYS_LOG_LEVEL_##level == TBSYS_LOG_LEVEL_ERROR) { \
      prefix_str = "\033[1;31mERROR\033[0m : "; is_err = true; } \
    else if (TBSYS_LOG_LEVEL_##level == TBSYS_LOG_LEVEL_WARN) { \
      prefix_str = "\033[1;31mWARN\033[0m  : "; is_err = true; } \
    else prefix_str = "INFO  : "; \
    if (is_err) fprintf(stderr, "%s" format_str "\n", prefix_str, ##__VA_ARGS__); \
    else fprintf(stdout, "%s" format_str "\n", prefix_str, ##__VA_ARGS__); \
  }

// DRC data format protocol
#define COLUMN_VALUE_CHANGED_MARK    ""
#define COLUMN_VALUE_UNCHANGED_MARK  NULL

namespace oceanbase
{
  namespace liboblog
  {
    typedef common::ObArray<char *> TableNameArray;
    typedef common::ObArray<uint64_t> TableIDArray;

    class ObLogString : public std::string
    {
      public:
        void reset() {std::string::clear();};
    };

    static const int64_t MAX_BINLOG_RECORD_NUM = 10240;
    typedef common::ObResourcePool<ObLogString, 0, MAX_BINLOG_RECORD_NUM * 64> ObLogStringPool;

    static const int64_t OB_LOG_INVALID_TIMESTAMP    = -1;
    static const uint64_t OB_LOG_INVALID_CHECKPOINT  = 0;
    static const uint64_t OB_LOG_INVALID_PARTITION  = UINT64_MAX;

    static const int64_t PERF_PRINT_INTERVAL         = 10L * 1000000L;  // interval time to print perf information
    static const int64_t HEARTBEAT_INTERVAL          = 5L * 1000000L;

    /// FIXME: The default max table number does not limit actual selected table number.
    /// Actual selected table number can be greater than default max table number.
    static const int32_t DEFAULT_MAX_TABLE_NUM        = 1024;

    static const uint64_t MAX_OBSERVER_NUM            = 128;     // max number of IObLog instances
    static const uint64_t MAX_DB_PARTITION_NUM        = 1024LU;  // max number of DataBase Partition number
    static const uint64_t MAX_TB_PARTITION_NUM        = 1024LU;  // max number of Table Partition number
    static const int32_t MAX_LOG_FORMATER_THREAD_NUM  = 100;

    static const int32_t DEFAULT_QUERY_BACK_TIMEWAIT_US = 5 * 1000 * 1000;   // default query back timewait in micro seconds

    static const int32_t DEFAULT_FETCH_LOG_TIMEOUT_SEC = 120;   // default fetch log timeout in seconds

    static const int32_t MAX_QUERY_BACK_THREAD_NUM = 256;
    static const int32_t DEFAULT_QUERY_BACK_THREAD_NUM = 1;     // default query back thread number

    static const int64_t PRINT_LOG_POSITIOIN_INTERVAL = 10L * 1000000L;
    static const int64_t PRINT_QUERY_BACK_STAT_INFO_INTERVAL = 10L * 1000000L;
#define OBLOG_COMMON (ObLogCommon::get_instance())

#define HANDLE_ERROR(level, err_code, format_str, ...) \
    ObLogCommon::get_instance().handle_error(level, err_code, format_str, ##__VA_ARGS__)

    class ObLogCommon
    {
      protected:
        ObLogCommon() : err_cb_(NULL), alive_(true), handle_error_flag_(0)
        {}

      public:
        static ObLogCommon &get_instance();

      public:
        bool is_alive() { return alive_; }

        void reset() { alive_ = true; err_cb_ = NULL; handle_error_flag_ = 0; }

        void set_err_callback(ERROR_CALLBACK err_cb) { err_cb_ = err_cb; }

        void handle_error(ObLogError::ErrLevel level, int err_no, const char *fmt, ...);

      private:
        ERROR_CALLBACK err_cb_;
        volatile bool alive_;
        volatile int8_t handle_error_flag_;
    };

    struct string_equal_to
    {
      bool operator()(const char *a, const char *b) const
      {
        return 0 == strcmp(a, b);
      }
    };

    template <class value_type>
    class ObLogStringMap : public common::hash::ObHashMap<const char *, value_type,
                                                          common::hash::ReadWriteDefendMode,
                                                          common::hash::hash_func<const char *>,
                                                          string_equal_to>
    {
    };

    bool ob_log_alive(const bool stop_flag = false);
  }
}

#endif //OCEANBASE_LIBOBLOG_COMMON_H_
