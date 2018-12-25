////===================================================================
 //
 // ob_log_utils.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-06-07 by Yubai (yubai.lk@alipay.com) 
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

#ifndef  OCEANBASE_LIBOBLOG_UTILS_H_
#define  OCEANBASE_LIBOBLOG_UTILS_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "ob_log_meta_manager.h"
#include "ob_log_partitioner.h"
#include "ob_log_server_selector.h"
#include "liboblog.h"
#include "common/ob_string.h"           // ObString

//#define OB_MYSQL_USER     "admin"
//#define OB_MYSQL_PASSWD   "admin"
#define OB_MYSQL_DATABASE ""

#define TIMED_FUNC(func, timeout_us, args...) \
({\
    ObLogClock clock(timeout_us, #func); \
    func(args); \
})

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogClock
    {
      public:
        ObLogClock(const int64_t timeout_us, const char *func_name) : timeout_us_(timeout_us),
                                                                      func_name_(func_name),
                                                                      timeu_(tbsys::CTimeUtil::getTime())
        {
        };
        ~ObLogClock()
        {
          timeu_ = tbsys::CTimeUtil::getTime() - timeu_;
          if (timeout_us_ < timeu_)
          {
            TBSYS_LOG(WARN, "call [%s] long, timeu=%ld", func_name_, timeu_);
          }
        };
      private:
        const int64_t timeout_us_;
        const char *func_name_;
        int64_t timeu_;
    };

    class ObLogSeqMap
    {
      struct Item
      {
        uint64_t index;
        uint64_t data;
      };
      public:
        ObLogSeqMap() : items_(NULL),
                        limit_(0),
                        cursor_(0)
        {
        };
        ~ObLogSeqMap()
        {
          destroy();
        };
      public:
        int init(const uint64_t limit)
        {
          int ret = common::OB_SUCCESS;
          if (NULL != items_)
          {
            ret = common::OB_INIT_TWICE;
          }
          else if (0 >= limit)
          {
            ret = common::OB_INVALID_ARGUMENT;
          }
          else if (NULL == (items_ = new(std::nothrow) Item[limit]))
          {
            ret = common::OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            TBSYS_LOG(INFO, "init seq_map succ, limit=%lu", limit);
            memset(items_, 0, sizeof(Item) * limit);
            limit_ = limit;
            cursor_ = 1;
          }
          return ret;
        };
        void destroy()
        {
          cursor_ = 0;
          limit_ = 0;
          if (NULL != items_)
          {
            delete[] items_;
            items_ = NULL;
          }
        };
      public:
        int set(const uint64_t index, const uint64_t data)
        {
          int ret = common::OB_SUCCESS;
          if (NULL == items_)
          {
            ret = common::OB_NOT_INIT;
          }
          else if (cursor_ + limit_ <= index)
          {
            ret = common::OB_ENTRY_NOT_EXIST;
          }
          else
          {
            items_[index % limit_].index = index;
            items_[index % limit_].data = data;
          }
          return ret;
        };
        uint64_t next()
        {
          uint64_t ret = 0;
          while ((cursor_ + 1) == items_[(cursor_ + 1) % limit_].index)
          {
            cursor_ += 1;
          }
          ret = items_[cursor_ % limit_].data;
          return ret;
        };
      private:
        Item *items_;
        uint64_t limit_;
        uint64_t cursor_;
    };

    const char *print_dml_type(common::ObDmlType dml_type);

    const char *usec2str(int64_t usec, bool print_int_timevalue = true);

    const char *print_pos_type(ObLogPositionType pos_type);

    /// Print LOG position information
    /// @note If checkpoint and timestamp and heartbeat_timestamp are all invalid , print nothing
    ///       Print heartbeat information only when it is valid
    void print_log_position_info(int64_t checkpoint, int64_t timestamp, const char *mod, int64_t heartbeat_timestamp = -1);
  }
}

#endif //OCEANBASE_LIBOBLOG_UTILS_H_

