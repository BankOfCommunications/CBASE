////===================================================================
 //
 // ob_log_utils.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2014 Alipay.com, Inc.
 //
 // Created on 2014-08-12 by XiuMing (wanhong.wwh@alibaba-inc.com)
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

#include "ob_log_utils.h"

namespace oceanbase
{
  using namespace oceanbase::common;

  namespace liboblog
  {
    const char *print_dml_type(ObDmlType dml_type)
    {
      const char *cstr = NULL;

      switch (dml_type)
      {
        case OB_DML_UNKNOW:
          cstr = "UNKNOW";
          break;

        case OB_DML_REPLACE:
          cstr = "REPLACE";
          break;

        case OB_DML_INSERT:
          cstr = "INSERT";
          break;

        case OB_DML_UPDATE:
          cstr = "UPDATE";
          break;

        case OB_DML_DELETE:
          cstr = "DELETE";
          break;

        case OB_DML_NUM:
          cstr = "NUM";
          break;

        default:
          cstr = "UNKNOWN DML TYPE";
          break;
      }

      return cstr;
    }

    class MicroSecond
    {
      public:
        int64_t value_;
        bool print_int_timevalue_;

      public:
        MicroSecond(int64_t usec) : value_(usec), print_int_timevalue_(true) {}
        ~MicroSecond() {}

        void set_print_int_timevalue(bool print_int_timevalue) { print_int_timevalue_ = print_int_timevalue; }

        int64_t to_string(char* buf, const int64_t len) const
        {
          int64_t pos = 0;

          // NOTE: 直接打印微秒值，不格式化打印
          databuff_printf(buf, len, pos, "%ld", value_);

#if 0
          const char *str_fmt = "%Y-%m-%d %T.%f";

          ObString fmt(0, (ObString::obstr_size_t)strlen(str_fmt), str_fmt);

          if (print_int_timevalue_)
          {
            databuff_printf(buf, len, pos, "%ld@<", value_);
          }

          if (value_ > 0)
          {
            (void)ObTimeUtility::usec_format_to_str(value_, fmt, buf, len, pos);
          }
          else
          {
            databuff_printf(buf, len, pos, "%ld", value_);
          }

          if (print_int_timevalue_)
          {
            databuff_printf(buf, len, pos, ">");
          }
#endif

          return pos;
        }
    };

    const char *usec2str(int64_t usec, bool print_int_timevalue)
    {
      MicroSecond ms(usec);

      ms.set_print_int_timevalue(print_int_timevalue);

      return to_cstring(ms);
    }

    const char *print_pos_type(ObLogPositionType pos_type)
    {
      const char *ret = NULL;

      switch (pos_type)
      {
        case POS_TYPE_CHECKPOINT:
          ret = "CHECKPOINT";
          break;

        case POS_TYPE_TIMESTAMP_SECOND:
          ret = "TIMESTAMP_SECOND";
          break;

        default:
          ret = "UNKNOWN POSITION";
          break;
      }

      return ret;
    }

    void print_log_position_info(int64_t checkpoint, int64_t timestamp, const char *mod, int64_t heartbeat_timestamp)
    {
      if (0 < checkpoint || 0 < timestamp || 0 < heartbeat_timestamp)
      {
        int64_t cur_tm = tbsys::CTimeUtil::getTime();
        int64_t log_tm = std::max(timestamp, heartbeat_timestamp);

        double delta = (double)(cur_tm - log_tm) / 1000000.0;

        if (0 < heartbeat_timestamp)
        {
          TBSYS_LOG(INFO, "STAT: [%s] CHECKPOINT=%ld TIMESTAMP=[%s] DELAY=%.0lf sec  HEARTBEAT=[%s]",
              mod, checkpoint, usec2str(timestamp, false), delta, usec2str(heartbeat_timestamp, false));
        }
        else
        {
          TBSYS_LOG(INFO, "STAT: [%s] CHECKPOINT=%ld TIMESTAMP=[%s] DELAY=%.0lf sec",
              mod, checkpoint, usec2str(timestamp, false), delta);
        }
      }
    }
  }
}
