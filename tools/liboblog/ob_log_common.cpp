////===================================================================
 //
 // ob_log_commonc.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2014 Alipay.com, Inc.
 //
 // Created on 2013-07-26 by XiuMing (wanhong.wwh@alipay.com)
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

#include "ob_log_common.h"

namespace oceanbase
{
  namespace liboblog
  {
    ObLogCommon &ObLogCommon::get_instance()
    {
      static ObLogCommon log_common;
      return log_common;
    }

    void ObLogCommon::handle_error(ObLogError::ErrLevel level, int err_no, const char *fmt, ...)
    {
      static const int64_t MAX_ERR_MSG_LEN = 1024;
      static char err_msg[MAX_ERR_MSG_LEN];

      if (0 == ATOMIC_CAS(&handle_error_flag_, 0, 1))
      {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(err_msg, sizeof(err_msg), fmt, ap);
        va_end(ap);

        TBSYS_LOG(INFO, "%s(): err_cb_=%p, level=%d, errno=%d, errmsg=\"%s\"",
            __FUNCTION__, err_cb_, level, err_no, err_msg);

        LOG_AND_ERR(ERROR, "%s", err_msg);

        if (NULL != err_cb_)
        {
          ObLogError err;
          err.level_ = level;
          err.errno_ = err_no;
          err.errmsg_ = err_msg;

          TBSYS_LOG(INFO, "ERROR_CALLBACK %p begin", err_cb_);
          err_cb_(err);
          TBSYS_LOG(INFO, "ERROR_CALLBACK %p end", err_cb_);
        }
        else
        {
          TBSYS_LOG(INFO, "No ERROR CALLBACK function available, abort now");
          abort();
        }

        alive_ = false;
      }
    }

    bool ob_log_alive(const bool stop_flag) { return (! stop_flag) && OBLOG_COMMON.is_alive(); }
  }
}
