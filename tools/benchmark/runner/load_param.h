/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_param.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_PARAM_H_
#define LOAD_PARAM_H_

#include "common/ob_server.h"
#include "common/ob_define.h"
#include "load_define.h"

namespace oceanbase
{
  namespace tools
  {
    struct SourceParam
    {
      char file_[MAX_FILE_LEN];
      int64_t query_per_second_;
      int64_t max_query_count_;
      int64_t max_sleep_count_;
      common::ObServer stream_server_;
      int64_t iter_times_;
      SourceParam()
      {
        iter_times_ = 1;
        max_query_count_ = -1;
        query_per_second_ = 1000;
        max_sleep_count_ = 10000;
      }
    };

    struct ControlParam
    {
      bool compare_result_;
      bool enable_read_master_;
      int64_t status_interval_;
      int32_t thread_count_;
      common::ObServer new_server_;
      common::ObServer old_server_;
      char user_name_[common::OB_MAX_USERNAME_LENGTH];
      char password_[common::OB_MAX_PASSWORD_LENGTH];
      ControlParam()
      {
        compare_result_ = false;
        enable_read_master_ = false;
        status_interval_ = 1000 * 1000L;
        thread_count_ = 1;
      }
    };

    struct MeterParam
    {
      // input load
      SourceParam load_source_;
      // load control
      ControlParam ctrl_info_;
    };
  }
}

#endif // LOAD_PARAM_H_

