/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_context.h,  08/24/2012 10:28:35 AM xiaochu Exp $
 *
 * Author:
 *   lide.wd <lide.wd@alipay.com>
 * Description:
 *
 */

#ifndef _OB_REQUEST_PROFILE_DATA
#define _OB_REQUEST_PROFILE_DATA

#include "common/ob_array.h"
#include "common/ob_string.h"

#define PROFILE_ITEM(target) \
  int64_t target##_start_; \
  int64_t target##_end_;

namespace oceanbase
{
  namespace common
  {
    //POD type
    struct ObRequestProfileData
    {
      struct ObRpcLatency
      {
        int64_t channel_id_;
        int64_t rpc_start_;
        int64_t rpc_end_;
        int64_t pcode_;
      };
      int64_t trace_id_;
      PROFILE_ITEM(sql_to_logicalplan);
      PROFILE_ITEM(logicalplan_to_physicalplan);
      PROFILE_ITEM(handle_sql_time);
      PROFILE_ITEM(handle_request_time);
      //比较特殊，开始时间和结束时间由两个线程处理，不能有线程私有
      int64_t wait_sql_queue_time_;
      ObRpcLatency rpc_latency_arr_[256];
      char sql_[512];
      int32_t sql_len_;
      uint8_t count_;
      int64_t pcode_;
      int sql_queue_size_;
    };
  }
}
#endif // _OB_REQUEST_PROFILE_DATA
