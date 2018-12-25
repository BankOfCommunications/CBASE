/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_FETCHED_LOG_SRC_H_
#define OCEANBASE_UPDATESERVER_OB_FETCHED_LOG_SRC_H_
#include "common/ob_log_cursor.h"

namespace oceanbase
{
  namespace updateserver
  {
    // 备向主请求时，会带上session信息，第一次请求时为空，主UPS会返回给备一个非空
    // 的session, 后续请求时，备带上主返回的session。
    // 备不对session信息做解释，主UPS实际上向session中添加了下次请求日志的位置信息(包括文件ID，文件偏移，日志缓冲区中的偏移)
    struct ObSessionBuffer
    {
      ObSessionBuffer(): data_len_(0) {}
      ~ObSessionBuffer() {}
      static const int64_t MAX_SESSION_DATA_LEN = 128;
      int64_t data_len_;
      char data_[MAX_SESSION_DATA_LEN];
      int reset();
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
    };

    struct ObFetchLogReq
    {
      ObFetchLogReq(): session_(), start_id_(0), max_data_len_(0)
      {}
      ~ObFetchLogReq()
      {}
      ObSessionBuffer session_; // 备第一次向主请求时，session_为空，后续向主请求时，带上主上次返回的session_
      int64_t start_id_;
      int64_t max_data_len_;
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
      int64_t to_string(char* buf, const int64_t len) const;
    };

    struct ObFetchedLog
    {
      ObFetchedLog(): next_req_(), start_id_(0), end_id_(0), max_data_len_(0), log_data_(NULL), data_len_(0)
      {}
      ~ObFetchedLog()
      {}
      ObFetchLogReq next_req_; // 主向备返回结果时由主计算出下次请求的参数并返回给备。
      int64_t start_id_;
      int64_t end_id_;
      int64_t max_data_len_;
      char* log_data_;
      int64_t data_len_;
      int set_buf(char* buf, int64_t len);
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
      int64_t to_string(char* buf, const int64_t len) const;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase
#endif //OCEANBASE_UPDATESERVER_OB_FETCHED_LOG_SRC_H_
