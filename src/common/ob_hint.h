/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_hint.h,  08/09/2012 11:23:53 AM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   provide search hint, scan hint, get hint
 *
 */

#ifndef  OCEANBASE_COMMON_HINT_H_
#define OCEANBASE_COMMON_HINT_H_
#include "utility.h"
namespace oceanbase
{
  namespace common
  {
    enum ObConsistencyLevel
    {
      NO_CONSISTENCY = 0,
      STATIC,
      FROZEN,
      WEAK,
      STRONG,
    };
    const char* get_consistency_level_str(ObConsistencyLevel level);
    struct ObRpcScanHint
    {
      int64_t max_parallel_count;
      int64_t max_memory_limit;
      int64_t timeout_us;
      bool is_get_skip_empty_row_;
      int32_t read_method_;
      ObConsistencyLevel read_consistency_;

      ObRpcScanHint() :
        max_parallel_count(OB_DEFAULT_MAX_PARALLEL_COUNT),
        max_memory_limit(1024*1024*512),
        timeout_us(OB_DEFAULT_STMT_TIMEOUT),
        is_get_skip_empty_row_(true),
        read_method_(0),
        read_consistency_(NO_CONSISTENCY)
      {
      }
      int64_t to_string(char* buf, const int64_t buf_len) const
      {
        int64_t pos = 0;
        databuff_printf(buf, buf_len, pos, "Hint=<max_parallel_count=%ld, ", max_parallel_count);
        // max_memory_limit/timeout_us is removed because we are not sure if it is stable
        // databuff_printf(buf, buf_len, pos, "max_memory_limit=%ld, ", max_memory_limit);
        // databuff_printf(buf, buf_len, pos, "timeout_us=%ld, ", timeout_us);
        databuff_printf(buf, buf_len, pos, "is_get_skip_empty_row_=%s, ", is_get_skip_empty_row_ ? "true" : "false");
        databuff_printf(buf, buf_len, pos, "read_method_=%d, ", read_method_);
        databuff_printf(buf, buf_len, pos, "read_consistency=%s>", get_consistency_level_str(read_consistency_));
        return pos;
      }
    };
  }
}
#endif // end of header
