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
#include "ob_tsi_utils.h"
#include "utility.h"
namespace oceanbase
{
  namespace common
  {
    int64_t ExpStat::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      int64_t total_count = 0;
      int64_t total_value = 0;
      int64_t count[64];
      int64_t value[64];
      for(int64_t i = 0; i < 64; i++)
      {
        count[i] = count_[i].value();
        value[i] = value_[i].value();
        total_count += count[i];
        total_value += value[i];
      }
      databuff_printf(buf, len, pos, "total_count=%'ld, total_%s=%'ldus, avg=%'ld\n", total_count, metric_name_, total_value, total_count == 0? 0: total_value/total_count);
      for(int64_t i = 0; i < 64; i++)
      {
        if (0 != count[i])
        {
          databuff_printf(buf, len, pos, "stat[2**%2ld<=x<2**%2ld]: %5.2lf%%, count=%'12ld, avg=%'12ld, %s=%'ld\n",
                          i-1, i, total_count == 0? 0: 100.0*(double)count[i]/(double)total_count, count[i], count[i] == 0? 0: value[i]/count[i], metric_name_, value[i]);
        }
      }
      return pos;
    }
  }; // end namespace common
}; // end namespace oceanbase
