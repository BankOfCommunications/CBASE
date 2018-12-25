/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_lease_common.h,v 0.1 2010/12/01 09:23:46 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_COMMON_OB_LEASE_COMMON_H__
#define __OCEANBASE_COMMON_OB_LEASE_COMMON_H__
#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    struct ObLease
    {
      int64_t lease_time;      // lease start time
      int64_t lease_interval;  // lease interval, lease valid time [lease_time, lease_time + lease_interval]
      int64_t renew_interval;  // renew interval, slave will renew lease when lease expiration time is close

      ObLease() : lease_time(0), lease_interval(0), renew_interval(0) {}

      bool is_lease_valid(int64_t redun_time);

      NEED_SERIALIZE_AND_DESERIALIZE;
    };
  }
}

#endif //__OCEANBASE_COMMON_OB_LEASE_COMMON_H__

