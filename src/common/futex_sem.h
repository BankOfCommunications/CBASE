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

#ifndef __OB_COMMON_FUTEX_SEM_H__
#define __OB_COMMON_FUTEX_SEM_H__
#include <stdint.h>
#include <sys/time.h>

namespace oceanbase
{
  namespace common
  {
    struct fsem_t
    {
      int32_t val_;
      int32_t nwaiters_;
    };
    timespec* calc_abs_time(timespec* ts, const int64_t time_ns);
    int futex_post(fsem_t* p);
    int futex_wait(fsem_t* p);
    int futex_wait(fsem_t* p, const timespec* end_time);
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_FUTEX_SEM_H__ */
