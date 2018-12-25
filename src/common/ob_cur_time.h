/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cur_time.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_CUR_TIME_H
#define _OB_CUR_TIME_H 1

#include "ob_timer.h"

namespace oceanbase
{
  namespace common
  {
    extern volatile int64_t g_cur_time;

    class TimeUpdateDuty : public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 2000;
        TimeUpdateDuty() {};
        virtual ~TimeUpdateDuty() {};
        virtual void runTimerTask();
    };
  }
}

#endif /* _OB_CUR_TIME_H */

