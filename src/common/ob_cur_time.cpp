/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cur_time.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_cur_time.h"

using namespace oceanbase;
using namespace common;

namespace oceanbase
{
  namespace common
  {
    volatile int64_t g_cur_time = 0;

    void TimeUpdateDuty::runTimerTask()
    {
      g_cur_time = tbsys::CTimeUtil::getTime();
    }
  }
}
