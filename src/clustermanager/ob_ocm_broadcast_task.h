/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ocm_info_manager.h,v 0.1 2010/09/28 13:39:26 rongxuan Exp $
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef _OB_OCM_BROADCAST_TASK_H_
#define _OB_OCM_BROADCAST_TASK_H_

#include "common/ob_timer.h"

namespace oceanbase
{
  namespace clustermanager
  {
    class ObOcmBroadcastTask: public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 5000000L;
        ObOcmBroadcastTask();
        ~ObOcmBroadcastTask();
      public:
        void runTimerTask(void);
    };
  }
}
#endif

