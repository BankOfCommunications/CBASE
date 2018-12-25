/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_MMS_OB_HEARTBEAT_TASK_H_
#define OCEANBASE_MMS_OB_HEARTBEAT_TASK_H_

#include "common/ob_timer.h"

namespace oceanbase
{
  namespace mms
  {
    class ObMonitor;
    class ObMMSHeartbeatTask : public common::ObTimerTask
    {
      public:
        ObMMSHeartbeatTask(ObMonitor *monitor);
        ~ObMMSHeartbeatTask();
        void runTimerTask();
        const static int64_t DEFAULT_HEARTBEAT_INTERVAL = 10;
      private:
        ObMonitor *monitor_;
        int64_t timeout_;
        int64_t retry_times_;
    };
  }
}
#endif


