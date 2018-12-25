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
  *   rongxuan <rongxuan.lc@taobao.com>
  *     - some work details if you want
  */

#ifndef __OB_UPDATESERVER_OB_UPS_TIMER_TASK_H__
#define __OB_UPDATESERVER_OB_UPS_TIMER_TASK_H__

#include "common/ob_timer.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsCheckKeepAliveTask : public common::ObTimerTask
    {
      public:
        ObUpsCheckKeepAliveTask();
        virtual ~ObUpsCheckKeepAliveTask();
        virtual void runTimerTask(void);
    };

    class ObUpsGrantKeepAliveTask : public common::ObTimerTask
    {
      public:
        ObUpsGrantKeepAliveTask() {};
        virtual ~ObUpsGrantKeepAliveTask() {};
        virtual void runTimerTask();
    };
    class ObUpsLeaseTask : public common::ObTimerTask
    {
      public:
        ObUpsLeaseTask() {};
        virtual ~ObUpsLeaseTask() {};
        virtual void runTimerTask();
    };
  }
}

#endif /* __OB_UPDATESERVER_OB_UPS_TIMER_TASK_H__ */
