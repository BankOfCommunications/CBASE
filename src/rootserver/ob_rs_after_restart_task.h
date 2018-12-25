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

#ifndef __OB_ROOTSERVER_OB_RS_AFTER_RESTART_TIMER_TASK_H__
#define __OB_ROOTSERVER_OB_RS_AFTER_RESTART_TIMER_TASK_H__

#include "common/ob_timer.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootWorker;
    class ObRsAfterRestartTask: public common::ObTimerTask
    {
      public:
        ObRsAfterRestartTask();
        virtual ~ObRsAfterRestartTask();
        int init(ObRootWorker *worker);
        virtual void runTimerTask(void);
      private:
        ObRootWorker *worker_;
    };
  }
}

#endif /* __OB_ROOTSERVER_OB_RS_AFTER_RESTART_TIMER_TASK_H__ */
