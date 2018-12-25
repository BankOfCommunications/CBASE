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

#include "rootserver/ob_root_timer_task.h"
#include "rootserver/ob_root_worker.h"
#include  "common/ob_define.h"
namespace oceanbase
{
  namespace rootserver
  {
    int ObRootOperationDuty::init(ObRootWorker *worker)
    {
      int ret = OB_SUCCESS;
      if (NULL == worker)
      {
        TBSYS_LOG(WARN, "invalid argument. worker=NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        worker_ = worker;
      }
      return ret;
    }
    void ObRootOperationDuty::runTimerTask(void)
    {
      worker_->submit_check_task_process();
    }
  }
}

