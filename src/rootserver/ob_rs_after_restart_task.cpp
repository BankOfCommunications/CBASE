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

#include "common/ob_define.h"
#include "rootserver/ob_rs_after_restart_task.h"
#include "rootserver/ob_root_worker.h"
using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
  namespace rootserver
  {
    ObRsAfterRestartTask::ObRsAfterRestartTask()
    {
    }

    ObRsAfterRestartTask::~ObRsAfterRestartTask()
    {
    }

    int ObRsAfterRestartTask::init(ObRootWorker *worker)
    {
      int ret = OB_SUCCESS;
      if (NULL == worker)
      {
        TBSYS_LOG(WARN, "invalid argument. worker = %p", worker);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        worker_ = worker;
      }
      return ret;
    }
    void ObRsAfterRestartTask::runTimerTask(void)
    {
      if (NULL != worker_)
      {
        worker_->submit_restart_task();
      }
      else
      {
        TBSYS_LOG(ERROR, "invalid argument, worker = %p", worker_);
      }
    }
  }
}
