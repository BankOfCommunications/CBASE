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
#include "ob_define.h"
#include "ob_mutex_task.h"

namespace oceanbase
{
  namespace common
  {
    int ObMutexTask::run(void* arg)
    {
      int err = OB_SUCCESS;
      tbsys::CThreadGuard lock_guard(&lock_);
      is_running_ = true;
      if (OB_SUCCESS != (err = do_work(arg)))
      {
        TBSYS_LOG(ERROR, "do_work(arg)=>%d", err);
      }
      else
      {
        finished_count_++;
      }
      is_running_ = false;
      return err;
    }

    int64_t ObMutexTask::get_finished_count() const
    {
      return finished_count_;
    }

    bool ObMutexTask::is_running() const
    {
      return is_running_;
    }
  }; // end namespace common
}; // end namespace oceanbase
